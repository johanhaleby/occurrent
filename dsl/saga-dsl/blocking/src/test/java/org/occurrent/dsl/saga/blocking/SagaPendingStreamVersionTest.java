/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.dsl.saga.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A stream version that was handed to the saga and failed has not been applied, so it has to be applied when it arrives
 * again, however far the instance has moved on in the meantime. A broker may park the event and an operator replay it
 * later, long after the instance applied versions more than the reorder window ahead of it. The instance therefore
 * records the version as pending when the handler fails, whether or not quarantine is on, and applying the version
 * removes it again. Pending is not a failure record, so with quarantine off nothing is written that quarantine would
 * later count.
 */
@DisplayName("A stream version that failed in the saga")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaPendingStreamVersionTest {

    private static final String BASKET = "basket-1";
    private static final String STREAM = "basket-stream";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration BUDGET = Duration.ofMinutes(5);
    private static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();
    private static final SagaRunnerConfig WINDOW_OF_2 = SagaRunnerConfig.defaults().withReorderWindow(2);
    private static final SagaRunnerConfig WINDOW_OF_2_WITH_QUARANTINE = WINDOW_OF_2.withQuarantineAfter(BUDGET);
    private static final SagaRunnerConfig WINDOW_OF_1 = SagaRunnerConfig.defaults().withReorderWindow(1);
    private static final SagaRunnerConfig WINDOW_OF_1_WITH_QUARANTINE = WINDOW_OF_1.withQuarantineAfter(BUDGET);

    sealed interface BasketEvent permits BasketOpened, ItemAdded {
        String eventId();

        String basketId();
    }

    record BasketOpened(String eventId, String basketId) implements BasketEvent {
    }

    record ItemAdded(String eventId, String basketId, String item) implements BasketEvent {
    }

    sealed interface BasketCommand permits OpenLedger, Reserve {
    }

    record OpenLedger(String basketId) implements BasketCommand {
    }

    record Reserve(String basketId, String item) implements BasketCommand {
    }

    record Basket(int items) {
    }

    /** Every event issues a command, so a skipped event shows up as a missing command. */
    private static Saga<BasketEvent, Basket, BasketCommand> basketSaga() {
        return Saga.<BasketEvent, Basket, BasketCommand>builder(new Basket(0))
                .correlateAll(BasketEvent::basketId)
                .startsOn(BasketOpened.class)
                .startsOn(ItemAdded.class)
                .evolve(BasketOpened.class, (state, e) -> state)
                .react(BasketOpened.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.basketId()))))
                .evolve(ItemAdded.class, (state, e) -> new Basket(state.items() + 1))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.basketId(), e.item()))))
                .build();
    }

    private final IllegalStateException handlerFailure = new IllegalStateException("cannot reserve");
    private final OutOfMemoryError outOfMemory = new OutOfMemoryError("Java heap space");
    private final Set<String> failingItems = ConcurrentHashMap.newKeySet();
    private final Set<String> outOfMemoryItems = ConcurrentHashMap.newKeySet();
    private final List<BasketCommand> issued = new CopyOnWriteArrayList<>();
    private final AtomicReference<Instant> now = new AtomicReference<>(NOW);
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();
    private CloudEventConverter<BasketEvent> converter;
    private SagaStateStore<Basket> inMemory;
    private ObservedStore store;
    private ListAppender<ILoggingEvent> appender;
    private Logger sagaLog;

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<BasketEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(BasketEvent::eventId).build();
        inMemory = SagaStateStore.inMemory();
        store = new ObservedStore(inMemory);
        appender = new ListAppender<>();
        appender.start();
        // The executor and the support class it calls are in sibling packages, so their common parent catches both.
        sagaLog = (Logger) LoggerFactory.getLogger("org.occurrent.dsl.saga");
        sagaLog.addAppender(appender);
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        sagaLog.detachAppender(appender);
        appender.stop();
    }

    private SagaExecution<BasketEvent, Basket, BasketCommand> execution(SagaRunnerConfig config) {
        return new SagaExecution<>("baskets", basketSaga(), store, this::dispatch, converter, config, event -> true, now::get);
    }

    private void dispatch(BasketCommand command) {
        if (command instanceof Reserve reserve && outOfMemoryItems.contains(reserve.item())) {
            throw outOfMemory;
        }
        if (command instanceof Reserve reserve && failingItems.contains(reserve.item())) {
            throw handlerFailure;
        }
        issued.add(command);
    }

    private void deliver(SagaExecution<BasketEvent, Basket, BasketCommand> execution, CloudEvent event) {
        execution.onCloudEvent(event);
    }

    private void deliverFailing(SagaExecution<BasketEvent, Basket, BasketCommand> execution, CloudEvent event) {
        assertThatThrownBy(() -> execution.onCloudEvent(event)).isSameAs(handlerFailure);
    }

    /** Version 1 opens the basket and version 2 fails in the handler. */
    private SagaExecution<BasketEvent, Basket, BasketCommand> afterVersionTwoFailed(SagaRunnerConfig config) {
        SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(config);
        deliver(execution, openedAt(1));
        failingItems.add("two");
        deliverFailing(execution, itemAddedAt(2, "two"));
        return execution;
    }

    /** As {@link #afterVersionTwoFailed}, then versions 3 to 8 apply, which puts the mark at 8 and the horizon at 6 under a window of 2. */
    private SagaExecution<BasketEvent, Basket, BasketCommand> afterVersionTwoFailedAndThreeToEightApplied(SagaRunnerConfig config) {
        SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoFailed(config);
        for (int version = 3; version <= 8; version++) {
            deliver(execution, itemAddedAt(version, "item-" + version));
        }
        failingItems.clear();
        return execution;
    }

    /** Version 1 opens the basket, versions 2 and 3 fail in turn, and versions 4 to 9 apply, which puts the mark at 9 and the horizon at 7 under a window of 2. */
    private SagaExecution<BasketEvent, Basket, BasketCommand> afterVersionsTwoAndThreeFailedInTurnAndFourToNineApplied(SagaRunnerConfig config) {
        SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(config);
        deliver(execution, openedAt(1));
        failingItems.addAll(Set.of("two", "three"));
        deliverFailing(execution, itemAddedAt(2, "two"));
        deliverFailing(execution, itemAddedAt(3, "three"));
        for (int version = 4; version <= 9; version++) {
            deliver(execution, itemAddedAt(version, "item-" + version));
        }
        failingItems.clear();
        return execution;
    }

    private SagaEnvelope<Basket> stored() {
        return inMemory.find(BASKET).orElseThrow();
    }

    private List<Long> pending() {
        StreamMarks marks = inMemory.find(BASKET).map(instance -> instance.streamMarks().get(STREAM)).orElse(null);
        return marks == null ? List.of() : marks.pending().getOrDefault(ITEM_ADDED_TYPE, List.of());
    }

    private List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> warnings() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
    }

    /** The warning about pending versions growing past the window, which is the only one the executor and its support class log about the window. */
    private List<String> reorderWindowWarnings() {
        return warnings().stream().filter(message -> message.contains("failed versions of type")).toList();
    }

    private static BasketOpened opened() {
        return new BasketOpened("opened", BASKET);
    }

    private static ItemAdded added(String item) {
        return new ItemAdded("added-" + item, BASKET, item);
    }

    private static Reserve reserve(String item) {
        return new Reserve(BASKET, item);
    }

    private CloudEvent openedAt(long streamVersion) {
        return streamKeyed(streamVersion, opened());
    }

    private CloudEvent itemAddedAt(long streamVersion, String item) {
        return streamKeyed(streamVersion, added(item));
    }

    @Nested
    @DisplayName("when quarantine is off")
    class WhenQuarantineIsOff {

        @Test
        void the_exception_still_propagates_and_the_instance_lists_the_version_as_pending_without_a_failure_record() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.add("two");

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(pending()).containsExactly(2L),
                    () -> assertThat(after.failure()).isNull(),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(after.state()).isEqualTo(new Basket(0)),
                    () -> assertThat(issued).containsExactly(new OpenLedger(BASKET))
            );
        }

        @Test
        void a_first_event_that_fails_inserts_an_instance_that_has_not_started_with_the_version_pending() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            failingItems.add("one");

            // When
            deliverFailing(execution, itemAddedAt(1, "one"));

            // Then
            Optional<SagaEnvelope<Basket>> inserted = inMemory.find(BASKET);
            assertThat(inserted).isPresent();
            assertAll(
                    () -> assertThat(inserted.get().failure()).isNull(),
                    () -> assertThat(inserted.get().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(inserted.get().started()).isFalse(),
                    () -> assertThat(pending()).containsExactly(1L),
                    () -> assertThat(issued).isEmpty()
            );
        }

        @Test
        void the_first_event_is_applied_as_a_start_when_it_is_redelivered_and_is_no_longer_pending() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            failingItems.add("one");
            deliverFailing(execution, itemAddedAt(1, "one"));
            List<Long> pendingBefore = pending();
            failingItems.clear();

            // When
            deliver(execution, itemAddedAt(1, "one"));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(pendingBefore).containsExactly(1L),
                    () -> assertThat(issued).containsExactly(reserve("one")),
                    () -> assertThat(after.started()).isTrue(),
                    () -> assertThat(after.state()).isEqualTo(new Basket(1)),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void the_failed_version_is_applied_once_when_it_is_redelivered_after_the_mark_moved_past_the_window() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoFailedAndThreeToEightApplied(WINDOW_OF_2);
            List<Long> pendingBefore = pending();

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(pendingBefore).containsExactly(2L),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(after.state()).isEqualTo(new Basket(7)),
                    () -> assertThat(after.failure()).isNull(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void a_second_delivery_of_the_applied_version_is_skipped() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoFailedAndThreeToEightApplied(WINDOW_OF_2);
            deliver(execution, itemAddedAt(2, "two"));

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(7))
            );
        }

        @Test
        void a_version_that_did_not_fail_and_is_just_as_far_behind_is_still_skipped_and_the_marks_stay_where_they_were() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoFailedAndThreeToEightApplied(WINDOW_OF_2);

            // When
            deliver(execution, itemAddedAt(4, "item-4"));
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            StreamMarks marks = stored().streamMarks().get(STREAM);
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("item-4")),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(marks.byType().get(ITEM_ADDED_TYPE).mark()).isEqualTo(8L),
                    () -> assertThat(marks.byType().get(ITEM_ADDED_TYPE).horizon()).isEqualTo(6L)
            );
        }

        @Test
        void two_inputs_that_fail_in_turn_are_both_applied_when_they_are_redelivered_after_the_mark_moved_past_the_window() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionsTwoAndThreeFailedInTurnAndFourToNineApplied(WINDOW_OF_2);
            List<Long> pendingBefore = pending();

            // When
            deliver(execution, itemAddedAt(2, "two"));
            deliver(execution, itemAddedAt(3, "three"));

            // Then
            assertAll(
                    () -> assertThat(pendingBefore).containsExactly(2L, 3L),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(issued).containsOnlyOnce(reserve("three")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(8)),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void the_versions_beyond_the_window_that_failed_all_stay_pending_and_no_error_is_logged() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three", "four"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(3, "three"));

            // When
            deliverFailing(execution, itemAddedAt(4, "four"));

            // Then
            assertAll(
                    () -> assertThat(pending()).containsExactly(2L, 3L, 4L),
                    () -> assertThat(errors()).isEmpty()
            );
        }

        @Test
        void a_warning_naming_the_stream_is_logged_once_the_failed_versions_of_a_type_grow_past_the_window() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three", "four"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(3, "three"));
            List<String> warningsWithinTheWindow = reorderWindowWarnings();

            // When
            deliverFailing(execution, itemAddedAt(4, "four"));

            // Then
            assertAll(
                    () -> assertThat(warningsWithinTheWindow).isEmpty(),
                    () -> assertThat(reorderWindowWarnings()).singleElement().satisfies(message -> assertThat(message).contains(STREAM))
            );
        }

        @Test
        void the_warning_is_not_logged_again_for_the_next_failed_version() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three", "four", "five"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(3, "three"));
            deliverFailing(execution, itemAddedAt(4, "four"));

            // When
            deliverFailing(execution, itemAddedAt(5, "five"));

            // Then
            assertAll(
                    () -> assertThat(pending()).containsExactly(2L, 3L, 4L, 5L),
                    () -> assertThat(reorderWindowWarnings()).hasSize(1)
            );
        }

        @Test
        void both_inputs_that_failed_are_applied_when_they_are_redelivered_after_the_mark_moved_past_a_window_of_one() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionsTwoAndThreeFailedInTurnAndFourToNineApplied(WINDOW_OF_1);
            List<Long> pendingBefore = pending();

            // When
            deliver(execution, itemAddedAt(2, "two"));
            deliver(execution, itemAddedAt(3, "three"));

            // Then
            assertAll(
                    () -> assertThat(pendingBefore).containsExactly(2L, 3L),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(issued).containsOnlyOnce(reserve("three")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(8)),
                    () -> assertThat(pending()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when quarantine is on")
    class WhenQuarantineIsOn {

        @Test
        void the_failing_version_is_pending_and_the_failure_record_is_written_as_before() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2_WITH_QUARANTINE);
            deliver(execution, openedAt(1));
            failingItems.add("two");

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(pending()).containsExactly(2L),
                    () -> assertThat(after.failure()).isNotNull(),
                    () -> assertThat(after.failure().input()).isEqualTo(STREAM + "@2"),
                    () -> assertThat(after.failure().firstFailedAt()).isEqualTo(NOW),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void the_failure_record_and_the_pending_version_are_written_together_and_a_repeated_failure_of_the_input_writes_nothing() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2_WITH_QUARANTINE);
            deliver(execution, openedAt(1));
            failingItems.add("two");
            store.resetCounters();

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(store.writeAttempts()).isEqualTo(1),
                    () -> assertThat(pending()).containsExactly(2L),
                    () -> assertThat(stored().failure().input()).isEqualTo(STREAM + "@2")
            );
        }

        @Test
        void two_inputs_that_fail_in_turn_are_both_applied_when_they_are_redelivered_after_the_mark_moved_past_the_window() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionsTwoAndThreeFailedInTurnAndFourToNineApplied(WINDOW_OF_2_WITH_QUARANTINE);
            List<Long> pendingBefore = pending();

            // When
            deliver(execution, itemAddedAt(2, "two"));
            deliver(execution, itemAddedAt(3, "three"));

            // Then
            assertAll(
                    () -> assertThat(pendingBefore).containsExactly(2L, 3L),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(issued).containsOnlyOnce(reserve("three")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(8)),
                    () -> assertThat(stored().failure()).isNull(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void a_failed_version_is_applied_on_retry_after_later_versions_replaced_the_failure_record_and_moved_the_mark_past_the_window() {
            // Given version 3 replaced the failure record of version 2, and versions 4 and 5 put the mark at 5, which makes 2 three behind it
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2_WITH_QUARANTINE);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(3, "three"));
            deliver(execution, itemAddedAt(4, "four"));
            deliver(execution, itemAddedAt(5, "five"));
            failingItems.clear();
            SagaEnvelope<Basket> before = stored();

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(before.failure().input()).isEqualTo(STREAM + "@3"),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(3)),
                    () -> assertThat(pending()).containsExactly(3L),
                    () -> assertThat(warnings()).noneMatch(message -> message.contains("skipped version 2 of stream"))
            );
        }
    }

    @Nested
    @DisplayName("when quarantine is on and the budget runs per input")
    class WhenQuarantineIsOnAndTheBudgetRunsPerInput {

        private Instant minute(long minutes) {
            return NOW.plus(Duration.ofMinutes(minutes));
        }

        @Test
        void both_inputs_that_failed_are_applied_when_they_are_redelivered_after_the_mark_moved_past_a_window_of_one() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionsTwoAndThreeFailedInTurnAndFourToNineApplied(WINDOW_OF_1_WITH_QUARANTINE);
            List<Long> pendingBefore = pending();

            // When
            deliver(execution, itemAddedAt(2, "two"));
            deliver(execution, itemAddedAt(3, "three"));

            // Then
            assertAll(
                    () -> assertThat(pendingBefore).containsExactly(2L, 3L),
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(issued).containsOnlyOnce(reserve("three")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(8)),
                    () -> assertThat(stored().failure()).isNull(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void two_inputs_that_fail_in_turn_are_quarantined_once_one_of_them_has_failed_for_the_budget() {
            // Given versions 2 and 3 fail in turn as 2, 3, 2, which is five minutes past the first failure of 3 only once 3 fails again
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2_WITH_QUARANTINE);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three"));
            now.set(minute(0));
            deliverFailing(execution, itemAddedAt(2, "two"));
            now.set(minute(1));
            deliverFailing(execution, itemAddedAt(3, "three"));
            now.set(minute(4));
            deliverFailing(execution, itemAddedAt(2, "two"));
            SagaStatus statusBeforeTheLastFailure = stored().status();
            now.set(minute(6));

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(3, "three")));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(statusBeforeTheLastFailure).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(after.failure().input()).isEqualTo(STREAM + "@3"),
                    () -> assertThat(after.failure().firstFailedAt()).isEqualTo(minute(1))
            );
        }

        @Test
        void an_input_that_fails_for_the_first_time_is_not_quarantined_however_long_another_input_has_been_parked() {
            // Given version 2 failed at the start and has been pending for longer than the budget
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2_WITH_QUARANTINE);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three"));
            now.set(minute(0));
            deliverFailing(execution, itemAddedAt(2, "two"));
            now.set(minute(6));

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(3, "three")));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(after.failure().input()).isEqualTo(STREAM + "@3"),
                    () -> assertThat(after.failure().firstFailedAt()).isEqualTo(minute(6)),
                    () -> assertThat(pending()).containsExactly(2L, 3L)
            );
        }

        @Test
        void a_parked_input_that_is_replayed_after_the_budget_has_passed_is_quarantined_on_its_first_failure_of_the_replay() {
            // Given version 2 failed at the start, version 3 failed and then succeeded, which cleared the failure record
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2_WITH_QUARANTINE);
            deliver(execution, openedAt(1));
            failingItems.addAll(Set.of("two", "three"));
            now.set(minute(0));
            deliverFailing(execution, itemAddedAt(2, "two"));
            now.set(minute(1));
            deliverFailing(execution, itemAddedAt(3, "three"));
            failingItems.remove("three");
            now.set(minute(2));
            deliver(execution, itemAddedAt(3, "three"));
            SagaEnvelope<Basket> parked = stored();
            now.set(minute(5));

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(parked.failure()).isNull(),
                    () -> assertThat(parked.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(after.failure().input()).isEqualTo(STREAM + "@2"),
                    () -> assertThat(after.failure().firstFailedAt()).isEqualTo(minute(0))
            );
        }
    }

    @Nested
    @DisplayName("when the store misbehaves while the version is recorded as pending")
    class WhenTheStoreMisbehavesWhileTheVersionIsRecordedAsPending {

        private SagaExecution<BasketEvent, Basket, BasketCommand> afterOpened() {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.add("two");
            store.resetCounters();
            return execution;
        }

        @Test
        void a_store_that_throws_on_the_write_leaves_the_handler_exception_to_propagate_alone() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpened();
            store.failWrites(new IllegalStateException("store down"));

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertThat(store.writeAttempts()).isPositive();
        }

        @Test
        void a_store_that_throws_on_the_read_after_the_handler_failed_leaves_the_handler_exception_to_propagate_alone() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpened();
            store.failReadsAfter(1, new IllegalStateException("store down"));

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertThat(store.reads()).isGreaterThan(1);
        }

        @Test
        void a_lost_compare_and_set_is_retried_and_the_version_is_recorded() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpened();
            store.loseNextWrites(1);

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(store.writeAttempts()).isEqualTo(2),
                    () -> assertThat(pending()).containsExactly(2L)
            );
        }

        @Test
        void a_compare_and_set_that_is_lost_every_time_is_given_up_on_and_the_handler_exception_propagates_alone() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpened();
            store.loseNextWrites(Integer.MAX_VALUE);

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertThat(store.writeAttempts()).isGreaterThan(1);
        }

        @Test
        void the_same_input_failing_again_does_not_write_pending_again() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpened();

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(store.writeAttempts()).isEqualTo(1),
                    () -> assertThat(pending()).containsExactly(2L)
            );
        }

        @Test
        void each_distinct_input_that_fails_is_written_once() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpened();
            failingItems.add("three");

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(3, "three"));
            deliverFailing(execution, itemAddedAt(2, "two"));
            deliverFailing(execution, itemAddedAt(3, "three"));

            // Then
            assertAll(
                    () -> assertThat(store.writeAttempts()).isEqualTo(2),
                    () -> assertThat(pending()).containsExactly(2L, 3L)
            );
        }
    }

    @Nested
    @DisplayName("when quarantine is on and the store fails after the handler did")
    class WhenQuarantineIsOnAndTheStoreFailsAfterTheHandlerDid {

        private final IllegalStateException storeFailure = new IllegalStateException("store down");

        private SagaExecution<BasketEvent, Basket, BasketCommand> afterOpenedWithAFailingStore() {
            return afterOpenedWithAFailingStore(WINDOW_OF_2_WITH_QUARANTINE);
        }

        private SagaExecution<BasketEvent, Basket, BasketCommand> afterOpenedWithAFailingStore(SagaRunnerConfig config) {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(config);
            deliver(execution, openedAt(1));
            store.resetCounters();
            store.failReadsAfter(1, storeFailure);
            return execution;
        }

        @Test
        void the_handler_exception_propagates_with_the_store_failure_suppressed_under_it() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpenedWithAFailingStore();
            failingItems.add("two");

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertThat(handlerFailure.getSuppressed()).containsExactly(storeFailure);
        }

        @Test
        void one_error_says_the_version_could_not_be_listed_as_pending_and_names_its_redelivery_key() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpenedWithAFailingStore();
            failingItems.add("two");

            // When
            deliverFailing(execution, itemAddedAt(2, "two"));

            // Then
            assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("could not list it as pending", STREAM + "@2"));
        }

        @Test
        void an_event_without_stream_metadata_logs_no_such_error() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpenedWithAFailingStore(WINDOW_OF_2_WITH_QUARANTINE.withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));
            failingItems.add("two");

            // When
            deliverFailing(execution, converter.toCloudEvent(added("two")));

            // Then
            assertAll(
                    () -> assertThat(errors()).isEmpty(),
                    () -> assertThat(handlerFailure.getSuppressed()).isEmpty()
            );
        }

        @Test
        void a_failure_that_is_not_the_instances_logs_no_such_error() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterOpenedWithAFailingStore();
            outOfMemoryItems.add("two");

            // When
            assertThatThrownBy(() -> execution.onCloudEvent(itemAddedAt(2, "two"))).isSameAs(outOfMemory);

            // Then
            assertAll(
                    () -> assertThat(errors()).isEmpty(),
                    () -> assertThat(outOfMemory.getSuppressed()).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the events arrive through a push subscription")
    class WhenTheEventsArriveThroughAPushSubscription {

        private PushSubscriptionModel pushModel;

        @BeforeEach
        void subscribe() {
            pushModel = new PushSubscriptionModel();
            SagaSubscription subscription = SagaRunner.<BasketEvent, BasketCommand>agnostic(pushModel, converter)
                    .run("baskets", basketSaga(), inMemory, SagaPendingStreamVersionTest.this::dispatch, null,
                            WINDOW_OF_2.withTimerPollInterval(Duration.ofMillis(50)));
            subscriptionsToClose.add(subscription);
        }

        private void push(CloudEvent event) {
            pushModel.accept(event);
        }

        @Test
        void a_version_whose_handler_failed_is_applied_once_when_it_is_replayed_after_later_versions_moved_the_mark_past_the_window() {
            // Given version 5 fails, so the feed moves on without it
            push(openedAt(1));
            failingItems.add("five");
            assertThatThrownBy(() -> push(itemAddedAt(5, "five"))).isSameAs(handlerFailure);
            for (int version = 6; version <= 12; version++) {
                push(itemAddedAt(version, "item-" + version));
            }
            failingItems.clear();

            // When an operator replays the parked event, and then replays it again
            push(itemAddedAt(5, "five"));
            push(itemAddedAt(5, "five"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("five")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(8)),
                    () -> assertThat(pending()).isEmpty()
            );
        }
    }

    // The stream key an event store writes, with the stream id and the version chosen by the test.
    private CloudEvent streamKeyed(long streamVersion, BasketEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(STREAM, streamVersion)).build();
    }

    /**
     * Counts what the executor does to the store and can make it fail, without knowing which of the read and write
     * methods the executor uses for a given step.
     */
    private static final class ObservedStore implements SagaStateStore<Basket> {
        private final SagaStateStore<Basket> delegate;
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger writeAttempts = new AtomicInteger();
        private final AtomicInteger writesToLose = new AtomicInteger();
        private volatile int readsAllowedBeforeFailing = Integer.MAX_VALUE;
        private volatile @Nullable RuntimeException readFailure;
        private volatile @Nullable RuntimeException writeFailure;

        ObservedStore(SagaStateStore<Basket> delegate) {
            this.delegate = delegate;
        }

        void resetCounters() {
            reads.set(0);
            writeAttempts.set(0);
        }

        int reads() {
            return reads.get();
        }

        int writeAttempts() {
            return writeAttempts.get();
        }

        void failReadsAfter(int allowed, RuntimeException failure) {
            readsAllowedBeforeFailing = allowed;
            readFailure = failure;
        }

        void failWrites(RuntimeException failure) {
            writeFailure = failure;
        }

        void loseNextWrites(int count) {
            writesToLose.set(count);
        }

        private void aboutToRead() {
            int read = reads.incrementAndGet();
            RuntimeException failure = readFailure;
            if (failure != null && read > readsAllowedBeforeFailing) {
                throw failure;
            }
        }

        private boolean write(BooleanSupplier write) {
            writeAttempts.incrementAndGet();
            RuntimeException failure = writeFailure;
            if (failure != null) {
                throw failure;
            }
            if (writesToLose.getAndUpdate(remaining -> remaining > 0 ? remaining - 1 : 0) > 0) {
                return false;
            }
            return write.getAsBoolean();
        }

        @Override
        public Optional<SagaEnvelope<Basket>> find(String sagaId) {
            aboutToRead();
            return delegate.find(sagaId);
        }

        @Override
        public Optional<SagaEnvelope<Basket>> findWithoutState(String sagaId) {
            aboutToRead();
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<Basket> envelope, long expectedVersion) {
            return write(() -> delegate.compareAndSave(sagaId, envelope, expectedVersion));
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<Basket> envelope, long expectedVersion) {
            return write(() -> delegate.compareAndSaveWithoutState(sagaId, envelope, expectedVersion));
        }

        @Override
        public List<SagaEnvelope<Basket>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
