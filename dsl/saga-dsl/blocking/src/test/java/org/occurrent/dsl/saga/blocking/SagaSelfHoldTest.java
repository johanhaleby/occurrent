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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A stream version that failed in the saga is listed as pending on its instance, so that it is applied when it arrives
 * again however far the instance has moved on. When the instance cannot list it, because it already lists as many as it
 * may, because the write kept losing or failing, or because the instance moved past the version while the delivery was in
 * flight, acknowledging the event would drop its reaction. The instance then holds the event itself, as a self-held key
 * on its envelope and a held copy beside it, and the original failure still reaches the source.
 */
@DisplayName("A failed version the instance cannot list as pending")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaSelfHoldTest {

    private static final String BASKET = "basket-1";
    private static final String STREAM = "basket-stream";
    private static final String KEY_OF_TWO = STREAM + "@2";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration BUDGET = Duration.ofMinutes(5);
    private static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();
    private static final SagaRunnerConfig WINDOW_OF_2 = SagaRunnerConfig.defaults().withReorderWindow(2);
    private static final SagaRunnerConfig WINDOW_OF_2_WITH_QUARANTINE = WINDOW_OF_2.withQuarantineAfter(BUDGET);

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

    /** Every event issues a command, so an event that is applied twice shows up as a command issued twice. */
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
    private final Set<String> failingItems = ConcurrentHashMap.newKeySet();
    private final List<BasketCommand> issued = new CopyOnWriteArrayList<>();
    private volatile @Nullable Runnable whileDispatchingTwo;
    private CloudEventConverter<BasketEvent> converter;
    private HoldingStore store;
    private ListAppender<ILoggingEvent> appender;
    private Logger sagaLog;

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<BasketEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(BasketEvent::eventId).build();
        store = new HoldingStore();
        appender = new ListAppender<>();
        appender.start();
        sagaLog = (Logger) getLogger("org.occurrent.dsl.saga");
        sagaLog.addAppender(appender);
    }

    @AfterEach
    void shutdown() {
        sagaLog.detachAppender(appender);
        appender.stop();
    }

    private SagaExecution<BasketEvent, Basket, BasketCommand> execution(SagaRunnerConfig config) {
        return new SagaExecution<>("baskets", basketSaga(), store, this::dispatch, converter, config, event -> true, () -> NOW);
    }

    private SagaExecution<BasketEvent, Basket, BasketCommand> executionThatDoesNotHold(SagaRunnerConfig config) {
        return new SagaExecution<>("baskets", basketSaga(), store, this::dispatch, converter, config, event -> true, () -> NOW, false);
    }

    private void dispatch(BasketCommand command) {
        if (command instanceof Reserve reserve && reserve.item().equals("two")) {
            Runnable hook = whileDispatchingTwo;
            if (hook != null) {
                whileDispatchingTwo = null;
                hook.run();
            }
        }
        if (command instanceof Reserve reserve && failingItems.contains(reserve.item())) {
            throw handlerFailure;
        }
        issued.add(command);
    }

    private static void deliver(SagaExecution<BasketEvent, Basket, BasketCommand> execution, CloudEvent event) {
        execution.onCloudEvent(event);
    }

    private void deliverItems(SagaExecution<BasketEvent, Basket, BasketCommand> execution, int fromVersion, int toVersion) {
        for (int version = fromVersion; version <= toVersion; version++) {
            deliver(execution, itemAddedAt(version, "item-" + version));
        }
    }

    /** Version 1 opens the basket and version 2 is the one that is going to fail in the handler. */
    private SagaExecution<BasketEvent, Basket, BasketCommand> openedWithVersionTwoFailing(SagaRunnerConfig config) {
        SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(config);
        deliver(execution, openedAt(1));
        failingItems.add("two");
        return execution;
    }

    private SagaEnvelope<Basket> stored() {
        return store.delegate.find(BASKET).orElseThrow();
    }

    private List<Long> pending() {
        StreamMarks marks = stored().streamMarks().get(STREAM);
        return marks == null ? List.of() : marks.pending().getOrDefault(ITEM_ADDED_TYPE, List.of());
    }

    private List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> warnings() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
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

    // The stream key an event store writes, with the stream id and the version chosen by the test.
    private CloudEvent streamKeyed(long streamVersion, BasketEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(STREAM, streamVersion)).build();
    }

    private void saveInstanceListingPendingVersions(int count) {
        List<Long> versions = LongStream.rangeClosed(1, count).boxed().toList();
        StreamMarks otherStream = new StreamMarks(Long.MIN_VALUE, java.util.Map.of(), java.util.Map.of("other.Type", versions));
        SagaEnvelope<Basket> instance = new SagaEnvelope<>(BASKET, new Basket(0), SagaStatus.ACTIVE, 1, List.of(), java.util.Map.of(), null, NOW, NOW, null, null,
                true, null, java.util.Map.of("other-stream", otherStream), true, Set.of(), Set.of(), new StartMark(STREAM, 1, null));
        assertThat(store.delegate.compareAndSave(BASKET, instance, 0)).isTrue();
    }

    private HeldEventRef onlyHeldRef() {
        List<HeldEventRef> index = store.delegate.heldIndex(BASKET);
        assertThat(index).hasSize(1);
        return index.getFirst();
    }

    @Nested
    @DisplayName("when every attempt to list it as pending loses its compare-and-set")
    class WhenEveryAttemptToListItAsPendingLosesItsCompareAndSet {

        @Test
        void the_original_failure_is_rethrown() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(thrown).isSameAs(handlerFailure);
        }

        @Test
        void the_event_is_self_held_as_a_key_on_the_instance_and_a_held_copy() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            HeldEventRef ref = onlyHeldRef();
            assertAll(
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(ref.selfHeld()).isTrue(),
                    () -> assertThat(ref.outstanding()).isFalse(),
                    () -> assertThat(ref.streamId()).isEqualTo(STREAM),
                    () -> assertThat(ref.version()).isEqualTo(2L),
                    () -> assertThat(ref.type()).isEqualTo(ITEM_ADDED_TYPE)
            );
        }

        @Test
        void the_held_copy_is_the_event_that_was_delivered() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            CloudEvent delivered = itemAddedAt(2, "two");

            // When
            catchThrowable(() -> execution.onCloudEvent(delivered));

            // Then
            assertThat(store.delegate.held(BASKET, List.of(KEY_OF_TWO))).singleElement().satisfies(held -> assertThat(held.cloudEvent()).isEqualTo(delivered));
        }

        @Test
        void the_instance_is_marked_as_having_events_to_drain() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.delegate.markHeldEvents(BASKET, null);
            store.delegate.compareAndSave(BASKET, withHeldEventsChecked(stored()), stored().version());
            store.losingEveryWriteWithoutState = true;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(stored().heldEventsChecked()).isFalse();
        }

        @Test
        void the_instance_does_not_list_the_version_as_pending() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(pending()).isEmpty();
        }

        @Test
        void a_second_failure_of_the_same_event_is_the_failure_of_its_held_copy_and_keeps_one_key_and_one_held_copy() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).hasMessageContaining("could not apply the held event '" + KEY_OF_TWO + "'"),
                    () -> assertThat(thrown).hasCause(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).hasSize(1)
            );
        }

        private SagaEnvelope<Basket> withHeldEventsChecked(SagaEnvelope<Basket> instance) {
            return new SagaEnvelope<>(instance.sagaId(), instance.state(), instance.status(), instance.version() + 1, instance.timers(), instance.streamWatermarks(),
                    null, instance.createdAt(), instance.updatedAt(), null, null, true, null, instance.streamMarks(), true, Set.of(), Set.of(), instance.startMark());
        }
    }

    @Nested
    @DisplayName("when the state store throws on the write that lists it as pending")
    class WhenTheStateStoreThrowsOnTheWriteThatListsItAsPending {

        private final IllegalArgumentException storeFailure = new IllegalArgumentException("write refused");

        @Test
        void the_original_failure_is_rethrown_with_the_store_failure_suppressed() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.failureOnWriteWithoutState = storeFailure;

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(handlerFailure.getSuppressed()).containsExactly(storeFailure)
            );
        }

        @Test
        void the_event_is_self_held() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.failureOnWriteWithoutState = storeFailure;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue()
            );
        }
    }

    @Nested
    @DisplayName("when quarantine is configured and the write of the failure record throws")
    class WhenQuarantineIsConfiguredAndTheWriteOfTheFailureRecordThrows {

        @Test
        void the_event_is_self_held_and_the_original_failure_is_rethrown() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2_WITH_QUARANTINE);
            store.failureOnWriteWithoutState = new IllegalArgumentException("write refused");

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue()
            );
        }

        @Test
        void the_instance_is_not_quarantined() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2_WITH_QUARANTINE);
            store.failureOnWriteWithoutState = new IllegalArgumentException("write refused");

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(stored().status()).isEqualTo(SagaStatus.ACTIVE);
        }
    }

    @Nested
    @DisplayName("when the instance already lists as many pending versions as it may")
    class WhenTheInstanceAlreadyListsAsManyPendingVersionsAsItMay {

        @Test
        void the_event_is_self_held_and_not_listed_as_pending() {
            // Given
            saveInstanceListingPendingVersions(SagaExecutionSupport.PENDING_CAP);
            failingItems.add("two");
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue(),
                    () -> assertThat(pending()).isEmpty()
            );
        }

        @Test
        void a_version_below_the_cap_is_listed_as_pending_and_not_self_held() {
            // Given
            saveInstanceListingPendingVersions(SagaExecutionSupport.PENDING_CAP - 1);
            failingItems.add("two");
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(pending()).containsExactly(2L),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the instance moved past it while the delivery was in flight")
    class WhenTheInstanceMovedPastItWhileTheDeliveryWasInFlight {

        @Test
        void a_failure_after_the_delivery_saw_it_as_new_self_holds_it_and_rethrows_the_original() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            whileDispatchingTwo = () -> deliverItems(execution, 3, 8);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue()
            );
        }

        @Test
        void the_instance_does_not_list_the_overtaken_version_as_pending() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            whileDispatchingTwo = () -> deliverItems(execution, 3, 8);

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(pending()).isEmpty();
        }

        @Test
        void keeping_it_warns_that_its_reaction_runs_again_if_it_was_applied() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            whileDispatchingTwo = () -> deliverItems(execution, 3, 8);

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(warnings()).anySatisfy(warning -> assertThat(warning).contains("keeps the event '" + KEY_OF_TWO + "'").contains("behind the reorder window"));
        }

        @Test
        void a_lost_first_save_followed_by_an_instance_that_moved_past_it_throws_and_self_holds_it() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            store.beforeNextCompareAndSave = () -> deliverItems(execution, 3, 8);

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isNotNull().hasMessageContaining(KEY_OF_TWO),
                    () -> assertThat(stored().selfHeldKeys()).containsExactly(KEY_OF_TWO),
                    () -> assertThat(onlyHeldRef().selfHeld()).isTrue()
            );
        }

        @Test
        void a_lost_first_save_that_nothing_overtook_is_retried_and_applies_the_event_without_holding_it() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            store.beforeNextCompareAndSave = () -> {
            };

            // When
            execution.onCloudEvent(itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(stored().state()).isEqualTo(new Basket(1)),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the mark that self-holds it finds the event already settled")
    class WhenTheMarkThatSelfHoldsItFindsTheEventAlreadySettled {

        @Test
        void an_event_that_was_applied_in_the_meantime_has_its_key_taken_off_and_nothing_is_held() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            store.beforeNextMarkHeldEvents = () -> {
                failingItems.remove("two");
                deliver(execution, itemAddedAt(2, "two"));
            };

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty(),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(1))
            );
        }

        @Test
        void an_event_that_was_listed_pending_in_the_meantime_has_its_key_taken_off_and_nothing_is_held() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            store.beforeNextMarkHeldEvents = () -> {
                store.losingEveryWriteWithoutState = false;
                catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));
            };

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(pending()).containsExactly(2L),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the store fails while holding it")
    class WhenTheStoreFailsWhileHoldingIt {

        private final IllegalArgumentException storeFailure = new IllegalArgumentException("hold refused");

        @Test
        void exactly_one_error_says_the_event_could_not_be_kept() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            store.failureOnHold = storeFailure;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("could not keep it").contains(KEY_OF_TWO));
        }

        @Test
        void the_store_failure_is_suppressed_on_the_original_failure_that_is_rethrown() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            store.failureOnHold = storeFailure;

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(handlerFailure.getSuppressed()).containsExactly(storeFailure)
            );
        }

        @Test
        void a_self_hold_that_succeeds_logs_no_error() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(errors()).isEmpty();
        }
    }

    @Nested
    @DisplayName("when the instance is at the state store's document limit")
    class WhenTheInstanceIsAtTheStateStoresDocumentLimit {

        private SagaExecution<BasketEvent, Basket, BasketCommand> openedWithVersionTwoFailingAndTheKeyRefused() {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            store.refusingMarksCarryingAKey = true;
            return execution;
        }

        @Test
        void the_event_is_kept_on_its_held_copy_marked_outstanding_and_not_on_the_instance() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailingAndTheKeyRefused();

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            HeldEventRef ref = onlyHeldRef();
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(ref.key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(ref.outstanding()).isTrue(),
                    () -> assertThat(ref.selfHeld()).isFalse(),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty()
            );
        }

        @Test
        void the_instance_is_marked_again_after_the_held_copy_is_written() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailingAndTheKeyRefused();

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(store.markedKeys).containsExactly(KEY_OF_TWO, null, null),
                    () -> assertThat(stored().heldEventsChecked()).isFalse()
            );
        }

        @Test
        void one_error_says_the_instance_reached_the_document_limit() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailingAndTheKeyRefused();

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("document size limit").contains(KEY_OF_TWO));
        }

        @Test
        void a_held_copy_that_is_already_there_is_marked_outstanding_instead_of_held_again() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailingAndTheKeyRefused();
            CloudEvent delivered = itemAddedAt(2, "two");
            HeldEventRef alreadyThere = new HeldEventRef(KEY_OF_TWO, STREAM, 2L, null, ITEM_ADDED_TYPE, NOW.minusSeconds(60), false, false);
            store.delegate.hold(BASKET, new HeldEvent(BASKET, alreadyThere, delivered));

            // When
            catchThrowable(() -> execution.onCloudEvent(delivered));

            // Then
            assertAll(
                    () -> assertThat(store.delegate.heldIndex(BASKET)).singleElement().satisfies(ref -> {
                        assertThat(ref.outstanding()).isTrue();
                        assertThat(ref.heldAt()).isEqualTo(alreadyThere.heldAt());
                    })
            );
        }
    }

    @Nested
    @DisplayName("when the saga does not hold events")
    class WhenTheSagaDoesNotHoldEvents {

        @Test
        void the_original_failure_is_rethrown_and_nothing_is_held() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = executionThatDoesNotHold(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.add("two");
            store.losingEveryWriteWithoutState = true;

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }

        @Test
        void the_version_that_could_not_be_listed_is_logged_at_error() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = executionThatDoesNotHold(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            failingItems.add("two");
            store.losingEveryWriteWithoutState = true;

            // When
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertThat(errors()).singleElement().satisfies(error -> assertThat(error).contains("could not list it as pending").contains(KEY_OF_TWO));
        }
    }

    @Nested
    @DisplayName("when another input reaches the instance after the event was self-held")
    class WhenAnotherInputReachesTheInstanceAfterTheEventWasSelfHeld {

        private SagaExecution<BasketEvent, Basket, BasketCommand> afterVersionTwoWasSelfHeld() {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));
            store.losingEveryWriteWithoutState = false;
            failingItems.clear();
            return execution;
        }

        @Test
        void the_reaction_has_not_run_before_that_input_reaches_the_instance() {
            // When
            afterVersionTwoWasSelfHeld();

            // Then
            assertThat(issued).doesNotContain(reserve("two"));
        }

        @Test
        void that_input_applies_the_held_copy_before_it_applies_itself() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeld();

            // When
            deliver(execution, itemAddedAt(3, "three"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("two"), reserve("three")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(2))
            );
        }

        @Test
        void it_is_applied_once_however_far_the_inputs_that_follow_it_move_the_window() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeld();

            // When
            deliverItems(execution, 3, 8);

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(7))
            );
        }

        @Test
        void its_key_leaves_the_self_held_keys_and_the_held_copy_is_released_and_no_applied_key_is_kept_once_the_drain_has_ended() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeld();

            // When
            deliver(execution, itemAddedAt(3, "three"));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(after.selfHeldKeys()).isEmpty(),
                    () -> assertThat(after.drainedHeldKeys()).isEmpty(),
                    () -> assertThat(after.heldEventsChecked()).isTrue(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }

        @Test
        void a_delivery_of_it_after_that_is_skipped_and_its_reaction_does_not_run_again() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeld();
            deliverItems(execution, 3, 8);

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(7))
            );
        }
    }

    @Nested
    @DisplayName("when a self-held event that has no held copy is delivered again")
    class WhenASelfHeldEventThatHasNoHeldCopyIsDeliveredAgain {

        /**
         * Version 2 failed and its key was put on the instance, but the store failed while holding its copy, so no drain has anything
         * to apply. Versions 3 to 8 then applied, which puts version 2 further behind the highest than the window of 2 allows.
         */
        private SagaExecution<BasketEvent, Basket, BasketCommand> afterVersionTwoWasSelfHeldWithoutACopyAndThreeToEightApplied() {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            store.failureOnHold = new IllegalArgumentException("hold refused");
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));
            store.losingEveryWriteWithoutState = false;
            store.failureOnHold = null;
            failingItems.clear();
            deliverItems(execution, 3, 8);
            assertThat(store.delegate.heldIndex(BASKET)).as("there is no held copy for a drain to apply").isEmpty();
            return execution;
        }

        @Test
        void it_is_applied_once_however_far_behind_the_window_it_is() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeldWithoutACopyAndThreeToEightApplied();

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(7))
            );
        }

        @Test
        void its_key_moves_from_the_self_held_keys_to_the_drained_keys() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeldWithoutACopyAndThreeToEightApplied();

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            SagaEnvelope<Basket> after = stored();
            assertAll(
                    () -> assertThat(after.selfHeldKeys()).isEmpty(),
                    () -> assertThat(after.drainedHeldKeys()).containsExactly(KEY_OF_TWO)
            );
        }

        @Test
        void a_further_delivery_of_it_is_skipped_and_its_reaction_does_not_run_again() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterVersionTwoWasSelfHeldWithoutACopyAndThreeToEightApplied();
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
        void the_reaction_has_not_run_before_it_is_delivered_again() {
            // When
            afterVersionTwoWasSelfHeldWithoutACopyAndThreeToEightApplied();

            // Then
            assertThat(issued).doesNotContain(reserve("two"));
        }
    }

    @Nested
    @DisplayName("when a drain ends between the mark that self-holds it and its held copy")
    class WhenADrainEndsBetweenTheMarkThatSelfHoldsItAndItsHeldCopy {

        private final AtomicBoolean drainHadEndedWhenTheCopyWasHeld = new AtomicBoolean();

        /** Another delivery reaches the instance in that gap, finds nothing held, and its save records that no drain is due. */
        private SagaExecution<BasketEvent, Basket, BasketCommand> selfHeldWhileAnotherDeliveryDrains() {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            deliver(execution, itemAddedAt(3, "three"));
            assertThat(stored().heldEventsChecked()).as("no drain is due before the failure").isTrue();
            store.failureOnWriteWithoutState = new IllegalArgumentException("write refused");
            store.beforeNextHold = () -> {
                deliver(execution, itemAddedAt(4, "four"));
                drainHadEndedWhenTheCopyWasHeld.set(stored().heldEventsChecked());
            };
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));
            return execution;
        }

        @Test
        void the_instance_has_a_drain_due_for_the_held_copy() {
            // When
            selfHeldWhileAnotherDeliveryDrains();

            // Then
            assertAll(
                    () -> assertThat(drainHadEndedWhenTheCopyWasHeld).as("the other delivery ended a drain before the copy was held").isTrue(),
                    () -> assertThat(onlyHeldRef().key()).isEqualTo(KEY_OF_TWO),
                    () -> assertThat(stored().heldEventsChecked()).as("a drain is due").isFalse()
            );
        }

        @Test
        void the_next_input_applies_the_held_copy_once() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = selfHeldWhileAnotherDeliveryDrains();
            failingItems.clear();

            // When
            deliver(execution, itemAddedAt(5, "five"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a held copy was applied by another delivery after it failed in a drain")
    class WhenAHeldCopyWasAppliedByAnotherDeliveryAfterItFailedInADrain {

        /**
         * Version 2 is self-held, then the drain that version 3 drives fails on its held copy. While that failure is being kept, another
         * delivery applies the copy and versions 4 to 8, which releases the copy, ends the drain and puts version 2 behind the window of 2.
         */
        private SagaExecution<BasketEvent, Basket, BasketCommand> afterTheCopyWasAppliedWhileItsFailureWasBeingKept() {
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            store.losingEveryWriteWithoutState = true;
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));
            store.beforeNextMarkHeldEvents = () -> {
                failingItems.clear();
                deliverItems(execution, 4, 8);
            };
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(3, "three")));
            assertThat(issued).as("the other delivery applied the held copy").containsOnlyOnce(reserve("two"));
            return execution;
        }

        @Test
        void it_is_not_held_again() {
            // When
            afterTheCopyWasAppliedWhileItsFailureWasBeingKept();

            // Then
            assertAll(
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty(),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty()
            );
        }

        @Test
        void its_reaction_does_not_run_a_second_time_when_the_next_input_drains() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = afterTheCopyWasAppliedWhileItsFailureWasBeingKept();

            // When
            deliver(execution, itemAddedAt(9, "nine"));

            // Then
            assertThat(issued).containsOnlyOnce(reserve("two"));
        }
    }

    @Nested
    @DisplayName("when the event was never seen as new by the delivery")
    class WhenTheEventWasNeverSeenAsNewByTheDelivery {

        @Test
        void a_redelivery_behind_the_window_is_skipped_without_holding_it() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = execution(WINDOW_OF_2);
            deliver(execution, openedAt(1));
            deliver(execution, itemAddedAt(2, "two"));
            deliverItems(execution, 3, 8);

            // When
            deliver(execution, itemAddedAt(2, "two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsOnlyOnce(reserve("two")),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty()
            );
        }

        @Test
        void a_listed_pending_version_that_another_delivery_applied_and_moved_past_in_flight_is_not_self_held() {
            // Given
            SagaExecution<BasketEvent, Basket, BasketCommand> execution = openedWithVersionTwoFailing(WINDOW_OF_2);
            catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));
            whileDispatchingTwo = () -> {
                failingItems.remove("two");
                deliver(execution, itemAddedAt(2, "two"));
                deliverItems(execution, 3, 8);
                failingItems.add("two");
            };

            // When
            Throwable thrown = catchThrowable(() -> execution.onCloudEvent(itemAddedAt(2, "two")));

            // Then
            assertAll(
                    () -> assertThat(thrown).isSameAs(handlerFailure),
                    () -> assertThat(stored().selfHeldKeys()).isEmpty(),
                    () -> assertThat(store.delegate.heldIndex(BASKET)).isEmpty(),
                    () -> assertThat(pending()).isEmpty()
            );
        }
    }

    /**
     * A store that holds events, by delegating to the in-memory one, and that a test can make lose or fail the writes the
     * failure path makes, or run something at the moment of a write.
     */
    private static final class HoldingStore implements SagaStateStore<Basket>, SagaHeldEvents<Basket> {
        private final InMemorySagaStateStore<Basket> delegate = new InMemorySagaStateStore<>();
        private final List<@Nullable String> markedKeys = new CopyOnWriteArrayList<>();
        private volatile boolean losingEveryWriteWithoutState;
        private volatile boolean refusingMarksCarryingAKey;
        private volatile @Nullable RuntimeException failureOnWriteWithoutState;
        private volatile @Nullable RuntimeException failureOnHold;
        // Taken once, and when it is, the save it precedes loses.
        private volatile @Nullable Runnable beforeNextCompareAndSave;
        // Taken once, and the mark it precedes then goes through.
        private volatile @Nullable Runnable beforeNextMarkHeldEvents;
        // Taken once, and the hold it precedes then goes through.
        private volatile @Nullable Runnable beforeNextHold;

        @Override
        public Optional<SagaEnvelope<Basket>> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public Optional<SagaEnvelope<Basket>> findWithoutState(String sagaId) {
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<Basket> envelope, long expectedVersion) {
            Runnable before = beforeNextCompareAndSave;
            if (before != null) {
                beforeNextCompareAndSave = null;
                before.run();
                return false;
            }
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<Basket> envelope, long expectedVersion) {
            RuntimeException failure = failureOnWriteWithoutState;
            if (failure != null) {
                throw failure;
            }
            if (losingEveryWriteWithoutState) {
                return false;
            }
            return delegate.compareAndSaveWithoutState(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<Basket>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }

        @Override
        public boolean hold(String sagaId, HeldEvent event) {
            Runnable before = beforeNextHold;
            if (before != null) {
                beforeNextHold = null;
                before.run();
            }
            RuntimeException failure = failureOnHold;
            if (failure != null) {
                throw failure;
            }
            return delegate.hold(sagaId, event);
        }

        @Override
        public boolean markOutstanding(String sagaId, String key) {
            return delegate.markOutstanding(sagaId, key);
        }

        @Override
        public Optional<SagaEnvelope<Basket>> markHeldEvents(String sagaId, @Nullable String selfHeldKey) {
            markedKeys.add(selfHeldKey);
            Runnable before = beforeNextMarkHeldEvents;
            if (before != null) {
                beforeNextMarkHeldEvents = null;
                before.run();
            }
            if (selfHeldKey != null && refusingMarksCarryingAKey) {
                throw new RefusedForSizeException("The instance is at the document limit", new IllegalStateException("document too large"));
            }
            return delegate.markHeldEvents(sagaId, selfHeldKey);
        }

        @Override
        public void unmarkSelfHeld(String sagaId, String selfHeldKey) {
            delegate.unmarkSelfHeld(sagaId, selfHeldKey);
        }

        @Override
        public List<HeldEventRef> heldIndex(String sagaId) {
            return delegate.heldIndex(sagaId);
        }

        @Override
        public List<HeldEvent> held(String sagaId, Collection<String> keys) {
            return delegate.held(sagaId, keys);
        }

        @Override
        public void release(String sagaId, String key) {
            delegate.release(sagaId, key);
        }
    }
}
