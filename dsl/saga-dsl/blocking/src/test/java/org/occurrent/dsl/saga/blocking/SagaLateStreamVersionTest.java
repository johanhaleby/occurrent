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
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A feed that keeps one stream's events of one type in order does not keep them in order across types. Kafka with a
 * topic per event type does that, and a broker consumer with several messages in flight can reorder within a type as
 * well. Up to 0.34.0 a saga kept the highest stream version it had applied per stream and skipped everything at or
 * below it, so a version that arrived after a higher one was lost. It now keeps the versions it has applied per stream
 * and event type, and skips an event only when it has applied that version of that type or the event is further behind
 * than the reorder window.
 */
@DisplayName("A stream version delivered after a higher one")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaLateStreamVersionTest {

    private static final String BASKET = "basket-1";
    private static final String STREAM = "basket-stream";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final SagaRunnerConfig FAST_POLL_CONFIG = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50));
    private static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();

    sealed interface BasketEvent permits BasketOpened, ItemAdded, CheckedOut {
        String eventId();

        String basketId();
    }

    record BasketOpened(String eventId, String basketId) implements BasketEvent {
    }

    record ItemAdded(String eventId, String basketId, String item) implements BasketEvent {
    }

    record CheckedOut(String eventId, String basketId) implements BasketEvent {
    }

    sealed interface BasketCommand permits OpenLedger, Reserve, Charge {
    }

    record OpenLedger(String basketId) implements BasketCommand {
    }

    record Reserve(String basketId, String item) implements BasketCommand {
    }

    record Charge(String basketId) implements BasketCommand {
    }

    record Basket(int items, boolean checkedOut) {
    }

    /** Every event issues a command, so a skipped event shows up as a missing command. */
    private static Saga<BasketEvent, Basket, BasketCommand> basketSaga() {
        return Saga.<BasketEvent, Basket, BasketCommand>builder(new Basket(0, false))
                .correlateAll(BasketEvent::basketId)
                .startsOn(BasketOpened.class)
                .startsOn(ItemAdded.class)
                .evolve(BasketOpened.class, (state, e) -> state)
                .react(BasketOpened.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.basketId()))))
                .evolve(ItemAdded.class, (state, e) -> new Basket(state.items() + 1, state.checkedOut()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.basketId(), e.item()))))
                .evolve(CheckedOut.class, (state, e) -> new Basket(state.items(), true))
                .react(CheckedOut.class, (state, e) -> List.of(SagaEffect.issue(new Charge(e.basketId()))))
                .build();
    }

    private CloudEventConverter<BasketEvent> converter;
    private PushSubscriptionModel pushModel;
    private SagaStateStore<Basket> store;
    private final List<BasketCommand> issued = new CopyOnWriteArrayList<>();
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();
    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;
    private @Nullable Level originalLevel;

    @BeforeEach
    void createInstances() {
        converter = new JacksonCloudEventConverter.Builder<BasketEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(BasketEvent::eventId).build();
        pushModel = new PushSubscriptionModel();
        store = SagaStateStore.inMemory();
        appender = new ListAppender<>();
        appender.start();
        executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
        executionLog.addAppender(appender);
        // Restored in teardown, because the level is process-global and would otherwise leak into the next test.
        originalLevel = executionLog.getLevel();
        executionLog.setLevel(Level.WARN);
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        executionLog.setLevel(originalLevel);
        executionLog.detachAppender(appender);
        appender.stop();
    }

    private void run(SagaRunnerConfig config) {
        SagaSubscription subscription = SagaRunner.<BasketEvent, BasketCommand>agnostic(pushModel, converter)
                .run("baskets", basketSaga(), store, issued::add, null, config);
        subscriptionsToClose.add(subscription);
    }

    private void deliver(long streamVersion, BasketEvent event) {
        pushModel.accept(streamKeyed(STREAM, streamVersion, event));
    }

    private void deliverOn(String stream, long streamVersion, BasketEvent event) {
        pushModel.accept(streamKeyed(stream, streamVersion, event));
    }

    private SagaEnvelope<Basket> stored() {
        return store.find(BASKET).orElseThrow();
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

    private static CheckedOut checkedOut() {
        return new CheckedOut("checked-out", BASKET);
    }

    private static Reserve reserve(String item) {
        return new Reserve(BASKET, item);
    }

    @Nested
    @DisplayName("when events of one type arrive out of order")
    class WhenEventsOfOneTypeArriveOutOfOrder {

        @Test
        void a_version_delivered_after_a_higher_version_of_the_same_type_is_applied() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(1, opened());
            deliver(3, added("three"));
            deliver(2, added("two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("three"), reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(2, false))
            );
        }

        @Test
        void a_late_version_delivered_twice_is_applied_once() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(1, opened());
            deliver(3, added("three"));
            deliver(2, added("two"));

            // When
            deliver(2, added("two"));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("three"), reserve("two"));
        }

        @Test
        void the_highest_version_redelivered_after_a_late_one_is_still_skipped() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(1, opened());
            deliver(3, added("three"));
            deliver(2, added("two"));

            // When
            deliver(3, added("three"));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("three"), reserve("two"));
        }

        @Test
        void a_version_that_is_still_missing_is_applied_when_it_finally_arrives_after_several_higher_ones() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(1, opened());
            deliver(5, added("five"));
            deliver(4, added("four"));
            deliver(3, added("three"));

            // When
            deliver(2, added("two"));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("five"), reserve("four"), reserve("three"), reserve("two"));
        }
    }

    @Nested
    @DisplayName("when events of different types arrive far apart")
    class WhenEventsOfDifferentTypesArriveFarApart {

        @Test
        void a_low_version_of_a_type_is_applied_after_a_much_higher_version_of_another_type() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(1, added("one"));
            deliver(1000, checkedOut());
            deliver(2, added("two"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(reserve("one"), new Charge(BASKET), reserve("two")),
                    () -> assertThat(stored().state()).isEqualTo(new Basket(2, true))
            );
        }

        @Test
        void redelivering_the_high_version_and_the_low_one_issues_nothing_more() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(1, added("one"));
            deliver(1000, checkedOut());
            deliver(2, added("two"));

            // When
            deliver(1000, checkedOut());
            deliver(2, added("two"));

            // Then
            assertThat(issued).containsExactly(reserve("one"), new Charge(BASKET), reserve("two"));
        }

        @Test
        void the_highest_version_over_all_types_is_still_the_stream_watermark() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(1, added("one"));
            deliver(1000, checkedOut());

            // When
            deliver(2, added("two"));

            // Then
            assertThat(stored().streamWatermarks()).isEqualTo(Map.of(STREAM, 1000L));
        }
    }

    @Nested
    @DisplayName("when the reorder window is 2")
    class WhenTheReorderWindowIs2 {

        @Test
        void a_version_within_the_window_of_the_highest_one_is_applied() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliver(1, added("one"));
            deliver(10, added("ten"));

            // When
            deliver(9, added("nine"));

            // Then
            assertThat(issued).containsExactly(reserve("one"), reserve("ten"), reserve("nine"));
        }

        @Test
        void a_version_as_far_behind_as_the_window_is_skipped() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliver(1, added("one"));
            deliver(10, added("ten"));
            deliver(9, added("nine"));

            // When
            deliver(8, added("eight"));

            // Then
            assertThat(issued).containsExactly(reserve("one"), reserve("ten"), reserve("nine"));
        }

        @Test
        void redelivering_the_highest_version_and_the_late_one_issues_nothing_more() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliver(1, added("one"));
            deliver(10, added("ten"));
            deliver(9, added("nine"));

            // When
            deliver(9, added("nine"));
            deliver(10, added("ten"));

            // Then
            assertThat(issued).containsExactly(reserve("one"), reserve("ten"), reserve("nine"));
        }

        @Test
        void the_stored_marks_forget_the_version_that_fell_out_of_the_window() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));

            // When
            deliver(1, added("one"));
            deliver(10, added("ten"));
            deliver(9, added("nine"));

            // Then
            assertThat(stored().streamMarks().get(STREAM).byType().get(ITEM_ADDED_TYPE)).isEqualTo(new TypeMark(10, List.of(9L), 8));
        }
    }

    @Nested
    @DisplayName("when an event is skipped for being behind the window")
    class WhenAnEventIsSkippedForBeingBehindTheWindow {

        @Test
        void one_warning_names_the_stream_and_the_version_however_many_events_are_skipped() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliver(1, added("one"));
            deliver(10, added("ten"));

            // When
            deliver(8, added("eight"));
            deliver(7, added("seven"));
            deliver(8, added("eight"));

            // Then
            assertAll(
                    () -> assertThat(warnings()).hasSize(1),
                    () -> assertThat(warnings().getFirst()).contains(STREAM, "version 8", "reorderWindow")
            );
        }

        @Test
        void each_stream_is_warned_about_once() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliverOn("first-stream", 1, added("one"));
            deliverOn("first-stream", 10, added("ten"));
            deliverOn("second-stream", 11, added("eleven"));
            deliverOn("second-stream", 20, added("twenty"));

            // When
            deliverOn("first-stream", 8, added("eight"));
            deliverOn("second-stream", 18, added("eighteen"));
            deliverOn("first-stream", 7, added("seven"));
            deliverOn("second-stream", 17, added("seventeen"));

            // Then
            assertAll(
                    () -> assertThat(warnings()).hasSize(2),
                    () -> assertThat(warnings()).anySatisfy(warning -> assertThat(warning).contains("'first-stream'")),
                    () -> assertThat(warnings()).anySatisfy(warning -> assertThat(warning).contains("'second-stream'"))
            );
        }

        @Test
        void a_plain_redelivery_is_skipped_without_a_warning() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliver(1, added("one"));
            deliver(10, added("ten"));
            deliver(9, added("nine"));

            // When
            deliver(10, added("ten"));
            deliver(9, added("nine"));

            // Then
            assertThat(warnings()).isEmpty();
        }

        @Test
        void a_redelivery_of_a_version_that_fell_out_of_the_window_is_skipped_with_the_warning_because_it_looks_like_a_late_arrival() {
            // Given
            run(FAST_POLL_CONFIG.withReorderWindow(2));
            deliver(1, added("one"));
            deliver(10, added("ten"));

            // When
            deliver(1, added("one"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(reserve("one"), reserve("ten")),
                    () -> assertThat(warnings()).hasSize(1)
            );
        }
    }

    @Nested
    @DisplayName("when many events of one type arrive in order")
    class WhenManyEventsOfOneTypeArriveInOrder {

        @Test
        void the_instance_remembers_no_more_applied_versions_than_the_default_window() {
            // Given
            run(FAST_POLL_CONFIG);
            int events = 10 * SagaRunnerConfig.defaults().reorderWindow();

            // When
            for (int version = 1; version <= events; version++) {
                deliver(version, added("item-" + version));
            }

            // Then
            Map<String, StreamMarks> marks = stored().streamMarks();
            TypeMark itemAdded = marks.get(STREAM).byType().get(ITEM_ADDED_TYPE);
            assertAll(
                    () -> assertThat(issued).hasSize(events),
                    () -> assertThat(marks).hasSize(1),
                    () -> assertThat(marks.get(STREAM).byType()).hasSize(1),
                    () -> assertThat(itemAdded.mark()).isEqualTo(events),
                    () -> assertThat(itemAdded.applied()).hasSizeLessThanOrEqualTo(256),
                    () -> assertThat(itemAdded.horizon()).isEqualTo(events - 256L)
            );
        }
    }

    @Nested
    @DisplayName("when events arrive in order")
    class WhenEventsArriveInOrder {

        @Test
        void every_version_is_applied() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(1, opened());
            deliver(2, added("two"));
            deliver(3, checkedOut());

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("two"), new Charge(BASKET));
        }

        @Test
        void a_redelivered_version_is_skipped() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(1, opened());
            deliver(2, added("two"));

            // When
            deliver(2, added("two"));
            deliver(1, opened());

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("two"));
        }

        @Test
        void a_version_with_a_lower_position_than_an_earlier_one_is_applied_when_its_stream_version_is_higher() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            pushModel.accept(OccurrentCloudEventExtension.withPosition(streamKeyed(STREAM, 1, opened()), 42));
            pushModel.accept(OccurrentCloudEventExtension.withPosition(streamKeyed(STREAM, 2, added("two")), 41));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("two"));
        }

        @Test
        void the_same_version_on_another_stream_is_applied() {
            // Given
            run(FAST_POLL_CONFIG);
            deliverOn("first-stream", 1, opened());

            // When
            deliverOn("second-stream", 1, added("one"));

            // Then
            assertThat(issued).containsExactly(new OpenLedger(BASKET), reserve("one"));
        }
    }

    @Nested
    @DisplayName("when the instance was stored before 0.35.0 and has no marks")
    class WhenTheInstanceWasStoredBefore0_35_0AndHasNoMarks {

        @BeforeEach
        void storeAnInstanceThatHasFoldedUpToVersion5() {
            SagaEnvelope<Basket> legacy = new SagaEnvelope<>(BASKET, new Basket(1, false), SagaStatus.ACTIVE, 1, List.of(),
                    Map.of(STREAM, 5L), null, NOW, NOW, null, null, true, null);
            assertThat(store.compareAndSave(BASKET, legacy, 0)).isTrue();
        }

        @Test
        void floor_rule_skips_version_3_of_a_type_the_instance_has_never_seen() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(3, checkedOut());

            // Then
            assertThat(issued).isEmpty();
        }

        @Test
        void floor_rule_skips_version_3_of_any_type() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(3, added("three"));
            deliver(3, opened());

            // Then
            assertThat(issued).isEmpty();
        }

        @Test
        void floor_rule_skips_the_version_that_is_the_watermark_itself() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(5, added("five"));

            // Then
            assertThat(issued).isEmpty();
        }

        @Test
        void new_type_rule_applies_version_6_of_a_type_without_a_mark() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(6, added("six"));

            // Then
            assertAll(
                    () -> assertThat(issued).containsExactly(reserve("six")),
                    () -> assertThat(stored().streamMarks().get(STREAM).floor()).isEqualTo(5L),
                    () -> assertThat(stored().streamMarks().get(STREAM).byType().get(ITEM_ADDED_TYPE)).isEqualTo(new TypeMark(6, List.of(), -250))
            );
        }

        @Test
        void floor_rule_still_skips_version_4_after_version_6_was_applied() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(6, added("six"));

            // When
            deliver(4, added("four"));
            deliver(4, checkedOut());

            // Then
            assertThat(issued).containsExactly(reserve("six"));
        }

        @Test
        void floor_rule_still_skips_version_5_after_version_6_was_applied() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(6, added("six"));

            // When
            deliver(5, added("five"));

            // Then
            assertThat(issued).containsExactly(reserve("six"));
        }

        @Test
        void late_arrival_rule_applies_version_7_after_version_8_when_the_floor_is_5() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(6, added("six"));
            deliver(8, added("eight"));

            // When
            deliver(7, added("seven"));

            // Then
            assertThat(issued).containsExactly(reserve("six"), reserve("eight"), reserve("seven"));
        }

        @Test
        void redelivery_rule_skips_version_6_when_it_comes_back() {
            // Given
            run(FAST_POLL_CONFIG);
            deliver(6, added("six"));

            // When
            deliver(6, added("six"));

            // Then
            assertThat(issued).containsExactly(reserve("six"));
        }

        @Test
        void floor_stays_at_5_when_later_events_advance_the_watermark() {
            // Given
            run(FAST_POLL_CONFIG);

            // When
            deliver(6, added("six"));
            deliver(9, checkedOut());
            deliver(8, added("eight"));

            // Then
            assertAll(
                    () -> assertThat(stored().streamMarks().get(STREAM).floor()).isEqualTo(5L),
                    () -> assertThat(stored().streamWatermarks()).isEqualTo(Map.of(STREAM, 9L))
            );
        }
    }

    // The stream key an event store writes, with the stream id and the version chosen by the test.
    private CloudEvent streamKeyed(String stream, long streamVersion, BasketEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent(stream, streamVersion)).build();
    }
}
