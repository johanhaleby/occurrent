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

package org.occurrent.dsl.saga.internal;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaFailure;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SagaTimeout;
import org.occurrent.dsl.saga.TimerName;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.HeldContext;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.HeldCopyStep;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An event can reach a saga before the event that starts its instance, or below the start it ran on, when the feed does not
 * deliver in commit order. Acknowledging it would lose its reaction, so the decision step answers {@code held} for it and
 * the executor keeps it until it can be applied. The pure step decides when an event is held, what an attempt does with
 * each copy it finds held, in what order it applies them, and what the saves of a start and a held copy record on the
 * instance. These tests cover those decisions.
 */
@DisplayName("SagaExecutionSupport and the events held for an instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaExecutionSupportHeldEventsTest {

    private static final String SAGA_ID = "o1";
    private static final String STREAM = "order-stream";
    private static final String OTHER_STREAM = "other-stream";
    private static final String ITEM_ADDED_TYPE = ItemAdded.class.getName();
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final int WINDOW = 2;

    sealed interface OrderEvent permits Opening, ItemAdded, Intake, Booking {
        String orderId();
    }

    /** The events that can open an order, so that a saga can declare the one type for both. */
    sealed interface Opening extends OrderEvent permits OrderPlaced, PaymentReserved {
    }

    /** The intakes that can open an order, as an abstract class rather than an interface. It is sealed because a saga cannot derive its event types from an open hierarchy. */
    abstract static sealed class Intake implements OrderEvent permits PhoneIntake, WebIntake {
        private final String orderId;

        Intake(String orderId) {
            this.orderId = orderId;
        }

        @Override
        public String orderId() {
            return orderId;
        }
    }

    static final class PhoneIntake extends Intake {
        PhoneIntake(String orderId) {
            super(orderId);
        }
    }

    static final class WebIntake extends Intake {
        WebIntake(String orderId) {
            super(orderId);
        }
    }

    /** A concrete class that has a subclass, so that a saga can start on a class that other events on the stream extend. Sealed for the same reason as {@link Intake}. */
    static sealed class Booking implements OrderEvent permits Rebooking {
        private final String orderId;

        Booking(String orderId) {
            this.orderId = orderId;
        }

        @Override
        public String orderId() {
            return orderId;
        }
    }

    static final class Rebooking extends Booking {
        Rebooking(String orderId) {
            super(orderId);
        }
    }

    record OrderPlaced(String orderId) implements Opening {
    }

    record ItemAdded(String orderId, String item) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements Opening {
    }

    sealed interface OrderCommand permits OpenLedger, Reserve, Ship {
    }

    record OpenLedger(String orderId) implements OrderCommand {
    }

    record Reserve(String orderId, String item) implements OrderCommand {
    }

    record Ship(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits Open, Paid {
    }

    record Open(int items) implements OrderState {
    }

    record Paid(int items) implements OrderState {
    }

    /** The state is {@code null} until the start, so applying any other event first would fail with a NullPointerException. */
    private static Saga<OrderEvent, OrderState, OrderCommand> orderSaga() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Open(0))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> new Open(((Open) state).items() + 1))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .evolve(PaymentReserved.class, (state, e) -> new Paid(((Open) state).items()))
                .react(PaymentReserved.class, (state, e) -> List.of(SagaEffect.issue(new Ship(e.orderId()))))
                .isTerminal(state -> state instanceof Paid)
                .build();
    }

    private static Outcome<OrderState, OrderCommand> process(@Nullable SagaEnvelope<OrderState> current, SagaInput<OrderEvent> input, EventMeta meta, HeldContext held) {
        return SagaExecutionSupport.process(orderSaga(), SAGA_ID, current, input, meta, NOW, WINDOW, held);
    }

    private static SagaInput<OrderEvent> itemAdded() {
        return SagaInput.event(new ItemAdded(SAGA_ID, "book"));
    }

    private static EventMeta metaAt(String streamId, long version) {
        return new EventMeta(streamId, version, null, ITEM_ADDED_TYPE);
    }

    private static SagaFailure aFailure() {
        return new SagaFailure(STREAM + "@1", null, NOW, "java.lang.IllegalStateException", "cannot start");
    }

    /** An instance whose very first event failed, which holds nothing but that failure and has not started. */
    private static SagaEnvelope<OrderState> notStarted(SagaStatus status) {
        return new SagaEnvelope<>(SAGA_ID, null, status, 1, List.of(), Map.of(), null, NOW, NOW,
                status == SagaStatus.COMPLETED ? NOW : null, null, false, aFailure(), Map.of());
    }

    /** A started instance with the marks an attempt reads, which is everything the decision about a held copy looks at. */
    private static SagaEnvelope<OrderState> started(Map<String, Long> streamWatermarks, Map<String, StreamMarks> streamMarks, @Nullable StartMark startMark) {
        return new SagaEnvelope<>(SAGA_ID, new Open(0), SagaStatus.ACTIVE, 3, List.of(), streamWatermarks, null, NOW, NOW, null, null, true, null,
                streamMarks, startMark);
    }

    private static SagaEnvelope<OrderState> startedWith(Map<String, StreamMarks> streamMarks) {
        return started(Map.of(), streamMarks, null);
    }

    /** The marks of an instance that has applied versions of the item type up to {@code mark}, with the horizon the window gives. */
    private static Map<String, StreamMarks> marksUpTo(long mark, Long... applied) {
        return Map.of(STREAM, new StreamMarks(0, Map.of(ITEM_ADDED_TYPE, new TypeMark(mark, List.of(applied), mark - WINDOW))));
    }

    private static HeldEventRef held(String streamId, long version) {
        return heldAt(streamId, version, NOW);
    }

    private static HeldEventRef heldAt(String streamId, long version, Instant heldAt) {
        return new HeldEventRef(streamId + "@" + version, streamId, version, null, ITEM_ADDED_TYPE, heldAt, false, null);
    }

    /** A copy whose {@code startEvent} was computed at hold time as true. */
    private static HeldEventRef startEventCopy(String streamId, long version) {
        return new HeldEventRef(streamId + "@" + version, streamId, version, null, ITEM_ADDED_TYPE, NOW, true, null);
    }

    private static HeldEventRef withoutStreamKey(String key, Instant heldAt) {
        return new HeldEventRef(key, null, null, null, ITEM_ADDED_TYPE, heldAt, false, null);
    }

    @Nested
    @DisplayName("when a non-start event arrives for an instance that has not started")
    class WhenANonStartEventArrivesForAnInstanceThatHasNotStarted {

        @Test
        void the_event_is_held_when_the_delivery_holds_and_no_instance_exists() {
            // Given
            HeldContext holds = new HeldContext(true, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, itemAdded(), metaAt(STREAM, 2), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isTrue(),
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull(),
                    () -> assertThat(outcome.commands()).isEmpty());
        }

        @Test
        void the_event_is_held_when_the_delivery_holds_and_the_instance_exists_without_having_started() {
            // Given
            HeldContext holds = new HeldContext(true, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(notStarted(SagaStatus.ACTIVE), itemAdded(), metaAt(STREAM, 2), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isTrue(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void the_event_is_skipped_when_the_delivery_does_not_hold_and_no_instance_exists() {
            // Given
            HeldContext doesNotHold = new HeldContext(false, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, itemAdded(), metaAt(STREAM, 2), doesNotHold);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void the_event_is_skipped_when_the_delivery_does_not_hold_and_the_instance_exists_without_having_started() {
            // Given
            HeldContext doesNotHold = new HeldContext(false, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(notStarted(SagaStatus.ACTIVE), itemAdded(), metaAt(STREAM, 2), doesNotHold);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void the_event_is_skipped_by_default_because_nothing_is_held_unless_the_executor_says_so() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(orderSaga(), SAGA_ID, null, itemAdded(), metaAt(STREAM, 2), NOW);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void a_timeout_is_never_held_when_no_instance_exists() {
            // Given
            HeldContext holds = new HeldContext(true, null);
            SagaInput<OrderEvent> timeout = SagaInput.timeout(new SagaTimeout(SAGA_ID, TimerName.parse("payment")));

            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, timeout, EventMeta.NONE, holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void a_timeout_is_never_held_when_the_instance_has_not_started() {
            // Given
            HeldContext holds = new HeldContext(true, null);
            SagaInput<OrderEvent> timeout = SagaInput.timeout(new SagaTimeout(SAGA_ID, TimerName.parse("payment")));

            // When
            Outcome<OrderState, OrderCommand> outcome = process(notStarted(SagaStatus.ACTIVE), timeout, EventMeta.NONE, holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void a_start_event_is_processed_and_not_held_when_the_delivery_holds() {
            // Given
            HeldContext holds = new HeldContext(true, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, SagaInput.event(new OrderPlaced(SAGA_ID)), metaAt(STREAM, 1), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isTrue());
        }
    }

    @Nested
    @DisplayName("when the instance has completed or been quarantined")
    class WhenTheInstanceHasCompletedOrBeenQuarantined {

        @Test
        void a_completed_instance_skips_a_non_start_event_instead_of_holding_it_even_though_it_has_not_started() {
            // Given
            HeldContext holds = new HeldContext(true, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(notStarted(SagaStatus.COMPLETED), itemAdded(), metaAt(STREAM, 2), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }

        @Test
        void a_quarantined_instance_skips_a_non_start_event_instead_of_holding_it_even_though_it_has_not_started() {
            // Given
            HeldContext holds = new HeldContext(true, null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(notStarted(SagaStatus.QUARANTINED), itemAdded(), metaAt(STREAM, 2), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }
    }


    @Nested
    @DisplayName("when an attempt decides what to do with a held copy")
    class WhenAnAttemptDecidesWhatToDoWithAHeldCopy {

        private final SagaEnvelope<OrderState> startedAtTen = started(Map.of(), Map.of(), new StartMark(STREAM, 10, null));

        @Test
        void the_copy_is_released_when_the_mark_of_its_type_is_its_version() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 10));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.RELEASE);
        }

        @Test
        void the_copy_is_released_when_its_version_is_among_the_applied_ones_of_its_type() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10, 9L));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 9));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.RELEASE);
        }

        @Test
        void a_copy_the_marks_prove_applied_is_released_even_though_it_sits_below_the_start_and_is_not_a_start_event() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(4), new StartMark(STREAM, 10, null));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 4));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.RELEASE);
        }

        @Test
        void a_copy_without_a_stream_key_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, withoutStreamKey("event-1 from urn:test", NOW));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_copy_below_the_start_on_the_start_stream_that_is_not_a_start_event_waits() {
            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(startedAtTen, held(STREAM, 4));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.WAIT);
        }

        @Test
        void a_copy_below_the_start_on_the_start_stream_that_is_a_start_event_is_applied() {
            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(startedAtTen, startEventCopy(STREAM, 4));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_later_version_of_the_stream_that_started_the_instance_is_applied() {
            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(startedAtTen, held(STREAM, 11));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void the_version_the_start_sits_at_is_not_below_the_start_and_is_applied() {
            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(startedAtTen, held(STREAM, 10));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void an_earlier_version_of_another_stream_than_the_one_that_started_the_instance_is_applied() {
            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(startedAtTen, held(OTHER_STREAM, 4));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_version_below_the_start_is_applied_when_the_instance_has_no_start_mark_because_it_was_stored_before_0_35_0() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), null);

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 4));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_copy_the_instance_has_not_seen_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 11));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_copy_of_a_stream_the_instance_has_never_seen_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(OTHER_STREAM, 1));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_copy_behind_the_reorder_window_that_the_marks_do_not_prove_applied_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 5));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_copy_at_or_below_the_floor_of_an_instance_stored_before_0_35_0_is_applied_because_a_held_event_was_never_applied_then() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(STREAM, 10L), Map.of(), null);

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 8));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }

        @Test
        void a_copy_above_the_floor_of_an_instance_stored_before_0_35_0_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(STREAM, 10L), Map.of(), null);

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(instance, held(STREAM, 11));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.APPLY);
        }
    }

    @Nested
    @DisplayName("when a drain orders the held events")
    class WhenADrainOrdersTheHeldEvents {

        @Test
        void the_events_are_applied_in_the_order_they_were_held() {
            // Given
            HeldEventRef first = heldAt(STREAM, 1, NOW);
            HeldEventRef second = heldAt(OTHER_STREAM, 1, NOW.plusSeconds(1));
            HeldEventRef third = heldAt("third-stream", 1, NOW.plusSeconds(2));

            // When
            List<HeldEventRef> ordered = SagaExecutionSupport.drainOrder(List.of(third, first, second));

            // Then
            assertThat(ordered).containsExactly(first, second, third);
        }

        @Test
        void events_held_at_the_same_instant_are_ordered_by_their_key_so_the_order_is_the_same_on_every_drain() {
            // Given
            HeldEventRef a = heldAt("a-stream", 1, NOW);
            HeldEventRef b = heldAt("b-stream", 1, NOW);

            // When
            List<HeldEventRef> ordered = SagaExecutionSupport.drainOrder(List.of(b, a));

            // Then
            assertThat(ordered).containsExactly(a, b);
        }

        @Test
        void the_versions_of_one_stream_are_sorted_into_the_places_the_stream_takes_whichever_arrived_first() {
            // Given
            HeldEventRef versionTwoHeldFirst = heldAt(STREAM, 2, NOW);
            HeldEventRef otherStream = heldAt(OTHER_STREAM, 1, NOW.plusSeconds(1));
            HeldEventRef versionOneHeldLast = heldAt(STREAM, 1, NOW.plusSeconds(2));

            // When
            List<HeldEventRef> ordered = SagaExecutionSupport.drainOrder(List.of(versionTwoHeldFirst, otherStream, versionOneHeldLast));

            // Then
            assertThat(ordered).containsExactly(versionOneHeldLast, otherStream, versionTwoHeldFirst);
        }

        @Test
        void an_event_without_a_stream_key_keeps_the_place_it_was_held_in_between_the_sorted_versions_of_a_stream() {
            // Given
            HeldEventRef versionThreeHeldFirst = heldAt(STREAM, 3, NOW);
            HeldEventRef withoutKey = withoutStreamKey("event-1 from urn:test", NOW.plusSeconds(1));
            HeldEventRef versionOneHeldLast = heldAt(STREAM, 1, NOW.plusSeconds(2));

            // When
            List<HeldEventRef> ordered = SagaExecutionSupport.drainOrder(List.of(versionThreeHeldFirst, withoutKey, versionOneHeldLast));

            // Then
            assertThat(ordered).containsExactly(versionOneHeldLast, withoutKey, versionThreeHeldFirst);
        }

        @Test
        void a_drain_of_nothing_is_ordered_as_nothing() {
            assertThat(SagaExecutionSupport.drainOrder(List.of())).isEmpty();
        }
    }

    @Nested
    @DisplayName("when a held copy is applied")
    class WhenAHeldCopyIsApplied {

        @Test
        void the_event_is_applied_without_being_classified_even_though_the_marks_would_skip_it_as_behind_the_reorder_window() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));
            HeldContext heldCopy = new HeldContext(false, STREAM + "@5");

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 5), heldCopy);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Reserve(SAGA_ID, "book")));
        }

        @Test
        void the_same_event_is_skipped_as_behind_the_reorder_window_when_it_is_not_a_held_copy() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 5), HeldContext.NONE);

            // Then
            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void a_copy_below_the_start_is_applied_without_being_held_again() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), new StartMark(STREAM, 5, null));
            HeldContext heldCopy = new HeldContext(true, STREAM + "@3");

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 3), heldCopy);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Reserve(SAGA_ID, "book")));
        }
    }

    @Nested
    @DisplayName("when an input starts the instance")
    class WhenAnInputStartsTheInstance {

        @Test
        void the_save_records_the_stream_version_and_position_of_the_start_so_an_event_below_it_can_wait() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, SagaInput.event(new OrderPlaced(SAGA_ID)),
                    new EventMeta(STREAM, 3L, 17L, OrderPlaced.class.getName()), new HeldContext(true, null));

            // Then
            assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, 17L));
        }

        @Test
        void the_save_records_no_start_mark_for_a_start_that_carries_no_stream_key() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, SagaInput.event(new OrderPlaced(SAGA_ID)), EventMeta.NONE, new HeldContext(true, null));

            // Then
            assertThat(outcome.envelope().startMark()).isNull();
        }

        @Test
        void the_save_of_a_start_that_follows_a_failed_first_attempt_records_the_start_mark() {
            // Given
            SagaEnvelope<OrderState> failedFirstAttempt = notStarted(SagaStatus.ACTIVE);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(failedFirstAttempt, SagaInput.event(new OrderPlaced(SAGA_ID)), metaAt(STREAM, 3), new HeldContext(true, null));

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, null)));
        }

        @Test
        void a_later_save_keeps_the_start_mark_of_the_start() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(10), new StartMark(STREAM, 3, 17L));

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), HeldContext.NONE);

            // Then
            assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, 17L));
        }
    }

    @Nested
    @DisplayName("when an event of the stream that started the instance sits below the start")
    class WhenAnEventOfTheStreamThatStartedTheInstanceSitsBelowTheStart {

        private final SagaEnvelope<OrderState> startedAtFive = started(Map.of(), Map.of(), new StartMark(STREAM, 5, null));

        private final HeldContext holds = new HeldContext(true, null);

        /** As {@link #orderSaga()}, with the item type a start type too, so the one event is of a start type here and of none there. */
        private Saga<OrderEvent, OrderState, OrderCommand> sagaStartingOnOrderPlacedAndItemAdded() {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .startsOn(ItemAdded.class)
                    .evolve(OrderPlaced.class, (state, e) -> new Open(0))
                    .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                    .evolve(ItemAdded.class, (state, e) -> new Open(((Open) state).items() + 1))
                    .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                    .react(PaymentReserved.class, (state, e) -> List.of(SagaEffect.issue(new Ship(e.orderId()))))
                    .build();
        }

        /** The one start event type is an interface of the placement and the payment, so it stands for two event types. */
        private Saga<OrderEvent, OrderState, OrderCommand> sagaStartingOnTheInterfaceOfPlacedAndPayment() {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(Opening.class)
                    .evolve(OrderPlaced.class, (state, e) -> new Open(0))
                    .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                    .evolve(PaymentReserved.class, (state, e) -> new Open(state == null ? 0 : ((Open) state).items()))
                    .react(PaymentReserved.class, (state, e) -> List.of(SagaEffect.issue(new Ship(e.orderId()))))
                    .evolve(ItemAdded.class, (state, e) -> new Open(((Open) state).items() + 1))
                    .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                    .build();
        }

        /** The one start event type is an abstract class with two concrete subclasses, so it stands for two event types. */
        private Saga<OrderEvent, OrderState, OrderCommand> sagaStartingOnTheAbstractClassOfPhoneAndWebIntake() {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(Intake.class)
                    .evolve(PhoneIntake.class, (state, e) -> new Open(0))
                    .react(PhoneIntake.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                    .evolve(WebIntake.class, (state, e) -> new Open(state == null ? 0 : ((Open) state).items()))
                    .react(WebIntake.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                    .evolve(ItemAdded.class, (state, e) -> new Open(((Open) state).items() + 1))
                    .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                    .build();
        }

        /** The one start event type is a concrete class that has a subclass, sealed so that a saga can derive its event types, so it stands for two event types. */
        private Saga<OrderEvent, OrderState, OrderCommand> sagaStartingOnTheSealedConcreteClassBooking() {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(Booking.class)
                    .evolve(Booking.class, (state, e) -> new Open(state == null ? 0 : ((Open) state).items()))
                    .react(Booking.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                    .evolve(ItemAdded.class, (state, e) -> new Open(((Open) state).items() + 1))
                    .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                    .build();
        }

        private SagaInput<OrderEvent> paymentReserved() {
            return SagaInput.event(new PaymentReserved(SAGA_ID));
        }

        private EventMeta paymentMetaAt(String streamId, long version) {
            return new EventMeta(streamId, version, null, PaymentReserved.class.getName());
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_not_processed() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(startedAtFive, itemAdded(), metaAt(STREAM, 3), HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull(),
                    () -> assertThat(outcome.commands()).isEmpty());
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_held_and_does_nothing_when_the_delivery_holds() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(startedAtFive, itemAdded(), metaAt(STREAM, 3), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isTrue(),
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull(),
                    () -> assertThat(outcome.commands()).isEmpty());
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_below_the_start_when_the_saga_has_one_concrete_start_type() {
            assertThat(SagaExecutionSupport.isBelowTheStart(orderSaga(), startedAtFive, paymentReserved(), paymentMetaAt(STREAM, 3))).isTrue();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_below_the_start_when_the_saga_has_several_start_types() {
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnOrderPlacedAndItemAdded(), startedAtFive, paymentReserved(), paymentMetaAt(STREAM, 3))).isTrue();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_below_the_start_when_the_one_start_type_is_an_interface() {
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnTheInterfaceOfPlacedAndPayment(), startedAtFive, itemAdded(), metaAt(STREAM, 3))).isTrue();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_below_the_start_when_the_one_start_type_is_an_abstract_class() {
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnTheAbstractClassOfPhoneAndWebIntake(), startedAtFive, itemAdded(), metaAt(STREAM, 3))).isTrue();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_below_the_start_when_the_one_start_type_is_a_sealed_concrete_class() {
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnTheSealedConcreteClassBooking(), startedAtFive, itemAdded(), metaAt(STREAM, 3))).isTrue();
        }

        @Test
        void an_event_of_a_start_type_is_not_below_the_start_when_the_saga_has_one_concrete_start_type() {
            // Given
            SagaInput<OrderEvent> startType = SagaInput.event(new OrderPlaced(SAGA_ID));

            // Then
            assertThat(SagaExecutionSupport.isBelowTheStart(orderSaga(), startedAtFive, startType, new EventMeta(STREAM, 3L, null, OrderPlaced.class.getName()))).isFalse();
        }

        @Test
        void an_event_of_a_start_type_is_not_below_the_start_when_the_saga_has_several_start_types() {
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnOrderPlacedAndItemAdded(), startedAtFive, itemAdded(), metaAt(STREAM, 3))).isFalse();
        }

        @Test
        void an_event_of_a_subtype_of_the_abstract_start_type_is_not_below_the_start() {
            // Given
            SagaInput<OrderEvent> phoneIntake = SagaInput.event(new PhoneIntake(SAGA_ID));

            // Then
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnTheAbstractClassOfPhoneAndWebIntake(), startedAtFive, phoneIntake,
                    new EventMeta(STREAM, 3L, null, PhoneIntake.class.getName()))).isFalse();
        }

        @Test
        void an_event_of_a_subclass_of_a_concrete_start_type_is_not_below_the_start() {
            // Given
            SagaInput<OrderEvent> rebooking = SagaInput.event(new Rebooking(SAGA_ID));

            // Then
            assertThat(SagaExecutionSupport.isBelowTheStart(sagaStartingOnTheSealedConcreteClassBooking(), startedAtFive, rebooking,
                    new EventMeta(STREAM, 3L, null, Rebooking.class.getName()))).isFalse();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_held_when_the_saga_has_several_start_types_and_the_delivery_holds() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    paymentReserved(), paymentMetaAt(STREAM, 3), NOW, WINDOW, holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isTrue(),
                    () -> assertThat(outcome.commands()).isEmpty());
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_held_when_the_one_start_type_is_a_sealed_concrete_class_and_the_delivery_holds() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnTheSealedConcreteClassBooking(), SAGA_ID, startedAtFive,
                    itemAdded(), metaAt(STREAM, 3), NOW, WINDOW, holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isTrue(),
                    () -> assertThat(outcome.commands()).isEmpty());
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_skipped_when_the_saga_has_several_start_types_and_the_delivery_does_not_hold() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    paymentReserved(), paymentMetaAt(STREAM, 3), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.held()).isFalse());
        }

        @Test
        void a_held_copy_below_the_start_that_the_instance_has_already_applied_is_released_as_applied() {
            // Given the item type applied up to version 3 of the stream
            SagaEnvelope<OrderState> applied = started(Map.of(), marksUpTo(3), new StartMark(STREAM, 5, null));

            // When
            HeldCopyStep step = SagaExecutionSupport.heldCopyStep(applied, held(STREAM, 3));

            // Then
            assertThat(step).isEqualTo(HeldCopyStep.RELEASE);
        }

        @Test
        void a_start_event_applied_below_the_start_moves_the_start_mark_down_to_it() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    itemAdded(), new EventMeta(STREAM, 3L, 21L, ITEM_ADDED_TYPE), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Reserve(SAGA_ID, "book")),
                    () -> assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, 21L)));
        }

        @Test
        void a_subclass_of_the_start_type_applied_below_the_start_moves_the_start_mark_down_to_it() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnTheSealedConcreteClassBooking(), SAGA_ID, startedAtFive,
                    SagaInput.event(new Rebooking(SAGA_ID)), new EventMeta(STREAM, 3L, null, Rebooking.class.getName()), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, null)));
        }

        @Test
        void a_start_event_applied_above_the_start_leaves_the_start_mark_where_it_is() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    itemAdded(), metaAt(STREAM, 7), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 5, null));
        }

        @Test
        void a_start_event_of_another_stream_applied_below_the_start_version_leaves_the_start_mark_where_it_is() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    itemAdded(), metaAt(OTHER_STREAM, 3), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 5, null)));
        }

        @Test
        void an_event_of_a_start_type_is_processed() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    itemAdded(), metaAt(STREAM, 3), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Reserve(SAGA_ID, "book")));
        }

        @Test
        void an_event_above_the_start_is_processed() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(startedAtFive, itemAdded(), metaAt(STREAM, 6), HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Reserve(SAGA_ID, "book")));
        }

        @Test
        void an_event_at_the_version_of_the_start_is_processed() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(startedAtFive, itemAdded(), metaAt(STREAM, 5), HeldContext.NONE);

            // Then
            assertThat(outcome.processed()).isTrue();
        }

        @Test
        void an_event_of_another_stream_with_a_version_below_the_start_is_processed() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(startedAtFive, itemAdded(), metaAt(OTHER_STREAM, 3), HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Reserve(SAGA_ID, "book")));
        }

        @Test
        void an_event_without_a_stream_key_is_processed() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(startedAtFive, itemAdded(), EventMeta.NONE, HeldContext.NONE);

            // Then
            assertThat(outcome.processed()).isTrue();
        }

        @Test
        void an_event_is_processed_when_the_instance_has_no_start_mark_because_it_was_stored_before_0_35_0() {
            // Given
            SagaEnvelope<OrderState> withoutStartMark = started(Map.of(), Map.of(), null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(withoutStartMark, itemAdded(), metaAt(STREAM, 3), HeldContext.NONE);

            // Then
            assertThat(outcome.processed()).isTrue();
        }
    }

    @Nested
    @DisplayName("when the budget of a failing held copy runs out")
    class WhenTheBudgetOfAFailingHeldCopyRunsOut {

        private final Instant firstFailedAt = NOW.minusSeconds(60);
        private final Instant now = NOW.plusSeconds(5);

        @Test
        void the_instance_is_quarantined_with_a_failure_that_names_the_key_of_the_copy() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));
            HeldEventRef copy = new HeldEventRef("copy-key", STREAM, 12L, 42L, ITEM_ADDED_TYPE, NOW, false, firstFailedAt);

            // When
            FailureRecord<OrderState> record = SagaExecutionSupport.quarantinedOnHeldCopy(instance, copy, new IllegalStateException("cannot reserve"), firstFailedAt, now);

            // Then
            assertAll(
                    () -> assertThat(record.quarantined()).isTrue(),
                    () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(record.envelope().failure())
                            .isEqualTo(new SagaFailure("copy-key", 42L, firstFailedAt, IllegalStateException.class.getName(), "cannot reserve")));
        }

        @Test
        void the_save_expects_the_version_that_was_read_and_raises_it_by_one() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            FailureRecord<OrderState> record = SagaExecutionSupport.quarantinedOnHeldCopy(instance, held(STREAM, 12), new IllegalStateException("x"), firstFailedAt, now);

            // Then
            assertAll(
                    () -> assertThat(record.expectedVersion()).isEqualTo(instance.version()),
                    () -> assertThat(record.envelope().version()).isEqualTo(instance.version() + 1),
                    () -> assertThat(record.envelope().updatedAt()).isEqualTo(now));
        }

        @Test
        void the_copy_is_not_listed_as_pending_because_it_stays_held() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            FailureRecord<OrderState> record = SagaExecutionSupport.quarantinedOnHeldCopy(instance, held(STREAM, 12), new IllegalStateException("x"), firstFailedAt, now);

            // Then
            assertThat(record.envelope().streamMarks()).isEqualTo(instance.streamMarks());
        }

        @Test
        void the_state_the_start_mark_and_the_watermarks_are_carried_over() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(STREAM, 4L), marksUpTo(10), new StartMark(STREAM, 3, 17L));

            // When
            FailureRecord<OrderState> record = SagaExecutionSupport.quarantinedOnHeldCopy(instance, held(STREAM, 12), new IllegalStateException("x"), firstFailedAt, now);

            // Then
            assertAll(
                    () -> assertThat(record.envelope().state()).isEqualTo(instance.state()),
                    () -> assertThat(record.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, 17L)),
                    () -> assertThat(record.envelope().streamWatermarks()).isEqualTo(instance.streamWatermarks()));
        }
    }

    @Nested
    @DisplayName("when it asks whether an event is a start event")
    class WhenItAsksWhetherAnEventIsAStartEvent {

        @Test
        void an_event_of_the_start_type_is() {
            assertThat(SagaExecutionSupport.isStartEvent(orderSaga(), new OrderPlaced(SAGA_ID))).isTrue();
        }

        @Test
        void an_event_of_another_type_is_not() {
            assertThat(SagaExecutionSupport.isStartEvent(orderSaga(), new ItemAdded(SAGA_ID, "book"))).isFalse();
        }
    }
}
