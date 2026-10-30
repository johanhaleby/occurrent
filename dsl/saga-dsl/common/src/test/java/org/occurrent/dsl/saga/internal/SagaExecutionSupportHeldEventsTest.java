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
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.HeldContext;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.HeldStep;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An event can reach a saga before the event that starts its instance, when the feed does not deliver in commit order.
 * Acknowledging it would lose its reaction, so the decision step answers {@code held} for it and the executor keeps it
 * until the start has been applied. This covers the decisions the pure step makes about held events: when an event is
 * held, what a drain does with each event it finds held, in what order it applies them, and what the saves of a start,
 * a held copy and the end of a drain record on the instance.
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

    sealed interface OrderEvent permits Opening, ItemAdded, Intake {
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

    /** A started instance with the marks a drain reads, which is everything the decision about a held event looks at. */
    private static SagaEnvelope<OrderState> started(Map<String, Long> streamWatermarks, Map<String, StreamMarks> streamMarks,
                                                    Set<String> drainedHeldKeys, Set<String> selfHeldKeys, @Nullable StartMark startMark) {
        return new SagaEnvelope<>(SAGA_ID, new Open(0), SagaStatus.ACTIVE, 3, List.of(), streamWatermarks, null, NOW, NOW, null, null, true, null,
                streamMarks, false, drainedHeldKeys, selfHeldKeys, startMark);
    }

    private static SagaEnvelope<OrderState> startedWith(Map<String, StreamMarks> streamMarks) {
        return started(Map.of(), streamMarks, Set.of(), Set.of(), null);
    }

    private static SagaEnvelope<OrderState> startedWith(Set<String> drainedHeldKeys, Set<String> selfHeldKeys) {
        return started(Map.of(), Map.of(), drainedHeldKeys, selfHeldKeys, null);
    }

    /** The marks of an instance that has applied versions of the item type up to {@code mark}, with the horizon the window gives. */
    private static Map<String, StreamMarks> marksUpTo(long mark, Long... applied) {
        return Map.of(STREAM, new StreamMarks(0, Map.of(ITEM_ADDED_TYPE, new TypeMark(mark, List.of(applied), mark - WINDOW))));
    }

    private static HeldEventRef held(String streamId, long version) {
        return new HeldEventRef(streamId + "@" + version, streamId, version, null, ITEM_ADDED_TYPE, NOW, false, false);
    }

    private static HeldEventRef heldAt(String streamId, long version, Instant heldAt) {
        return new HeldEventRef(streamId + "@" + version, streamId, version, null, ITEM_ADDED_TYPE, heldAt, false, false);
    }

    private static HeldEventRef selfHeld(String streamId, long version) {
        return new HeldEventRef(streamId + "@" + version, streamId, version, null, ITEM_ADDED_TYPE, NOW, true, false);
    }

    private static HeldEventRef outstanding(String streamId, long version) {
        return new HeldEventRef(streamId + "@" + version, streamId, version, null, ITEM_ADDED_TYPE, NOW, false, true);
    }

    private static HeldEventRef withoutStreamKey(String key, Instant heldAt) {
        return new HeldEventRef(key, null, null, null, ITEM_ADDED_TYPE, heldAt, false, false);
    }

    @Nested
    @DisplayName("when a non-start event arrives for an instance that has not started")
    class WhenANonStartEventArrivesForAnInstanceThatHasNotStarted {

        @Test
        void the_event_is_held_when_the_delivery_holds_and_no_instance_exists() {
            // Given
            HeldContext holds = new HeldContext(true, null, false);

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
            HeldContext holds = new HeldContext(true, null, false);

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
            HeldContext doesNotHold = new HeldContext(false, null, false);

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
            HeldContext doesNotHold = new HeldContext(false, null, false);

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
            HeldContext holds = new HeldContext(true, null, false);
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
            HeldContext holds = new HeldContext(true, null, false);
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
            HeldContext holds = new HeldContext(true, null, false);

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
            HeldContext holds = new HeldContext(true, null, false);

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
            HeldContext holds = new HeldContext(true, null, false);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(notStarted(SagaStatus.QUARANTINED), itemAdded(), metaAt(STREAM, 2), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.held()).isFalse(),
                    () -> assertThat(outcome.processed()).isFalse());
        }
    }

    @Nested
    @DisplayName("when a drain decides what to do with a held event")
    class WhenADrainDecidesWhatToDoWithAHeldEvent {

        @Test
        void the_event_is_released_as_applied_when_the_mark_of_its_type_is_its_version() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 10), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_APPLIED);
        }

        @Test
        void the_event_is_released_as_applied_when_its_version_is_among_the_applied_ones_of_its_type() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10, 9L));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 9), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_APPLIED);
        }

        @Test
        void the_event_is_released_as_applied_when_its_key_is_among_the_drained_held_keys_even_though_the_marks_forgot_it() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(100), Set.of(STREAM + "@5"), Set.of(), null);

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 5), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_APPLIED);
        }

        @Test
        void an_event_without_a_stream_key_is_released_as_applied_when_its_key_is_among_the_drained_held_keys() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(Set.of("event-1 from urn:test"), Set.of());

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, withoutStreamKey("event-1 from urn:test", NOW), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_APPLIED);
        }

        @Test
        void an_event_without_a_stream_key_is_applied_when_its_key_is_not_among_the_drained_held_keys() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(Set.of("event-2 from urn:test"), Set.of());

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, withoutStreamKey("event-1 from urn:test", NOW), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void an_earlier_version_of_the_stream_that_started_the_instance_is_released_without_being_applied() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), Set.of(), Set.of(), new StartMark(STREAM, 10, null));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 4), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_BEFORE_START);
        }

        @Test
        void a_later_version_of_the_stream_that_started_the_instance_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), Set.of(), Set.of(), new StartMark(STREAM, 10, null));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 11), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void the_version_the_start_sits_at_is_not_earlier_than_the_start_and_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), Set.of(), Set.of(), new StartMark(STREAM, 10, null));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 10), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void an_earlier_version_of_another_stream_than_the_one_that_started_the_instance_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), Set.of(), Set.of(), new StartMark(STREAM, 10, null));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(OTHER_STREAM, 4), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_version_below_the_start_is_applied_when_the_instance_has_no_start_mark_because_it_was_stored_before_0_35_0() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), Map.of(), Set.of(), Set.of(), null);

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 4), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_self_held_key_keeps_the_event_applicable_however_far_behind_the_reorder_window_it_is() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(50), Set.of(), Set.of(STREAM + "@5"), null);

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, selfHeld(STREAM, 5), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void an_outstanding_copy_is_applied_however_far_behind_the_reorder_window_it_is() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, outstanding(STREAM, 5), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void an_event_the_instance_has_not_seen_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 11), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void an_event_of_a_stream_the_instance_has_never_seen_is_applied() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(OTHER_STREAM, 1), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_plain_copy_behind_the_reorder_window_is_left_held_because_nothing_keeps_it_outstanding() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 5), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.LEAVE_BEHIND_WINDOW);
        }

        @Test
        void a_self_held_copy_that_the_instance_no_longer_names_is_left_held_when_it_is_behind_the_reorder_window() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, selfHeld(STREAM, 5), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.LEAVE_SELF_HELD);
        }

        @Test
        void a_self_held_copy_that_the_instance_no_longer_names_is_applied_when_it_is_still_new_to_the_instance() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, selfHeld(STREAM, 11), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_plain_copy_at_or_below_the_floor_of_an_instance_stored_before_0_35_0_is_applied_because_a_held_event_was_never_applied_then() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(STREAM, 10L), Map.of(), Set.of(), Set.of(), null);

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 8), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY_FROM_THE_FLOOR);
        }

        @Test
        void a_plain_copy_above_the_floor_of_an_instance_stored_before_0_35_0_is_applied_as_new() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(STREAM, 10L), Map.of(), Set.of(), Set.of(), null);

            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), instance, held(STREAM, 11), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
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
            HeldContext heldCopy = new HeldContext(false, STREAM + "@5", false);

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
        void the_save_adds_the_key_of_the_copy_to_the_drained_held_keys() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(50), Set.of("earlier"), Set.of(), null);
            HeldContext heldCopy = new HeldContext(false, STREAM + "@5", false);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 5), heldCopy);

            // Then
            assertThat(outcome.envelope().drainedHeldKeys()).containsExactlyInAnyOrder("earlier", STREAM + "@5");
        }

        @Test
        void the_save_forgets_the_released_keys_and_keeps_the_others_beside_the_key_of_the_copy_it_adds() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(Set.of("released@1", "waiting@2"), Set.of());
            HeldContext heldCopy = new HeldContext(false, STREAM + "@5", false, Set.of("released@1"));

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 5), heldCopy);

            // Then
            assertThat(outcome.envelope().drainedHeldKeys()).containsExactlyInAnyOrder("waiting@2", STREAM + "@5");
        }

        @Test
        void the_save_leaves_the_instance_with_a_drain_due_when_none_ended_before_it() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(50));
            HeldContext heldCopy = new HeldContext(false, STREAM + "@5", false);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 5), heldCopy);

            // Then
            assertThat(outcome.envelope().heldEventsChecked()).isFalse();
        }

        @Test
        void the_save_of_a_self_held_version_moves_its_key_from_the_self_held_keys_to_the_drained_held_keys() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(10), Set.of(), Set.of(STREAM + "@11", "other@1"), null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.envelope().selfHeldKeys()).containsExactly("other@1"),
                    () -> assertThat(outcome.envelope().drainedHeldKeys()).containsExactly(STREAM + "@11"));
        }

        @Test
        void completing_the_instance_empties_the_self_held_keys() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(10), Set.of(), Set.of("other@1"), null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, SagaInput.event(new PaymentReserved(SAGA_ID)),
                    new EventMeta(STREAM, 11L, null, PaymentReserved.class.getName()), HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.envelope().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(outcome.envelope().selfHeldKeys()).isEmpty());
        }
    }

    @Nested
    @DisplayName("when a save ends a drain")
    class WhenASaveEndsADrain {

        @Test
        void the_save_records_that_no_drain_is_due() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(marksUpTo(10));
            HeldContext endsDrain = new HeldContext(false, null, true);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), endsDrain);

            // Then
            assertThat(outcome.envelope().heldEventsChecked()).isTrue();
        }

        @Test
        void the_save_forgets_the_keys_the_drain_applied_because_their_copies_are_released_by_then() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(Set.of("a@1", "b@2"), Set.of());
            HeldContext endsDrain = new HeldContext(false, null, true);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), endsDrain);

            // Then
            assertThat(outcome.envelope().drainedHeldKeys()).isEmpty();
        }

        @Test
        void the_save_keeps_only_the_key_it_applies_when_it_is_also_a_held_copy() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(Set.of("a@1", "b@2"), Set.of());
            HeldContext endsDrainAndAppliesACopy = new HeldContext(false, STREAM + "@11", true);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), endsDrainAndAppliesACopy);

            // Then
            assertAll(
                    () -> assertThat(outcome.envelope().heldEventsChecked()).isTrue(),
                    () -> assertThat(outcome.envelope().drainedHeldKeys()).containsExactly(STREAM + "@11"));
        }

        @Test
        void the_save_keeps_only_the_key_it_applies_when_the_input_is_a_self_held_version() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(10), Set.of("a@1"), Set.of(STREAM + "@11"), null);
            HeldContext endsDrain = new HeldContext(false, null, true);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), endsDrain);

            // Then
            assertThat(outcome.envelope().drainedHeldKeys()).containsExactly(STREAM + "@11");
        }

        @Test
        void a_save_that_does_not_end_a_drain_keeps_the_drained_held_keys_of_the_drain_that_is_still_running() {
            // Given
            SagaEnvelope<OrderState> instance = startedWith(Set.of("a@1", "b@2"), Set.of());

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.envelope().drainedHeldKeys()).containsExactlyInAnyOrder("a@1", "b@2"),
                    () -> assertThat(outcome.envelope().heldEventsChecked()).isFalse());
        }

        @Test
        void a_save_does_not_unset_a_drain_that_has_ended_already() {
            // Given
            SagaEnvelope<OrderState> drainedOnce = new SagaEnvelope<>(SAGA_ID, new Open(0), SagaStatus.ACTIVE, 3, List.of(), Map.of(), null, NOW, NOW, null,
                    null, true, null, marksUpTo(10), true, Set.of(), Set.of(), null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(drainedOnce, itemAdded(), metaAt(STREAM, 11), HeldContext.NONE);

            // Then
            assertThat(outcome.envelope().heldEventsChecked()).isTrue();
        }

        @Test
        void an_instance_whose_drain_ended_in_a_save_of_its_own_records_that_no_drain_is_due_and_forgets_the_drained_keys() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(10), Set.of("a@1"), Set.of("self@2"), new StartMark(STREAM, 1, 7L));
            Instant later = NOW.plusSeconds(5);

            // When
            SagaEnvelope<OrderState> ended = SagaExecutionSupport.withDrainEnded(instance, later);

            // Then
            assertAll(
                    () -> assertThat(ended.heldEventsChecked()).isTrue(),
                    () -> assertThat(ended.drainedHeldKeys()).isEmpty(),
                    () -> assertThat(ended.selfHeldKeys()).containsExactly("self@2"),
                    () -> assertThat(ended.startMark()).isEqualTo(new StartMark(STREAM, 1, 7L)),
                    () -> assertThat(ended.version()).isEqualTo(instance.version() + 1),
                    () -> assertThat(ended.updatedAt()).isEqualTo(later),
                    () -> assertThat(ended.state()).isEqualTo(instance.state()),
                    () -> assertThat(ended.streamMarks()).isEqualTo(instance.streamMarks()));
        }
    }

    @Nested
    @DisplayName("when an input starts the instance")
    class WhenAnInputStartsTheInstance {

        @Test
        void the_save_records_that_a_drain_is_due() {
            // Given
            HeldContext holds = new HeldContext(true, null, false);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, SagaInput.event(new OrderPlaced(SAGA_ID)), metaAt(STREAM, 3), holds);

            // Then
            assertAll(
                    () -> assertThat(outcome.envelope().heldEventsChecked()).isFalse(),
                    () -> assertThat(SagaExecutionSupport.isDrainDue(outcome.envelope())).isTrue());
        }

        @Test
        void the_save_records_the_stream_version_and_position_of_the_start_so_the_drain_can_tell_what_committed_before_it() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, SagaInput.event(new OrderPlaced(SAGA_ID)),
                    new EventMeta(STREAM, 3L, 17L, OrderPlaced.class.getName()), new HeldContext(true, null, false));

            // Then
            assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, 17L));
        }

        @Test
        void the_save_records_no_start_mark_for_a_start_that_carries_no_stream_key() {
            // When
            Outcome<OrderState, OrderCommand> outcome = process(null, SagaInput.event(new OrderPlaced(SAGA_ID)), EventMeta.NONE, new HeldContext(true, null, false));

            // Then
            assertThat(outcome.envelope().startMark()).isNull();
        }

        @Test
        void the_save_of_a_start_that_follows_a_failed_first_attempt_records_a_drain_as_due_and_the_start_mark() {
            // Given
            SagaEnvelope<OrderState> failedFirstAttempt = notStarted(SagaStatus.ACTIVE);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(failedFirstAttempt, SagaInput.event(new OrderPlaced(SAGA_ID)), metaAt(STREAM, 3), new HeldContext(true, null, false));

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().heldEventsChecked()).isFalse(),
                    () -> assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, null)));
        }

        @Test
        void a_later_save_keeps_the_start_mark_of_the_start() {
            // Given
            SagaEnvelope<OrderState> instance = started(Map.of(), marksUpTo(10), Set.of(), Set.of(), new StartMark(STREAM, 3, 17L));

            // When
            Outcome<OrderState, OrderCommand> outcome = process(instance, itemAdded(), metaAt(STREAM, 11), HeldContext.NONE);

            // Then
            assertThat(outcome.envelope().startMark()).isEqualTo(new StartMark(STREAM, 3, 17L));
        }
    }

    @Nested
    @DisplayName("when an event of the stream that started the instance sits below the start")
    class WhenAnEventOfTheStreamThatStartedTheInstanceSitsBelowTheStart {

        private final SagaEnvelope<OrderState> startedAtFive = started(Map.of(), Map.of(), Set.of(), Set.of(), new StartMark(STREAM, 5, null));

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
        void an_event_of_another_type_than_a_start_type_is_before_the_start_when_the_saga_has_one_concrete_start_type() {
            assertThat(SagaExecutionSupport.isBeforeTheStart(orderSaga(), startedAtFive, paymentReserved(), paymentMetaAt(STREAM, 3))).isTrue();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_not_before_the_start_when_the_saga_has_several_start_types() {
            assertThat(SagaExecutionSupport.isBeforeTheStart(sagaStartingOnOrderPlacedAndItemAdded(), startedAtFive, paymentReserved(), paymentMetaAt(STREAM, 3))).isFalse();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_not_before_the_start_when_the_one_start_type_is_an_interface() {
            assertThat(SagaExecutionSupport.isBeforeTheStart(sagaStartingOnTheInterfaceOfPlacedAndPayment(), startedAtFive, itemAdded(), metaAt(STREAM, 3))).isFalse();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_not_before_the_start_when_the_one_start_type_is_an_abstract_class() {
            assertThat(SagaExecutionSupport.isBeforeTheStart(sagaStartingOnTheAbstractClassOfPhoneAndWebIntake(), startedAtFive, itemAdded(), metaAt(STREAM, 3))).isFalse();
        }

        @Test
        void an_event_of_a_start_type_is_not_before_the_start_when_the_saga_has_one_concrete_start_type() {
            // Given
            SagaInput<OrderEvent> startType = SagaInput.event(new OrderPlaced(SAGA_ID));

            // Then
            assertThat(SagaExecutionSupport.isBeforeTheStart(orderSaga(), startedAtFive, startType, new EventMeta(STREAM, 3L, null, OrderPlaced.class.getName()))).isFalse();
        }

        @Test
        void an_event_of_another_type_than_a_start_type_is_processed_when_the_saga_has_several_start_types() {
            // When
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(sagaStartingOnOrderPlacedAndItemAdded(), SAGA_ID, startedAtFive,
                    paymentReserved(), paymentMetaAt(STREAM, 3), NOW, WINDOW, HeldContext.NONE);

            // Then
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new Ship(SAGA_ID)));
        }

        @Test
        void a_held_copy_below_the_start_is_released_by_a_drain_when_the_saga_has_one_concrete_start_type() {
            // When
            HeldStep step = SagaExecutionSupport.drainStep(orderSaga(), startedAtFive, held(STREAM, 3), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_BEFORE_START);
        }

        @Test
        void a_held_copy_below_the_start_is_classified_by_a_drain_and_applied_as_new_when_the_one_start_type_is_an_interface() {
            // When
            HeldStep step = SagaExecutionSupport.drainStep(sagaStartingOnTheInterfaceOfPlacedAndPayment(), startedAtFive, held(STREAM, 3), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_held_copy_below_the_start_is_classified_by_a_drain_and_applied_as_new_when_the_one_start_type_is_an_abstract_class() {
            // When
            HeldStep step = SagaExecutionSupport.drainStep(sagaStartingOnTheAbstractClassOfPhoneAndWebIntake(), startedAtFive, held(STREAM, 3), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_held_copy_below_the_start_is_classified_by_a_drain_and_applied_as_new_when_the_saga_has_several_start_types() {
            // When
            HeldStep step = SagaExecutionSupport.drainStep(sagaStartingOnOrderPlacedAndItemAdded(), startedAtFive, held(STREAM, 3), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.APPLY);
        }

        @Test
        void a_held_copy_below_the_start_that_the_instance_has_already_applied_is_released_as_applied_when_the_saga_has_several_start_types() {
            // Given the item type applied up to version 3 of the stream
            SagaEnvelope<OrderState> applied = started(Map.of(), marksUpTo(3), Set.of(), Set.of(), new StartMark(STREAM, 5, null));

            // When
            HeldStep step = SagaExecutionSupport.drainStep(sagaStartingOnOrderPlacedAndItemAdded(), applied, held(STREAM, 3), WINDOW);

            // Then
            assertThat(step).isEqualTo(HeldStep.RELEASE_APPLIED);
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
            SagaEnvelope<OrderState> withoutStartMark = started(Map.of(), Map.of(), Set.of(), Set.of(), null);

            // When
            Outcome<OrderState, OrderCommand> outcome = process(withoutStartMark, itemAdded(), metaAt(STREAM, 3), HeldContext.NONE);

            // Then
            assertThat(outcome.processed()).isTrue();
        }
    }

    @Nested
    @DisplayName("when it asks whether a drain is due")
    class WhenItAsksWhetherADrainIsDue {

        @Test
        void no_drain_is_due_for_an_instance_that_does_not_exist() {
            assertThat(SagaExecutionSupport.isDrainDue(null)).isFalse();
        }

        @Test
        void no_drain_is_due_for_an_instance_that_has_not_started_because_the_events_wait_for_its_start() {
            assertThat(SagaExecutionSupport.isDrainDue(notStarted(SagaStatus.ACTIVE))).isFalse();
        }

        @Test
        void a_drain_is_due_for_an_active_started_instance_that_has_not_checked_its_held_events() {
            assertThat(SagaExecutionSupport.isDrainDue(startedWith(Map.of()))).isTrue();
        }

        @Test
        void no_drain_is_due_for_an_active_started_instance_that_has_checked_its_held_events() {
            // Given
            SagaEnvelope<OrderState> checked = SagaExecutionSupport.withDrainEnded(startedWith(Map.of()), NOW);

            // Then
            assertThat(SagaExecutionSupport.isDrainDue(checked)).isFalse();
        }

        @Test
        void no_drain_is_due_for_a_quarantined_instance_because_its_held_events_wait_for_a_release() {
            // Given
            SagaEnvelope<OrderState> quarantined = new SagaEnvelope<>(SAGA_ID, new Open(0), SagaStatus.QUARANTINED, 3, List.of(), Map.of(), null, NOW, NOW, null,
                    null, true, aFailure(), Map.of());

            // Then
            assertThat(SagaExecutionSupport.isDrainDue(quarantined)).isFalse();
        }

        @Test
        void no_drain_is_due_for_a_completed_instance_because_it_skips_everything_addressed_to_it() {
            // Given
            SagaEnvelope<OrderState> completed = new SagaEnvelope<>(SAGA_ID, new Paid(0), SagaStatus.COMPLETED, 3, List.of(), Map.of(), null, NOW, NOW, NOW, null);

            // Then
            assertThat(SagaExecutionSupport.isDrainDue(completed)).isFalse();
        }
    }
}
