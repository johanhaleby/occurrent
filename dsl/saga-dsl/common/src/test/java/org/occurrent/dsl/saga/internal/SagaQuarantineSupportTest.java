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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("Quarantining a saga instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineSupportTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration BUDGET = Duration.ofMinutes(5);
    private static final String TIMER = "payment";

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Paid {
        String orderId();
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Paid(String orderId) implements OrderState {
    }

    private static Saga<OrderEvent, OrderState, OrderCommand> saga() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .onStart((state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.startTimeout(TIMER, Duration.ofMinutes(5))))
                .evolve(PaymentReserved.class, (state, e) -> new Paid(e.orderId()))
                .isTerminal(state -> state instanceof Paid)
                .build();
    }

    private static EventMeta at(long position) {
        return new EventMeta("o1", position, position);
    }

    private static SagaEnvelope<OrderState> active(long version, Map<String, Long> streamWatermarks, Long positionWatermark) {
        return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, version, List.of(new TimerEntry(TIMER, NOW.toEpochMilli())),
                streamWatermarks, positionWatermark, NOW.minusSeconds(60), NOW.minusSeconds(30), null, null, true, null);
    }

    private static SagaEnvelope<OrderState> withFailure(SagaStatus status, SagaFailure failure) {
        return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), status, 4, List.of(new TimerEntry(TIMER, NOW.toEpochMilli())),
                Map.of("o1", 6L), 6L, NOW.minusSeconds(60), NOW, null, null, true, failure);
    }

    private static SagaFailure failedOn(long position, Instant firstFailedAt) {
        return new SagaFailure("o1@" + position, position, firstFailedAt, IllegalStateException.class.getName(), "boom", null);
    }

    @Nested
    class TheTimeBudget {

        @Test
        void the_first_failure_of_an_input_records_when_it_started_failing_and_does_not_quarantine() {
            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", active(3, Map.of("o1", 6L), 6L), at(7), new IllegalStateException("boom"), NOW, BUDGET);

            assertAll(
                    () -> assertThat(record).isNotNull(),
                    () -> assertThat(record.quarantined()).isFalse(),
                    () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(record.envelope().failure()).isEqualTo(
                            new SagaFailure("o1@7", 7, NOW, IllegalStateException.class.getName(), "boom", null)),
                    () -> assertThat(record.expectedVersion()).isEqualTo(3),
                    () -> assertThat(record.envelope().version()).isEqualTo(4)
            );
        }

        @Test
        void a_later_failure_of_the_same_input_inside_the_budget_writes_nothing_at_all() {
            SagaEnvelope<OrderState> failing = withFailure(SagaStatus.ACTIVE, failedOn(7, NOW.minus(Duration.ofMinutes(4))));

            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", failing, at(7), new IllegalStateException("boom"), NOW, BUDGET);

            assertThat(record).isNull();
        }

        @Test
        void a_failure_past_the_budget_quarantines_the_instance_at_the_failing_position() {
            SagaEnvelope<OrderState> failing = withFailure(SagaStatus.ACTIVE, failedOn(7, NOW.minus(Duration.ofMinutes(5))));

            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", failing, at(7), new IllegalArgumentException("still broken"), NOW, BUDGET);

            assertAll(
                    () -> assertThat(record).isNotNull(),
                    () -> assertThat(record.quarantined()).isTrue(),
                    () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(record.envelope().failure().position()).isEqualTo(7),
                    // The instant the failing started is kept, the exception is refreshed to the latest one.
                    () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(5))),
                    () -> assertThat(record.envelope().failure().failureType()).isEqualTo(IllegalArgumentException.class.getName()),
                    () -> assertThat(record.envelope().failure().failureMessage()).isEqualTo("still broken")
            );
        }

        @Test
        void quarantining_advances_neither_watermark_so_a_replay_can_still_deliver_the_event() {
            SagaEnvelope<OrderState> failing = withFailure(SagaStatus.ACTIVE, failedOn(7, NOW.minus(Duration.ofMinutes(5))));

            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", failing, at(7), new IllegalStateException("boom"), NOW, BUDGET);

            assertAll(
                    () -> assertThat(record.envelope().streamWatermarks()).isEqualTo(Map.of("o1", 6L)),
                    () -> assertThat(record.envelope().positionWatermark()).isEqualTo(6L),
                    // Timers stay armed so a release restores them; the store's due-timer query is what stops them firing.
                    () -> assertThat(record.envelope().timers()).hasSize(1)
            );
        }

        @Test
        void a_different_input_failing_starts_the_budget_over_rather_than_inheriting_the_previous_one() {
            SagaEnvelope<OrderState> failing = withFailure(SagaStatus.ACTIVE, failedOn(7, NOW.minus(Duration.ofHours(1))));

            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", failing, at(8), new IllegalStateException("boom"), NOW, BUDGET);

            assertAll(
                    () -> assertThat(record.quarantined()).isFalse(),
                    () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(NOW),
                    () -> assertThat(record.envelope().failure().position()).isEqualTo(8)
            );
        }

        @Test
        void an_event_carrying_no_position_is_never_quarantined_because_nothing_could_replay_it() {
            SagaEnvelope<OrderState> failing = withFailure(SagaStatus.ACTIVE, failedOn(7, NOW.minus(Duration.ofHours(1))));

            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", failing, new EventMeta("o1", 7L, null), new IllegalStateException("boom"), NOW, BUDGET);

            assertThat(record).isNull();
        }
    }

    @Nested
    class AnInstanceThatFailedBeforeItStarted {

        @Test
        void gets_an_envelope_inserted_that_says_it_never_started() {
            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(
                    saga(), "o1", null, at(1), new IllegalStateException("boom"), NOW, BUDGET);

            assertAll(
                    () -> assertThat(record.expectedVersion()).isEqualTo(0),
                    () -> assertThat(record.envelope().version()).isEqualTo(1),
                    () -> assertThat(record.envelope().started()).isFalse(),
                    () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(record.envelope().failure().position()).isEqualTo(1)
            );
        }

        @Test
        void still_runs_onStart_when_its_start_event_comes_back_because_start_detection_reads_the_marker() {
            SagaEnvelope<OrderState> quarantineOnly = new SagaEnvelope<>("o1", null, SagaStatus.ACTIVE, 1, List.of(), Map.of(),
                    null, NOW, NOW, null, null, false, failedOn(1, NOW));

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", quarantineOnly, SagaInput.event(new OrderPlaced("o1")), at(1), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    // The ShipOrder command only comes from onStart, so its presence is what proves onStart ran.
                    () -> assertThat(outcome.commands()).containsExactly(new ShipOrder("o1")),
                    () -> assertThat(outcome.envelope().started()).isTrue(),
                    () -> assertThat(outcome.envelope().failure()).isNull(),
                    // Written against the existing document rather than inserted a second time.
                    () -> assertThat(outcome.expectedVersion()).isEqualTo(1)
            );
        }
    }

    @Nested
    class AQuarantinedInstance {

        @Test
        void skips_every_event_addressed_to_it() {
            SagaEnvelope<OrderState> quarantined = withFailure(SagaStatus.QUARANTINED, failedOn(7, NOW));

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", quarantined, SagaInput.event(new PaymentReserved("o1")), at(9), NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void skips_a_due_timer() {
            SagaEnvelope<OrderState> quarantined = withFailure(SagaStatus.QUARANTINED, failedOn(7, NOW));

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", quarantined, SagaInput.timeout(new SagaTimeout("o1", TimerName.parse(TIMER))), EventMeta.NONE, NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void is_not_reported_as_completed_so_a_caller_can_tell_the_two_apart() {
            SagaEnvelope<OrderState> quarantined = withFailure(SagaStatus.QUARANTINED, failedOn(7, NOW));

            assertAll(
                    () -> assertThat(quarantined.isQuarantined()).isTrue(),
                    () -> assertThat(quarantined.isCompleted()).isFalse()
            );
        }
    }

    @Nested
    class AReleasedInstance {

        // The watermarks sit well below the recorded position on purpose. Put them right underneath it and an event
        // between the two is skipped as a redelivery whatever the release gate does, so the test would pass with the
        // gate removed.
        private SagaEnvelope<OrderState> released() {
            SagaEnvelope<OrderState> quarantined = new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.QUARANTINED, 4,
                    List.of(new TimerEntry(TIMER, NOW.toEpochMilli())), Map.of("o1", 4L), 4L, NOW.minusSeconds(60), NOW, null,
                    null, true, failedOn(9, NOW.minusSeconds(600)));
            return SagaExecutionSupport.onRelease(quarantined, NOW).envelope();
        }

        @Test
        void stays_quarantined_until_the_replay_reaches_the_position_it_stopped_at() {
            // Position 6 is past this instance's watermarks, so nothing but the release gate can be what skips it.
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", released(), SagaInput.event(new PaymentReserved("o1")), at(6), NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void skips_an_event_past_the_position_it_stopped_at_rather_than_resuming_across_the_gap() {
            // A release marks the instance before the subscription is paused, so a live event can arrive in that window
            // sitting past the recorded position without being the replay. Opening on it would leave a gap in the state.
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", released(), SagaInput.event(new PaymentReserved("o1")), at(11), NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void folds_the_event_it_stopped_on_and_clears_the_record() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", released(), SagaInput.event(new PaymentReserved("o1")), at(9), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().failure()).isNull(),
                    () -> assertThat(outcome.envelope().state()).isEqualTo(new Paid("o1")),
                    () -> assertThat(outcome.envelope().status()).isEqualTo(SagaStatus.COMPLETED)
            );
        }

        @Test
        void is_marked_released_without_leaving_quarantine_or_losing_where_it_stopped() {
            SagaEnvelope<OrderState> envelope = released();

            assertAll(
                    () -> assertThat(envelope.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(envelope.failure().isReleased()).isTrue(),
                    () -> assertThat(envelope.failure().releasedAt()).isEqualTo(NOW),
                    () -> assertThat(envelope.failure().position()).isEqualTo(9)
            );
        }

        @Test
        void goes_back_to_unreleased_when_the_replay_could_not_be_started() {
            FailureRecord<OrderState> reverted = SagaExecutionSupport.onReleaseUndone(released(), NOW);

            assertAll(
                    () -> assertThat(reverted.envelope().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(reverted.envelope().failure().isReleased()).isFalse(),
                    () -> assertThat(reverted.envelope().failure().position()).isEqualTo(9)
            );
        }

        @Test
        void can_be_released_again_so_a_replay_that_never_started_has_a_way_back() {
            FailureRecord<OrderState> second = SagaExecutionSupport.onRelease(released(), NOW.plusSeconds(60));

            assertAll(
                    () -> assertThat(second).isNotNull(),
                    () -> assertThat(second.envelope().failure().releasedAt()).isEqualTo(NOW.plusSeconds(60)),
                    () -> assertThat(second.envelope().failure().position()).isEqualTo(9)
            );
        }
    }

    @Nested
    class AnActiveInstance {

        @Test
        void has_nothing_to_release() {
            assertThat(SagaExecutionSupport.onRelease(active(3, Map.of(), null), NOW)).isNull();
        }

        @Test
        void clears_a_failure_record_as_soon_as_any_input_gets_through() {
            SagaEnvelope<OrderState> failing = withFailure(SagaStatus.ACTIVE, failedOn(7, NOW.minusSeconds(30)));

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga(), "o1", failing, SagaInput.event(new PaymentReserved("o1")), at(7), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().failure()).isNull()
            );
        }
    }
}
