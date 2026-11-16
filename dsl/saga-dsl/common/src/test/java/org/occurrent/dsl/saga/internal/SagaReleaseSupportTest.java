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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("Recording what a quarantined saga instance skips and releasing it")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseSupportTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant QUARANTINED_AT = NOW.minus(Duration.ofHours(1));
    private static final Duration BUDGET = Duration.ofMinutes(5);
    private static final String TIMER = "payment";
    private static final String QUARANTINE_ID = "q1";

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
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.startTimeout(TIMER, Duration.ofMinutes(5))))
                .evolve(PaymentReserved.class, (state, e) -> new Paid(e.orderId()))
                .isTerminal(state -> state instanceof Paid)
                .build();
    }

    private static EventMeta at(long streamVersion) {
        return new EventMeta("o1", streamVersion, streamVersion);
    }

    private static SagaFailure failedOn(long streamVersion) {
        return new SagaFailure("o1@" + streamVersion, streamVersion, QUARANTINED_AT, IllegalStateException.class.getName(), "boom", null, QUARANTINE_ID);
    }

    private static SagaFailure releasingOn(long streamVersion) {
        return new SagaFailure("o1@" + streamVersion, streamVersion, NOW, IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
    }

    private static SagaFailure failedBy034(long streamVersion) {
        return new SagaFailure("o1@" + streamVersion, streamVersion, QUARANTINED_AT, IllegalStateException.class.getName(), "boom");
    }

    // Applied up to version 6 of stream o1, with version 9 failing since it was quarantined
    private static SagaEnvelope<OrderState> quarantined(SagaFailure failure, Long skipCount) {
        return instance(SagaStatus.QUARANTINED, true, failure, skipCount);
    }

    private static SagaEnvelope<OrderState> instance(SagaStatus status, boolean started, SagaFailure failure, Long skipCount) {
        return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), status, 4, List.of(new TimerEntry(TIMER, NOW.toEpochMilli())),
                Map.of("o1", 6L), 6L, NOW.minusSeconds(7200), QUARANTINED_AT, null, null, started, failure,
                Map.of("o1", new StreamMarks(6, Map.of(), Map.of("", List.of(9L)), List.of(), Map.of(9L, QUARANTINED_AT))),
                skipCount);
    }

    private static Outcome<OrderState, OrderCommand> deliver(SagaEnvelope<OrderState> current, SagaInput<OrderEvent> input, EventMeta meta) {
        return SagaExecutionSupport.process(saga(), "o1", current, input, meta, NOW, SagaExecutionSupport.DEFAULT_REORDER_WINDOW);
    }

    private static Outcome<OrderState, OrderCommand> deliver(SagaEnvelope<OrderState> current, EventMeta meta) {
        return deliver(current, SagaInput.event(new PaymentReserved("o1")), meta);
    }

    @Nested
    class AQuarantinedInstanceSkippingAnInput {

        @Test
        void asks_to_record_its_stream_key_under_the_skip_count_and_writes_nothing_itself() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 4L);

            Outcome<OrderState, OrderCommand> outcome = deliver(current, at(11));

            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull(),
                    () -> assertThat(outcome.skipToRecord()).isEqualTo(new SkippedInput(4, "o1@11"))
            );
        }

        @Test
        void asks_to_record_an_event_without_a_stream_version_by_its_cloud_event_id_and_source() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 0L);

            Outcome<OrderState, OrderCommand> outcome = deliver(current, new EventMeta(null, null, 40L, null, "e:1", "urn:orders"));

            assertThat(outcome.skipToRecord()).isEqualTo(new SkippedInput(0, SkippedInputKey.cloudEventKey("e:1", "urn:orders")));
        }

        @Test
        void asks_to_record_it_too_when_it_was_quarantined_on_its_start_event_and_never_started() {
            SagaEnvelope<OrderState> current = instance(SagaStatus.QUARANTINED, false, failedOn(1), 2L);

            Outcome<OrderState, OrderCommand> outcome = deliver(current, at(11));

            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.skipToRecord()).isEqualTo(new SkippedInput(2, "o1@11"))
            );
        }

        @Test
        void takes_the_next_number_once_the_first_write_of_a_skip_has_landed() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 4L);

            SagaEnvelope<OrderState> counted = SagaExecutionSupport.withSkipCounted(current);

            assertAll(
                    () -> assertThat(counted.quarantineSkipCount()).isEqualTo(5L),
                    () -> assertThat(counted.version()).isEqualTo(current.version() + 1),
                    () -> assertThat(deliver(counted, at(12)).skipToRecord()).isEqualTo(new SkippedInput(5, "o1@12"))
            );
        }

        @Test
        void changes_only_the_version_and_the_count_in_the_first_write_and_only_the_version_in_the_last() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 4L);

            SagaEnvelope<OrderState> counted = SagaExecutionSupport.withSkipCounted(current);
            SagaEnvelope<OrderState> finished = SagaExecutionSupport.withSkipFinished(counted);

            assertAll(
                    () -> assertThat(finished.version()).isEqualTo(current.version() + 2),
                    () -> assertThat(finished.quarantineSkipCount()).isEqualTo(5L),
                    () -> assertThat(finished).usingRecursiveComparison().ignoringFields("version", "quarantineSkipCount", "updatedAt", "positionWatermark").isEqualTo(current),
                    () -> assertThat(counted).usingRecursiveComparison().ignoringFields("version", "quarantineSkipCount", "updatedAt", "positionWatermark").isEqualTo(current)
            );
        }

        @Test
        void asks_for_nothing_for_a_timer() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 0L);

            Outcome<OrderState, OrderCommand> outcome = deliver(current, SagaInput.timeout(new SagaTimeout("o1", TimerName.parse(TIMER))), EventMeta.NONE);

            assertAll(() -> assertThat(outcome.envelope()).isNull(), () -> assertThat(outcome.skipToRecord()).isNull());
        }

        @Test
        void asks_for_nothing_for_an_event_with_neither_a_stream_version_nor_an_id_and_source() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 0L);

            Outcome<OrderState, OrderCommand> outcome = deliver(current, new EventMeta(null, null, 40L));

            assertThat(outcome.skipToRecord()).isNull();
        }

        @Test
        void asks_for_nothing_when_it_was_quarantined_by_0_34_0_and_has_no_quarantine_id() {
            SagaEnvelope<OrderState> current = quarantined(failedBy034(9), null);

            Outcome<OrderState, OrderCommand> outcome = deliver(current, at(11));

            assertAll(() -> assertThat(outcome.processed()).isFalse(), () -> assertThat(outcome.envelope()).isNull(), () -> assertThat(outcome.skipToRecord()).isNull());
        }

        @Test
        void asks_for_nothing_when_it_has_a_quarantine_id_but_no_skip_count() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), null);

            assertThat(deliver(current, at(11)).skipToRecord()).isNull();
        }

        @Test
        void asks_for_nothing_for_an_input_it_applied_before_it_was_quarantined() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 0L);

            assertThat(deliver(current, at(5)).skipToRecord()).isNull();
        }

        @Test
        void asks_for_nothing_for_the_input_it_was_quarantined_on() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 0L);

            assertThat(deliver(current, at(9)).skipToRecord()).isNull();
        }
    }

    @Nested
    class TheWriteThatQuarantines {

        private final SagaEnvelope<OrderState> failing = instance(SagaStatus.ACTIVE, true,
                new SagaFailure("o1@9", 9L, NOW.minus(BUDGET).minusSeconds(1), IllegalStateException.class.getName(), "boom"), null);

        private FailureRecord<OrderState> quarantine() {
            return SagaExecutionSupport.onFailure(saga(), "o1", failing, at(9), new IllegalStateException("boom"), NOW, BUDGET);
        }

        @Test
        void sets_the_skip_count_to_0() {
            FailureRecord<OrderState> record = quarantine();

            assertAll(
                    () -> assertThat(record.quarantined()).isTrue(),
                    () -> assertThat(record.envelope().quarantineSkipCount()).isZero()
            );
        }

        @Test
        void gives_the_failure_record_a_quarantine_id() {
            assertThat(quarantine().envelope().quarantineId()).isNotBlank();
        }

        @Test
        void sets_the_skip_count_to_0_and_an_id_on_the_instance_it_inserts_when_the_first_event_is_the_one_quarantined() {
            SagaEnvelope<OrderState> none = null;

            FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(saga(), "o1", none, at(1), new IllegalStateException("boom"), NOW, Duration.ZERO);

            assertAll(
                    () -> assertThat(record.quarantined()).isTrue(),
                    () -> assertThat(record.envelope().quarantineSkipCount()).isZero(),
                    () -> assertThat(record.envelope().quarantineId()).isNotBlank()
            );
        }

        @Test
        void gives_each_quarantine_an_id_of_its_own() {
            assertThat(quarantine().envelope().quarantineId()).isNotEqualTo(quarantine().envelope().quarantineId());
        }
    }

    @Nested
    class StartingARelease {

        @Test
        void sets_released_at_and_restarts_the_budget_of_the_input_it_was_quarantined_on() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 2L);

            SagaEnvelope<OrderState> releasing = SagaExecutionSupport.startRelease(current, NOW);

            assertAll(
                    () -> assertThat(releasing.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(releasing.isReleasing()).isTrue(),
                    () -> assertThat(releasing.failure().releasedAt()).isEqualTo(NOW),
                    () -> assertThat(releasing.failure().firstFailedAt()).isEqualTo(NOW),
                    () -> assertThat(releasing.failure().input()).isEqualTo("o1@9"),
                    () -> assertThat(releasing.failure().position()).isEqualTo(9),
                    () -> assertThat(releasing.streamMarks().get("o1").pendingSince()).containsEntry(9L, NOW)
            );
        }

        @Test
        void keeps_the_quarantine_id_and_the_skip_count() {
            SagaEnvelope<OrderState> releasing = SagaExecutionSupport.startRelease(quarantined(failedOn(9), 2L), NOW);

            assertAll(
                    () -> assertThat(releasing.quarantineId()).isEqualTo(QUARANTINE_ID),
                    () -> assertThat(releasing.quarantineSkipCount()).isEqualTo(2L)
            );
        }

        @Test
        void records_the_input_it_was_quarantined_on_before_every_other_record() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 0L);

            SkippedInput head = SagaExecutionSupport.releaseHead(current);

            assertAll(
                    () -> assertThat(head).isEqualTo(new SkippedInput(-1, "o1@9")),
                    () -> assertThat(head.seq()).isLessThan(deliver(current, at(11)).skipToRecord().seq())
            );
        }

        @Test
        void returns_nothing_for_an_instance_that_is_releasing_already() {
            assertThat(SagaExecutionSupport.startRelease(quarantined(releasingOn(9), 2L), NOW.plusSeconds(1))).isNull();
        }

        @Test
        void refuses_an_instance_that_is_not_quarantined() {
            SagaEnvelope<OrderState> current = instance(SagaStatus.ACTIVE, true, null, null);

            assertThatThrownBy(() -> SagaExecutionSupport.startRelease(current, NOW))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not QUARANTINED");
        }

        @Test
        void refuses_an_instance_quarantined_by_0_34_0_and_names_delete_as_the_only_way_out() {
            SagaEnvelope<OrderState> current = quarantined(failedBy034(9), null);

            assertThatThrownBy(() -> SagaExecutionSupport.startRelease(current, NOW))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("quarantined by 0.34.0")
                    .hasMessageContaining("SagaStateStore.delete(\"o1\")")
                    .hasMessageNotContaining("restart");
        }
    }

    @Nested
    class ReplayingARecord {

        private Outcome<OrderState, OrderCommand> replay(SagaEnvelope<OrderState> current, SkippedInput record, OrderEvent event, EventMeta meta) {
            return SagaExecutionSupport.replay(saga(), "o1", current, record, SagaInput.event(event), meta, NOW, SagaExecutionSupport.DEFAULT_REORDER_WINDOW);
        }

        @Test
        void applies_it_and_keeps_the_instance_quarantined_and_releasing_with_its_id_and_count() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);

            Outcome<OrderState, OrderCommand> outcome = replay(current, new SkippedInput(0, "o1@7"), new OrderPlaced("o1"), at(7));

            SagaEnvelope<OrderState> next = outcome.envelope();
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(next.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(next.failure()).isEqualTo(current.failure()),
                    () -> assertThat(next.quarantineSkipCount()).isEqualTo(3L),
                    () -> assertThat(next.streamWatermarks()).containsEntry("o1", 7L)
            );
        }

        @Test
        void ends_the_release_with_the_instance_when_the_record_completes_it() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);

            Outcome<OrderState, OrderCommand> outcome = replay(current, new SkippedInput(0, "o1@7"), new PaymentReserved("o1"), at(7));

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(outcome.envelope().failure()).isNull()
            );
        }

        @Test
        void applies_a_record_with_a_cloud_event_key_by_the_stream_key_of_the_event_read_back() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);
            SkippedInput record = new SkippedInput(0, SkippedInputKey.cloudEventKey("e:1", "urn:orders"));

            Outcome<OrderState, OrderCommand> outcome = replay(current, record, new OrderPlaced("o1"), at(7));

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().streamWatermarks()).containsEntry("o1", 7L)
            );
        }

        @Test
        void skips_a_record_whose_event_the_instance_applied_already_without_writing_the_instance() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);

            Outcome<OrderState, OrderCommand> outcome = replay(current, new SkippedInput(0, "o1@5"), new PaymentReserved("o1"), at(5));

            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull()
            );
        }

        @Test
        void skips_a_cloud_event_record_whose_event_the_instance_applied_already() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);
            SkippedInput record = new SkippedInput(0, SkippedInputKey.cloudEventKey("e:1", "urn:orders"));

            Outcome<OrderState, OrderCommand> outcome = replay(current, record, new PaymentReserved("o1"), at(5));

            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull()
            );
        }

        @Test
        void skips_a_record_that_is_not_a_start_event_on_an_instance_that_never_started() {
            SagaEnvelope<OrderState> current = instance(SagaStatus.QUARANTINED, false, releasingOn(1), 3L);

            Outcome<OrderState, OrderCommand> outcome = replay(current, new SkippedInput(0, "o1@7"), new PaymentReserved("o1"), at(7));

            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull()
            );
        }

        @Test
        void refuses_an_event_read_back_without_a_stream_id_and_version() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);

            assertThatThrownBy(() -> replay(current, new SkippedInput(0, "o1@7"), new OrderPlaced("o1"), new EventMeta(null, null, 7L)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no stream id and stream version");
        }

        @Test
        void refuses_an_instance_that_is_not_releasing() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 3L);

            assertThatThrownBy(() -> replay(current, new SkippedInput(0, "o1@7"), new OrderPlaced("o1"), at(7))).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class ReleasingTheInstance {

        @Test
        void makes_it_active_and_clears_the_failure_record_the_quarantine_id_and_the_skip_count() {
            SagaEnvelope<OrderState> current = quarantined(releasingOn(9), 3L);

            SagaEnvelope<OrderState> released = SagaExecutionSupport.withReleased(current, NOW);

            assertAll(
                    () -> assertThat(released.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(released.failure()).isNull(),
                    () -> assertThat(released.quarantineId()).isNull(),
                    () -> assertThat(released.quarantineSkipCount()).isNull(),
                    () -> assertThat(released.version()).isEqualTo(current.version() + 1)
            );
        }

        @Test
        void refuses_an_instance_that_is_not_releasing() {
            assertThatThrownBy(() -> SagaExecutionSupport.withReleased(quarantined(failedOn(9), 3L), NOW)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class ALostWriteDuringARelease {

        private SagaEnvelope<OrderState> withState(SagaEnvelope<OrderState> from, OrderState state, Map<String, Long> watermarks) {
            return new SagaEnvelope<>("o1", state, from.status(), from.version(), from.timers(), watermarks, null, from.createdAt(),
                    from.updatedAt(), null, null, true, from.failure(), from.streamMarks(), from.quarantineSkipCount());
        }

        @Test
        void is_an_append_when_only_the_version_and_the_skip_count_moved() {
            SagaEnvelope<OrderState> before = quarantined(releasingOn(9), 3L);
            SagaEnvelope<OrderState> after = SagaExecutionSupport.withSkipFinished(SagaExecutionSupport.withSkipCounted(before));

            assertThat(SagaExecutionSupport.differsOnlyByRecordedSkips(before, after)).isTrue();
        }

        @Test
        void is_not_an_append_when_nothing_moved() {
            SagaEnvelope<OrderState> before = quarantined(releasingOn(9), 3L);

            assertThat(SagaExecutionSupport.differsOnlyByRecordedSkips(before, before)).isFalse();
        }

        @Test
        void is_not_an_append_when_the_state_moved_as_well() {
            SagaEnvelope<OrderState> before = quarantined(releasingOn(9), 3L);
            SagaEnvelope<OrderState> counted = SagaExecutionSupport.withSkipCounted(before);

            assertThat(SagaExecutionSupport.differsOnlyByRecordedSkips(before, withState(counted, new Paid("o1"), counted.streamWatermarks()))).isFalse();
        }

        @Test
        void is_not_an_append_when_the_watermarks_moved_as_well() {
            SagaEnvelope<OrderState> before = quarantined(releasingOn(9), 3L);
            SagaEnvelope<OrderState> counted = SagaExecutionSupport.withSkipCounted(before);

            assertThat(SagaExecutionSupport.differsOnlyByRecordedSkips(before, withState(counted, counted.state(), Map.of("o1", 7L)))).isFalse();
        }

        @Test
        void is_rewritten_one_version_above_the_instance_read_again_with_its_skip_count() {
            SagaEnvelope<OrderState> before = quarantined(releasingOn(9), 3L);
            SagaEnvelope<OrderState> after = SagaExecutionSupport.withSkipFinished(SagaExecutionSupport.withSkipCounted(before));
            SagaEnvelope<OrderState> computed = SagaExecutionSupport.replay(saga(), "o1", before, new SkippedInput(0, "o1@7"),
                    SagaInput.event(new OrderPlaced("o1")), at(7), NOW, SagaExecutionSupport.DEFAULT_REORDER_WINDOW).envelope();

            SagaEnvelope<OrderState> rebased = SagaExecutionSupport.rebased(computed, after);

            assertAll(
                    () -> assertThat(rebased.version()).isEqualTo(after.version() + 1),
                    () -> assertThat(rebased.quarantineSkipCount()).isEqualTo(after.quarantineSkipCount()),
                    () -> assertThat(rebased.streamWatermarks()).isEqualTo(computed.streamWatermarks())
            );
        }

        static Stream<Arguments> fieldsThatMoveAlone() {
            return Stream.of(
                    Arguments.of("status", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), SagaStatus.ACTIVE, a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt(), a.completedAt(), a.currentStep(), a.started(), a.failure(), a.streamMarks(), a.quarantineSkipCount())),
                    Arguments.of("timers", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), List.of(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt(), a.completedAt(), a.currentStep(), a.started(), a.failure(), a.streamMarks(), a.quarantineSkipCount())),
                    Arguments.of("stream marks", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt(), a.completedAt(), a.currentStep(), a.started(), a.failure(), Map.of("o1", new StreamMarks(7, Map.of())), a.quarantineSkipCount())),
                    Arguments.of("createdAt", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt().plusSeconds(1), a.updatedAt(), a.completedAt(), a.currentStep(), a.started(), a.failure(), a.streamMarks(), a.quarantineSkipCount())),
                    Arguments.of("updatedAt", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt().plusSeconds(1), a.completedAt(), a.currentStep(), a.started(), a.failure(), a.streamMarks(), a.quarantineSkipCount())),
                    Arguments.of("completedAt", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt(), NOW, a.currentStep(), a.started(), a.failure(), a.streamMarks(), a.quarantineSkipCount())),
                    Arguments.of("started", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt(), a.completedAt(), a.currentStep(), false, a.failure(), a.streamMarks(), a.quarantineSkipCount())),
                    Arguments.of("failure", (UnaryOperator<SagaEnvelope<OrderState>>) a -> new SagaEnvelope<>(a.sagaId(), a.state(), a.status(), a.version(), a.timers(),
                            a.streamWatermarks(), null, a.createdAt(), a.updatedAt(), a.completedAt(), a.currentStep(), a.started(), failedOn(9), a.streamMarks(), a.quarantineSkipCount()))
            );
        }

        @ParameterizedTest(name = "is not an append when only the {0} moved as well")
        @MethodSource("fieldsThatMoveAlone")
        void is_not_an_append_when_one_more_field_moved_as_well(String field, UnaryOperator<SagaEnvelope<OrderState>> move) {
            SagaEnvelope<OrderState> before = quarantined(releasingOn(9), 3L);
            SagaEnvelope<OrderState> counted = SagaExecutionSupport.withSkipFinished(SagaExecutionSupport.withSkipCounted(before));

            assertAll(
                    () -> assertThat(SagaExecutionSupport.differsOnlyByRecordedSkips(before, counted)).as("control: nothing but the skip moved").isTrue(),
                    () -> assertThat(SagaExecutionSupport.differsOnlyByRecordedSkips(before, move.apply(counted))).as(field).isFalse()
            );
        }
    }

    @Nested
    class AReleaseThatFailsOnARecord {

        private FailureRecord<OrderState> failOn(SagaEnvelope<OrderState> current, EventMeta meta) {
            return SagaExecutionSupport.onReleaseFailure(current, "o1@9", meta, new IllegalStateException("again"), NOW, BUDGET,
                    SagaExecutionSupport.DEFAULT_REORDER_WINDOW);
        }

        @Test
        void keeps_the_quarantine_id_and_the_skip_count_when_its_budget_runs_out() {
            SagaEnvelope<OrderState> releasing = quarantined(releasingOn(9), 3L);
            SagaEnvelope<OrderState> expired = new SagaEnvelope<>("o1", releasing.state(), releasing.status(), releasing.version(), releasing.timers(),
                    releasing.streamWatermarks(), null, releasing.createdAt(), releasing.updatedAt(), null, null, true, releasing.failure(),
                    Map.of("o1", new StreamMarks(6, Map.of(), Map.of("", List.of(9L)), List.of(), Map.of(9L, NOW.minus(BUDGET).minusSeconds(1)))),
                    3L);

            FailureRecord<OrderState> record = failOn(expired, at(9));

            assertAll(
                    () -> assertThat(record.quarantined()).isTrue(),
                    () -> assertThat(record.envelope().isReleasing()).isFalse(),
                    () -> assertThat(record.envelope().quarantineId()).isEqualTo(QUARANTINE_ID),
                    () -> assertThat(record.envelope().quarantineSkipCount()).isEqualTo(3L)
            );
        }

        @Test
        void throws_when_the_event_has_no_stream_id_and_version_since_a_release_never_replays_one() {
            SagaEnvelope<OrderState> releasing = quarantined(releasingOn(9), 3L);

            assertThatThrownBy(() -> failOn(releasing, new EventMeta(null, null, 40L)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no stream id and stream version");
        }
    }

    @Nested
    class AReleaseThatCannotReplayARecord {

        @Test
        void quarantines_again_on_a_missing_event_keeping_the_id_and_the_count_and_naming_the_key() {
            SagaEnvelope<OrderState> releasing = quarantined(releasingOn(9), 3L);

            SagaEnvelope<OrderState> again = SagaExecutionSupport.withMissingInputQuarantined(releasing, "o1@7", NOW);

            assertAll(
                    () -> assertThat(again.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(again.isReleasing()).isFalse(),
                    () -> assertThat(again.failure().input()).isEqualTo("o1@7"),
                    () -> assertThat(again.failure().failureType()).isEqualTo(SagaExecutionSupport.MISSING_EVENT_FAILURE_TYPE),
                    () -> assertThat(again.quarantineId()).isEqualTo(QUARANTINE_ID),
                    () -> assertThat(again.quarantineSkipCount()).isEqualTo(3L)
            );
        }

        @Test
        void quarantines_again_on_an_event_without_a_stream_key_with_a_failure_type_of_its_own() {
            SagaEnvelope<OrderState> releasing = quarantined(releasingOn(9), 3L);

            SagaEnvelope<OrderState> again = SagaExecutionSupport.withInputWithoutStreamKeyQuarantined(releasing, "o1@7", NOW);

            assertAll(
                    () -> assertThat(again.failure().failureType()).isEqualTo("EventWithoutStreamKey"),
                    () -> assertThat(again.failure().input()).isEqualTo("o1@7"),
                    () -> assertThat(again.quarantineId()).isEqualTo(QUARANTINE_ID),
                    () -> assertThat(again.quarantineSkipCount()).isEqualTo(3L)
            );
        }

        @Test
        void keeps_the_position_of_the_input_it_was_quarantined_on_but_not_of_a_recorded_one() {
            SagaEnvelope<OrderState> releasing = quarantined(releasingOn(9), 3L);

            assertAll(
                    () -> assertThat(SagaExecutionSupport.withMissingInputQuarantined(releasing, "o1@9", NOW).failure().position()).isEqualTo(9L),
                    () -> assertThat(SagaExecutionSupport.withMissingInputQuarantined(releasing, "o1@7", NOW).failure().position()).isNull()
            );
        }

        @Test
        void refuses_an_instance_that_is_not_releasing() {
            SagaEnvelope<OrderState> current = quarantined(failedOn(9), 3L);

            assertThatThrownBy(() -> SagaExecutionSupport.withMissingInputQuarantined(current, "o1@7", NOW)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class ClassifyingAnInputTheFailureRecordNames {

        private static final String TYPE = "org.example.PaymentReserved";

        // Version 10 of stream S is below the type's mark of 310 by more than the reorder window and not among the applied
        // versions, so the stream marks have forgotten it. Only a failure record naming it can say it was never applied
        private SagaEnvelope<OrderState> withFailureOn(SagaStatus status, SagaFailure failure) {
            StreamMarks marks = new StreamMarks(5, Map.of(TYPE, new TypeMark(310, List.of(), 0)));
            return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), status, 4, List.of(), Map.of("S", 5L), null, NOW.minusSeconds(7200), QUARANTINED_AT,
                    null, null, true, failure, Map.of("S", marks), 0L);
        }

        private EventMeta s10() {
            return new EventMeta("S", 10L, 10L, TYPE);
        }

        private SagaFailure failureOnS10(@Nullable Instant releasedAt) {
            return new SagaFailure("S@10", 10L, QUARANTINED_AT, IllegalStateException.class.getName(), "boom", releasedAt, QUARANTINE_ID);
        }

        @Test
        void is_not_new_while_the_instance_is_releasing_since_the_stream_marks_decide_alone() {
            SagaEnvelope<OrderState> releasing = withFailureOn(SagaStatus.QUARANTINED, failureOnS10(NOW));

            assertAll(
                    () -> assertThat(releasing.isReleasing()).as("control: releasing").isTrue(),
                    () -> assertThat(SagaExecutionSupport.classify(releasing, s10(), SagaExecutionSupport.DEFAULT_REORDER_WINDOW)).isEqualTo(SagaExecutionSupport.Delivery.BEHIND_WINDOW)
            );
        }

        @Test
        void is_not_new_while_the_instance_is_releasing_when_it_is_at_or_below_the_floor_too_and_is_new_as_a_live_delivery() {
            SagaFailure releasingOnS3 = new SagaFailure("S@3", 3L, QUARANTINED_AT, IllegalStateException.class.getName(), "boom", NOW, QUARANTINE_ID);
            SagaFailure notReleasingOnS3 = new SagaFailure("S@3", 3L, QUARANTINED_AT, IllegalStateException.class.getName(), "boom", null, QUARANTINE_ID);
            EventMeta s3 = new EventMeta("S", 3L, 3L, TYPE);

            assertAll(
                    () -> assertThat(SagaExecutionSupport.classify(withFailureOn(SagaStatus.QUARANTINED, releasingOnS3), s3, SagaExecutionSupport.DEFAULT_REORDER_WINDOW))
                            .as("releasing").isEqualTo(SagaExecutionSupport.Delivery.REDELIVERY),
                    () -> assertThat(SagaExecutionSupport.classify(withFailureOn(SagaStatus.ACTIVE, notReleasingOnS3), s3, SagaExecutionSupport.DEFAULT_REORDER_WINDOW))
                            .as("not releasing").isEqualTo(SagaExecutionSupport.Delivery.NEW)
            );
        }

        @Test
        void is_not_applied_again_by_a_release_replaying_it() {
            SagaEnvelope<OrderState> releasing = withFailureOn(SagaStatus.QUARANTINED, failureOnS10(NOW));

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.replay(saga(), "o1", releasing, new SkippedInput(0, "S@10"),
                    SagaInput.event(new PaymentReserved("o1")), s10(), NOW, SagaExecutionSupport.DEFAULT_REORDER_WINDOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isFalse(),
                    () -> assertThat(outcome.envelope()).isNull()
            );
        }

        @Test
        void is_new_as_a_live_delivery_when_the_failure_record_names_it_and_no_release_is_running() {
            SagaEnvelope<OrderState> notReleasing = withFailureOn(SagaStatus.ACTIVE, failureOnS10(null));

            assertAll(
                    () -> assertThat(notReleasing.isReleasing()).as("control: not releasing").isFalse(),
                    () -> assertThat(SagaExecutionSupport.classify(notReleasing, s10(), SagaExecutionSupport.DEFAULT_REORDER_WINDOW)).isEqualTo(SagaExecutionSupport.Delivery.NEW),
                    () -> assertThat(deliver(notReleasing, SagaInput.event(new PaymentReserved("o1")), s10()).processed()).isTrue()
            );
        }
    }
}
