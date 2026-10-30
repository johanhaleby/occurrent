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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Delivery;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.FailureRecord;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SagaExecutionSupport")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaExecutionSupportTest {

    // --- A tiny order/payment domain, used across all nested test classes below ---

    private static final String PAYMENT_TIMER = "payment";
    private static final String RETRY_TIMER = "retry";
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved, PaymentFailed {
        String orderId();
    }

    record OrderPlaced(String orderId, int amount) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    record PaymentFailed(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ReservePayment, NotifyWarehouse, ShipOrder, CancelOrder {
    }

    record ReservePayment(String orderId, int amount) implements OrderCommand {
    }

    record NotifyWarehouse(String orderId) implements OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Paid, Cancelled {
        String orderId();
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Paid(String orderId) implements OrderState {
    }

    record Cancelled(String orderId) implements OrderState {
    }

    /**
     * The canonical saga used by most tests below: {@code onStart} reserves payment, {@code react} on the same start
     * event notifies the warehouse and arms the payment timer; a reservation ships and cancels the timer, a failure or
     * timeout cancels the order.
     */
    private static Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .onStart((state, e) -> List.of(SagaEffect.issue(new ReservePayment(e.orderId(), ((OrderPlaced) e).amount()))))
                .react(OrderPlaced.class, (state, e) -> List.of(
                        SagaEffect.issue(new NotifyWarehouse(e.orderId())),
                        SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(5))))
                .evolve(PaymentReserved.class, (state, e) -> new Paid(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> List.of(
                        SagaEffect.issue(new ShipOrder(e.orderId())),
                        SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                .evolve(PaymentFailed.class, (state, e) -> new Cancelled(e.orderId()))
                .react(PaymentFailed.class, (state, e) -> List.of(
                        SagaEffect.startTimeout(RETRY_TIMER, Duration.ofMinutes(1)),
                        SagaEffect.issue(new CancelOrder(e.orderId()))))
                .evolveOnTimeout(PAYMENT_TIMER, (state, t) -> new Cancelled(t.sagaId()))
                .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                .isTerminal(state -> state instanceof Paid || state instanceof Cancelled)
                .build();
    }

    private static SagaEnvelope<OrderState> activeEnvelope(String sagaId, OrderState state, long version, List<TimerEntry> timers,
                                                            Map<String, Long> streamWatermarks, Long positionWatermark) {
        return new SagaEnvelope<>(sagaId, state, SagaStatus.ACTIVE, version, timers, streamWatermarks, positionWatermark, NOW, NOW, null, null);
    }

    @Nested
    class StartingANewInstance {

        @Test
        void processes_a_start_event_and_combines_onStart_and_react_commands_in_order() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", null, SagaInput.event(new OrderPlaced("o1", 100)), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().version()).isEqualTo(1),
                    () -> assertThat(outcome.envelope().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(outcome.envelope().state()).isEqualTo(new AwaitingPayment("o1")),
                    () -> assertThat(outcome.commands()).containsExactly(
                            new ReservePayment("o1", 100),
                            new NotifyWarehouse("o1")),
                    () -> assertThat(outcome.envelope().createdAt()).isEqualTo(NOW),
                    () -> assertThat(outcome.expectedVersion()).isEqualTo(0)
            );
        }

        @Test
        void skips_a_non_start_event_when_no_instance_exists() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", null, SagaInput.event(new PaymentReserved("o1")), EventMeta.NONE, NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void skips_a_timeout_when_no_instance_exists() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", null, SagaInput.timeout(new SagaTimeout("o1", TimerName.parse(PAYMENT_TIMER))), EventMeta.NONE, NOW);

            assertThat(outcome.processed()).isFalse();
        }
    }

    @Nested
    class TerminalInstance {

        private SagaEnvelope<OrderState> completed(OrderState state) {
            return new SagaEnvelope<>("o1", state, SagaStatus.COMPLETED, 2, List.of(), Map.of(), null, NOW.minusSeconds(120), NOW.minusSeconds(60), NOW.minusSeconds(60), null);
        }

        @Test
        void skips_any_event_delivered_to_a_completed_instance() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", completed(new Paid("o1")), SagaInput.event(new PaymentReserved("o1")), EventMeta.NONE, NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void skips_any_timeout_delivered_to_a_completed_instance() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", completed(new Cancelled("o1")), SagaInput.timeout(new SagaTimeout("o1", TimerName.parse(PAYMENT_TIMER))), EventMeta.NONE, NOW);

            assertThat(outcome.processed()).isFalse();
        }
    }

    @Nested
    class RedeliveryDedup {

        @Test
        void skips_a_stream_event_at_or_below_the_stored_stream_watermark() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta("s1", 5L, null), NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void processes_a_stream_event_above_the_stored_stream_watermark_and_advances_it() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta("s1", 6L, null), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().streamWatermarks()).containsEntry("s1", 6L)
            );
        }

        @Test
        void processes_a_stream_event_with_a_lower_position_than_an_earlier_one_when_its_stream_version_is_above_the_watermark() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta("s1", 6L, 3L), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().streamWatermarks()).containsEntry("s1", 6L)
            );
        }

        @Test
        void skips_a_stream_event_at_or_below_the_watermark_whatever_its_position_is() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta("s1", 5L, Long.MAX_VALUE), NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void tracks_the_watermark_per_stream_id() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta("s2", 1L, 2L), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().streamWatermarks()).containsEntry("s1", 5L).containsEntry("s2", 1L)
            );
        }

        @Test
        void processes_a_position_only_event_whose_position_is_at_the_stored_position_watermark() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of(), 5L);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta(null, null, 5L), NOW);

            assertThat(outcome.processed()).isTrue();
        }

        @Test
        void does_not_advance_a_position_watermark_for_a_position_only_event() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of(), 5L);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta(null, null, 6L), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().positionWatermark()).isNull()
            );
        }

        @Test
        void a_position_only_event_is_not_a_redelivery_of_anything() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), Long.MAX_VALUE);

            assertThat(SagaExecutionSupport.isRedelivery(current, new EventMeta(null, null, 1L))).isFalse();
        }

        @Test
        void re_processes_an_event_that_carries_no_dedup_key_at_all_because_there_is_nothing_to_compare() {
            // Given an instance that has already recorded a watermark from a properly tagged event
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), null);

            // When the same logical event arrives again with none of the three extensions, as a broker feed that drops
            // them would deliver it
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), EventMeta.NONE, NOW);

            // Then it is processed again, and nothing new is recorded to compare a third delivery against
            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of("s1", 5L)),
                    () -> assertThat(outcome.envelope().positionWatermark()).isNull()
            );
        }

        @Test
        void treats_an_event_without_both_a_stream_id_and_a_version_as_no_dedup_key() {
            assertAll(
                    () -> assertThat(new EventMeta("s1", 5L, null).carriesRedeliveryKey()).isTrue(),
                    () -> assertThat(new EventMeta(null, null, 6L).carriesRedeliveryKey()).isFalse(),
                    () -> assertThat(new EventMeta(null, 5L, 6L).carriesRedeliveryKey()).isFalse(),
                    () -> assertThat(new EventMeta("s1", null, null).carriesRedeliveryKey()).isFalse(),
                    () -> assertThat(EventMeta.NONE.carriesRedeliveryKey()).isFalse()
            );
        }
    }

    @Nested
    class RedeliveryKey {

        @Test
        void is_the_stream_id_with_the_stream_version() {
            assertThat(new EventMeta("s1", 5L, 99L).redeliveryKey()).isEqualTo("s1@5");
        }

        @Test
        void does_not_exist_for_a_position_only_event() {
            assertThat(new EventMeta(null, null, 7L).redeliveryKey()).isNull();
        }

        @Test
        void does_not_exist_for_a_stream_id_without_a_version_whatever_the_position_is() {
            assertThat(new EventMeta("s1", null, 7L).redeliveryKey()).isNull();
        }
    }

    @Nested
    class AStoredPositionWatermark {

        private static SagaEnvelope<OrderState> envelopeWith(Long positionWatermark) {
            return activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of("s1", 5L), positionWatermark);
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(longs = {1L, 7L, Long.MAX_VALUE})
        @DisplayName("does not change whether a stream event is a redelivery")
        void does_not_change_whether_a_stream_event_is_a_redelivery(Long positionWatermark) {
            SagaEnvelope<OrderState> current = envelopeWith(positionWatermark);

            assertAll(
                    () -> assertThat(SagaExecutionSupport.isRedelivery(current, new EventMeta("s1", 5L, 0L))).isTrue(),
                    () -> assertThat(SagaExecutionSupport.isRedelivery(current, new EventMeta("s1", 6L, 0L))).isFalse(),
                    () -> assertThat(SagaExecutionSupport.isRedelivery(current, new EventMeta(null, null, 7L))).isFalse(),
                    () -> assertThat(SagaExecutionSupport.isRedelivery(current, EventMeta.NONE)).isFalse()
            );
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(longs = {1L, 7L, Long.MAX_VALUE})
        @DisplayName("is not written back by process")
        void is_not_written_back_by_process(Long positionWatermark) {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", envelopeWith(positionWatermark), SagaInput.event(new PaymentReserved("o1")), new EventMeta("s1", 6L, 8L), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().positionWatermark()).isNull(),
                    () -> assertThat(outcome.envelope().streamWatermarks()).containsEntry("s1", 6L)
            );
        }

        @ParameterizedTest
        @NullSource
        @ValueSource(longs = {1L, 7L, Long.MAX_VALUE})
        @DisplayName("does not change what process decides about a position-only event")
        void does_not_change_what_process_decides_about_a_position_only_event(Long positionWatermark) {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", envelopeWith(positionWatermark), SagaInput.event(new PaymentReserved("o1")), new EventMeta(null, null, 7L), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).containsExactly(new ShipOrder("o1")),
                    () -> assertThat(outcome.envelope().positionWatermark()).isNull()
            );
        }
    }

    @Nested
    class TimerEffects {

        @Test
        void a_startTimeout_effect_adds_a_timer_entry_resolved_against_now() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", null, SagaInput.event(new OrderPlaced("o1", 100)), EventMeta.NONE, NOW);

            assertThat(outcome.envelope().timers()).containsExactly(new TimerEntry(PAYMENT_TIMER, NOW.plus(Duration.ofMinutes(5)).toEpochMilli()));
        }

        @Test
        void a_cancelTimeout_effect_removes_a_previously_started_timer() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry(PAYMENT_TIMER, NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), EventMeta.NONE, NOW);

            assertThat(outcome.envelope().timers()).isEmpty();
        }

        @Test
        void reaching_a_terminal_state_clears_all_timers_even_one_started_by_the_same_reaction() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry(PAYMENT_TIMER, NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);

            // PaymentFailed's react both starts RETRY_TIMER and issues CancelOrder, but also evolves to the terminal
            // Cancelled state; the terminal clear must win over the just-started timer, not merely leave the
            // inherited PAYMENT_TIMER behind.
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentFailed("o1")), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.envelope().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(outcome.envelope().timers()).isEmpty(),
                    () -> assertThat(outcome.commands()).containsExactly(new CancelOrder("o1"))
            );
        }
    }

    @Nested
    class Versioning {

        @Test
        void a_new_instance_is_saved_at_version_1_with_expectedVersion_0() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", null, SagaInput.event(new OrderPlaced("o1", 100)), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.envelope().version()).isEqualTo(1),
                    () -> assertThat(outcome.expectedVersion()).isEqualTo(0)
            );
        }

        @Test
        void an_existing_instance_increments_its_version_and_expectedVersion_is_the_prior_version() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of(), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.envelope().version()).isEqualTo(2),
                    () -> assertThat(outcome.expectedVersion()).isEqualTo(1)
            );
        }

        @Test
        void a_new_instance_preserves_createdAt_across_subsequent_saves() {
            Outcome<OrderState, OrderCommand> started = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", null, SagaInput.event(new OrderPlaced("o1", 100)), EventMeta.NONE, NOW);
            Instant later = NOW.plusSeconds(60);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", started.envelope(), SagaInput.event(new PaymentReserved("o1")), EventMeta.NONE, later);

            assertAll(
                    () -> assertThat(outcome.envelope().createdAt()).isEqualTo(NOW),
                    () -> assertThat(outcome.envelope().updatedAt()).isEqualTo(later)
            );
        }
    }

    @Nested
    class UnmatchedTimer {

        private ListAppender<ILoggingEvent> appender;
        private ch.qos.logback.classic.Logger logger;

        @BeforeEach
        void attachAppender() {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            logger = context.getLogger(SagaExecutionSupport.class);
            appender = new ListAppender<>();
            appender.start();
            logger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            logger.detachAppender(appender);
        }

        @Test
        void a_fired_timer_with_no_registered_handler_is_consumed_without_effect_and_the_instance_does_not_advance() {
            // "typo" is not registered on the saga: neither evolveOnTimeout nor reactOnTimeout knows it, mimicking a
            // StartTimeout armed under one name and its handler registered under another.
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry("typo", NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.timeout(new SagaTimeout("o1", TimerName.parse("typo"))), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.commands()).isEmpty(),
                    () -> assertThat(outcome.envelope().state()).isEqualTo(new AwaitingPayment("o1")),
                    () -> assertThat(outcome.envelope().timers()).as("the fired timer is still consumed").isEmpty()
            );
        }

        @Test
        void warns_when_a_fired_timer_resolves_to_no_handler() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry("typo", NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);

            SagaExecutionSupport.process(orderFulfillment(), "o1",
                    current, SagaInput.timeout(new SagaTimeout("o1", TimerName.parse("typo"))), EventMeta.NONE, NOW);

            assertThat(appender.list)
                    .filteredOn(event -> event.getLevel() == Level.WARN)
                    .anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("typo"));
        }

        @Test
        void does_not_warn_for_a_timer_whose_handler_folds_and_reacts() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry(PAYMENT_TIMER, NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);

            SagaExecutionSupport.process(orderFulfillment(), "o1",
                    current, SagaInput.timeout(new SagaTimeout("o1", TimerName.parse(PAYMENT_TIMER))), EventMeta.NONE, NOW);

            assertThat(appender.list).noneSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.WARN));
        }
    }

    @Nested
    class TimerNamesReadBackFromStorage {

        private static final String STORED_NAME = "step:awaiting-players";

        private static Saga<OrderEvent, OrderState, OrderCommand> sagaHandling(TimerName timerName) {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .evolveOnTimeout(timerName, (state, t) -> new Cancelled(t.sagaId()))
                    .reactOnTimeout(timerName, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                    .build();
        }

        private static Saga<OrderEvent, OrderState, OrderCommand> sagaHandling(String timerName) {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .evolveOnTimeout(timerName, (state, t) -> new Cancelled(t.sagaId()))
                    .reactOnTimeout(timerName, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                    .build();
        }

        private static SagaEnvelope<OrderState> pendingSince_0_32_0() {
            return activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry(STORED_NAME, NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);
        }

        @Test
        void a_timer_stored_under_a_namespaced_name_fires_for_a_handler_registered_with_the_same_string() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    sagaHandling(STORED_NAME), "o1", pendingSince_0_32_0(),
                    SagaInput.timeout("o1", TimerName.parse(STORED_NAME)), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.envelope().state()).isEqualTo(new Cancelled("o1")),
                    () -> assertThat(outcome.commands()).containsExactly(new CancelOrder("o1")),
                    () -> assertThat(outcome.envelope().timers()).isEmpty()
            );
        }

        @Test
        void a_timer_stored_under_a_namespaced_name_fires_for_a_handler_registered_with_a_namespace_and_a_name() {
            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    sagaHandling(TimerName.of("step", "awaiting-players")), "o1", pendingSince_0_32_0(),
                    SagaInput.timeout("o1", TimerName.parse(STORED_NAME)), EventMeta.NONE, NOW);

            assertAll(
                    () -> assertThat(outcome.envelope().state()).isEqualTo(new Cancelled("o1")),
                    () -> assertThat(outcome.commands()).containsExactly(new CancelOrder("o1")),
                    () -> assertThat(outcome.envelope().timers()).isEmpty()
            );
        }

        @Test
        void a_started_timer_is_stored_under_the_string_its_name_encodes_to() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .react(OrderPlaced.class, (state, e) -> List.of(
                            SagaEffect.startTimeout(TimerName.of("step", "awaiting-players"), Duration.ofMinutes(5))))
                    .build();

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    saga, "o1", null, SagaInput.event(new OrderPlaced("o1", 100)), EventMeta.NONE, NOW);

            assertThat(outcome.envelope().timers()).extracting(TimerEntry::name).containsExactly(STORED_NAME);
        }
    }

    @Nested
    class Robustness {

        @Test
        void tolerates_processing_the_same_start_event_object_twice_without_mutating_it() {
            OrderEvent event = new OrderPlaced("o1", 100);

            assertThatCode(() -> {
                SagaExecutionSupport.process(orderFulfillment(), "o1", null, SagaInput.event(event), EventMeta.NONE, NOW);
                SagaExecutionSupport.process(orderFulfillment(), "o1", null, SagaInput.event(event), EventMeta.NONE, NOW);
            }).doesNotThrowAnyException();
        }
    }

    /**
     * A feed can deliver one stream's events of one type out of order, and one type's events a long way ahead of
     * another's. The executor therefore keeps, per stream and cloud event type, the highest version folded and the
     * versions folded below it, and fixes a floor for the stream, below which everything counts as folded whatever its
     * type.
     */
    @Nested
    @DisplayName("reordered stream events")
    class ReorderedStreamEvents {

        private static final String STREAM = "s1";
        private static final String TYPE_A = "org.example.A";
        private static final String TYPE_B = "org.example.B";
        private static final int WINDOW = 4;

        /** Stream s1 has a floor of 10 and a watermark of 30. Type A is at 30 with 27 and 28 applied, type B is at 20 with nothing below it. */
        private SagaEnvelope<OrderState> marked() {
            return envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10, Map.of(
                    TYPE_A, new TypeMark(30, List.of(27L, 28L), 26),
                    TYPE_B, new TypeMark(20, List.of(), 16)))));
        }

        private SagaEnvelope<OrderState> envelope(Map<String, Long> streamWatermarks, Map<String, StreamMarks> streamMarks) {
            return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(), streamWatermarks, null,
                    NOW, NOW, null, null, true, null, streamMarks);
        }

        /** An instance stored before 0.35.0, which has a watermark per stream and no marks. */
        private SagaEnvelope<OrderState> withoutMarks(Map<String, Long> streamWatermarks) {
            return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(), streamWatermarks, null,
                    NOW, NOW, null, null, true, null);
        }

        /** As {@link #marked()}, with the given versions pending for stream s1. */
        private SagaEnvelope<OrderState> markedWithPending(Map<String, List<Long>> pending) {
            return envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10, Map.of(
                    TYPE_A, new TypeMark(30, List.of(27L, 28L), 26),
                    TYPE_B, new TypeMark(20, List.of(), 16)), pending)));
        }

        /** As {@link #markedWithPending(Map)}, with the instant each pending version first failed. */
        private SagaEnvelope<OrderState> markedWithPending(Map<String, List<Long>> pending, Map<Long, Instant> pendingSince) {
            return envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10, Map.of(
                    TYPE_A, new TypeMark(30, List.of(27L, 28L), 26),
                    TYPE_B, new TypeMark(20, List.of(), 16)), pending, List.of(), pendingSince)));
        }

        private List<Long> pendingOf(SagaEnvelope<OrderState> envelope, String stream, String type) {
            StreamMarks marks = envelope.streamMarks().get(stream);
            return marks == null ? List.of() : marks.pending().getOrDefault(type, List.of());
        }

        private Outcome<OrderState, OrderCommand> apply(SagaEnvelope<OrderState> current, EventMeta meta) {
            // OrderPlaced does not end the instance, so the next event can still be applied to what comes out.
            return SagaExecutionSupport.process(orderFulfillment(), "o1", current, SagaInput.event(new OrderPlaced("o1", 100)), meta, NOW, WINDOW);
        }

        private EventMeta meta(String type, long version) {
            return new EventMeta(STREAM, version, null, type);
        }

        @Nested
        @TestInstance(TestInstance.Lifecycle.PER_CLASS)
        @DisplayName("when classifying a delivery against an instance with marks")
        class WhenClassifyingADeliveryAgainstAnInstanceWithMarks {

            Stream<Arguments> deliveries() {
                return Stream.of(
                        Arguments.of("at the floor, of a type with marks", TYPE_A, 10L, Delivery.REDELIVERY),
                        Arguments.of("below the floor, of a type with marks", TYPE_A, 5L, Delivery.REDELIVERY),
                        Arguments.of("at the floor, of another type with marks", TYPE_B, 10L, Delivery.REDELIVERY),
                        Arguments.of("below the floor, of a type without marks", "org.example.C", 3L, Delivery.REDELIVERY),
                        Arguments.of("just above the floor and well below the watermark, of a type without marks", "org.example.C", 11L, Delivery.NEW),
                        Arguments.of("above the highest folded of its type", TYPE_A, 31L, Delivery.NEW),
                        Arguments.of("far above the highest folded of its type", TYPE_A, 1_000L, Delivery.NEW),
                        Arguments.of("at the highest folded of its type", TYPE_A, 30L, Delivery.REDELIVERY),
                        Arguments.of("among the applied below the highest of its type", TYPE_A, 28L, Delivery.REDELIVERY),
                        Arguments.of("the lowest of the applied below the highest of its type", TYPE_A, 27L, Delivery.REDELIVERY),
                        Arguments.of("a late arrival one below the highest of its type", TYPE_A, 29L, Delivery.NEW),
                        Arguments.of("a late arrival of the other type, one below its highest", TYPE_B, 19L, Delivery.NEW),
                        Arguments.of("a late arrival of the other type, just inside the window", TYPE_B, 17L, Delivery.NEW),
                        Arguments.of("exactly the window behind the highest of the other type", TYPE_B, 16L, Delivery.BEHIND_WINDOW),
                        Arguments.of("exactly the window behind the highest of its type", TYPE_A, 26L, Delivery.BEHIND_WINDOW),
                        Arguments.of("further than the window behind the highest of its type", TYPE_A, 25L, Delivery.BEHIND_WINDOW),
                        Arguments.of("far behind the highest of its type but above the floor", TYPE_B, 11L, Delivery.BEHIND_WINDOW)
                );
            }

            @ParameterizedTest(name = "{0}: version {2} of {1} is {3}")
            @MethodSource("deliveries")
            @DisplayName("classifies the delivery")
            void classifies_the_delivery(String description, String type, long version, Delivery expected) {
                // Given
                SagaEnvelope<OrderState> current = marked();

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(type, version), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(expected);
            }

            @Test
            void a_stream_the_instance_has_no_marks_or_watermark_for_is_new_at_any_version() {
                // Given
                SagaEnvelope<OrderState> current = marked();

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, new EventMeta("s2", 1L, null, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void an_event_without_a_stream_id_is_new() {
                // When
                Delivery delivery = SagaExecutionSupport.classify(marked(), new EventMeta(null, 1L, 1L, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void an_event_without_a_stream_version_is_new() {
                // When
                Delivery delivery = SagaExecutionSupport.classify(marked(), new EventMeta(STREAM, null, 1L, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void an_event_with_nothing_but_a_position_is_new() {
                // When
                Delivery delivery = SagaExecutionSupport.classify(marked(), new EventMeta(null, null, 1L, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void isRedelivery_is_true_for_a_redelivery_and_for_an_event_behind_the_window_and_false_for_a_new_one() {
                // Given
                SagaEnvelope<OrderState> current = marked();

                // When
                List<Boolean> answers = List.of(
                        SagaExecutionSupport.isRedelivery(current, meta(TYPE_A, 30), WINDOW),
                        SagaExecutionSupport.isRedelivery(current, meta(TYPE_A, 25), WINDOW),
                        SagaExecutionSupport.isRedelivery(current, meta(TYPE_A, 29), WINDOW));

                // Then
                assertThat(answers).containsExactly(true, true, false);
            }

            @Test
            void the_window_decides_how_far_behind_a_late_arrival_may_be() {
                // Given
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), Long.MIN_VALUE)))));

                // When
                List<Delivery> deliveries = List.of(
                        SagaExecutionSupport.classify(current, meta(TYPE_A, 29), 1),
                        SagaExecutionSupport.classify(current, meta(TYPE_A, 29), 2),
                        SagaExecutionSupport.classify(current, meta(TYPE_A, 10), 20),
                        SagaExecutionSupport.classify(current, meta(TYPE_A, 11), 20));

                // Then
                assertThat(deliveries).containsExactly(Delivery.BEHIND_WINDOW, Delivery.NEW, Delivery.BEHIND_WINDOW, Delivery.NEW);
            }
        }

        @Nested
        @DisplayName("when the event carries no cloud event type")
        class WhenTheEventCarriesNoCloudEventType {

            private SagaEnvelope<OrderState> withTheUntypedMark() {
                return envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0, Map.of(
                        "", new TypeMark(30, List.of(28L), 26),
                        TYPE_A, new TypeMark(5, List.of(), 1)))));
            }

            @Test
            void is_a_redelivery_when_it_is_the_highest_version_of_the_type_named_by_the_empty_string() {
                // When
                Delivery delivery = SagaExecutionSupport.classify(withTheUntypedMark(), new EventMeta(STREAM, 30L, null), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void is_a_redelivery_when_it_is_among_the_applied_versions_of_the_type_named_by_the_empty_string() {
                // When
                Delivery delivery = SagaExecutionSupport.classify(withTheUntypedMark(), new EventMeta(STREAM, 28L, null, null), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void is_a_late_arrival_when_it_is_inside_the_window_of_the_type_named_by_the_empty_string() {
                // When
                Delivery delivery = SagaExecutionSupport.classify(withTheUntypedMark(), new EventMeta(STREAM, 29L, null), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void is_not_covered_by_the_marks_of_a_named_type() {
                // Given an instance that has only folded events of a named type
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0, Map.of(
                        TYPE_A, new TypeMark(30, List.of(), 26)))));

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, new EventMeta(STREAM, 20L, null), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }
        }

        @Nested
        @TestInstance(TestInstance.Lifecycle.PER_CLASS)
        @DisplayName("when classifying a delivery against an instance stored without marks")
        class WhenClassifyingADeliveryAgainstAnInstanceStoredWithoutMarks {

            Stream<Arguments> deliveries() {
                return Stream.of(
                        Arguments.of("at the watermark", TYPE_A, 5L, Delivery.REDELIVERY),
                        Arguments.of("below the watermark", TYPE_A, 4L, Delivery.REDELIVERY),
                        Arguments.of("below the watermark, of another type", TYPE_B, 1L, Delivery.REDELIVERY),
                        Arguments.of("below the watermark, with no type", null, 3L, Delivery.REDELIVERY),
                        Arguments.of("above the watermark", TYPE_A, 6L, Delivery.NEW),
                        Arguments.of("far above the watermark", TYPE_B, 5_000L, Delivery.NEW)
                );
            }

            @ParameterizedTest(name = "{0}: version {2} of {1} is {3}")
            @MethodSource("deliveries")
            @DisplayName("classifies the delivery by the watermark alone")
            void classifies_the_delivery_by_the_watermark_alone(String description, @Nullable String type, long version, Delivery expected) {
                // Given
                SagaEnvelope<OrderState> current = withoutMarks(Map.of(STREAM, 5L));

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(type, version), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(expected);
            }

            @Test
            void a_stream_the_watermarks_do_not_name_is_new() {
                // Given
                SagaEnvelope<OrderState> current = withoutMarks(Map.of(STREAM, 5L));

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, new EventMeta("s2", 1L, null, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }
        }

        @Nested
        @DisplayName("when processing a delivery")
        class WhenProcessingADelivery {

            @Test
            void a_higher_version_becomes_the_mark_and_the_old_mark_is_added_to_the_applied_versions() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), meta(TYPE_A, 31));

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.envelope().streamMarks().get(STREAM).byType().get(TYPE_A)).isEqualTo(new TypeMark(31, List.of(28L, 30L), 27)),
                        () -> assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of(STREAM, 31L))
                );
            }

            @Test
            void a_late_version_is_added_to_the_applied_versions_and_leaves_the_mark_and_the_watermark_where_they_were() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), meta(TYPE_A, 29));

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.envelope().streamMarks().get(STREAM).byType().get(TYPE_A)).isEqualTo(new TypeMark(30, List.of(27L, 28L, 29L), 26)),
                        () -> assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of(STREAM, 30L))
                );
            }

            @Test
            void the_applied_versions_stay_ascending_when_a_late_version_is_below_the_ones_already_there() {
                // Given type A at 30 with 28 applied, and 27 still inside the window of 4 as 27 is above 26
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(28L), 26)))));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 27));

                // Then
                assertThat(outcome.envelope().streamMarks().get(STREAM).byType().get(TYPE_A).applied()).containsExactly(27L, 28L);
            }

            @Test
            void a_version_of_a_type_without_a_mark_gets_a_mark_of_its_own_and_leaves_the_other_types_alone() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), meta("org.example.C", 11));

                // Then
                Map<String, TypeMark> byType = outcome.envelope().streamMarks().get(STREAM).byType();
                assertAll(
                        () -> assertThat(byType.get("org.example.C")).isEqualTo(new TypeMark(11, List.of(), 7)),
                        () -> assertThat(byType.get(TYPE_A)).isEqualTo(new TypeMark(30, List.of(27L, 28L), 26)),
                        () -> assertThat(byType.get(TYPE_B)).isEqualTo(new TypeMark(20, List.of(), 16)),
                        () -> assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of(STREAM, 30L))
                );
            }

            @Test
            void the_watermark_is_the_highest_version_over_every_type() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), meta(TYPE_B, 21));

                // Then
                assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of(STREAM, 30L));
            }

            @Test
            void the_applied_versions_at_or_below_the_mark_minus_the_window_are_dropped_and_the_one_above_is_kept() {
                // When the mark moves to 31 and then 32, the edges being 31 - 4 = 27 and 32 - 4 = 28
                Outcome<OrderState, OrderCommand> first = apply(marked(), meta(TYPE_A, 31));
                Outcome<OrderState, OrderCommand> second = apply(first.envelope(), meta(TYPE_A, 32));

                // Then
                assertAll(
                        () -> assertThat(first.envelope().streamMarks().get(STREAM).byType().get(TYPE_A).applied()).containsExactly(28L, 30L),
                        () -> assertThat(second.envelope().streamMarks().get(STREAM).byType().get(TYPE_A).applied()).containsExactly(30L, 31L)
                );
            }

            @Test
            void a_version_dropped_from_the_applied_versions_is_then_behind_the_window() {
                // Given
                SagaEnvelope<OrderState> afterTrimming = apply(apply(marked(), meta(TYPE_A, 31)).envelope(), meta(TYPE_A, 32)).envelope();

                // When
                Delivery delivery = SagaExecutionSupport.classify(afterTrimming, meta(TYPE_A, 28), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.BEHIND_WINDOW);
            }

            @Test
            void in_order_delivery_keeps_fewer_applied_versions_than_the_window() {
                // Given
                SagaEnvelope<OrderState> current = null;

                // When
                for (long version = 1; version <= 50; version++) {
                    current = apply(current, meta(TYPE_A, version)).envelope();
                }

                // Then
                assertThat(current.streamMarks().get(STREAM).byType().get(TYPE_A)).isEqualTo(new TypeMark(50, List.of(47L, 48L, 49L), 46));
            }

            @Test
            void the_floor_is_the_watermark_the_stream_had_when_it_first_got_marks() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(withoutMarks(Map.of(STREAM, 5L)), meta(TYPE_A, 7));

                // Then
                assertThat(outcome.envelope().streamMarks().get(STREAM)).isEqualTo(new StreamMarks(5, Map.of(TYPE_A, new TypeMark(7, List.of(), 3))));
            }

            @Test
            void the_floor_stays_where_it_was_fixed_when_later_deliveries_raise_the_watermark() {
                // Given
                SagaEnvelope<OrderState> afterFirst = apply(withoutMarks(Map.of(STREAM, 5L)), meta(TYPE_A, 7)).envelope();

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(afterFirst, meta(TYPE_B, 9));

                // Then
                assertAll(
                        () -> assertThat(outcome.envelope().streamMarks().get(STREAM).floor()).isEqualTo(5L),
                        () -> assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of(STREAM, 9L))
                );
            }

            @Test
            void a_stream_first_seen_by_this_delivery_has_no_floor_so_a_lower_version_of_another_type_is_new_afterwards() {
                // Given the instance started on another stream, so stream s1 gets its marks from this delivery
                SagaEnvelope<OrderState> started = apply(null, new EventMeta("s0", 1L, null, TYPE_A)).envelope();
                SagaEnvelope<OrderState> afterFirst = apply(started, new EventMeta(STREAM, 10L, null, TYPE_A)).envelope();

                // When
                Delivery delivery = SagaExecutionSupport.classify(afterFirst, meta(TYPE_B, 3), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void another_stream_gets_its_own_marks_and_leaves_the_first_streams_alone() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), new EventMeta("s2", 4L, null, TYPE_A));

                // Then
                Map<String, StreamMarks> marks = outcome.envelope().streamMarks();
                assertAll(
                        () -> assertThat(marks.keySet()).containsExactlyInAnyOrder(STREAM, "s2"),
                        () -> assertThat(marks.get(STREAM)).isEqualTo(marked().streamMarks().get(STREAM)),
                        () -> assertThat(marks.get("s2").byType().get(TYPE_A)).isEqualTo(new TypeMark(4, List.of(), 0))
                );
            }

            @Test
            void an_event_without_a_stream_key_leaves_the_marks_as_they_were() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), EventMeta.NONE);

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.envelope().streamMarks()).isEqualTo(marked().streamMarks())
                );
            }

            @Test
            void a_redelivery_is_skipped() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), meta(TYPE_A, 28));

                // Then
                assertThat(outcome.processed()).isFalse();
            }

            @Test
            void a_delivery_behind_the_window_is_skipped() {
                // When
                Outcome<OrderState, OrderCommand> outcome = apply(marked(), meta(TYPE_A, 26));

                // Then
                assertThat(outcome.processed()).isFalse();
            }

            @Test
            void a_late_arrival_runs_the_reaction_for_its_event() {
                // When
                Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(orderFulfillment(), "o1", marked(),
                        SagaInput.event(new OrderPlaced("o1", 100)), meta(TYPE_A, 29), NOW, WINDOW);

                // Then
                assertThat(outcome.commands()).containsExactly(new NotifyWarehouse("o1"));
            }

            @Test
            void the_overload_without_a_window_uses_the_default_window() {
                // Given type A at 300 with nothing below it, so with the default window of 256 the edge is 300 - 256 = 44
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 300L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(300, List.of(), 44)))));

                // When
                Outcome<OrderState, OrderCommand> atTheEdge = SagaExecutionSupport.process(orderFulfillment(), "o1", current,
                        SagaInput.event(new OrderPlaced("o1", 100)), meta(TYPE_A, 44), NOW);
                Outcome<OrderState, OrderCommand> insideTheWindow = SagaExecutionSupport.process(orderFulfillment(), "o1", current,
                        SagaInput.event(new OrderPlaced("o1", 100)), meta(TYPE_A, 45), NOW);

                // Then
                assertAll(
                        () -> assertThat(SagaExecutionSupport.DEFAULT_REORDER_WINDOW).isEqualTo(256),
                        () -> assertThat(atTheEdge.processed()).isFalse(),
                        () -> assertThat(insideTheWindow.processed()).isTrue()
                );
            }
        }

        @Nested
        @DisplayName("when the reorder window changes between deliveries")
        class WhenTheReorderWindowChangesBetweenDeliveries {

            private static final int NARROW = 2;
            private static final int WIDE = 256;

            private Outcome<OrderState, OrderCommand> applyWith(@Nullable SagaEnvelope<OrderState> current, EventMeta meta, int reorderWindow) {
                return SagaExecutionSupport.process(orderFulfillment(), "o1", current, SagaInput.event(new OrderPlaced("o1", 100)), meta, NOW, reorderWindow);
            }

            private TypeMark typeAMark(SagaEnvelope<OrderState> current) {
                return current.streamMarks().get(STREAM).byType().get(TYPE_A);
            }

            /** Versions 9 and 11 of type A were applied under a window of 2, which dropped 9 from the applied versions. */
            private SagaEnvelope<OrderState> afterNinePrunedByNarrowWindow() {
                SagaEnvelope<OrderState> afterNine = applyWith(null, meta(TYPE_A, 9), NARROW).envelope();
                return applyWith(afterNine, meta(TYPE_A, 11), NARROW).envelope();
            }

            @Test
            void a_version_pruned_under_a_narrow_window_is_recorded_as_the_horizon_of_its_type() {
                // When
                SagaEnvelope<OrderState> current = afterNinePrunedByNarrowWindow();

                // Then
                assertThat(typeAMark(current)).isEqualTo(new TypeMark(11, List.of(), 9));
            }

            @Test
            void a_version_pruned_under_a_narrow_window_is_still_behind_the_window_when_the_window_is_widened() {
                // Given
                SagaEnvelope<OrderState> current = afterNinePrunedByNarrowWindow();

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 9), WIDE);

                // Then
                assertThat(delivery).isEqualTo(Delivery.BEHIND_WINDOW);
            }

            @Test
            void a_version_pruned_under_a_narrow_window_does_not_run_its_reaction_again_when_the_window_is_widened() {
                // Given
                SagaEnvelope<OrderState> current = afterNinePrunedByNarrowWindow();

                // When
                Outcome<OrderState, OrderCommand> outcome = applyWith(current, meta(TYPE_A, 9), WIDE);

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isFalse(),
                        () -> assertThat(outcome.commands()).isEmpty()
                );
            }

            @Test
            void a_version_that_was_never_applied_above_the_horizon_is_still_new_when_the_window_is_widened() {
                // Given
                SagaEnvelope<OrderState> current = afterNinePrunedByNarrowWindow();

                // When
                Outcome<OrderState, OrderCommand> outcome = applyWith(current, meta(TYPE_A, 10), WIDE);

                // Then
                assertAll(
                        () -> assertThat(SagaExecutionSupport.classify(current, meta(TYPE_A, 10), WIDE)).isEqualTo(Delivery.NEW),
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.commands()).containsExactly(new NotifyWarehouse("o1")),
                        () -> assertThat(typeAMark(outcome.envelope())).isEqualTo(new TypeMark(11, List.of(10L), 9))
                );
            }

            @Test
            void a_wider_window_does_not_lower_the_horizon_when_a_later_version_is_applied() {
                // Given
                SagaEnvelope<OrderState> current = afterNinePrunedByNarrowWindow();

                // When
                SagaEnvelope<OrderState> afterTwelve = applyWith(current, meta(TYPE_A, 12), WIDE).envelope();

                // Then
                assertAll(
                        () -> assertThat(typeAMark(afterTwelve)).isEqualTo(new TypeMark(12, List.of(11L), 9)),
                        () -> assertThat(SagaExecutionSupport.classify(afterTwelve, meta(TYPE_A, 9), WIDE)).isEqualTo(Delivery.BEHIND_WINDOW)
                );
            }

            @Test
            void the_horizon_never_decreases_however_the_window_changes_between_deliveries() {
                // When the window goes from 256 to 2 and back to 256 across three consecutive versions
                SagaEnvelope<OrderState> underWide = applyWith(null, meta(TYPE_A, 100), WIDE).envelope();
                SagaEnvelope<OrderState> underNarrow = applyWith(underWide, meta(TYPE_A, 101), NARROW).envelope();
                SagaEnvelope<OrderState> underWideAgain = applyWith(underNarrow, meta(TYPE_A, 102), WIDE).envelope();

                // Then the horizon is 100 - 256 first, then 101 - 2, and stays there
                assertAll(
                        () -> assertThat(typeAMark(underWide)).isEqualTo(new TypeMark(100, List.of(), -156)),
                        () -> assertThat(typeAMark(underNarrow)).isEqualTo(new TypeMark(101, List.of(100L), 99)),
                        () -> assertThat(typeAMark(underWideAgain)).isEqualTo(new TypeMark(102, List.of(100L, 101L), 99))
                );
            }
        }

        @Nested
        @DisplayName("when the instance holds a failure record and the input is at or below the floor")
        class WhenTheInstanceHoldsAFailureRecordAndTheInputIsAtOrBelowTheFloor {

            private static final String OTHER_STREAM = "s2";

            private SagaFailure failureOf(String input) {
                return new SagaFailure(input, null, NOW, IllegalStateException.class.getName(), "boom");
            }

            /** An instance stored before 0.35.0, with a watermark of 30 for s1 and for s2 and no marks. */
            private SagaEnvelope<OrderState> withoutMarksFailingOn(String input) {
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(),
                        Map.of(STREAM, 30L, OTHER_STREAM, 30L), null, NOW, NOW, null, null, true, failureOf(input), Map.of());
            }

            /** An instance with marks, which fixed a floor of 30 for s1 and for s2. */
            private SagaEnvelope<OrderState> withMarksFailingOn(String input) {
                StreamMarks marks = new StreamMarks(30, Map.of(TYPE_A, new TypeMark(30, List.of(), 26)));
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(),
                        Map.of(STREAM, 30L, OTHER_STREAM, 30L), null, NOW, NOW, null, null, true, failureOf(input),
                        Map.of(STREAM, marks, OTHER_STREAM, marks));
            }

            @Test
            void the_input_the_record_names_is_new_for_an_instance_without_marks() {
                // Given
                SagaEnvelope<OrderState> current = withoutMarksFailingOn(STREAM + "@5");

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void another_input_at_or_below_the_watermark_is_a_redelivery_for_an_instance_without_marks() {
                // Given
                SagaEnvelope<OrderState> current = withoutMarksFailingOn(STREAM + "@5");

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void the_input_the_record_names_is_new_for_an_instance_with_marks() {
                // Given
                SagaEnvelope<OrderState> current = withMarksFailingOn(STREAM + "@5");

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void another_input_at_or_below_the_floor_is_a_redelivery_for_an_instance_with_marks() {
                // Given
                SagaEnvelope<OrderState> current = withMarksFailingOn(STREAM + "@5");

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void the_same_version_of_another_stream_is_a_redelivery_for_an_instance_without_marks() {
                // Given
                SagaEnvelope<OrderState> current = withoutMarksFailingOn(STREAM + "@5");

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, new EventMeta(OTHER_STREAM, 5L, null, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void the_same_version_of_another_stream_is_a_redelivery_for_an_instance_with_marks() {
                // Given
                SagaEnvelope<OrderState> current = withMarksFailingOn(STREAM + "@5");

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, new EventMeta(OTHER_STREAM, 5L, null, TYPE_A), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }
        }

        @Nested
        @DisplayName("when the instance holds a failure record and the input is behind the window")
        class WhenTheInstanceHoldsAFailureRecordAndTheInputIsBehindTheWindow {

            private final Duration budget = Duration.ofMinutes(5);
            private final Throwable failure = new IllegalStateException("boom");

            /** Type A is at 30 under a window of 4, so version 5 is far behind it and was never applied. */
            private SagaEnvelope<OrderState> failingOn(String input, Instant firstFailedAt) {
                SagaFailure record = new SagaFailure(input, null, firstFailedAt, IllegalStateException.class.getName(), "boom");
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(), Map.of(STREAM, 30L), null,
                        NOW, NOW, null, null, true, record, Map.of(STREAM, new StreamMarks(0, Map.of(TYPE_A, new TypeMark(30, List.of(), 26)))));
            }

            /** As {@link #failingOn}, with the given versions pending for stream s1. */
            private SagaEnvelope<OrderState> failingOnWithPending(String input, Instant firstFailedAt, Map<String, List<Long>> pending) {
                SagaFailure record = new SagaFailure(input, null, firstFailedAt, IllegalStateException.class.getName(), "boom");
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(), Map.of(STREAM, 30L), null,
                        NOW, NOW, null, null, true, record, Map.of(STREAM, new StreamMarks(0, Map.of(TYPE_A, new TypeMark(30, List.of(), 26)), pending)));
            }

            private Outcome<OrderState, OrderCommand> applyOrderPlaced(SagaEnvelope<OrderState> current, EventMeta meta) {
                return SagaExecutionSupport.process(orderFulfillment(), "o1", current, SagaInput.event(new OrderPlaced("o1", 100)), meta, NOW, WINDOW);
            }

            @Test
            void the_input_the_record_names_is_new_however_far_behind_the_window_it_is() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW);

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void the_input_the_record_names_is_applied_and_clears_the_record() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW);

                // When
                Outcome<OrderState, OrderCommand> outcome = applyOrderPlaced(current, meta(TYPE_A, 5));

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.commands()).containsExactly(new NotifyWarehouse("o1")),
                        () -> assertThat(outcome.envelope().failure()).isNull()
                );
            }

            @Test
            void the_input_the_record_named_is_behind_the_window_again_once_it_has_been_applied() {
                // Given
                SagaEnvelope<OrderState> afterApplied = applyOrderPlaced(failingOn(STREAM + "@5", NOW), meta(TYPE_A, 5)).envelope();

                // When
                Outcome<OrderState, OrderCommand> redelivery = applyOrderPlaced(afterApplied, meta(TYPE_A, 5));

                // Then
                assertAll(
                        () -> assertThat(SagaExecutionSupport.classify(afterApplied, meta(TYPE_A, 5), WINDOW)).isEqualTo(Delivery.BEHIND_WINDOW),
                        () -> assertThat(redelivery.processed()).isFalse(),
                        () -> assertThat(redelivery.commands()).isEmpty()
                );
            }

            @Test
            void a_record_that_names_another_input_leaves_the_input_behind_the_window() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@6", NOW);

                // When
                Outcome<OrderState, OrderCommand> outcome = applyOrderPlaced(current, meta(TYPE_A, 5));

                // Then
                assertAll(
                        () -> assertThat(SagaExecutionSupport.classify(current, meta(TYPE_A, 5), WINDOW)).isEqualTo(Delivery.BEHIND_WINDOW),
                        () -> assertThat(outcome.processed()).isFalse(),
                        () -> assertThat(outcome.commands()).isEmpty()
                );
            }

            @Test
            void a_record_written_before_0_35_0_with_a_position_key_leaves_the_input_behind_the_window() {
                // Given
                SagaEnvelope<OrderState> current = failingOn("position:5", NOW);

                // When
                Outcome<OrderState, OrderCommand> outcome = applyOrderPlaced(current, meta(TYPE_A, 5));

                // Then
                assertAll(
                        () -> assertThat(SagaExecutionSupport.classify(current, meta(TYPE_A, 5), WINDOW)).isEqualTo(Delivery.BEHIND_WINDOW),
                        () -> assertThat(outcome.processed()).isFalse()
                );
            }

            @Test
            void the_same_input_failing_again_after_the_budget_is_used_up_quarantines_the_instance() {
                // Given a record that has been naming version 5 for as long as the budget
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW.minus(budget));

                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, 5), failure, NOW, budget, WINDOW);

                // Then
                assertAll(
                        () -> assertThat(record).isNotNull(),
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.QUARANTINED),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo(STREAM + "@5"),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(NOW.minus(budget))
                );
            }

            @Test
            void the_same_input_failing_again_inside_the_budget_produces_no_new_record_when_it_is_pending_already() {
                // Given
                SagaEnvelope<OrderState> current = failingOnWithPending(STREAM + "@5", NOW, Map.of(TYPE_A, List.of(5L)));

                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, 5), failure, NOW, budget, WINDOW);

                // Then
                assertThat(record).isNull();
            }

            @Test
            void the_same_input_failing_again_inside_the_budget_lists_it_as_pending_when_the_record_was_written_before_pending_existed() {
                // Given a record naming version 5 that does not list it as pending
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW);

                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, 5), failure, NOW.plusSeconds(60), budget, WINDOW);

                // Then
                assertAll(
                        () -> assertThat(record).isNotNull(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L),
                        () -> assertThat(record.envelope().failure()).isEqualTo(current.failure()),
                        () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.ACTIVE),
                        () -> assertThat(record.quarantined()).isFalse()
                );
            }

            @Test
            void a_version_listed_as_pending_by_a_retry_is_still_new_after_the_instance_moved_past_the_window_and_failed_on_another_input() {
                // Given an instance stored before 0.35.0, which has a watermark of 4 and no marks, failing on version 5
                SagaFailure legacyRecord = new SagaFailure(STREAM + "@5", null, NOW, IllegalStateException.class.getName(), "boom");
                SagaEnvelope<OrderState> legacy = new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(),
                        Map.of(STREAM, 4L), null, NOW, NOW, null, null, true, legacyRecord);
                FailureRecord<OrderState> retry = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", legacy, meta(TYPE_A, 5), failure, NOW.plusSeconds(60), budget, WINDOW);

                // When versions 6 to 30 are applied and then version 31 fails
                SagaEnvelope<OrderState> applied = retry == null ? legacy : retry.envelope();
                for (long version = 6; version <= 30; version++) {
                    applied = applyOrderPlaced(applied, meta(TYPE_A, version)).envelope();
                }
                FailureRecord<OrderState> failedOn31 = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", applied, meta(TYPE_A, 31), failure, NOW.plusSeconds(120), budget, WINDOW);
                assertThat(failedOn31).isNotNull();

                // Then the record names version 31 and version 5 is new because it is pending
                assertAll(
                        () -> assertThat(failedOn31.envelope().failure().input()).isEqualTo(STREAM + "@31"),
                        () -> assertThat(pendingOf(failedOn31.envelope(), STREAM, TYPE_A)).containsExactly(5L, 31L),
                        () -> assertThat(SagaExecutionSupport.classify(failedOn31.envelope(), meta(TYPE_A, 5), WINDOW)).isEqualTo(Delivery.NEW)
                );
            }

            @Test
            void another_input_behind_the_window_failing_produces_no_record() {
                // Given a record naming version 6, so version 5 is a redelivery of nothing the record knows about
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@6", NOW.minus(budget));

                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, 5), failure, NOW, budget, WINDOW);

                // Then
                assertThat(record).isNull();
            }
        }

        @Nested
        @DisplayName("when an input fails")
        class WhenAnInputFails {

            private final Duration budget = Duration.ofMinutes(5);
            private final Throwable failure = new IllegalStateException("boom");

            private FailureRecord<OrderState> failing(SagaEnvelope<OrderState> current, EventMeta meta) {
                return SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta, failure, NOW, budget, WINDOW);
            }

            @Test
            void a_new_version_produces_a_record_that_only_adds_the_version_as_pending_and_keeps_the_watermarks_untouched() {
                // When
                FailureRecord<OrderState> record = failing(marked(), meta(TYPE_A, 31));

                // Then
                assertAll(
                        () -> assertThat(record).isNotNull(),
                        () -> assertThat(record.envelope().streamMarks()).isEqualTo(markedWithPending(Map.of(TYPE_A, List.of(31L)), Map.of(31L, NOW)).streamMarks()),
                        () -> assertThat(record.envelope().streamWatermarks()).isEqualTo(marked().streamWatermarks()),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@31")
                );
            }

            @Test
            void a_late_arrival_produces_a_record_because_it_has_not_been_folded() {
                // When
                FailureRecord<OrderState> record = failing(marked(), meta(TYPE_A, 29));

                // Then
                assertAll(
                        () -> assertThat(record).isNotNull(),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@29"),
                        () -> assertThat(record.envelope().streamMarks()).isEqualTo(markedWithPending(Map.of(TYPE_A, List.of(29L)), Map.of(29L, NOW)).streamMarks())
                );
            }

            @Test
            void a_redelivery_produces_no_record_because_the_committed_outcome_stands() {
                // When
                FailureRecord<OrderState> record = failing(marked(), meta(TYPE_A, 28));

                // Then
                assertThat(record).isNull();
            }

            @Test
            void a_delivery_behind_the_window_produces_no_record() {
                // When
                FailureRecord<OrderState> record = failing(marked(), meta(TYPE_A, 26));

                // Then
                assertThat(record).isNull();
            }

            @Test
            void whether_a_late_arrival_produces_a_record_depends_on_the_window_given() {
                // Given type A at 30, so 27 is behind the window when the window is 3 or less and a late arrival when it is 4 or more
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), Long.MIN_VALUE)))));

                // When
                FailureRecord<OrderState> narrow = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, 27), failure, NOW, budget, 3);
                FailureRecord<OrderState> wide = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, 27), failure, NOW, budget, 4);

                // Then
                assertAll(
                        () -> assertThat(narrow).isNull(),
                        () -> assertThat(wide).isNotNull()
                );
            }

            @Test
            void the_overload_without_a_window_treats_a_late_arrival_as_new() {
                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", marked(), meta(TYPE_A, 29), failure, NOW, budget);

                // Then
                assertThat(record).isNotNull();
            }

            @Test
            void quarantining_only_adds_the_version_as_pending_to_the_marks() {
                // Given an input that has been failing for as long as the budget
                SagaEnvelope<OrderState> failingSince = failing(marked(), meta(TYPE_A, 31)).envelope();

                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", failingSince, meta(TYPE_A, 31), failure,
                        NOW.plus(budget), budget, WINDOW);

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(record.envelope().streamMarks()).isEqualTo(markedWithPending(Map.of(TYPE_A, List.of(31L)), Map.of(31L, NOW)).streamMarks()),
                        () -> assertThat(record.envelope().streamWatermarks()).isEqualTo(marked().streamWatermarks())
                );
            }

            @Test
            void a_first_event_that_fails_inserts_a_record_with_no_applied_marks_and_the_version_pending() {
                // When
                FailureRecord<OrderState> record = failing(null, meta(TYPE_A, 1));

                // Then
                assertThat(record.envelope().streamMarks()).isEqualTo(
                        Map.of(STREAM, new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of(TYPE_A, List.of(1L)), List.of(), Map.of(1L, NOW))));
            }
        }

        @Nested
        @DisplayName("when an input fails and the instance holds more pending versions than the window")
        class WhenAnInputFailsAndTheInstanceHoldsMorePendingVersionsThanTheWindow {

            private static final int NARROW_WINDOW = 2;

            private ListAppender<ILoggingEvent> appender;
            private ch.qos.logback.classic.Logger logger;

            @BeforeEach
            void attachAppender() {
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                logger = context.getLogger(SagaExecutionSupport.class);
                appender = new ListAppender<>();
                appender.start();
                logger.addAppender(appender);
            }

            @AfterEach
            void detachAppender() {
                logger.detachAppender(appender);
            }

            /** Six versions of type A are pending, which a window of 2 could not have recorded. */
            private SagaEnvelope<OrderState> withSixPendingVersionsOfTypeA() {
                return envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20)), Map.of(TYPE_A, List.of(5L, 6L, 7L, 8L, 9L, 10L)))));
            }

            private List<String> warnings() {
                return appender.list.stream()
                        .filter(event -> event.getLevel() == Level.WARN)
                        .map(ILoggingEvent::getFormattedMessage)
                        .toList();
            }

            private FailureRecord<OrderState> failingAt(SagaEnvelope<OrderState> current, long version, int window) {
                return SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, version),
                        new IllegalStateException("boom"), NOW, Duration.ofMinutes(5), window);
            }

            @Test
            void every_pending_version_is_kept_however_many_the_window_allows() {
                // Given
                SagaEnvelope<OrderState> current = withSixPendingVersionsOfTypeA();

                // When
                FailureRecord<OrderState> record = failingAt(current, 31, NARROW_WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L, 6L, 7L, 8L, 9L, 10L, 31L);
            }

            @Test
            void no_error_is_logged_for_a_version_kept_past_the_window() {
                // Given
                SagaEnvelope<OrderState> current = withSixPendingVersionsOfTypeA();

                // When
                failingAt(current, 31, NARROW_WINDOW);

                // Then
                assertThat(appender.list).noneMatch(event -> event.getLevel() == Level.ERROR);
            }

            @Test
            void a_warning_naming_the_stream_the_type_and_the_window_is_logged_when_the_pending_versions_of_a_type_grow_past_the_window() {
                // Given a window of 2 and 2 versions of type A pending already
                SagaEnvelope<OrderState> current = failingAt(failingAt(null, 31, NARROW_WINDOW).envelope(), 32, NARROW_WINDOW).envelope();

                // When
                failingAt(current, 33, NARROW_WINDOW);

                // Then
                assertThat(warnings()).singleElement().satisfies(message -> assertAll(
                        () -> assertThat(message).contains("Stream s1"),
                        () -> assertThat(message).contains("'" + TYPE_A + "'"),
                        () -> assertThat(message).contains("more than 2")
                ));
            }

            @Test
            void no_warning_is_logged_while_the_pending_versions_of_a_type_fit_the_window() {
                // Given a window of 2 and 1 version of type A pending already
                SagaEnvelope<OrderState> current = failingAt(null, 31, NARROW_WINDOW).envelope();

                // When
                failingAt(current, 32, NARROW_WINDOW);

                // Then
                assertThat(warnings()).isEmpty();
            }

            @Test
            void the_warning_is_not_logged_again_for_the_next_version_that_fails() {
                // Given a window of 2 and 3 versions of type A pending, which logged the warning
                SagaEnvelope<OrderState> crossed = failingAt(failingAt(failingAt(null, 31, NARROW_WINDOW).envelope(), 32, NARROW_WINDOW).envelope(), 33, NARROW_WINDOW).envelope();

                // When
                failingAt(crossed, 34, NARROW_WINDOW);

                // Then
                assertThat(warnings()).hasSize(1);
            }

            @Test
            void the_warning_is_not_logged_again_when_a_version_that_is_pending_already_fails_again() {
                // Given a window of 2 and 4 versions of type A pending, the last of them failing now, which logged the warning once
                SagaEnvelope<OrderState> crossed = failingAt(failingAt(failingAt(failingAt(null, 31, NARROW_WINDOW).envelope(), 32, NARROW_WINDOW).envelope(),
                        33, NARROW_WINDOW).envelope(), 34, NARROW_WINDOW).envelope();

                // When
                failingAt(crossed, 33, NARROW_WINDOW);

                // Then
                assertThat(warnings()).hasSize(1);
            }

            @Test
            void the_warning_is_not_logged_for_an_instance_that_was_past_the_window_before() {
                // Given 6 versions pending and a window of 2, which is the state after the window was lowered
                SagaEnvelope<OrderState> current = withSixPendingVersionsOfTypeA();

                // When
                failingAt(current, 31, NARROW_WINDOW);

                // Then
                assertThat(warnings()).isEmpty();
            }
        }

        @Nested
        @TestInstance(TestInstance.Lifecycle.PER_CLASS)
        @DisplayName("when classifying a delivery against an instance with pending versions")
        class WhenClassifyingADeliveryAgainstAnInstanceWithPendingVersions {

            Stream<Arguments> pendingDeliveries() {
                return Stream.of(
                        Arguments.of("just above the floor and well behind the horizon", TYPE_A, 11L),
                        Arguments.of("behind the horizon", TYPE_A, 12L),
                        Arguments.of("exactly the window behind the highest of its type", TYPE_A, 26L),
                        Arguments.of("far behind the highest of the other type", TYPE_B, 11L),
                        Arguments.of("exactly the window behind the highest of the other type", TYPE_B, 16L)
                );
            }

            @ParameterizedTest(name = "{0}: version {2} of {1} is NEW")
            @MethodSource("pendingDeliveries")
            @DisplayName("a pending version is new however far behind the highest of its type it is")
            void a_pending_version_is_new_however_far_behind_the_highest_of_its_type_it_is(String description, String type, long version) {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(11L, 12L, 26L), TYPE_B, List.of(11L, 16L)));

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(type, version), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void a_version_that_is_not_pending_in_the_same_position_is_still_behind_the_window() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L)));

                // When
                Delivery pending = SagaExecutionSupport.classify(current, meta(TYPE_A, 12), WINDOW);
                Delivery below = SagaExecutionSupport.classify(current, meta(TYPE_A, 11), WINDOW);
                Delivery above = SagaExecutionSupport.classify(current, meta(TYPE_A, 13), WINDOW);

                // Then
                assertAll(
                        () -> assertThat(pending).isEqualTo(Delivery.NEW),
                        () -> assertThat(below).isEqualTo(Delivery.BEHIND_WINDOW),
                        () -> assertThat(above).isEqualTo(Delivery.BEHIND_WINDOW)
                );
            }

            @Test
            void a_pending_version_of_another_type_does_not_make_the_version_new() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_B, List.of(12L)));

                // When
                Delivery ofTheType = SagaExecutionSupport.classify(current, meta(TYPE_B, 12), WINDOW);
                Delivery ofTheOtherType = SagaExecutionSupport.classify(current, meta(TYPE_A, 12), WINDOW);

                // Then
                assertAll(
                        () -> assertThat(ofTheType).isEqualTo(Delivery.NEW),
                        () -> assertThat(ofTheOtherType).isEqualTo(Delivery.BEHIND_WINDOW)
                );
            }

            @Test
            void a_pending_version_of_another_stream_does_not_make_the_version_new() {
                // Given stream s2 has version 12 pending and stream s1 has nothing pending
                Map<String, TypeMark> byType = Map.of(TYPE_A, new TypeMark(30, List.of(27L, 28L), 26));
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L, "s2", 30L), Map.of(
                        STREAM, new StreamMarks(10, byType),
                        "s2", new StreamMarks(10, byType, Map.of(TYPE_A, List.of(12L)))));

                // When
                Delivery inTheStream = SagaExecutionSupport.classify(current, meta(TYPE_A, 12), WINDOW);
                Delivery inTheOtherStream = SagaExecutionSupport.classify(current, new EventMeta("s2", 12L, null, TYPE_A), WINDOW);

                // Then
                assertAll(
                        () -> assertThat(inTheStream).isEqualTo(Delivery.BEHIND_WINDOW),
                        () -> assertThat(inTheOtherStream).isEqualTo(Delivery.NEW)
                );
            }

            @Test
            void a_version_at_or_below_the_floor_is_new_when_it_is_listed_as_pending() {
                // Given the floor is 10
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(3L, 10L, 12L)));

                // When
                Delivery belowTheFloor = SagaExecutionSupport.classify(current, meta(TYPE_A, 3), WINDOW);
                Delivery atTheFloor = SagaExecutionSupport.classify(current, meta(TYPE_A, 10), WINDOW);
                Delivery aboveTheFloor = SagaExecutionSupport.classify(current, meta(TYPE_A, 12), WINDOW);

                // Then
                assertAll(
                        () -> assertThat(belowTheFloor).isEqualTo(Delivery.NEW),
                        () -> assertThat(atTheFloor).isEqualTo(Delivery.NEW),
                        () -> assertThat(aboveTheFloor).isEqualTo(Delivery.NEW)
                );
            }

            /** The floor is 30 and type A is at 30, with version 5 of type A pending. */
            private SagaEnvelope<OrderState> withFloorOf30AndVersion5Pending() {
                return envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(30,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 26)), Map.of(TYPE_A, List.of(5L)))));
            }

            @Test
            void a_pending_version_below_the_floor_is_new() {
                // Given
                SagaEnvelope<OrderState> current = withFloorOf30AndVersion5Pending();

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void a_version_below_the_floor_that_is_not_pending_is_a_redelivery() {
                // Given
                SagaEnvelope<OrderState> current = withFloorOf30AndVersion5Pending();

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void a_version_below_the_floor_is_a_redelivery_when_only_another_type_has_that_version_pending() {
                // Given
                SagaEnvelope<OrderState> current = withFloorOf30AndVersion5Pending();

                // When
                Delivery delivery = SagaExecutionSupport.classify(current, meta(TYPE_B, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void a_pending_version_of_an_event_without_a_cloud_event_type_is_new_under_the_empty_type() {
                // Given
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10,
                        Map.of("", new TypeMark(30, List.of(), 26)), Map.of("", List.of(12L)))));

                // When
                Delivery pending = SagaExecutionSupport.classify(current, new EventMeta(STREAM, 12L, null), WINDOW);
                Delivery notPending = SagaExecutionSupport.classify(current, new EventMeta(STREAM, 13L, null), WINDOW);

                // Then
                assertAll(
                        () -> assertThat(pending).isEqualTo(Delivery.NEW),
                        () -> assertThat(notPending).isEqualTo(Delivery.BEHIND_WINDOW)
                );
            }
        }

        @Nested
        @DisplayName("when processing a delivery against an instance with pending versions")
        class WhenProcessingADeliveryAgainstAnInstanceWithPendingVersions {

            @Test
            void a_pending_version_far_behind_the_window_is_applied() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L)));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 12));

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.commands()).containsExactly(new NotifyWarehouse("o1"))
                );
            }

            @Test
            void applying_a_pending_version_far_behind_the_window_removes_it_and_keeps_the_other_pending_versions_of_its_type() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L, 15L)));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 12));

                // Then
                assertThat(outcome.processed()).isTrue();
                assertThat(pendingOf(outcome.envelope(), STREAM, TYPE_A)).containsExactly(15L);
            }

            @Test
            void applying_a_pending_version_inside_the_window_removes_it_and_keeps_the_other_pending_versions_of_its_type() {
                // Given 29 is a late arrival that is new whether or not it is pending
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L, 29L)));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 29));

                // Then
                assertThat(pendingOf(outcome.envelope(), STREAM, TYPE_A)).containsExactly(12L);
            }

            @Test
            void applying_the_last_pending_version_of_a_type_leaves_that_type_with_nothing_pending() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L)));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 12));

                // Then
                assertThat(outcome.processed()).isTrue();
                assertThat(pendingOf(outcome.envelope(), STREAM, TYPE_A)).isEmpty();
            }

            @Test
            void applying_a_version_that_is_not_pending_leaves_every_pending_version_of_every_type_and_stream_where_it_was() {
                // Given
                Map<String, TypeMark> byType = Map.of(TYPE_A, new TypeMark(30, List.of(27L, 28L), 26), TYPE_B, new TypeMark(20, List.of(), 16));
                StreamMarks otherStream = new StreamMarks(10, byType, Map.of(TYPE_A, List.of(12L)));
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L, "s2", 30L), Map.of(
                        STREAM, new StreamMarks(10, byType, Map.of(TYPE_A, List.of(12L), TYPE_B, List.of(11L))),
                        "s2", otherStream));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 31));

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(outcome.envelope().streamMarks().get(STREAM).pending()).isEqualTo(Map.of(TYPE_A, List.of(12L), TYPE_B, List.of(11L))),
                        () -> assertThat(outcome.envelope().streamMarks().get("s2")).isEqualTo(otherStream)
                );
            }

            @Test
            void applying_a_version_of_one_type_leaves_a_pending_version_of_the_other_type_with_the_same_number() {
                // Given 29 is a late arrival of type A, and type B has its own 29 pending
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_B, List.of(29L)));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 29));

                // Then
                assertAll(
                        () -> assertThat(outcome.processed()).isTrue(),
                        () -> assertThat(pendingOf(outcome.envelope(), STREAM, TYPE_B)).containsExactly(29L)
                );
            }

            @Test
            void applying_a_pending_version_moves_neither_the_floor_nor_the_mark_nor_the_horizon() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L)));

                // When
                Outcome<OrderState, OrderCommand> outcome = apply(current, meta(TYPE_A, 12));

                // Then
                assertThat(outcome.processed()).isTrue();
                StreamMarks after = outcome.envelope().streamMarks().get(STREAM);
                assertAll(
                        () -> assertThat(after.floor()).isEqualTo(10L),
                        () -> assertThat(after.byType()).isEqualTo(marked().streamMarks().get(STREAM).byType()),
                        () -> assertThat(outcome.envelope().streamWatermarks()).isEqualTo(Map.of(STREAM, 30L))
                );
            }

            @Test
            void a_second_delivery_of_an_applied_pending_version_is_skipped() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L)));
                Outcome<OrderState, OrderCommand> first = apply(current, meta(TYPE_A, 12));
                assertThat(first.processed()).isTrue();

                // When
                Outcome<OrderState, OrderCommand> second = apply(first.envelope(), meta(TYPE_A, 12));

                // Then
                assertAll(
                        () -> assertThat(SagaExecutionSupport.classify(first.envelope(), meta(TYPE_A, 12), WINDOW)).isEqualTo(Delivery.BEHIND_WINDOW),
                        () -> assertThat(second.processed()).isFalse(),
                        () -> assertThat(second.commands()).isEmpty()
                );
            }

            @Test
            void a_version_that_is_not_pending_is_skipped_while_the_pending_one_beside_it_is_applied() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L)));

                // When
                Outcome<OrderState, OrderCommand> notPending = apply(current, meta(TYPE_A, 13));
                Outcome<OrderState, OrderCommand> pending = apply(current, meta(TYPE_A, 12));

                // Then
                assertAll(
                        () -> assertThat(notPending.processed()).isFalse(),
                        () -> assertThat(pending.processed()).isTrue()
                );
            }
        }

        @Nested
        @DisplayName("when recording that an input failed as pending")
        class WhenRecordingThatAnInputFailedAsPending {

            private final Instant later = NOW.plusSeconds(60);

            private FailureRecord<OrderState> recording(SagaEnvelope<OrderState> current, EventMeta meta) {
                return recording(current, meta, WINDOW);
            }

            private FailureRecord<OrderState> recording(SagaEnvelope<OrderState> current, EventMeta meta, int window) {
                return SagaExecutionSupport.pendingRecord(orderFulfillment(), "o1", current, meta, later, window);
            }

            /** Proves that a null answer below is about the case under test, since the same call for a late arrival that was not applied answers with a record. */
            private boolean anEligibleVersionIsRecorded() {
                return recording(marked(), meta(TYPE_A, 29)) != null;
            }

            private SagaEnvelope<OrderState> withStatus(SagaStatus status) {
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), status, 1, List.of(), Map.of(STREAM, 30L), null, NOW, NOW,
                        status == SagaStatus.COMPLETED ? NOW : null, null, true, null, marked().streamMarks());
            }

            @Test
            void a_late_arrival_that_the_instance_has_not_applied_is_recorded_as_pending_and_nothing_else_changes() {
                // Given
                SagaFailure otherFailure = new SagaFailure(STREAM + "@31", null, NOW, IllegalStateException.class.getName(), "boom");
                List<TimerEntry> timers = List.of(new TimerEntry(PAYMENT_TIMER, 123_000L));
                Map<String, TypeMark> byType = marked().streamMarks().get(STREAM).byType();
                SagaEnvelope<OrderState> current = new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 7, timers, Map.of(STREAM, 30L),
                        null, NOW, NOW, null, null, true, otherFailure, Map.of(STREAM, new StreamMarks(10, byType, Map.of(TYPE_B, List.of(11L)))));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 29));

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.quarantined()).isFalse(),
                        () -> assertThat(record.expectedVersion()).isEqualTo(7L),
                        () -> assertThat(record.envelope()).isEqualTo(new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 8, timers,
                                Map.of(STREAM, 30L), null, NOW, later, null, null, true, otherFailure,
                                Map.of(STREAM, new StreamMarks(10, byType, Map.of(TYPE_A, List.of(29L), TYPE_B, List.of(11L)), List.of(), Map.of(29L, later)))))
                );
            }

            @Test
            void a_version_of_a_type_the_instance_has_no_mark_for_is_recorded_however_far_it_is_behind_the_highest_version_of_the_stream() {
                // When
                FailureRecord<OrderState> record = recording(marked(), meta("org.example.C", 11));

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, "org.example.C")).containsExactly(11L);
            }

            @Test
            void a_version_above_the_highest_applied_of_its_type_is_recorded_too() {
                // When
                FailureRecord<OrderState> record = recording(marked(), meta(TYPE_A, 31));

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(31L);
            }

            @Test
            void the_pending_versions_stay_ascending_when_one_is_added_below_the_ones_already_there() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(31L, 35L)));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 29));

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(29L, 31L, 35L);
            }

            @Test
            void the_pending_versions_stay_ascending_when_one_is_added_between_the_ones_already_there() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(29L, 35L)));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 31));

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(29L, 31L, 35L);
            }

            @Test
            void a_version_of_a_type_is_recorded_under_that_type_and_leaves_the_pending_versions_of_other_types_and_streams_alone() {
                // Given
                Map<String, TypeMark> byType = marked().streamMarks().get(STREAM).byType();
                StreamMarks otherStream = new StreamMarks(10, byType, Map.of(TYPE_A, List.of(12L)));
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L, "s2", 30L), Map.of(
                        STREAM, new StreamMarks(10, byType, Map.of(TYPE_B, List.of(11L))),
                        "s2", otherStream));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 29));

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.envelope().streamMarks().get(STREAM).pending()).isEqualTo(Map.of(TYPE_A, List.of(29L), TYPE_B, List.of(11L))),
                        () -> assertThat(record.envelope().streamMarks().get("s2")).isEqualTo(otherStream)
                );
            }

            @Test
            void every_pending_version_stays_pending_when_there_are_more_than_the_window() {
                // Given a window of 3, a mark at 30 with nothing applied below it, and 28 and 29 being new
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20)), Map.of(TYPE_A, List.of(5L, 6L, 28L)))));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 29), 3);

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L, 6L, 28L, 29L);
            }

            @Test
            void nothing_is_dropped_when_the_pending_versions_just_fit_in_the_window() {
                // Given
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20)), Map.of(TYPE_A, List.of(5L, 6L)))));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 29), 3);

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L, 6L, 29L);
            }

            @Test
            void a_list_that_grows_past_the_window_leaves_the_list_of_another_type_alone() {
                // Given
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20), TYPE_B, new TypeMark(20, List.of(), 10)),
                        Map.of(TYPE_A, List.of(5L, 6L, 7L), TYPE_B, List.of(1L, 2L, 3L)))));

                // When
                FailureRecord<OrderState> record = recording(current, meta(TYPE_A, 29), 3);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L, 6L, 7L, 29L),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_B)).containsExactly(1L, 2L, 3L)
                );
            }

            @Test
            void an_event_without_a_cloud_event_type_is_recorded_under_the_empty_type() {
                // Given
                SagaEnvelope<OrderState> current = envelope(Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10,
                        Map.of("", new TypeMark(30, List.of(), 26)))));

                // When
                FailureRecord<OrderState> record = recording(current, new EventMeta(STREAM, 29L, null));

                // Then
                assertThat(record).isNotNull();
                assertThat(record.envelope().streamMarks().get(STREAM).pending()).isEqualTo(Map.of("", List.of(29L)));
            }

            @Test
            void a_first_event_that_failed_inserts_an_active_instance_that_has_not_started_with_the_version_pending() {
                // When
                FailureRecord<OrderState> record = recording(null, meta(TYPE_A, 1));

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.quarantined()).isFalse(),
                        () -> assertThat(record.expectedVersion()).isZero(),
                        () -> assertThat(record.envelope()).isEqualTo(new SagaEnvelope<>("o1", orderFulfillment().initialState(), SagaStatus.ACTIVE, 1,
                                List.of(), Map.of(), null, later, later, null, null, false, null,
                                Map.of(STREAM, new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of(TYPE_A, List.of(1L)), List.of(), Map.of(1L, later)))))
                );
            }

            @Test
            void a_first_event_without_a_cloud_event_type_is_inserted_with_the_version_pending_under_the_empty_type() {
                // When
                FailureRecord<OrderState> record = recording(null, new EventMeta(STREAM, 1L, null));

                // Then
                assertThat(record).isNotNull();
                assertThat(record.envelope().streamMarks().get(STREAM).pending()).isEqualTo(Map.of("", List.of(1L)));
            }

            @Test
            void an_input_without_a_stream_id_is_not_recorded() {
                // When
                FailureRecord<OrderState> record = recording(marked(), new EventMeta(null, 29L, null, TYPE_A));

                // Then
                assertAll(
                        () -> assertThat(record).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void an_input_without_a_stream_version_is_not_recorded() {
                // When
                FailureRecord<OrderState> record = recording(marked(), new EventMeta(STREAM, null, null, TYPE_A));

                // Then
                assertAll(
                        () -> assertThat(record).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void a_timer_input_is_not_recorded() {
                // When
                FailureRecord<OrderState> record = recording(marked(), EventMeta.NONE);

                // Then
                assertAll(
                        () -> assertThat(record).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void a_completed_instance_is_not_recorded_on() {
                // When
                FailureRecord<OrderState> record = recording(withStatus(SagaStatus.COMPLETED), meta(TYPE_A, 29));

                // Then
                assertAll(
                        () -> assertThat(record).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void a_quarantined_instance_is_not_recorded_on() {
                // When
                FailureRecord<OrderState> record = recording(withStatus(SagaStatus.QUARANTINED), meta(TYPE_A, 29));

                // Then
                assertAll(
                        () -> assertThat(record).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void a_version_the_instance_has_applied_is_not_recorded() {
                // When
                FailureRecord<OrderState> applied = recording(marked(), meta(TYPE_A, 28));
                FailureRecord<OrderState> theMark = recording(marked(), meta(TYPE_A, 30));
                FailureRecord<OrderState> belowTheFloor = recording(marked(), meta(TYPE_A, 10));

                // Then
                assertAll(
                        () -> assertThat(applied).isNull(),
                        () -> assertThat(theMark).isNull(),
                        () -> assertThat(belowTheFloor).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void a_version_behind_the_window_that_is_not_pending_is_not_recorded() {
                // When
                FailureRecord<OrderState> record = recording(marked(), meta(TYPE_A, 26));

                // Then
                assertAll(
                        () -> assertThat(record).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }

            @Test
            void a_version_that_is_pending_already_is_not_recorded_again() {
                // Given
                SagaEnvelope<OrderState> current = markedWithPending(Map.of(TYPE_A, List.of(12L, 29L)));

                // When
                FailureRecord<OrderState> farBehind = recording(current, meta(TYPE_A, 12));
                FailureRecord<OrderState> insideTheWindow = recording(current, meta(TYPE_A, 29));

                // Then
                assertAll(
                        () -> assertThat(farBehind).isNull(),
                        () -> assertThat(insideTheWindow).isNull(),
                        () -> assertThat(anEligibleVersionIsRecorded()).isTrue()
                );
            }
        }

        @Nested
        @DisplayName("when an input fails and the instance is quarantined after a budget")
        class WhenAnInputFailsAndTheInstanceIsQuarantinedAfterABudget {

            private final Duration budget = Duration.ofMinutes(5);
            private final Throwable failure = new IllegalStateException("boom");

            private FailureRecord<OrderState> failing(SagaEnvelope<OrderState> current, EventMeta meta, Instant now) {
                return SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta, failure, now, budget, WINDOW);
            }

            private SagaEnvelope<OrderState> failingOn(String input, SagaEnvelope<OrderState> current) {
                SagaFailure record = new SagaFailure(input, null, NOW, IllegalStateException.class.getName(), "boom");
                return new SagaEnvelope<>("o1", current.state(), SagaStatus.ACTIVE, 1, List.of(), current.streamWatermarks(), null,
                        NOW, NOW, null, null, true, record, current.streamMarks());
            }

            @Test
            void the_first_failure_of_a_version_carries_the_version_in_pending_and_keeps_the_rest_of_the_marks() {
                // Given
                SagaEnvelope<OrderState> current = marked();

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW);

                // Then
                assertThat(record).isNotNull();
                StreamMarks marks = record.envelope().streamMarks().get(STREAM);
                assertAll(
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@31"),
                        () -> assertThat(marks.pending()).isEqualTo(Map.of(TYPE_A, List.of(31L))),
                        () -> assertThat(marks.floor()).isEqualTo(10L),
                        () -> assertThat(marks.byType()).isEqualTo(current.streamMarks().get(STREAM).byType())
                );
            }

            @Test
            void a_late_arrival_that_fails_carries_the_version_in_pending() {
                // When
                FailureRecord<OrderState> record = failing(marked(), meta(TYPE_A, 29), NOW);

                // Then
                assertThat(record).isNotNull();
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(29L);
            }

            @Test
            void a_different_input_that_fails_later_records_its_own_first_instant_and_keeps_the_version_pending_from_before() {
                // Given the instance is failing on version 31 since NOW and holds it as pending
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@31", markedWithPending(Map.of(TYPE_A, List.of(31L)), Map.of(31L, NOW)));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 29), NOW.plusSeconds(1));

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@29"),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(NOW.plusSeconds(1)),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(29L, 31L),
                        () -> assertThat(record.envelope().streamMarks().get(STREAM).pendingSince()).isEqualTo(Map.of(31L, NOW, 29L, NOW.plusSeconds(1)))
                );
            }

            @Test
            void the_record_that_quarantines_the_instance_carries_the_version_in_pending() {
                // Given
                SagaEnvelope<OrderState> current = failing(marked(), meta(TYPE_A, 31), NOW).envelope();

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW.plus(budget));

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(31L)
                );
            }

            @Test
            void the_record_that_quarantines_the_instance_does_not_list_a_version_that_is_pending_already_twice() {
                // Given the instance holds 31 as pending and has been failing on it since NOW
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@31", markedWithPending(Map.of(TYPE_A, List.of(31L))));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW.plus(budget));

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(31L)
                );
            }

            @Test
            void a_first_event_that_fails_inserts_a_record_with_the_version_pending_on_a_stream_the_instance_has_not_seen() {
                // When
                FailureRecord<OrderState> record = failing(null, meta(TYPE_A, 1), NOW);

                // Then
                assertThat(record).isNotNull();
                assertThat(record.envelope().streamMarks()).isEqualTo(
                        Map.of(STREAM, new StreamMarks(Long.MIN_VALUE, Map.of(), Map.of(TYPE_A, List.of(1L)), List.of(), Map.of(1L, NOW))));
            }
        }

        @Nested
        @TestInstance(TestInstance.Lifecycle.PER_CLASS)
        @DisplayName("when a failure record from before pending existed is replaced by another input")
        class WhenAFailureRecordFromBeforePendingExistedIsReplacedByAnotherInput {

            private static final int NARROW_WINDOW = 2;

            private final Duration budget = Duration.ofMinutes(5);
            private final Throwable failure = new IllegalStateException("boom");

            private ListAppender<ILoggingEvent> appender;
            private ch.qos.logback.classic.Logger logger;

            @BeforeEach
            void attachAppender() {
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                logger = context.getLogger(SagaExecutionSupport.class);
                appender = new ListAppender<>();
                appender.start();
                logger.addAppender(appender);
            }

            @AfterEach
            void detachAppender() {
                logger.detachAppender(appender);
            }

            Stream<Arguments> storedShapes() {
                return Stream.of(false, true).flatMap(withMarks -> Stream.of(4, 2).map(window -> Arguments.of(withMarks, window)));
            }

            Stream<Arguments> storedShapesAndTypes() {
                return Stream.of(false, true).flatMap(withMarks -> Stream.of(4, 2).flatMap(window ->
                        Stream.of(TYPE_A, null).map(type -> Arguments.of(withMarks, window, type))));
            }

            private SagaEnvelope<OrderState> failingOn(String input, Instant firstFailedAt, Map<String, Long> streamWatermarks,
                                                      Map<String, StreamMarks> streamMarks) {
                SagaFailure record = new SagaFailure(input, null, firstFailedAt, IllegalStateException.class.getName(), "boom");
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(), streamWatermarks, null,
                        NOW, NOW, null, null, true, record, streamMarks);
            }

            /** An instance stored before pending existed. It is failing on s1@5 with nothing pending and a watermark of 4, with or without marks of floor 4. */
            private SagaEnvelope<OrderState> storedBeforePending(boolean withMarks) {
                Map<String, StreamMarks> marks = withMarks ? Map.of(STREAM, new StreamMarks(4, Map.of())) : Map.of();
                return failingOn(STREAM + "@5", NOW, Map.of(STREAM, 4L), marks);
            }

            /** The same, with a watermark and a floor of 30, so version 5 is well below the floor. */
            private SagaEnvelope<OrderState> storedBeforePendingBelowTheFloor() {
                return failingOn(STREAM + "@5", NOW, Map.of(STREAM, 30L),
                        Map.of(STREAM, new StreamMarks(30, Map.of(TYPE_A, new TypeMark(30, List.of(), 26)))));
            }

            /** The given versions of s1 are known to have failed with an event type that is not known, and type A is applied up to 32. */
            private SagaEnvelope<OrderState> withLegacyFailed(List<Long> legacyFailed) {
                return envelope(Map.of(STREAM, 32L), Map.of(STREAM, new StreamMarks(4,
                        Map.of(TYPE_A, new TypeMark(32, List.of(), 28)), Map.of(), legacyFailed)));
            }

            private FailureRecord<OrderState> failing(SagaEnvelope<OrderState> current, EventMeta meta, Instant now, int window) {
                return SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta, failure, now, budget, window);
            }

            private SagaEnvelope<OrderState> applying(SagaEnvelope<OrderState> current, EventMeta meta, int window) {
                Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(orderFulfillment(), "o1", current,
                        SagaInput.event(new OrderPlaced("o1", 100)), meta, NOW, window);
                assertThat(outcome.processed()).isTrue();
                return outcome.envelope();
            }

            private List<Long> legacyFailedOf(SagaEnvelope<OrderState> envelope, String stream) {
                StreamMarks marks = envelope.streamMarks().get(stream);
                return marks == null ? List.of() : marks.legacyFailed();
            }

            private List<String> errors() {
                return appender.list.stream()
                        .filter(event -> event.getLevel() == Level.ERROR)
                        .map(ILoggingEvent::getFormattedMessage)
                        .toList();
            }

            @ParameterizedTest(name = "stored with marks {0}, window {1}")
            @MethodSource("storedShapes")
            @DisplayName("keeps the version the replaced record named as a failure of unknown type")
            void keeps_the_version_the_replaced_record_named_as_a_failure_of_unknown_type(boolean withMarks, int window) {
                // Given
                SagaEnvelope<OrderState> current = storedBeforePending(withMarks);

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, window);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@31"),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).containsExactly(5L),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(31L)
                );
            }

            @ParameterizedTest(name = "stored with marks {0}, window {1}, type {2}")
            @MethodSource("storedShapesAndTypes")
            @DisplayName("treats the kept version as new when the instance is far ahead of it")
            void treats_the_kept_version_as_new_when_the_instance_is_far_ahead_of_it(boolean withMarks, int window, String type) {
                // Given version 32 applied after 31 failed, so version 5 is 27 behind the highest applied
                SagaEnvelope<OrderState> failed = failing(storedBeforePending(withMarks), meta(type, 31), NOW, window).envelope();
                SagaEnvelope<OrderState> ahead = applying(failed, meta(type, 32), window);

                // When
                Delivery delivery = SagaExecutionSupport.classify(ahead, meta(type, 5), window);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @ParameterizedTest(name = "stored with marks {0}")
            @ValueSource(booleans = {false, true})
            @DisplayName("still skips another version behind the window")
            void still_skips_another_version_behind_the_window(boolean withMarks) {
                // Given
                SagaEnvelope<OrderState> failed = failing(storedBeforePending(withMarks), meta(TYPE_A, 31), NOW, WINDOW).envelope();
                SagaEnvelope<OrderState> ahead = applying(failed, meta(TYPE_A, 32), WINDOW);

                // When
                Delivery delivery = SagaExecutionSupport.classify(ahead, meta(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.BEHIND_WINDOW);
            }

            @ParameterizedTest(name = "type {0}")
            @NullSource
            @ValueSource(strings = TYPE_A)
            @DisplayName("treats the kept version as new when it is at or below the floor")
            void treats_the_kept_version_as_new_when_it_is_at_or_below_the_floor(String type) {
                // Given
                SagaEnvelope<OrderState> failed = failing(storedBeforePendingBelowTheFloor(), meta(type, 31), NOW, WINDOW).envelope();

                // When
                Delivery delivery = SagaExecutionSupport.classify(failed, meta(type, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void another_version_at_or_below_the_floor_is_still_a_redelivery() {
                // Given
                SagaEnvelope<OrderState> failed = failing(storedBeforePendingBelowTheFloor(), meta(TYPE_A, 31), NOW, WINDOW).envelope();

                // When
                Delivery delivery = SagaExecutionSupport.classify(failed, meta(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void a_record_whose_input_is_pending_already_under_the_same_type_keeps_nothing() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW, Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 26)), Map.of(TYPE_A, List.of(5L)))));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).isEmpty(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L, 31L)
                );
            }

            @Test
            void a_record_whose_input_is_pending_already_under_another_type_keeps_nothing() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW, Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(10,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 26)), Map.of(TYPE_B, List.of(5L)))));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).isEmpty(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_B)).containsExactly(5L)
                );
            }

            @ParameterizedTest(name = "record input {0}")
            @ValueSource(strings = {"position:7", "nothing-with-at", "s1@notanumber", "s1@"})
            @DisplayName("keeps nothing and does not throw when the replaced record does not name a stream version")
            void keeps_nothing_and_does_not_throw_when_the_replaced_record_does_not_name_a_stream_version(String input) {
                // Given
                SagaEnvelope<OrderState> current = failingOn(input, NOW, Map.of(STREAM, 30L), Map.of());

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@31"),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).isEmpty(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(31L)
                );
            }

            @Test
            void a_record_naming_a_version_of_another_stream_is_kept_under_that_stream() {
                // Given
                SagaEnvelope<OrderState> current = failingOn("s2@5", NOW, Map.of(STREAM, 30L), Map.of());

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(legacyFailedOf(record.envelope(), "s2")).containsExactly(5L),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).isEmpty()
                );
            }

            private static final String EMPTY_STREAM = "";

            private EventMeta onEmptyStream(String type, long version) {
                return new EventMeta(EMPTY_STREAM, version, null, type);
            }

            /** An instance stored before pending existed. It is failing on the redelivery key @5 of the empty stream id, with a watermark of 4 for that stream. */
            private SagaEnvelope<OrderState> storedBeforePendingOnEmptyStream() {
                return failingOn(EMPTY_STREAM + "@5", NOW, Map.of(EMPTY_STREAM, 4L), Map.of());
            }

            /** The same, with a watermark and a floor of 30, so version 5 is well below the floor. */
            private SagaEnvelope<OrderState> storedBeforePendingOnEmptyStreamBelowTheFloor() {
                return failingOn(EMPTY_STREAM + "@5", NOW, Map.of(EMPTY_STREAM, 30L),
                        Map.of(EMPTY_STREAM, new StreamMarks(30, Map.of(TYPE_A, new TypeMark(30, List.of(), 26)))));
            }

            /** The given versions of the empty stream id are known to have failed with an event type that is not known, and type A is applied up to 32. */
            private SagaEnvelope<OrderState> withLegacyFailedOnEmptyStream(List<Long> legacyFailed) {
                return envelope(Map.of(EMPTY_STREAM, 32L), Map.of(EMPTY_STREAM, new StreamMarks(4,
                        Map.of(TYPE_A, new TypeMark(32, List.of(), 28)), Map.of(), legacyFailed)));
            }

            @ParameterizedTest(name = "replaced by a failure on stream \"{0}\"")
            @ValueSource(strings = {EMPTY_STREAM, STREAM})
            @DisplayName("keeps the version a record naming the empty stream id named as a failure of unknown type")
            void keeps_the_version_a_record_naming_the_empty_stream_id_named_as_a_failure_of_unknown_type(String replacingStream) {
                // Given
                SagaEnvelope<OrderState> current = storedBeforePendingOnEmptyStream();

                // When
                FailureRecord<OrderState> record = failing(current, new EventMeta(replacingStream, 31L, null, TYPE_A), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.envelope().failure().input()).isEqualTo(replacingStream + "@31"),
                        () -> assertThat(legacyFailedOf(record.envelope(), EMPTY_STREAM)).containsExactly(5L)
                );
            }

            @ParameterizedTest(name = "type {0}")
            @NullSource
            @ValueSource(strings = TYPE_A)
            @DisplayName("treats the kept version of the empty stream id as new when the instance is far ahead of it")
            void treats_the_kept_version_of_the_empty_stream_id_as_new_when_the_instance_is_far_ahead_of_it(String type) {
                // Given version 32 applied after 31 failed, so version 5 is 27 behind the highest applied
                SagaEnvelope<OrderState> failed = failing(storedBeforePendingOnEmptyStream(), onEmptyStream(type, 31), NOW, WINDOW).envelope();
                SagaEnvelope<OrderState> ahead = applying(failed, onEmptyStream(type, 32), WINDOW);

                // When
                Delivery delivery = SagaExecutionSupport.classify(ahead, onEmptyStream(type, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void another_version_of_the_empty_stream_id_behind_the_window_is_still_skipped() {
                // Given
                SagaEnvelope<OrderState> failed = failing(storedBeforePendingOnEmptyStream(), onEmptyStream(TYPE_A, 31), NOW, WINDOW).envelope();
                SagaEnvelope<OrderState> ahead = applying(failed, onEmptyStream(TYPE_A, 32), WINDOW);

                // When
                Delivery delivery = SagaExecutionSupport.classify(ahead, onEmptyStream(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.BEHIND_WINDOW);
            }

            @ParameterizedTest(name = "type {0}")
            @NullSource
            @ValueSource(strings = TYPE_A)
            @DisplayName("treats the kept version of the empty stream id as new when it is at or below the floor")
            void treats_the_kept_version_of_the_empty_stream_id_as_new_when_it_is_at_or_below_the_floor(String type) {
                // Given
                SagaEnvelope<OrderState> failed = failing(storedBeforePendingOnEmptyStreamBelowTheFloor(), onEmptyStream(type, 31), NOW, WINDOW).envelope();

                // When
                Delivery delivery = SagaExecutionSupport.classify(failed, onEmptyStream(type, 5), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void another_version_of_the_empty_stream_id_at_or_below_the_floor_is_still_a_redelivery() {
                // Given
                SagaEnvelope<OrderState> failed = failing(storedBeforePendingOnEmptyStreamBelowTheFloor(), onEmptyStream(TYPE_A, 31), NOW, WINDOW).envelope();

                // When
                Delivery delivery = SagaExecutionSupport.classify(failed, onEmptyStream(TYPE_A, 6), WINDOW);

                // Then
                assertThat(delivery).isEqualTo(Delivery.REDELIVERY);
            }

            @Test
            void applying_a_kept_version_of_the_empty_stream_id_removes_it_and_keeps_the_others() {
                // Given
                SagaEnvelope<OrderState> current = withLegacyFailedOnEmptyStream(List.of(5L, 6L));

                // When
                SagaEnvelope<OrderState> applied = applying(current, onEmptyStream(TYPE_A, 5), WINDOW);

                // Then
                assertThat(legacyFailedOf(applied, EMPTY_STREAM)).containsExactly(6L);
            }

            @Test
            void a_kept_version_of_the_empty_stream_id_that_fails_again_with_its_type_known_moves_to_pending() {
                // Given
                SagaEnvelope<OrderState> current = withLegacyFailedOnEmptyStream(List.of(5L, 6L));

                // When
                FailureRecord<OrderState> record = failing(current, onEmptyStream(TYPE_A, 5), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(pendingOf(record.envelope(), EMPTY_STREAM, TYPE_A)).containsExactly(5L),
                        () -> assertThat(legacyFailedOf(record.envelope(), EMPTY_STREAM)).containsExactly(6L)
                );
            }

            @Test
            void applying_a_kept_version_removes_it_and_keeps_the_others() {
                // Given
                SagaEnvelope<OrderState> current = withLegacyFailed(List.of(5L, 6L));

                // When
                SagaEnvelope<OrderState> applied = applying(current, meta(TYPE_A, 5), WINDOW);

                // Then
                assertThat(legacyFailedOf(applied, STREAM)).containsExactly(6L);
            }

            @Test
            void applying_another_version_keeps_every_kept_version() {
                // Given a late arrival that is new whether or not it is kept
                SagaEnvelope<OrderState> current = withLegacyFailed(List.of(5L, 6L));

                // When
                SagaEnvelope<OrderState> applied = applying(current, meta(TYPE_A, 31), WINDOW);

                // Then
                assertThat(legacyFailedOf(applied, STREAM)).containsExactly(5L, 6L);
            }

            @Test
            void a_kept_version_that_fails_again_with_its_type_known_moves_to_pending() {
                // Given
                SagaEnvelope<OrderState> current = withLegacyFailed(List.of(5L, 6L));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 5), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).containsExactly(6L)
                );
            }

            @Test
            void a_kept_version_that_the_failure_record_names_and_fails_again_moves_to_pending() {
                // Given
                SagaEnvelope<OrderState> kept = withLegacyFailed(List.of(5L));
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW, kept.streamWatermarks(), kept.streamMarks());

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 5), NOW, WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(5L),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).isEmpty()
                );
            }

            @Test
            void every_kept_version_stays_and_no_error_is_logged_when_more_than_the_window_would_be_kept() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW, Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20)), Map.of(), List.of(3L, 4L))));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, NARROW_WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).containsExactly(3L, 4L, 5L),
                        () -> assertThat(errors()).isEmpty()
                );
            }

            @Test
            void nothing_is_dropped_or_logged_when_the_kept_versions_just_fit_the_window() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@5", NOW, Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20)), Map.of(), List.of(4L))));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW, NARROW_WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).containsExactly(4L, 5L),
                        () -> assertThat(errors()).isEmpty()
                );
            }

            @Test
            void the_replacement_that_quarantines_the_instance_keeps_the_version_as_well() {
                // Given the instance has been failing on s1@5 since NOW, and version 31 is pending since NOW
                SagaEnvelope<OrderState> current = SagaExecutionSupport.pendingRecord(orderFulfillment(), "o1", storedBeforePending(false), meta(TYPE_A, 31), NOW, WINDOW).envelope();

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW.plus(budget), WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@31"),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).containsExactly(5L)
                );
            }

            @Test
            void the_record_that_quarantines_the_instance_on_the_input_it_names_keeps_nothing_since_the_input_becomes_pending() {
                // Given the instance has been failing on s1@31 for as long as the budget
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@31", NOW, Map.of(STREAM, 30L), Map.of());

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW.plus(budget), WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).isEmpty(),
                        () -> assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(31L)
                );
            }

            @Test
            void the_record_that_quarantines_the_instance_on_the_input_it_names_does_not_push_a_kept_version_out_of_a_full_list() {
                // Given
                SagaEnvelope<OrderState> current = failingOn(STREAM + "@31", NOW, Map.of(STREAM, 30L), Map.of(STREAM, new StreamMarks(0,
                        Map.of(TYPE_A, new TypeMark(30, List.of(), 20)), Map.of(), List.of(3L, 4L))));

                // When
                FailureRecord<OrderState> record = failing(current, meta(TYPE_A, 31), NOW.plus(budget), NARROW_WINDOW);

                // Then
                assertThat(record).isNotNull();
                assertAll(
                        () -> assertThat(legacyFailedOf(record.envelope(), STREAM)).containsExactly(3L, 4L),
                        () -> assertThat(errors()).isEmpty()
                );
            }
        }

        @Nested
        @DisplayName("when each input runs its own quarantine budget")
        class WhenEachInputRunsItsOwnQuarantineBudget {

            private final Duration budget = Duration.ofMinutes(5);
            private final Throwable failure = new IllegalStateException("boom");

            private Instant after(long minutes) {
                return NOW.plus(Duration.ofMinutes(minutes));
            }

            private FailureRecord<OrderState> failing(SagaEnvelope<OrderState> current, long version, Instant now) {
                return failing(current, version, now, WINDOW);
            }

            private FailureRecord<OrderState> failing(SagaEnvelope<OrderState> current, long version, Instant now, int window) {
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta(TYPE_A, version), failure, now, budget, window);
                assertThat(record).as("a record for version %s", version).isNotNull();
                return record;
            }

            /** The input succeeds, which clears the failure record when it names that input. */
            private SagaEnvelope<OrderState> succeeding(SagaEnvelope<OrderState> current, long version, int window) {
                Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(orderFulfillment(), "o1", current,
                        SagaInput.event(new OrderPlaced("o1", 100)), meta(TYPE_A, version), NOW, window);
                assertThat(outcome.processed()).as("version %s is processed", version).isTrue();
                return outcome.envelope();
            }

            private SagaEnvelope<OrderState> succeeding(SagaEnvelope<OrderState> current, long version) {
                return succeeding(current, version, WINDOW);
            }

            @Test
            void an_input_that_fails_for_the_first_time_is_not_quarantined_however_long_another_input_has_been_parked() {
                // Given version 7 failed at the start, so it has been pending for longer than the budget
                SagaEnvelope<OrderState> parked = failing(null, 7, after(0)).envelope();

                // When
                FailureRecord<OrderState> record = failing(parked, 8, after(6));

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isFalse(),
                        () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.ACTIVE),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@8"),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(after(6))
                );
            }

            @Test
            void a_pending_input_that_is_replayed_after_the_budget_has_passed_is_quarantined_on_its_first_failure_of_the_replay() {
                // Given the instance started at version 1, version 7 failed and was parked, version 8 failed and then succeeded, which cleared the record
                SagaEnvelope<OrderState> started = succeeding(null, 1);
                SagaEnvelope<OrderState> parked = failing(started, 7, after(0)).envelope();
                SagaEnvelope<OrderState> failedOn8 = failing(parked, 8, after(1)).envelope();
                SagaEnvelope<OrderState> cleared = succeeding(failedOn8, 8);
                assertThat(cleared.failure()).as("the record is cleared").isNull();

                // When
                FailureRecord<OrderState> record = failing(cleared, 7, after(5));

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(record.envelope().status()).isEqualTo(SagaStatus.QUARANTINED),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@7"),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(after(0))
                );
            }

            @Test
            void a_pending_input_that_is_replayed_before_the_budget_has_passed_is_not_quarantined() {
                // Given
                SagaEnvelope<OrderState> started = succeeding(null, 1);
                SagaEnvelope<OrderState> parked = failing(started, 7, after(0)).envelope();
                SagaEnvelope<OrderState> cleared = succeeding(failing(parked, 8, after(1)).envelope(), 8);

                // When
                FailureRecord<OrderState> record = failing(cleared, 7, after(5).minusSeconds(1));

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isFalse(),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(after(0))
                );
            }

            /** An instance stored before pending existed. It is failing on s1@5 since NOW with nothing pending and a watermark of 4. */
            private SagaEnvelope<OrderState> storedBeforePending() {
                SagaFailure record = new SagaFailure(STREAM + "@5", null, NOW, IllegalStateException.class.getName(), "boom");
                return new SagaEnvelope<>("o1", new AwaitingPayment("o1"), SagaStatus.ACTIVE, 1, List.of(), Map.of(STREAM, 4L), null,
                        NOW, NOW, null, null, true, record, Map.of());
            }

            @Test
            void an_input_whose_failure_record_was_replaced_by_another_input_keeps_its_own_instant_when_it_fails_again() {
                // Given s1@5 has failed since NOW, then s1@31 failed and replaced its record, and s1@31 then succeeded
                SagaEnvelope<OrderState> replaced = failing(storedBeforePending(), 31, after(1)).envelope();
                SagaEnvelope<OrderState> cleared = succeeding(replaced, 31);
                assertThat(cleared.failure()).as("the record is cleared").isNull();

                // When
                FailureRecord<OrderState> record = failing(cleared, 5, after(5));

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@5"),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(NOW)
                );
            }

            @Test
            void an_input_whose_failure_record_was_replaced_by_another_input_is_not_quarantined_by_the_replacing_input() {
                // Given s1@5 has failed since NOW and s1@31 fails for the first time after the budget has passed
                SagaEnvelope<OrderState> current = storedBeforePending();

                // When
                FailureRecord<OrderState> record = failing(current, 31, after(6));

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isFalse(),
                        () -> assertThat(record.envelope().failure().input()).isEqualTo("s1@31"),
                        () -> assertThat(record.envelope().failure().firstFailedAt()).isEqualTo(after(6))
                );
            }

            @Test
            void a_version_that_fails_again_keeps_the_instant_it_first_failed() {
                // Given versions 7 and 8 failed in turn
                SagaEnvelope<OrderState> failedOn8 = failing(failing(null, 7, after(0)).envelope(), 8, after(1)).envelope();

                // When
                FailureRecord<OrderState> record = failing(failedOn8, 7, after(2));

                // Then
                assertThat(record.envelope().streamMarks().get(STREAM).pendingSince()).isEqualTo(Map.of(7L, after(0), 8L, after(1)));
            }

            @Test
            void applying_a_pending_version_removes_its_instant_and_keeps_the_others() {
                // Given versions 7 and 8 failed in turn
                SagaEnvelope<OrderState> failedOn8 = failing(failing(null, 7, after(0)).envelope(), 8, after(1)).envelope();

                // When
                SagaEnvelope<OrderState> applied = succeeding(failedOn8, 7);

                // Then
                assertThat(applied.streamMarks().get(STREAM).pendingSince()).isEqualTo(Map.of(8L, after(1)));
            }

            @Test
            void both_versions_that_failed_are_pending_at_a_window_of_one() {
                // Given a window of 1
                SagaEnvelope<OrderState> failedOn7 = failing(null, 7, after(0), 1).envelope();

                // When
                FailureRecord<OrderState> record = failing(failedOn7, 8, after(1), 1);

                // Then
                assertThat(pendingOf(record.envelope(), STREAM, TYPE_A)).containsExactly(7L, 8L);
            }

            @Test
            void a_replay_of_the_first_failed_version_is_applied_after_the_instance_moved_far_past_it_at_a_window_of_one() {
                // Given a window of 1, the instance started at version 1, versions 7 and 8 failed, and the instance then applied version 20
                SagaEnvelope<OrderState> started = succeeding(null, 1, 1);
                SagaEnvelope<OrderState> failedOn8 = failing(failing(started, 7, after(0), 1).envelope(), 8, after(1), 1).envelope();
                SagaEnvelope<OrderState> ahead = succeeding(failedOn8, 20, 1);

                // When
                Delivery delivery = SagaExecutionSupport.classify(ahead, meta(TYPE_A, 7), 1);

                // Then
                assertThat(delivery).isEqualTo(Delivery.NEW);
            }

            @Test
            void the_replay_of_the_first_failed_version_at_a_window_of_one_is_processed() {
                // Given
                SagaEnvelope<OrderState> started = succeeding(null, 1, 1);
                SagaEnvelope<OrderState> failedOn8 = failing(failing(started, 7, after(0), 1).envelope(), 8, after(1), 1).envelope();
                SagaEnvelope<OrderState> ahead = succeeding(failedOn8, 20, 1);

                // When
                Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(orderFulfillment(), "o1", ahead,
                        SagaInput.event(new OrderPlaced("o1", 100)), meta(TYPE_A, 7), NOW, 1);

                // Then
                assertThat(outcome.processed()).isTrue();
            }
        }
    }
}
