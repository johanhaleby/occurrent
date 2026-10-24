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
                // Given
                SagaEnvelope<OrderState> afterFirst = apply(null, new EventMeta(STREAM, 10L, null, TYPE_A)).envelope();

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
        @DisplayName("when an input fails")
        class WhenAnInputFails {

            private final Duration budget = Duration.ofMinutes(5);
            private final Throwable failure = new IllegalStateException("boom");

            private FailureRecord<OrderState> failing(SagaEnvelope<OrderState> current, EventMeta meta) {
                return SagaExecutionSupport.onFailure(orderFulfillment(), "o1", current, meta, failure, NOW, budget, WINDOW);
            }

            @Test
            void a_new_version_produces_a_record_that_keeps_the_marks_and_the_watermarks_untouched() {
                // When
                FailureRecord<OrderState> record = failing(marked(), meta(TYPE_A, 31));

                // Then
                assertAll(
                        () -> assertThat(record).isNotNull(),
                        () -> assertThat(record.envelope().streamMarks()).isEqualTo(marked().streamMarks()),
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
                        () -> assertThat(record.envelope().streamMarks()).isEqualTo(marked().streamMarks())
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
            void quarantining_keeps_the_marks_untouched() {
                // Given an instance that has been failing for longer than the budget
                SagaEnvelope<OrderState> failingSince = failing(marked(), meta(TYPE_A, 31)).envelope();

                // When
                FailureRecord<OrderState> record = SagaExecutionSupport.onFailure(orderFulfillment(), "o1", failingSince, meta(TYPE_A, 31), failure,
                        NOW.plus(budget), budget, WINDOW);

                // Then
                assertAll(
                        () -> assertThat(record.quarantined()).isTrue(),
                        () -> assertThat(record.envelope().streamMarks()).isEqualTo(marked().streamMarks()),
                        () -> assertThat(record.envelope().streamWatermarks()).isEqualTo(marked().streamWatermarks())
                );
            }

            @Test
            void a_first_event_that_fails_inserts_a_record_with_no_marks() {
                // When
                FailureRecord<OrderState> record = failing(null, meta(TYPE_A, 1));

                // Then
                assertThat(record.envelope().streamMarks()).isEmpty();
            }
        }
    }
}
