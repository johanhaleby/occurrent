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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.Outcome;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

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
                    orderFulfillment(), "o1", null, SagaInput.timeout(new SagaTimeout("o1", PAYMENT_TIMER)), EventMeta.NONE, NOW);

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
                    orderFulfillment(), "o1", completed(new Cancelled("o1")), SagaInput.timeout(new SagaTimeout("o1", PAYMENT_TIMER)), EventMeta.NONE, NOW);

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
        void skips_a_positioned_event_at_or_below_the_stored_position_watermark() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of(), 5L);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta(null, null, 5L), NOW);

            assertThat(outcome.processed()).isFalse();
        }

        @Test
        void processes_a_positioned_event_above_the_stored_position_watermark_and_advances_it() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1, List.of(), Map.of(), 5L);

            Outcome<OrderState, OrderCommand> outcome = SagaExecutionSupport.process(
                    orderFulfillment(), "o1", current, SagaInput.event(new PaymentReserved("o1")), new EventMeta(null, null, 6L), NOW);

            assertAll(
                    () -> assertThat(outcome.processed()).isTrue(),
                    () -> assertThat(outcome.envelope().positionWatermark()).isEqualTo(6L)
            );
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
        void treats_a_stream_id_without_a_version_as_no_dedup_key() {
            assertAll(
                    () -> assertThat(new EventMeta("s1", 5L, null).carriesRedeliveryKey()).isTrue(),
                    () -> assertThat(new EventMeta(null, null, 6L).carriesRedeliveryKey()).isTrue(),
                    () -> assertThat(new EventMeta("s1", null, null).carriesRedeliveryKey()).isFalse(),
                    () -> assertThat(EventMeta.NONE.carriesRedeliveryKey()).isFalse()
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
                    orderFulfillment(), "o1", current, SagaInput.timeout(new SagaTimeout("o1", "typo")), EventMeta.NONE, NOW);

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
                    current, SagaInput.timeout(new SagaTimeout("o1", "typo")), EventMeta.NONE, NOW);

            assertThat(appender.list)
                    .filteredOn(event -> event.getLevel() == Level.WARN)
                    .anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("typo"));
        }

        @Test
        void does_not_warn_for_a_timer_whose_handler_folds_and_reacts() {
            SagaEnvelope<OrderState> current = activeEnvelope("o1", new AwaitingPayment("o1"), 1,
                    List.of(new TimerEntry(PAYMENT_TIMER, NOW.plus(Duration.ofMinutes(5)).toEpochMilli())), Map.of(), null);

            SagaExecutionSupport.process(orderFulfillment(), "o1",
                    current, SagaInput.timeout(new SagaTimeout("o1", PAYMENT_TIMER)), EventMeta.NONE, NOW);

            assertThat(appender.list).noneSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.WARN));
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
}
