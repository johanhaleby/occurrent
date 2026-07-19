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

package org.occurrent.dsl.saga.flow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaTimeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Covers the Java {@link FlowSaga} builder surface with the same order-fulfillment retry-loop scenario the Kotlin
 * {@code SagaFlowExtensionsTest} covers via the {@code saga { }} DSL, proving the lowering is correct through the
 * machine-core {@link Saga} contract regardless of which surface built it.
 */
@DisplayName("FlowSaga")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class FlowSagaTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved, PaymentFailed {
        String orderId();
    }

    record OrderPlaced(String orderId, int amount) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    record PaymentFailed(String orderId, int amount) implements OrderEvent {
    }

    sealed interface OrderCommand permits ReservePayment, ShipOrder, CancelOrder {
    }

    record ReservePayment(String orderId, int amount) implements OrderCommand {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }

    private static final String PAYMENT_TIMER = "step:awaiting-payment";

    private static Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> orderFulfillmentSaga() {
        return FlowSaga.<OrderEvent, OrderCommand>builder()
                .startsOn(OrderPlaced.class, OrderPlaced::orderId, o -> List.of(new ReservePayment(o.orderId(), o.amount())))
                .correlate(PaymentReserved.class, PaymentReserved::orderId)
                .correlate(PaymentFailed.class, PaymentFailed::orderId)
                .step("awaiting-payment", step -> step
                        .on(PaymentReserved.class, Continuation.end(), p -> List.of(new ShipOrder(p.orderId())))
                        .on(PaymentFailed.class,
                                (f, received) -> received.count(PaymentFailed.class) < 3,
                                Continuation.goTo("awaiting-payment"),
                                f -> List.of(new ReservePayment(f.orderId(), f.amount())))
                        .on(PaymentFailed.class, Continuation.end(), f -> List.of(new CancelOrder(f.orderId())))
                        .timeout(Duration.ofMinutes(30), Continuation.end(),
                                r -> List.of(new CancelOrder(r.initiating(OrderPlaced.class).orderId()))))
                .build();
    }

    /** Applies a start event the way an executor would: evolve, then concatenate onStart's and react's effects. */
    private static Saga.Step<FlowState<OrderEvent>, OrderCommand> start(Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga, OrderEvent event) {
        FlowState<OrderEvent> state = saga.evolve(saga.initialState(), SagaInput.event(event));
        List<SagaEffect<OrderCommand>> effects = new ArrayList<>(saga.onStart(state, event));
        effects.addAll(saga.react(state, SagaInput.event(event)));
        return new Saga.Step<>(state, effects);
    }

    @Nested
    class OrderFulfillmentRetryLoop {

        private final Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = orderFulfillmentSaga();

        @Test
        void the_start_event_reserves_payment_and_arms_the_step_timeout() {
            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));

            assertThat(started.effects()).containsExactly(
                    SagaEffect.issue(new ReservePayment("o1", 100)),
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)));
        }

        @Test
        void a_reserved_payment_ships_the_order_completes_the_saga_and_cancels_the_timeout() {
            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));

            Saga.Step<FlowState<OrderEvent>, OrderCommand> step = saga.step(started.state(), SagaInput.event(new PaymentReserved("o1")));

            assertAll(
                    () -> assertThat(saga.isTerminal(step.state())).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(
                            SagaEffect.issue(new ShipOrder("o1")),
                            SagaEffect.cancelTimeout(PAYMENT_TIMER))
            );
        }

        @Test
        void a_payment_failure_below_the_retry_cap_re_arms_payment_and_re_arms_the_timeout() {
            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));

            Saga.Step<FlowState<OrderEvent>, OrderCommand> firstFailure =
                    saga.step(started.state(), SagaInput.event(new PaymentFailed("o1", 100)));

            assertAll(
                    () -> assertThat(firstFailure.state().currentStep()).isEqualTo("awaiting-payment"),
                    () -> assertThat(saga.isTerminal(firstFailure.state())).isFalse(),
                    () -> assertThat(firstFailure.effects()).containsExactly(
                            SagaEffect.issue(new ReservePayment("o1", 100)),
                            SagaEffect.cancelTimeout(PAYMENT_TIMER),
                            SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)))
            );
        }

        @Test
        void a_second_payment_failure_below_the_retry_cap_retries_again_with_the_same_effect_shape() {
            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> firstFailure =
                    saga.step(started.state(), SagaInput.event(new PaymentFailed("o1", 100)));

            Saga.Step<FlowState<OrderEvent>, OrderCommand> secondFailure =
                    saga.step(firstFailure.state(), SagaInput.event(new PaymentFailed("o1", 100)));

            assertAll(
                    () -> assertThat(secondFailure.state().currentStep()).isEqualTo("awaiting-payment"),
                    () -> assertThat(saga.isTerminal(secondFailure.state())).isFalse(),
                    () -> assertThat(secondFailure.effects()).containsExactly(
                            SagaEffect.issue(new ReservePayment("o1", 100)),
                            SagaEffect.cancelTimeout(PAYMENT_TIMER),
                            SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)))
            );
        }

        @Test
        void a_third_payment_failure_exhausts_the_retry_cap_cancels_the_order_and_completes_the_saga() {
            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> firstFailure =
                    saga.step(started.state(), SagaInput.event(new PaymentFailed("o1", 100)));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> secondFailure =
                    saga.step(firstFailure.state(), SagaInput.event(new PaymentFailed("o1", 100)));

            Saga.Step<FlowState<OrderEvent>, OrderCommand> thirdFailure =
                    saga.step(secondFailure.state(), SagaInput.event(new PaymentFailed("o1", 100)));

            assertAll(
                    () -> assertThat(saga.isTerminal(thirdFailure.state())).isTrue(),
                    () -> assertThat(thirdFailure.effects()).containsExactly(
                            SagaEffect.issue(new CancelOrder("o1")),
                            SagaEffect.cancelTimeout(PAYMENT_TIMER))
            );
        }

        @Test
        void the_step_timeout_firing_before_any_resolution_cancels_the_order_and_completes_the_saga() {
            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));

            Saga.Step<FlowState<OrderEvent>, OrderCommand> step =
                    saga.step(started.state(), SagaInput.timeout(new SagaTimeout("o1", PAYMENT_TIMER)));

            assertAll(
                    () -> assertThat(saga.isTerminal(step.state())).isTrue(),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new CancelOrder("o1")))
            );
        }
    }
}
