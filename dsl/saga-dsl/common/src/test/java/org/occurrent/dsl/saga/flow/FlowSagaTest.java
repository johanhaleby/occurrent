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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaTimeout;
import org.occurrent.cloudevents.EventMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Covers the Java {@link FlowSaga} builder surface with the same order-fulfillment retry-loop scenario the Kotlin
 * {@code SagaFlowExtensionsTest} covers via the {@code saga { }} DSL, proving the lowering is correct through the
 * core {@link Saga} contract regardless of which surface built it.
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
                .startsOn(OrderPlaced.class, o -> List.of(new ReservePayment(o.orderId(), o.amount())))
                .correlate(OrderPlaced.class, OrderPlaced::orderId)
                .correlate(PaymentReserved.class, PaymentReserved::orderId)
                .correlate(PaymentFailed.class, PaymentFailed::orderId)
                .step("awaiting-payment", step -> step
                        .on(PaymentReserved.class, Continuation.end(), p -> List.of(new ShipOrder(p.orderId())))
                        .on(PaymentFailed.class,
                                (f, received) -> received.count(PaymentFailed.class) < 3,
                                Continuation.transitionTo("awaiting-payment"),
                                f -> List.of(new ReservePayment(f.orderId(), f.amount())))
                        .on(PaymentFailed.class, Continuation.end(), f -> List.of(new CancelOrder(f.orderId())))
                        .timeout(Duration.ofMinutes(30), Continuation.end(),
                                r -> List.of(new CancelOrder(r.initiating(OrderPlaced.class).orderId()))))
                .build();
    }

    /** Applies a start event the way an executor would: evolve, then concatenate onStart's and react's effects. */
    private static Saga.Step<FlowState<OrderEvent>, OrderCommand> start(Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga, OrderEvent event) {
        FlowState<OrderEvent> state = saga.evolve(saga.initialState(), SagaInput.event(event));
        // Call the metadata-carrying onStart the executor actually calls, so this exercises the same override the runtime hits.
        List<SagaEffect<OrderCommand>> effects = new ArrayList<>(saga.onStart(state, EventMetadata.empty(), event));
        effects.addAll(saga.react(state, SagaInput.event(event)));
        return new Saga.Step<>(state, effects);
    }

    @Nested
    class BranchMetadata {

        @Test
        void a_metadata_carrying_on_branch_receives_the_triggering_events_metadata() {
            AtomicReference<EventMetadata> seen = new AtomicReference<>();
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step
                            .on(PaymentReserved.class, Continuation.end(), (metadata, p) -> {
                                seen.set(metadata);
                                return List.of(new ShipOrder(p.orderId()));
                            }))
                    .build();

            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            EventMetadata metadata = new EventMetadata(Map.of(
                    OccurrentCloudEventExtension.STREAM_ID, "stream-1",
                    OccurrentCloudEventExtension.STREAM_VERSION, 5L,
                    OccurrentCloudEventExtension.POSITION, 88L));

            Saga.Step<FlowState<OrderEvent>, OrderCommand> step = saga.step(started.state(), SagaInput.event(new PaymentReserved("o1"), metadata));

            assertAll(
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new ShipOrder("o1"))),
                    () -> assertThat(seen.get().getStreamId()).isEqualTo("stream-1"),
                    () -> assertThat(seen.get().getStreamVersion()).isEqualTo(5L),
                    () -> assertThat(seen.get().getPosition()).isEqualTo(88L)
            );
        }
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

    @Nested
    class BuilderGuards {

        @Test
        void historyWindow_rejects_a_negative_value() {
            assertThatThrownBy(() -> FlowSaga.<OrderEvent, OrderCommand>builder().historyWindow(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("historyWindow");
        }

        // startsOn no longer takes a correlation, so the start type is covered by correlate or correlateAll like any
        // other type. Nothing else pins the start type specifically: the neighbouring case covers a type used by a step.
        @Test
        void build_throws_naming_the_start_type_when_nothing_correlates_it() {
            assertThatThrownBy(() -> FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class, o -> List.of(new ReservePayment(o.orderId(), o.amount())))
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step.on(PaymentReserved.class, Continuation.end(), p -> List.of()))
                    .build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("OrderPlaced")
                    .hasMessageContaining("correlateAll");
        }
    }

    @Nested
    class BoundedRetentionWindow {

        sealed interface WinEvent permits Begin, Tick {
            String id();
        }

        record Begin(String id) implements WinEvent {
        }

        record Tick(String id) implements WinEvent {
        }

        sealed interface WinCommand permits Noop {
        }

        record Noop() implements WinCommand {
        }

        /** A two-step flow that ping-pongs between "a" and "b" on every Tick, so every event drives a transition. */
        private static Saga<WinEvent, FlowState<WinEvent>, WinCommand> pingPong(int historyWindow) {
            return FlowSaga.<WinEvent, WinCommand>builder()
                    .historyWindow(historyWindow)
                    .startsOn(Begin.class)
                    .correlate(Begin.class, Begin::id)
                    .correlate(Tick.class, Tick::id)
                    .step("a", step -> step.on(Tick.class, Continuation.transitionTo("b"), t -> List.of()))
                    .step("b", step -> step.on(Tick.class, Continuation.transitionTo("a"), t -> List.of()))
                    .build();
        }

        private static FlowState<WinEvent> runTicks(Saga<WinEvent, FlowState<WinEvent>, WinCommand> saga, int ticks) {
            FlowState<WinEvent> state = saga.evolve(saga.initialState(), SagaInput.event(new Begin("w")));
            for (int i = 0; i < ticks; i++) {
                state = saga.evolve(state, SagaInput.event(new Tick("w")));
            }
            return state;
        }

        @Test
        void a_join_still_matches_when_its_events_outnumber_the_history_window() {
            // historyWindow(0) keeps no carry-over history, yet a join must still see every event received since the step
            // was entered: the current step's own events are never dropped mid-step, only earlier history is bounded.
            Saga<WinEvent, FlowState<WinEvent>, WinCommand> saga = FlowSaga.<WinEvent, WinCommand>builder()
                    .historyWindow(0)
                    .startsOn(Begin.class)
                    .correlate(Begin.class, Begin::id)
                    .correlate(Tick.class, Tick::id)
                    .step("wait", step -> step.join(List.of(Expectation.of(Tick.class, 3)), Continuation.end(), r -> List.of()))
                    .build();

            FlowState<WinEvent> beforeThird = runTicks(saga, 2);
            FlowState<WinEvent> afterThird = saga.step(beforeThird, SagaInput.event(new Tick("w"))).state();

            assertAll(
                    () -> assertThat(saga.isTerminal(beforeThird)).as("not fulfilled after two ticks").isFalse(),
                    () -> assertThat(saga.isTerminal(afterThird)).as("the three-tick join fulfils on the third").isTrue()
            );
        }

        @Test
        void the_retained_event_count_stays_bounded_no_matter_how_many_events_arrive() {
            Saga<WinEvent, FlowState<WinEvent>, WinCommand> saga = pingPong(3);

            int atTen = runTicks(saga, 10).received().size();
            int atHundred = runTicks(saga, 100).received().size();
            int atThousand = runTicks(saga, 1000).received().size();

            assertAll(
                    () -> assertThat(atHundred).as("constant once past the window").isEqualTo(atThousand),
                    () -> assertThat(atTen).isEqualTo(atThousand),
                    () -> assertThat(atThousand).as("bounded by the window plus the pinned initiating event").isLessThanOrEqualTo(3 + 2)
            );
        }

        @Test
        void events_older_than_the_window_are_dropped_but_the_initiating_event_is_kept() {
            Saga<WinEvent, FlowState<WinEvent>, WinCommand> saga = pingPong(2);

            FlowState<WinEvent> state = runTicks(saga, 50);

            assertAll(
                    () -> assertThat(state.received().get(0)).as("the initiating event is pinned at position 0").isEqualTo(new Begin("w")),
                    () -> assertThat(state.received()).as("the whole 50-event history is not retained").hasSizeLessThan(10),
                    () -> assertThat(state.receivedEvents().initiating(Begin.class)).isEqualTo(new Begin("w"))
            );
        }

        @Test
        void a_guard_reads_only_the_retained_window() {
            // With historyWindow(1) the retained window holds the initiating event plus at most a couple of recent ticks, so
            // a guard counting ticks sees a bounded count rather than the full run.
            Saga<WinEvent, FlowState<WinEvent>, WinCommand> saga = pingPong(1);

            FlowState<WinEvent> state = runTicks(saga, 40);
            ReceivedEvents<WinEvent> received = state.receivedEvents();

            assertThat(received.count(Tick.class)).isLessThan(40);
        }
    }
}
