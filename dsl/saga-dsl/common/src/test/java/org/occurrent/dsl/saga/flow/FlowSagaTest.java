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

import org.junit.jupiter.api.*;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaTimeout;

import java.time.Duration;
import java.time.Instant;
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

    /** As {@link #start(Saga, OrderEvent)}, generic over any flow saga's event and command types. */
    private static <E, C> Saga.Step<FlowState<E>, C> start(Saga<E, FlowState<E>, C> saga, E event) {
        FlowState<E> state = saga.evolve(saga.initialState(), SagaInput.event(event));
        List<SagaEffect<C>> effects = new ArrayList<>(saga.onStart(state, EventMetadata.empty(), event));
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
    class StartMetadata {

        @Test
        void a_bifunction_start_reaction_receives_the_starting_events_metadata() {
            AtomicReference<EventMetadata> seen = new AtomicReference<>();
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class, (metadata, o) -> {
                        seen.set(metadata);
                        return List.of(new ReservePayment(o.orderId(), o.amount()));
                    })
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step
                            .on(PaymentReserved.class, Continuation.end(), p -> List.of(new ShipOrder(p.orderId()))))
                    .build();

            OrderPlaced startEvent = new OrderPlaced("o1", 100);
            FlowState<OrderEvent> state = saga.evolve(saga.initialState(), SagaInput.event(startEvent));
            EventMetadata metadata = new EventMetadata(Map.of(
                    OccurrentCloudEventExtension.STREAM_ID, "stream-1",
                    OccurrentCloudEventExtension.STREAM_VERSION, 5L));

            List<SagaEffect<OrderCommand>> effects = saga.onStart(state, metadata, startEvent);

            assertAll(
                    () -> assertThat(effects).contains(SagaEffect.issue(new ReservePayment("o1", 100))),
                    () -> assertThat(seen.get().getStreamId()).isEqualTo("stream-1"),
                    () -> assertThat(seen.get().getStreamVersion()).isEqualTo(5L)
            );
        }
    }

    @Nested
    class NoCommandsOverloads {

        @Test
        void an_unguarded_branch_with_no_commands_follows_its_continuation_and_issues_nothing() {
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step.on(PaymentReserved.class, Continuation.end()))
                    .build();

            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> step = saga.step(started.state(), SagaInput.event(new PaymentReserved("o1")));

            assertAll(
                    () -> assertThat(saga.isTerminal(step.state())).isTrue(),
                    () -> assertThat(step.effects()).isEmpty()
            );
        }

        @Test
        void a_guarded_branch_with_no_commands_follows_its_continuation_only_when_the_guard_matches() {
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentFailed.class, PaymentFailed::orderId)
                    .step("awaiting-payment", step -> step
                            .on(PaymentFailed.class, (f, received) -> f.amount() > 50, Continuation.end())
                            .on(PaymentFailed.class, Continuation.transitionTo("awaiting-payment")))
                    .build();

            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> guardFalse = saga.step(started.state(), SagaInput.event(new PaymentFailed("o1", 10)));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> guardTrue = saga.step(started.state(), SagaInput.event(new PaymentFailed("o1", 100)));

            assertAll(
                    () -> assertThat(saga.isTerminal(guardFalse.state())).as("falls through to the unguarded branch").isFalse(),
                    () -> assertThat(guardFalse.state().currentStep()).isEqualTo("awaiting-payment"),
                    () -> assertThat(guardFalse.effects()).isEmpty(),
                    () -> assertThat(saga.isTerminal(guardTrue.state())).as("matches the guarded branch").isTrue(),
                    () -> assertThat(guardTrue.effects()).isEmpty()
            );
        }

        @Test
        void a_join_with_no_commands_follows_its_continuation_once_fulfilled_and_issues_nothing() {
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step.join(List.of(Expectation.of(PaymentReserved.class, 1)), Continuation.end()))
                    .build();

            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> step = saga.step(started.state(), SagaInput.event(new PaymentReserved("o1")));

            assertAll(
                    () -> assertThat(saga.isTerminal(step.state())).isTrue(),
                    () -> assertThat(step.effects()).isEmpty()
            );
        }

        @Test
        void a_relative_timeout_with_no_commands_follows_its_continuation_and_issues_nothing() {
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step
                            .on(PaymentReserved.class, Continuation.end(), p -> List.of(new ShipOrder(p.orderId())))
                            .timeout(Duration.ofMinutes(30), Continuation.end()))
                    .build();

            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> step =
                    saga.step(started.state(), SagaInput.timeout(new SagaTimeout("o1", PAYMENT_TIMER)));

            assertAll(
                    () -> assertThat(saga.isTerminal(step.state())).isTrue(),
                    () -> assertThat(step.effects()).isEmpty()
            );
        }

        @Test
        void a_data_derived_timeout_with_no_commands_follows_its_continuation_and_issues_nothing() {
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step
                            .on(PaymentReserved.class, Continuation.end(), p -> List.of(new ShipOrder(p.orderId())))
                            .timeout(r -> r.initiating(OrderPlaced.class).amount() > 0
                                    ? Instant.EPOCH.plusSeconds(1)
                                    : Instant.EPOCH, Continuation.end()))
                    .build();

            Saga.Step<FlowState<OrderEvent>, OrderCommand> started = start(saga, new OrderPlaced("o1", 100));
            Saga.Step<FlowState<OrderEvent>, OrderCommand> step =
                    saga.step(started.state(), SagaInput.timeout(new SagaTimeout("o1", PAYMENT_TIMER)));

            assertAll(
                    () -> assertThat(saga.isTerminal(step.state())).isTrue(),
                    () -> assertThat(step.effects()).isEmpty()
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
                    .hasMessageContaining("starts the saga")
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

    @Nested
    class StepConditions {

        sealed interface CondEvent permits Started, EventA, EventB, EventC {
            String id();
        }

        record Started(String id) implements CondEvent {
        }

        record EventA(String id, int value) implements CondEvent {
        }

        record EventB(String id) implements CondEvent {
        }

        record EventC(String id) implements CondEvent {
        }

        sealed interface CondCommand permits Noop {
        }

        record Noop() implements CondCommand {
        }

        private static Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga(StepCondition<CondEvent> condition) {
            return FlowSaga.<CondEvent, CondCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(CondEvent::id)
                    .step("wait", step -> step.on(condition, Continuation.end()))
                    .build();
        }

        @Test
        void allOf_fulfils_only_once_every_leaf_is() {
            StepCondition<CondEvent> condition = StepCondition.allOf(StepCondition.event(EventA.class, 2), StepCondition.event(EventB.class));
            Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga = saga(condition);
            FlowState<CondEvent> state = start(saga, new Started("s1")).state();

            FlowState<CondEvent> afterFirstA = saga.evolve(state, SagaInput.event(new EventA("s1", 1)));
            FlowState<CondEvent> afterSecondA = saga.evolve(afterFirstA, SagaInput.event(new EventA("s1", 2)));
            FlowState<CondEvent> afterB = saga.evolve(afterSecondA, SagaInput.event(new EventB("s1")));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterFirstA)).as("one EventA is not two").isFalse(),
                    () -> assertThat(saga.isTerminal(afterSecondA)).as("two EventA but no EventB yet").isFalse(),
                    () -> assertThat(saga.isTerminal(afterB)).as("both leaves now satisfied").isTrue()
            );
        }

        @Test
        void anyOf_fires_on_the_first_satisfied_alternative() {
            StepCondition<CondEvent> condition = StepCondition.anyOf(StepCondition.event(EventB.class), StepCondition.event(EventC.class));
            Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga = saga(condition);
            FlowState<CondEvent> state = start(saga, new Started("s1")).state();

            FlowState<CondEvent> afterB = saga.evolve(state, SagaInput.event(new EventB("s1")));

            assertThat(saga.isTerminal(afterB)).as("either alternative alone is enough").isTrue();
        }

        @Test
        void a_nested_tree_combines_a_count_and_an_alternative() {
            // allOf(event(A, 2), anyOf(B, C)): two A's, plus either a B or a C.
            StepCondition<CondEvent> condition = StepCondition.allOf(
                    StepCondition.event(EventA.class, 2),
                    StepCondition.anyOf(StepCondition.event(EventB.class), StepCondition.event(EventC.class)));
            Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga = saga(condition);
            FlowState<CondEvent> state = start(saga, new Started("s1")).state();

            FlowState<CondEvent> afterOneA = saga.evolve(state, SagaInput.event(new EventA("s1", 1)));
            FlowState<CondEvent> afterC = saga.evolve(afterOneA, SagaInput.event(new EventC("s1")));
            FlowState<CondEvent> afterSecondA = saga.evolve(afterC, SagaInput.event(new EventA("s1", 2)));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterC)).as("only one A so far, the count leaf is not met").isFalse(),
                    () -> assertThat(saga.isTerminal(afterSecondA)).as("two A's plus the earlier C fulfils the tree").isTrue()
            );
        }

        @Test
        void a_predicate_leaf_only_counts_a_matching_event_of_its_type() {
            StepCondition<CondEvent> condition = StepCondition.event(EventA.class, 1, (EventA a) -> a.value() > 10);
            Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga = saga(condition);
            FlowState<CondEvent> state = start(saga, new Started("s1")).state();

            FlowState<CondEvent> afterLow = saga.evolve(state, SagaInput.event(new EventA("s1", 1)));
            FlowState<CondEvent> afterHigh = saga.evolve(afterLow, SagaInput.event(new EventA("s1", 20)));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterLow)).as("EventA arrived but the predicate rejects it").isFalse(),
                    () -> assertThat(saga.isTerminal(afterHigh)).as("a later EventA that satisfies the predicate fulfils it").isTrue()
            );
        }

        @Test
        void a_mixed_step_fires_the_first_declared_satisfied_branch_even_when_a_later_one_would_also_match() {
            Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga = FlowSaga.<CondEvent, CondCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(CondEvent::id)
                    .step("wait", step -> step
                            .on(EventA.class, Continuation.transitionTo("first-won"))
                            .on(StepCondition.event(EventA.class), Continuation.transitionTo("second-won")))
                    .step("first-won", step -> step.on(EventB.class, Continuation.end()))
                    .step("second-won", step -> step.on(EventC.class, Continuation.end()))
                    .build();
            FlowState<CondEvent> state = start(saga, new Started("s1")).state();

            FlowState<CondEvent> afterA = saga.evolve(state, SagaInput.event(new EventA("s1", 1)));

            assertThat(afterA.currentStep())
                    .as("the classic branch is declared first, so it wins over the condition branch behind it even though both match")
                    .isEqualTo("first-won");
        }

        @Test
        void a_guarded_classic_branch_never_fires_on_a_later_unrelated_event_unlike_a_window_condition_on_the_same_step() {
            // A window-condition branch re-evaluates on every arriving event and so responds to a later EventA regardless
            // of its own type. A guarded classic branch only ever tests its own type's arrival (EventB here), so two
            // EventA's never reach its guard, even though the guard's own predicate (count(EventA) >= 2) is true in the
            // received log by the second one, because no EventB ever arrives to trigger the check at all. This is the
            // guard-non-lowering divergence, a guard is not a window leaf, see StepBuilder's javadoc and ADR 120.
            Saga<CondEvent, FlowState<CondEvent>, CondCommand> saga = FlowSaga.<CondEvent, CondCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(CondEvent::id)
                    .step("wait", step -> step
                            .on(EventB.class, (b, received) -> received.count(EventA.class) >= 2, Continuation.transitionTo("guard-won"))
                            .on(StepCondition.event(EventA.class, 2), Continuation.transitionTo("condition-won")))
                    .step("guard-won", step -> step.on(EventC.class, Continuation.end()))
                    .step("condition-won", step -> step.on(EventC.class, Continuation.end()))
                    .build();
            FlowState<CondEvent> state = start(saga, new Started("s1")).state();

            FlowState<CondEvent> afterFirstA = saga.evolve(state, SagaInput.event(new EventA("s1", 1)));
            FlowState<CondEvent> afterSecondA = saga.evolve(afterFirstA, SagaInput.event(new EventA("s1", 2)));

            assertThat(afterSecondA.currentStep())
                    .as("the window condition fires on the second EventA, the guarded branch never runs, since no EventB ever arrived")
                    .isEqualTo("condition-won");
        }
    }

    @Nested
    class Retention {

        sealed interface RetentionEvent permits Begin, Tick {
            String id();
        }

        record Begin(String id) implements RetentionEvent {
        }

        record Tick(String id) implements RetentionEvent {
        }

        sealed interface RetentionCommand permits Noop {
        }

        record Noop() implements RetentionCommand {
        }

        @Test
        void a_window_condition_still_matches_when_its_events_outnumber_the_history_window() {
            // historyWindow(0) keeps no carry-over history, yet a window condition must still see every event received
            // since the step was entered. The current step's own events are never dropped mid-step, only earlier history
            // is bounded. Replicates BoundedRetentionWindow's join-era guarantee for the on(StepCondition) path.
            Saga<RetentionEvent, FlowState<RetentionEvent>, RetentionCommand> saga = FlowSaga.<RetentionEvent, RetentionCommand>builder()
                    .historyWindow(0)
                    .startsOn(Begin.class)
                    .correlate(Begin.class, Begin::id)
                    .correlate(Tick.class, Tick::id)
                    .step("wait", step -> step.on(StepCondition.allOf(StepCondition.event(Tick.class, 3)), Continuation.end()))
                    .build();

            FlowState<RetentionEvent> state = saga.evolve(saga.initialState(), SagaInput.event(new Begin("w")));
            FlowState<RetentionEvent> afterFirst = saga.evolve(state, SagaInput.event(new Tick("w")));
            FlowState<RetentionEvent> beforeThird = saga.evolve(afterFirst, SagaInput.event(new Tick("w")));
            FlowState<RetentionEvent> afterThird = saga.evolve(beforeThird, SagaInput.event(new Tick("w")));

            assertAll(
                    () -> assertThat(saga.isTerminal(beforeThird)).as("not fulfilled after two ticks").isFalse(),
                    () -> assertThat(saga.isTerminal(afterThird)).as("the three-tick condition fulfils on the third").isTrue()
            );
        }
    }

    @Nested
    class Reaction {

        sealed interface ReactEvent permits Started, EventX, EventY {
            String id();
        }

        record Started(String id) implements ReactEvent {
        }

        record EventX(String id) implements ReactEvent {
        }

        record EventY(String id) implements ReactEvent {
        }

        sealed interface ReactCommand permits Recorded {
        }

        record Recorded(ReactEvent tippingEvent) implements ReactCommand {
        }

        @Test
        void the_tipping_event_is_the_last_element_of_the_received_window() {
            Saga<ReactEvent, FlowState<ReactEvent>, ReactCommand> saga = FlowSaga.<ReactEvent, ReactCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(ReactEvent::id)
                    .step("wait", step -> step.on(
                            StepCondition.anyOf(StepCondition.event(EventX.class), StepCondition.event(EventY.class)),
                            Continuation.end(),
                            received -> List.of(new Recorded(received.asList().get(received.asList().size() - 1)))))
                    .build();
            Saga.Step<FlowState<ReactEvent>, ReactCommand> started = start(saga, new Started("s1"));

            Saga.Step<FlowState<ReactEvent>, ReactCommand> step = saga.step(started.state(), SagaInput.event(new EventY("s1")));

            assertThat(step.effects()).containsExactly(SagaEffect.issue(new Recorded(new EventY("s1"))));
        }

        @Test
        void timers_are_cancelled_and_re_armed_across_a_condition_transition_the_same_as_a_classic_one() {
            Saga<ReactEvent, FlowState<ReactEvent>, ReactCommand> saga = FlowSaga.<ReactEvent, ReactCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(ReactEvent::id)
                    .step("wait", step -> step
                            .on(StepCondition.event(EventX.class), Continuation.transitionTo("next"))
                            .timeout(Duration.ofMinutes(5), Continuation.end()))
                    .step("next", step -> step
                            .on(EventY.class, Continuation.end())
                            .timeout(Duration.ofMinutes(1), Continuation.end()))
                    .build();
            Saga.Step<FlowState<ReactEvent>, ReactCommand> started = start(saga, new Started("s1"));

            Saga.Step<FlowState<ReactEvent>, ReactCommand> step = saga.step(started.state(), SagaInput.event(new EventX("s1")));

            assertAll(
                    () -> assertThat(step.state().currentStep()).isEqualTo("next"),
                    () -> assertThat(step.effects()).containsExactly(
                            SagaEffect.cancelTimeout("step:wait"),
                            SagaEffect.startTimeout("step:next", Duration.ofMinutes(1)))
            );
        }
    }

    @Nested
    class StatedRules {

        sealed interface RuleEvent permits Started, EventA, EventB {
            String id();
        }

        record Started(String id) implements RuleEvent {
        }

        record EventA(String id) implements RuleEvent {
        }

        record EventB(String id) implements RuleEvent {
        }

        sealed interface RuleCommand permits Noop {
        }

        record Noop() implements RuleCommand {
        }

        @Test
        void a_classic_branch_self_loop_resets_a_sibling_window_conditions_partial_count() {
            Saga<RuleEvent, FlowState<RuleEvent>, RuleCommand> saga = FlowSaga.<RuleEvent, RuleCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(RuleEvent::id)
                    .step("wait", step -> step
                            .on(StepCondition.event(EventA.class, 2), Continuation.end())
                            .on(EventB.class, Continuation.transitionTo("wait")))
                    .build();
            FlowState<RuleEvent> state = start(saga, new Started("s1")).state();

            FlowState<RuleEvent> afterFirstA = saga.evolve(state, SagaInput.event(new EventA("s1")));
            FlowState<RuleEvent> afterSelfLoop = saga.evolve(afterFirstA, SagaInput.event(new EventB("s1")));
            FlowState<RuleEvent> afterSecondAOverall = saga.evolve(afterSelfLoop, SagaInput.event(new EventA("s1")));
            FlowState<RuleEvent> afterThirdAOverall = saga.evolve(afterSecondAOverall, SagaInput.event(new EventA("s1")));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterSecondAOverall))
                            .as("the self-loop wiped the first EventA's count, so this second-overall EventA is only the first since re-entry")
                            .isFalse(),
                    () -> assertThat(saga.isTerminal(afterThirdAOverall))
                            .as("the second EventA since re-entry finally fulfils the count-2 leaf")
                            .isTrue()
            );
        }

        @Test
        void a_first_step_condition_naming_the_start_type_never_counts_the_start_delivery_itself() {
            Saga<RuleEvent, FlowState<RuleEvent>, RuleCommand> saga = FlowSaga.<RuleEvent, RuleCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(RuleEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Started.class, 1), Continuation.end()))
                    .build();

            FlowState<RuleEvent> afterStart = saga.evolve(saga.initialState(), SagaInput.event(new Started("s1")));

            assertThat(saga.isTerminal(afterStart))
                    .as("the start delivery only enters the first step, its window opens after it, so the start event itself never counts")
                    .isFalse();
        }
    }

    @Nested
    class CompileShapes {

        // A clean sealed hierarchy, the nearest common supertype of the two leaf types is E itself, no lub surprises.
        sealed interface CleanEvent permits Started, Cancelled, TimedOut {
            String id();
        }

        record Started(String id) implements CleanEvent {
        }

        record Cancelled(String id) implements CleanEvent {
        }

        record TimedOut(String id) implements CleanEvent {
        }

        sealed interface CleanCommand permits Noop {
        }

        record Noop() implements CleanCommand {
        }

        @Test
        void a_condition_tree_built_from_a_clean_sealed_hierarchys_leaves_is_reusable_across_two_steps() {
            var cancelledOrTimedOut = StepCondition.anyOf(StepCondition.event(Cancelled.class), StepCondition.event(TimedOut.class));
            Saga<CleanEvent, FlowState<CleanEvent>, CleanCommand> saga = FlowSaga.<CleanEvent, CleanCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(CleanEvent::id)
                    .step("first", step -> step.on(cancelledOrTimedOut, Continuation.next()))
                    .step("second", step -> step.on(cancelledOrTimedOut, Continuation.end()))
                    .build();
            FlowState<CleanEvent> state = start(saga, new Started("s1")).state();

            FlowState<CleanEvent> afterFirstCancel = saga.evolve(state, SagaInput.event(new Cancelled("s1")));
            FlowState<CleanEvent> afterSecondTimeout = saga.evolve(afterFirstCancel, SagaInput.event(new TimedOut("s1")));

            assertAll(
                    () -> assertThat(afterFirstCancel.currentStep()).isEqualTo("second"),
                    () -> assertThat(saga.isTerminal(afterSecondTimeout)).isTrue()
            );
        }

        // The lub-inference trap. Every leaf here also shares a second interface (Identified) beyond the sealed domain
        // hierarchy, the shape that can push a compiler's inferred least-upper-bound somewhere other than the plain
        // domain type. on(StepCondition<? extends E>, ...) accepts whatever E is inferred here without a cast, the
        // use-site-variance reason StepBuilder.on(StepCondition, ...) is declared that way rather than invariantly.
        interface Identified {
            String id();
        }

        sealed interface LubEvent extends Identified permits OtherEvent, Cancelled2, TimedOut2 {
        }

        record OtherEvent(String id) implements LubEvent {
        }

        record Cancelled2(String id) implements LubEvent {
        }

        record TimedOut2(String id) implements LubEvent {
        }

        sealed interface LubCommand permits LubNoop {
        }

        record LubNoop() implements LubCommand {
        }

        @Test
        void a_tree_built_from_leaves_sharing_a_second_interface_still_infers_correctly_and_binds_to_on() {
            var cancelledOrTimedOut = StepCondition.anyOf(StepCondition.event(Cancelled2.class), StepCondition.event(TimedOut2.class));
            Saga<LubEvent, FlowState<LubEvent>, LubCommand> saga = FlowSaga.<LubEvent, LubCommand>builder()
                    .startsOn(OtherEvent.class)
                    .correlateAll(LubEvent::id)
                    .step("first", step -> step.on(cancelledOrTimedOut, Continuation.next()))
                    .step("second", step -> step.on(cancelledOrTimedOut, Continuation.end()))
                    .build();
            FlowState<LubEvent> state = start(saga, new OtherEvent("s1")).state();

            FlowState<LubEvent> afterFirstCancel = saga.evolve(state, SagaInput.event(new Cancelled2("s1")));
            FlowState<LubEvent> afterSecondTimeout = saga.evolve(afterFirstCancel, SagaInput.event(new TimedOut2("s1")));

            assertAll(
                    () -> assertThat(afterFirstCancel.currentStep()).isEqualTo("second"),
                    () -> assertThat(saga.isTerminal(afterSecondTimeout)).isTrue()
            );
        }
    }
}
