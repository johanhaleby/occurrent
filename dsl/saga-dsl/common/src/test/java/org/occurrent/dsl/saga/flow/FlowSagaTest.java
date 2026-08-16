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
import org.occurrent.dsl.saga.TimerName;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.ActionKind;
import org.occurrent.dsl.saga.flow.internal.FlowStateImpl.StepConditionProgress;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.dsl.saga.flow.FlowSaga.stepTimer;

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

    private static final TimerName PAYMENT_TIMER = stepTimer("awaiting-payment");

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
        void a_single_leaf_allOf_with_no_commands_follows_its_continuation_once_fulfilled_and_issues_nothing() {
            Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                    .startsOn(OrderPlaced.class)
                    .correlate(OrderPlaced.class, OrderPlaced::orderId)
                    .correlate(PaymentReserved.class, PaymentReserved::orderId)
                    .step("awaiting-payment", step -> step.on(
                            StepCondition.allOf(StepCondition.event(PaymentReserved.class, 1)), Continuation.end()))
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
                    saga.step(started.state(), SagaInput.timeout("o1", PAYMENT_TIMER));

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
                    saga.step(started.state(), SagaInput.timeout("o1", PAYMENT_TIMER));

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
                    saga.step(started.state(), SagaInput.timeout("o1", PAYMENT_TIMER));

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
        void a_single_leaf_allOf_with_a_reaction_still_matches_when_its_events_outnumber_the_history_window() {
            // historyWindow(0) keeps no carry-over history, yet the condition must still see every event received since the
            // step was entered: the current step's own events are never dropped mid-step, only earlier history is bounded.
            // A single-leaf allOf with a reaction, since that combination is otherwise only covered by Retention's
            // bare-leaf, no-reaction on(StepCondition, ...) case below.
            Saga<WinEvent, FlowState<WinEvent>, WinCommand> saga = FlowSaga.<WinEvent, WinCommand>builder()
                    .historyWindow(0)
                    .startsOn(Begin.class)
                    .correlate(Begin.class, Begin::id)
                    .correlate(Tick.class, Tick::id)
                    .step("wait", step -> step.on(StepCondition.allOf(StepCondition.event(Tick.class, 3)), Continuation.end(), r -> List.of()))
                    .build();

            FlowState<WinEvent> beforeThird = runTicks(saga, 2);
            FlowState<WinEvent> afterThird = saga.step(beforeThird, SagaInput.event(new Tick("w"))).state();

            assertAll(
                    () -> assertThat(saga.isTerminal(beforeThird)).as("not fulfilled after two ticks").isFalse(),
                    () -> assertThat(saga.isTerminal(afterThird)).as("the three-tick condition fulfils on the third").isTrue()
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
    class StepWindowCap {

        sealed interface CapEvent permits Opened, Approved, Noise {
            String id();
        }

        record Opened(String id) implements CapEvent {
        }

        record Approved(String id, int score) implements CapEvent {
        }

        record Noise(String id) implements CapEvent {
        }

        sealed interface CapCommand permits Report {
        }

        record Report(String what) implements CapCommand {
        }

        /** Waits for 3 Approved in one step, with the step's own retained events capped at {@code stepWindow}. */
        private static Saga<CapEvent, FlowState<CapEvent>, CapCommand> waitingForThree(int stepWindow, List<String> saw) {
            return FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(stepWindow)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Approved.class, 3), Continuation.end(),
                            received -> {
                                saw.add("approved=" + received.count(Approved.class) + " kept=" + received.asList().size());
                                return List.of(new Report("done"));
                            }))
                    .build();
        }

        private static FlowState<CapEvent> deliver(Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga, FlowState<CapEvent> state, CapEvent... events) {
            FlowState<CapEvent> current = state;
            for (CapEvent event : events) {
                current = saga.evolve(current, SagaInput.event(event));
            }
            return current;
        }

        private static Noise[] noise(int count) {
            Noise[] events = new Noise[count];
            for (int i = 0; i < count; i++) {
                events[i] = new Noise("c1");
            }
            return events;
        }

        @Test
        void a_count_still_completes_on_the_same_event_after_the_cap_dropped_the_earlier_matches() {
            List<String> saw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = waitingForThree(2, saw);
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            // The first two Approved are pushed out by the noise that follows them, so nothing could recount them.
            FlowState<CapEvent> pushedOut = deliver(saga, opened, new Approved("c1", 1), new Approved("c1", 2));
            FlowState<CapEvent> afterNoise = deliver(saga, pushedOut, noise(10));
            FlowState<CapEvent> afterThird = saga.evolve(afterNoise, SagaInput.event(new Approved("c1", 3)));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterNoise)).as("two of three approved, so still waiting").isFalse(),
                    () -> assertThat(afterNoise.received()).as("the cap keeps 2 of the step's events plus the initiating one").hasSize(3),
                    () -> assertThat(saga.isTerminal(afterThird)).as("the third Approved completes it, carried counts and all").isTrue()
            );
        }

        @Test
        void the_retained_event_count_stays_capped_however_long_the_step_is_parked_on() {
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = waitingForThree(5, new ArrayList<>());
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            int atTen = deliver(saga, opened, noise(10)).received().size();
            int atThousand = deliver(saga, opened, noise(1000)).received().size();

            assertAll(
                    () -> assertThat(atTen).as("already at the cap after ten").isEqualTo(6),
                    () -> assertThat(atThousand).as("constant, which is what an unset stepWindow does not give you").isEqualTo(atTen)
            );
        }

        @Test
        void the_cap_evicts_the_steps_own_oldest_events_rather_than_the_carry_over_ahead_of_them() {
            // The tail holds carry-over from the first step ahead of the second step's own events. Advancing the tail's start
            // by the excess alone would drop a carry-over event and leave all three of the step's, capping nothing.
            List<String> saw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .historyWindow(10)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("first", step -> step.on(Noise.class, Continuation.next()))
                    .step("second", step -> step.on(StepCondition.event(Approved.class, 9), Continuation.end(),
                            received -> {
                                saw.add("unused");
                                return List.of();
                            }))
                    .build();
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));
            FlowState<CapEvent> inSecond = saga.evolve(opened, SagaInput.event(new Noise("c1")));

            FlowState<CapEvent> afterTwo = deliver(saga, inSecond, new Approved("c1", 1), new Approved("c1", 2));
            FlowState<CapEvent> afterThree = saga.evolve(afterTwo, SagaInput.event(new Approved("c1", 3)));

            assertAll(
                    () -> assertThat(afterTwo.received()).as("the initiating event, the first step's Noise, and both Approved")
                            .hasSize(4),
                    () -> assertThat(afterThree.receivedEvents().count(Approved.class))
                            .as("the third Approved evicts the oldest Approved, not the Noise standing ahead of it")
                            .isEqualTo(2),
                    () -> assertThat(afterThree.received()).as("the initiating event plus the two newest Approved").hasSize(3),
                    () -> assertThat(afterThree.receivedEvents().count(Noise.class))
                            .as("the carry-over goes only because the step's events had to be reached past it")
                            .isZero()
            );
        }

        @Test
        void the_peak_retained_count_across_a_multi_step_run_is_the_bound_the_javadoc_states() {
            // A transition keeps the leaving step's own events for its reaction, on top of the carry-over historyWindow
            // grants, and the entered step then fills its own cap before anything is evicted. So the peak is one step's cap
            // more than a single step's worth, which is what the javadoc has to say rather than historyWindow + n + 1.
            int historyWindow = 4;
            int stepWindow = 3;
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(stepWindow)
                    .historyWindow(historyWindow)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("a", step -> step.on(StepCondition.event(Noise.class, 3), Continuation.transitionTo("b")))
                    .step("b", step -> step.on(StepCondition.event(Noise.class, 3), Continuation.transitionTo("a")))
                    .build();

            FlowState<CapEvent> state = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));
            int peak = state.received().size();
            for (int i = 0; i < 200; i++) {
                state = saga.evolve(state, SagaInput.event(new Noise("c1")));
                peak = Math.max(peak, state.received().size());
            }

            int finalPeak = peak;
            assertAll(
                    () -> assertThat(finalPeak).as("the stated bound holds").isLessThanOrEqualTo(historyWindow + 2 * stepWindow + 1),
                    () -> assertThat(finalPeak).as("and the narrower bound this once claimed does not")
                            .isGreaterThan(historyWindow + stepWindow + 1)
            );
        }

        @Test
        void an_unset_step_window_keeps_every_event_the_step_receives() {
            List<String> saw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> unbounded = FlowSaga.<CapEvent, CapCommand>builder()
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Approved.class, 3), Continuation.end(),
                            received -> {
                                saw.add("kept=" + received.asList().size());
                                return List.of();
                            }))
                    .build();
            FlowState<CapEvent> opened = unbounded.evolve(unbounded.initialState(), SagaInput.event(new Opened("c1")));

            FlowState<CapEvent> afterNoise = deliver(unbounded, opened, noise(30));

            assertThat(afterNoise.received()).as("30 noise events plus the initiating one, none dropped").hasSize(31);
        }

        @Test
        void the_event_that_fired_the_condition_and_the_initiating_event_are_both_still_readable() {
            List<String> saw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = waitingForThree(1, saw);
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            FlowState<CapEvent> almost = deliver(saga, opened, new Approved("c1", 1), new Approved("c1", 2), noise(4)[0]);
            FlowState<CapEvent> fired = saga.step(almost, SagaInput.event(new Approved("c1", 9))).state();

            assertAll(
                    () -> assertThat(saga.isTerminal(fired)).isTrue(),
                    () -> assertThat(saw).as("a cap of 1 leaves the reaction the tipping event and nothing else of the window")
                            .containsExactly("approved=1 kept=1"),
                    () -> assertThat(fired.receivedEvents().asList().getLast()).as("the tipping event is the last element")
                            .isEqualTo(new Approved("c1", 9)),
                    () -> assertThat(fired.receivedEvents().initiating(Opened.class)).isEqualTo(new Opened("c1"))
            );
        }

        @Test
        void a_guard_reads_only_the_events_the_cap_kept() {
            List<Integer> guardSaw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(3)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(Approved.class, (approved, received) -> {
                        guardSaw.add(received.count(Noise.class));
                        return true;
                    }, Continuation.end()))
                    .build();
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            FlowState<CapEvent> afterNoise = deliver(saga, opened, noise(20));
            saga.evolve(afterNoise, SagaInput.event(new Approved("c1", 1)));

            assertThat(guardSaw).as("20 noise events arrived, the cap keeps 3 including the Approved that just arrived")
                    .containsExactly(2);
        }

        @Test
        void a_timeout_reaction_reads_only_the_events_the_cap_kept() {
            List<Integer> expirySaw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step
                            .on(StepCondition.event(Approved.class, 3), Continuation.end())
                            .timeout(Duration.ofMinutes(5), Continuation.end(), received -> {
                                expirySaw.add(received.count(Noise.class));
                                return List.of();
                            }))
                    .build();
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            FlowState<CapEvent> afterNoise = deliver(saga, opened, noise(15));
            saga.step(afterNoise, SagaInput.timeout("c1", stepTimer("wait")));

            assertThat(expirySaw).as("15 noise events arrived and the cap keeps 2 of them").containsExactly(2);
        }

        @Test
        void a_self_loop_still_resets_a_partial_count_under_the_cap() {
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step
                            .on(StepCondition.event(Approved.class, 2), Continuation.end())
                            .on(Noise.class, Continuation.transitionTo("wait")))
                    .build();
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            FlowState<CapEvent> afterFirst = saga.evolve(opened, SagaInput.event(new Approved("c1", 1)));
            FlowState<CapEvent> afterSelfLoop = saga.evolve(afterFirst, SagaInput.event(new Noise("c1")));
            FlowState<CapEvent> afterSecondOverall = saga.evolve(afterSelfLoop, SagaInput.event(new Approved("c1", 2)));
            FlowState<CapEvent> afterThirdOverall = saga.evolve(afterSecondOverall, SagaInput.event(new Approved("c1", 3)));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterSecondOverall)).as("the self-loop dropped the first count, carried counts included").isFalse(),
                    () -> assertThat(saga.isTerminal(afterThirdOverall)).isTrue()
            );
        }

        @Test
        void a_first_step_condition_naming_the_start_type_still_never_counts_the_start_delivery_under_the_cap() {
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Opened.class, 1), Continuation.end()))
                    .build();

            FlowState<CapEvent> afterStart = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            assertThat(saga.isTerminal(afterStart)).as("the start delivery only enters the step, its window opens after it").isFalse();
        }

        @Test
        void a_reordered_declaration_recounts_from_the_window_when_nothing_was_dropped() {
            // The counts an unbounded flow carries stop describing the step once the leaves move, so they are re-derived from
            // the window instead. Two sagas over the same state is what a redeploy looks like to evolve, which is pure.
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> before = FlowSaga.<CapEvent, CapCommand>builder()
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.allOf(
                            StepCondition.event(Approved.class, 2), StepCondition.event(Noise.class, 1)), Continuation.end()))
                    .build();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> after = FlowSaga.<CapEvent, CapCommand>builder()
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.allOf(
                            StepCondition.event(Noise.class, 1), StepCondition.event(Approved.class, 2)), Continuation.end()))
                    .build();
            FlowState<CapEvent> opened = before.evolve(before.initialState(), SagaInput.event(new Opened("c1")));
            FlowState<CapEvent> parked = deliver(before, opened, new Approved("c1", 1), new Approved("c1", 2));

            FlowState<CapEvent> afterRedeploy = after.evolve(parked, SagaInput.event(new Noise("c1")));

            assertAll(
                    () -> assertThat(before.isTerminal(parked)).as("waiting for its Noise").isFalse(),
                    () -> assertThat(after.isTerminal(afterRedeploy)).as("the reordered leaves are counted afresh and the tree is satisfied").isTrue()
            );
        }

        @Test
        void a_declaration_that_changed_while_the_cap_had_dropped_events_refuses_the_delivery() {
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> before = waitingForThree(1, new ArrayList<>());
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> after = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(1)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.allOf(
                            StepCondition.event(Approved.class, 3), StepCondition.event(Noise.class, 1)), Continuation.end()))
                    .build();
            FlowState<CapEvent> opened = before.evolve(before.initialState(), SagaInput.event(new Opened("c1")));
            FlowState<CapEvent> parked = deliver(before, deliver(before, opened, new Approved("c1", 1)), noise(2));

            assertThatThrownBy(() -> after.evolve(parked, SagaInput.event(new Approved("c1", 2))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("step 'wait'")
                    .hasMessageContaining("declaration changed")
                    .hasMessageContaining("Retrying the delivery cannot help");
        }

        @Test
        void a_raised_count_still_completes_after_the_cap_dropped_events() {
            // The carried counts are raw totals rather than something capped at the count a leaf asks for, so raising that
            // count on a redeploy leaves them meaning what they meant. The instance stays below its old threshold, since a
            // completed one is returned unchanged and would exercise nothing.
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> before = waitingForThree(1, new ArrayList<>());
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> after = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(1)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Approved.class, 4), Continuation.end()))
                    .build();
            FlowState<CapEvent> opened = before.evolve(before.initialState(), SagaInput.event(new Opened("c1")));
            FlowState<CapEvent> parked = deliver(before, opened, new Approved("c1", 1), new Approved("c1", 2));

            FlowState<CapEvent> afterThird = after.evolve(parked, SagaInput.event(new Approved("c1", 3)));
            FlowState<CapEvent> afterFourth = after.evolve(afterThird, SagaInput.event(new Approved("c1", 4)));

            assertAll(
                    () -> assertThat(before.isTerminal(parked)).as("two of the three the old declaration asked for, so still running").isFalse(),
                    () -> assertThat(parked.received()).as("the cap left one event, so nothing could recount the first two").hasSize(2),
                    () -> assertThat(after.isTerminal(afterThird)).as("three no longer completes it, because the raised count asks for four").isFalse(),
                    () -> assertThat(after.isTerminal(afterFourth)).as("the fourth reaches the raised count, so the carried total was not capped at three").isTrue()
            );
        }

        @Test
        void a_pre_0_33_0_backlog_exceeding_a_newly_configured_cap_derives_counts_instead_of_refusing() {
            // What a document written before stepWindow existed looks like: windowStart never trimmed (still 1), a real
            // stepEntryIndex, and no carried counts (a store defaults the absent field to null). Turning stepWindow on then
            // delivers into a step whose backlog already exceeds the new cap, and the first such delivery has to count the
            // whole pre-trim backlog rather than either refusing (the bug this guards) or judging the condition on the
            // sliver the same delivery is about to keep.
            FlowStateImpl<CapEvent> seeded = new FlowStateImpl<>("wait",
                    List.of(new Opened("c1"), new Approved("c1", 1), new Approved("c1", 2), new Approved("c1", 3), new Approved("c1", 4)),
                    1, 1, false, null, -1, ActionKind.NONE, -1, null);
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = waitingForThree(2, new ArrayList<>());

            Saga.Step<FlowState<CapEvent>, CapCommand> fired = saga.step(seeded, SagaInput.event(new Approved("c1", 5)));

            assertAll(
                    () -> assertThat(saga.isTerminal(fired.state()))
                            .as("five Approved had already arrived before stepWindow was even configured, well past the count of three")
                            .isTrue(),
                    () -> assertThat(fired.effects()).containsExactly(SagaEffect.issue(new Report("done"))),
                    () -> assertThat(fired.state().received()).as("the newly configured cap still applies from this delivery on").hasSize(3)
            );
        }

        @Test
        void two_predicated_leaves_over_one_type_keep_counting_the_window_and_refuse_the_cap() {
            List<String> fired = new ArrayList<>();
            FlowSaga.Builder<CapEvent, CapCommand> ambiguous = FlowSaga.<CapEvent, CapCommand>builder()
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("decide", step -> step
                            .on(StepCondition.event(Approved.class, 1, (Approved a) -> a.score() > 100), Continuation.end(),
                                    received -> {
                                        fired.add("big");
                                        return List.of();
                                    })
                            .on(StepCondition.event(Approved.class, 1, (Approved a) -> a.score() < 0), Continuation.end(),
                                    received -> {
                                        fired.add("negative");
                                        return List.of();
                                    }));
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = ambiguous.build();
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            saga.step(opened, SagaInput.event(new Approved("c1", -5)));

            assertAll(
                    () -> assertThat(fired).as("an unnamed predicate counts the window, so the right branch still fires")
                            .containsExactly("negative"),
                    () -> assertThatThrownBy(ambiguous.stepWindow(4)::build)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("step 'decide'")
                            .hasMessageContaining("Approved")
                            .hasMessageContaining("carries a predicate with no name")
            );
        }

        @Test
        void a_named_predicate_keeps_its_count_across_a_redeploy_that_leaves_the_name_alone() {
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> before = highScoreSaga("isBig", 10);
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> after = highScoreSaga("isBig", 10);
            FlowState<CapEvent> parked = deliver(before, before.evolve(before.initialState(), SagaInput.event(new Opened("c1"))),
                    new Approved("c1", 50), new Noise("c1"), new Noise("c1"));

            FlowState<CapEvent> afterSecond = after.evolve(parked, SagaInput.event(new Approved("c1", 60)));

            assertAll(
                    () -> assertThat(parked.received()).as("the cap dropped the first Approved").hasSize(2),
                    () -> assertThat(before.isTerminal(parked)).isFalse(),
                    () -> assertThat(after.isTerminal(afterSecond))
                            .as("the name matched, so the count for the first Approved was still there").isTrue()
            );
        }

        @Test
        void a_changed_predicate_under_a_changed_name_refuses_rather_than_reusing_the_old_count() {
            // The hole a name closes. Without one, a count for events matching score > 10 would be read as a count of events
            // matching score > 100 and satisfy a test they were never put to.
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> before = highScoreSaga("over10", 10);
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> after = highScoreSaga("over100", 100);
            FlowState<CapEvent> parked = deliver(before, before.evolve(before.initialState(), SagaInput.event(new Opened("c1"))),
                    new Approved("c1", 50), new Noise("c1"), new Noise("c1"));

            assertAll(
                    () -> assertThat(before.isTerminal(parked)).isFalse(),
                    () -> assertThatThrownBy(() -> after.evolve(parked, SagaInput.event(new Approved("c1", 500))))
                            .as("the old count cannot be reused and the events it came from are gone")
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("step 'wait'")
            );
        }

        @Test
        void two_leaves_sharing_a_name_and_a_predicate_are_accepted_under_the_cap() {
            Predicate<Approved> approvedHigh = approved -> approved.score() > 10;
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step
                            .on(StepCondition.event(Approved.class, 2, "isBig", approvedHigh), Continuation.transitionTo("wait"))
                            .on(StepCondition.event(Approved.class, 1, "isBig", approvedHigh), Continuation.end()))
                    .build();

            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            assertThat(saga.isTerminal(saga.evolve(opened, SagaInput.event(new Approved("c1", 50)))))
                    .as("both leaves count the same events, so the count-1 leaf fires and crossing them would change nothing")
                    .isTrue();
        }

        @Test
        void two_leaves_sharing_a_name_while_holding_different_predicates_are_refused_the_cap() {
            FlowSaga.Builder<CapEvent, CapCommand> collidingNames = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("decide", step -> step
                            .on(StepCondition.event(Approved.class, 1, "big", (Approved a) -> a.score() > 100), Continuation.end())
                            .on(StepCondition.event(Approved.class, 1, "big", (Approved a) -> a.score() < 0), Continuation.end()));

            assertThatThrownBy(collidingNames::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("step 'decide'")
                    .hasMessageContaining("share the predicate name 'big'");
        }

        private static Saga<CapEvent, FlowState<CapEvent>, CapCommand> highScoreSaga(String predicateName, int threshold) {
            return FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(1)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(
                            StepCondition.event(Approved.class, 2, predicateName, (Approved a) -> a.score() > threshold),
                            Continuation.end()))
                    .build();
        }

        // The counts a real delivery produced, so a test that wants to corrupt them does not have to restate the fingerprint
        // format and quietly stop describing the step it means.
        private static StepConditionProgress progressOf(FlowState<CapEvent> state) {
            FlowStateImpl<CapEvent> impl = (FlowStateImpl<CapEvent>) state;
            StepConditionProgress progress = impl.stepConditionProgress();
            assertThat(progress).as("this state should be carrying counts").isNotNull();
            return progress;
        }

        private static FlowStateImpl<CapEvent> withCounts(FlowState<CapEvent> state, List<Integer> counts) {
            FlowStateImpl<CapEvent> impl = (FlowStateImpl<CapEvent>) state;
            return new FlowStateImpl<>(impl.currentStep(), impl.received(), impl.windowStart(), impl.stepEntryIndex(),
                    impl.completed(), impl.previousStep(), impl.previousStepEntryIndex(), impl.lastAction(),
                    impl.matchedBranchIndex(), new StepConditionProgress(progressOf(state).leafFingerprint(), counts));
        }

        @Test
        void counts_that_cannot_describe_the_declaration_are_derived_from_the_window_again() {
            // Both combinations a store can produce by defaulting or mangling the field on its own. Neither is a count list
            // any evolve wrote, and the flow is unbounded, so the window is still there to count.
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> oneLeaf = FlowSaga.<CapEvent, CapCommand>builder()
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Approved.class, 2), Continuation.end()))
                    .build();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> twoLeaves = FlowSaga.<CapEvent, CapCommand>builder()
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.allOf(
                            StepCondition.event(Approved.class, 2), StepCondition.event(Noise.class, 1)), Continuation.end()))
                    .build();
            FlowState<CapEvent> oneLeafParked = oneLeaf.evolve(
                    oneLeaf.evolve(oneLeaf.initialState(), SagaInput.event(new Opened("c1"))), SagaInput.event(new Approved("c1", 1)));
            FlowState<CapEvent> twoLeafParked = twoLeaves.evolve(
                    twoLeaves.evolve(twoLeaves.initialState(), SagaInput.event(new Opened("c1"))), SagaInput.event(new Noise("c1")));
            // Both keep the fingerprint a real delivery wrote and corrupt only the counts, one to a value no evolve writes and
            // one to a length that cannot belong to a two-leaf step, which is what reading a leaf's count off the end would be.
            FlowStateImpl<CapEvent> negativeCount = withCounts(oneLeafParked, List.of(-7));
            FlowStateImpl<CapEvent> tooFewCounts = withCounts(twoLeafParked, List.of(1));

            FlowState<CapEvent> fromNegative = oneLeaf.evolve(negativeCount, SagaInput.event(new Approved("c1", 2)));
            FlowState<CapEvent> fromTooFew = twoLeaves.evolve(tooFewCounts, SagaInput.event(new Approved("c1", 2)));
            FlowState<CapEvent> fromTooFewAgain = twoLeaves.evolve(fromTooFew, SagaInput.event(new Approved("c1", 3)));

            assertAll(
                    () -> assertThat(oneLeaf.isTerminal(fromNegative))
                            .as("two Approved are in the window, so counting it again satisfies the count-2 leaf that -7 would not")
                            .isTrue(),
                    () -> assertThat(twoLeaves.isTerminal(fromTooFew)).as("one Approved so far, so the count-2 leaf is short").isFalse(),
                    () -> assertThat(twoLeaves.isTerminal(fromTooFewAgain)).as("both leaves satisfied once counted afresh").isTrue()
            );
        }

        @Test
        void a_count_that_reached_the_integer_limit_stays_there_instead_of_wrapping() {
            // A second leaf that stays short keeps the step from completing, so the counts are still readable after the
            // delivery that would have wrapped the first one.
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(1)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("wait", step -> step.on(StepCondition.allOf(
                            StepCondition.event(Approved.class, 2), StepCondition.event(Noise.class, 5)), Continuation.end()))
                    .build();
            FlowState<CapEvent> parked = saga.evolve(
                    saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1"))), SagaInput.event(new Approved("c1", 1)));
            FlowStateImpl<CapEvent> atTheLimit = withCounts(parked, List.of(Integer.MAX_VALUE, 0));

            FlowState<CapEvent> afterAnother = saga.evolve(atTheLimit, SagaInput.event(new Approved("c1", 2)));

            assertAll(
                    () -> assertThat(progressOf(afterAnother).matchCounts())
                            .as("incrementing at the limit would wrap to a negative count")
                            .containsExactly(Integer.MAX_VALUE, 0),
                    () -> assertThat(saga.isTerminal(afterAnother)).as("the Noise leaf is still short, so the step waits").isFalse()
            );
        }

        @Test
        void step_window_rejects_a_cap_below_one() {
            FlowSaga.Builder<CapEvent, CapCommand> builder = FlowSaga.builder();

            assertAll(
                    () -> assertThatThrownBy(() -> builder.stepWindow(0))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("at least 1"),
                    () -> assertThatThrownBy(() -> builder.stepWindow(-3))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("at least 1")
            );
        }

        @Test
        void a_reaction_under_the_cap_reads_the_kept_step_events_and_not_an_earlier_step_s() {
            List<String> saw = new ArrayList<>();
            Saga<CapEvent, FlowState<CapEvent>, CapCommand> saga = FlowSaga.<CapEvent, CapCommand>builder()
                    .stepWindow(2)
                    .historyWindow(50)
                    .startsOn(Opened.class)
                    .correlateAll(CapEvent::id)
                    .step("first", step -> step.on(Noise.class, Continuation.next()))
                    .step("second", step -> step.on(StepCondition.event(Approved.class, 2), Continuation.end(),
                            received -> {
                                saw.add("kept=" + received.asList().size() + " noise=" + received.count(Noise.class));
                                return List.of();
                            }))
                    .build();
            FlowState<CapEvent> opened = saga.evolve(saga.initialState(), SagaInput.event(new Opened("c1")));

            FlowState<CapEvent> inSecond = deliver(saga, opened, new Noise("c1"));
            FlowState<CapEvent> parked = deliver(saga, deliver(saga, inSecond, new Approved("c1", 1)), noise(5));
            saga.step(parked, SagaInput.event(new Approved("c1", 2)));

            assertThat(saw).as("the cap left 2 of the second step's own events, and the first step's Noise is not among them")
                    .containsExactly("kept=2 noise=1");
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
            // is bounded. Replicates BoundedRetentionWindow's bare-leaf, no-reaction case for the on(StepCondition) path.
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
                            SagaEffect.cancelTimeout(stepTimer("wait")),
                            SagaEffect.startTimeout(stepTimer("next"), Duration.ofMinutes(1)))
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
    class FiredWindow {

        sealed interface WindowEvent permits Started, Approved, Rejected, Go {
            String id();
        }

        record Started(String id) implements WindowEvent {
        }

        record Approved(String id) implements WindowEvent {
        }

        record Rejected(String id) implements WindowEvent {
        }

        record Go(String id) implements WindowEvent {
        }

        sealed interface WindowCommand permits Saw {
        }

        record Saw(String what) implements WindowCommand {
        }

        @Test
        void a_reaction_does_not_see_an_event_an_earlier_step_left_behind() {
            // The Rejected arrives while step "first" is active and matches nothing there, so it stays in the retained log.
            // Step "second" then fires on its two approvals, and its reaction must not be able to mistake that leftover
            // Rejected for one of its own window's events, which is what the anyOf(...) plus received.none(...) pattern used
            // to teach.
            Saga<WindowEvent, FlowState<WindowEvent>, WindowCommand> saga = FlowSaga.<WindowEvent, WindowCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(WindowEvent::id)
                    .step("first", step -> step.on(Go.class, Continuation.next()))
                    .step("second", step -> step.on(
                            StepCondition.anyOf(StepCondition.event(Approved.class, 2), StepCondition.event(Rejected.class)),
                            Continuation.end(),
                            received -> List.of(new Saw(received.any(Rejected.class) ? "rejected" : "approved"))))
                    .build();

            FlowState<WindowEvent> afterStart = start(saga, new Started("w")).state();
            FlowState<WindowEvent> afterRejected = saga.evolve(afterStart, SagaInput.event(new Rejected("w")));
            FlowState<WindowEvent> inSecond = saga.evolve(afterRejected, SagaInput.event(new Go("w")));
            FlowState<WindowEvent> afterFirstApproval = saga.evolve(inSecond, SagaInput.event(new Approved("w")));
            Saga.Step<FlowState<WindowEvent>, WindowCommand> fired = saga.step(afterFirstApproval, SagaInput.event(new Approved("w")));

            assertAll(
                    () -> assertThat(afterRejected.received()).as("the Rejected is retained, it just matched nothing in step one")
                            .contains(new Rejected("w")),
                    () -> assertThat(fired.state().completed()).isTrue(),
                    () -> assertThat(fired.effects()).containsExactly(SagaEffect.issue(new Saw("approved")))
            );
        }

        @Test
        void a_reaction_counts_the_same_events_its_condition_counted_when_the_condition_names_the_start_type() {
            // The evaluator deliberately never counts the start delivery, so a reaction must not either. Handing the reaction
            // the initiating event on top of its window would make the two disagree by exactly one.
            Saga<WindowEvent, FlowState<WindowEvent>, WindowCommand> saga = FlowSaga.<WindowEvent, WindowCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(WindowEvent::id)
                    .step("wait", step -> step.on(
                            StepCondition.event(Started.class, 1),
                            Continuation.end(),
                            received -> List.of(new Saw("count=" + received.count(Started.class)))))
                    .build();

            FlowState<WindowEvent> afterStart = start(saga, new Started("w")).state();
            Saga.Step<FlowState<WindowEvent>, WindowCommand> fired = saga.step(afterStart, SagaInput.event(new Started("w")));

            assertAll(
                    () -> assertThat(fired.state().completed()).as("the second Started fulfils the count-one leaf").isTrue(),
                    () -> assertThat(fired.effects()).as("one Started in the window, the start delivery excluded")
                            .containsExactly(SagaEffect.issue(new Saw("count=1")))
            );
        }

        @Test
        void a_reaction_in_a_step_entered_after_the_retained_window_advanced_still_sees_only_its_own_window() {
            // historyWindow(0) makes windowStart advance on every transition, so by step three the retained list no longer
            // begins at absolute position one and the slice arithmetic is the only thing keeping the window right.
            Saga<WindowEvent, FlowState<WindowEvent>, WindowCommand> saga = FlowSaga.<WindowEvent, WindowCommand>builder()
                    .historyWindow(0)
                    .startsOn(Started.class)
                    .correlateAll(WindowEvent::id)
                    .step("one", step -> step.on(Go.class, Continuation.next()))
                    .step("two", step -> step.on(Go.class, Continuation.next()))
                    .step("three", step -> step.on(
                            StepCondition.event(Go.class, 1),
                            Continuation.end(),
                            received -> List.of(new Saw("window=" + received.asList() + " started=" + received.count(Started.class)
                                    + " initiating=" + received.initiating(Started.class).id()))))
                    .build();

            FlowState<WindowEvent> state = start(saga, new Started("w")).state();
            FlowState<WindowEvent> inTwo = saga.evolve(state, SagaInput.event(new Go("w")));
            FlowState<WindowEvent> inThree = saga.evolve(inTwo, SagaInput.event(new Go("w")));
            Saga.Step<FlowState<WindowEvent>, WindowCommand> fired = saga.step(inThree, SagaInput.event(new Go("w")));

            assertThat(fired.effects()).containsExactly(
                    SagaEffect.issue(new Saw("window=[" + new Go("w") + "] started=0 initiating=w")));
        }

        @Test
        void a_guard_and_a_timeout_reaction_still_read_the_whole_retained_history() {
            // The other half of the rule, since only a window condition's own reaction is narrowed. A guard counting across
            // steps is the documented reason the retained history exists at all.
            List<String> guardSaw = new ArrayList<>();
            Saga<WindowEvent, FlowState<WindowEvent>, WindowCommand> saga = FlowSaga.<WindowEvent, WindowCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(WindowEvent::id)
                    .step("first", step -> step.on(Go.class, Continuation.next()))
                    .step("second", step -> step
                            .on(Approved.class, (approved, received) -> {
                                guardSaw.add("rejected=" + received.count(Rejected.class));
                                return true;
                            }, Continuation.end())
                            .timeout(Duration.ofMinutes(1), Continuation.end(),
                                    received -> List.of(new Saw("rejected=" + received.count(Rejected.class)))))
                    .build();

            FlowState<WindowEvent> afterStart = start(saga, new Started("w")).state();
            FlowState<WindowEvent> afterRejected = saga.evolve(afterStart, SagaInput.event(new Rejected("w")));
            FlowState<WindowEvent> inSecond = saga.evolve(afterRejected, SagaInput.event(new Go("w")));
            Saga.Step<FlowState<WindowEvent>, WindowCommand> expired =
                    saga.step(inSecond, SagaInput.timeout("w", stepTimer("second")));
            saga.evolve(inSecond, SagaInput.event(new Approved("w")));

            assertAll(
                    () -> assertThat(guardSaw).as("the guard counts the Rejected an earlier step left behind").containsExactly("rejected=1"),
                    () -> assertThat(expired.effects()).as("so does the timeout reaction")
                            .containsExactly(SagaEffect.issue(new Saw("rejected=1")))
            );
        }
    }

    @Nested
    class ReconstructedState {

        // A store defaults each absent bookkeeping field on its own, so a document written before one of them existed can
        // decode to a combination no evolve ever produced, either a windowStart past stepEntryIndex or a stepEntryIndex past
        // the end of the retained list. Both are seeded here by hand, which is the only way to reach them, and neither may
        // reach a window-condition evaluation as a raw subList index.

        sealed interface SeedEvent permits Started, Tick {
            String id();
        }

        record Started(String id) implements SeedEvent {
        }

        record Tick(String id) implements SeedEvent {
        }

        sealed interface SeedCommand permits Noop {
        }

        record Noop() implements SeedCommand {
        }

        private static Saga<SeedEvent, FlowState<SeedEvent>, SeedCommand> countingTheStartType() {
            return FlowSaga.<SeedEvent, SeedCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(SeedEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Started.class, 1), Continuation.end()))
                    .build();
        }

        @Test
        void a_window_start_past_the_step_entry_does_not_pull_the_initiating_event_into_the_window() {
            FlowStateImpl<SeedEvent> seeded = new FlowStateImpl<>("wait", List.of(new Started("s"), new Tick("s")),
                    1, 0, false, null, ActionKind.NONE, -1);

            FlowState<SeedEvent> afterTick = countingTheStartType().evolve(seeded, SagaInput.event(new Tick("s")));

            assertThat(countingTheStartType().isTerminal(afterTick))
                    .as("the condition names the start type, and the initiating event is not in any step's window")
                    .isFalse();
        }

        @Test
        void a_step_entry_past_the_end_of_the_retained_list_yields_an_empty_window_rather_than_an_exception() {
            FlowStateImpl<SeedEvent> seeded = new FlowStateImpl<>("wait", List.of(new Started("s")),
                    1, 99, false, null, ActionKind.NONE, -1);

            FlowState<SeedEvent> afterTick = countingTheStartType().evolve(seeded, SagaInput.event(new Tick("s")));

            assertThat(countingTheStartType().isTerminal(afterTick)).isFalse();
        }

        @Test
        void a_previous_step_entry_past_the_end_of_the_retained_list_does_not_break_a_reaction() {
            // The react side of the same exposure. Saga.react is public and a test or a custom executor can call it on a state
            // a store handed back, so the clamp has to cover this path too.
            List<String> saw = new ArrayList<>();
            Saga<SeedEvent, FlowState<SeedEvent>, SeedCommand> saga = FlowSaga.<SeedEvent, SeedCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(SeedEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Tick.class, 1), Continuation.end(), received -> {
                        saw.add("ticks=" + received.count(Tick.class));
                        return List.of();
                    }))
                    .build();
            FlowStateImpl<SeedEvent> seeded = new FlowStateImpl<>(null, List.of(new Started("s"), new Tick("s")),
                    1, 2, true, "wait", 99, ActionKind.BRANCH, 0, null);

            saga.react(seeded, SagaInput.event(new Tick("s")));

            assertThat(saw).as("an out-of-range entry gives an empty window, not an IndexOutOfBoundsException").containsExactly("ticks=0");
        }

        @Test
        void a_state_built_through_the_pre_0_33_0_constructor_reads_the_whole_retained_history_in_a_reaction() {
            // What an out-of-repo store that never learned about the new field produces. The reaction degrades to what it saw
            // before the field existed rather than slicing on a value nobody wrote.
            List<String> saw = new ArrayList<>();
            Saga<SeedEvent, FlowState<SeedEvent>, SeedCommand> saga = FlowSaga.<SeedEvent, SeedCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(SeedEvent::id)
                    .step("wait", step -> step.on(StepCondition.event(Tick.class, 1), Continuation.end(), received -> {
                        saw.add("window=" + received.asList().size());
                        return List.of();
                    }))
                    .build();
            FlowStateImpl<SeedEvent> seeded = new FlowStateImpl<>(null, List.of(new Started("s"), new Tick("s")),
                    1, 2, true, "wait", ActionKind.BRANCH, 0);

            saga.react(seeded, SagaInput.event(new Tick("s")));

            assertAll(
                    () -> assertThat(seeded.previousStepEntryIndex()).isEqualTo(-1),
                    () -> assertThat(saw).containsExactly("window=2")
            );
        }
    }

    @Nested
    class AllOfWindowCondition {

        sealed interface AllOfEvent permits Started, Ready, Note, Go {
            String id();
        }

        record Started(String id) implements AllOfEvent {
        }

        record Ready(String id) implements AllOfEvent {
        }

        record Note(String id) implements AllOfEvent {
        }

        record Go(String id) implements AllOfEvent {
        }

        sealed interface AllOfCommand permits Noop, Saw {
        }

        record Noop() implements AllOfCommand {
        }

        record Saw(String what) implements AllOfCommand {
        }

        @Test
        void a_single_leaf_allOf_reaction_reads_only_its_own_window_not_the_whole_retained_history() {
            // A single-leaf allOf is the shape the deprecated join always lowered to, even for one expectation, so this
            // keeps that case under test now that join itself is gone. Its reaction reads the same window any other
            // on(StepCondition, ...) reaction does, the events received since the step it fired from was entered, so an
            // event an earlier step left behind is not visible to it. initiating() still reaches past the window regardless.
            Saga<AllOfEvent, FlowState<AllOfEvent>, AllOfCommand> saga = FlowSaga.<AllOfEvent, AllOfCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(AllOfEvent::id)
                    .step("first", step -> step.on(Go.class, Continuation.next()))
                    .step("second", step -> step.on(StepCondition.allOf(StepCondition.event(Ready.class, 1)), Continuation.end(),
                            received -> List.of(new Saw("notes=" + received.count(Note.class)
                                    + " initiating=" + received.initiating(Started.class).id()))))
                    .build();

            FlowState<AllOfEvent> afterStart = start(saga, new Started("j")).state();
            FlowState<AllOfEvent> afterNote = saga.evolve(afterStart, SagaInput.event(new Note("j")));
            FlowState<AllOfEvent> inSecond = saga.evolve(afterNote, SagaInput.event(new Go("j")));
            Saga.Step<FlowState<AllOfEvent>, AllOfCommand> fired = saga.step(inSecond, SagaInput.event(new Ready("j")));

            assertAll(
                    () -> assertThat(fired.state().completed()).isTrue(),
                    () -> assertThat(fired.effects())
                            .as("the Note from step one is outside this step's own window, but initiating() still reaches the start event")
                            .containsExactly(SagaEffect.issue(new Saw("notes=0 initiating=j")))
            );
        }

        @Test
        void a_single_leaf_allOf_reaction_does_not_count_an_earlier_steps_event_of_the_same_type_it_is_waiting_for() {
            // The window narrows by position, not by type, so an earlier Ready left behind by step one must drop out of
            // the second step's own count exactly as an unrelated type would, and not be added to the Ready that fired it.
            Saga<AllOfEvent, FlowState<AllOfEvent>, AllOfCommand> saga = FlowSaga.<AllOfEvent, AllOfCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(AllOfEvent::id)
                    .step("first", step -> step.on(Go.class, Continuation.next()))
                    .step("second", step -> step.on(StepCondition.allOf(StepCondition.event(Ready.class, 1)), Continuation.end(),
                            received -> List.of(new Saw("readies=" + received.count(Ready.class)))))
                    .build();

            FlowState<AllOfEvent> afterStart = start(saga, new Started("j")).state();
            FlowState<AllOfEvent> afterEarlyReady = saga.evolve(afterStart, SagaInput.event(new Ready("j")));
            FlowState<AllOfEvent> inSecond = saga.evolve(afterEarlyReady, SagaInput.event(new Go("j")));
            Saga.Step<FlowState<AllOfEvent>, AllOfCommand> fired = saga.step(inSecond, SagaInput.event(new Ready("j")));

            assertThat(fired.effects())
                    .as("only the Ready that fired this step's condition is in view, not the one step one left behind")
                    .containsExactly(SagaEffect.issue(new Saw("readies=1")));
        }

        @Test
        void a_first_steps_single_leaf_allOf_reaction_does_not_see_the_initiating_event_through_a_generic_accessor() {
            // The window a WindowCondition reaction reads always starts after index 0, the pinned initiating event, even
            // for a saga's first step. initiating() is the one accessor built to reach past that, so a reaction that
            // instead counts its own start type through count(...) sees zero, not one.
            Saga<AllOfEvent, FlowState<AllOfEvent>, AllOfCommand> saga = FlowSaga.<AllOfEvent, AllOfCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(AllOfEvent::id)
                    .step("first", step -> step.on(StepCondition.allOf(StepCondition.event(Ready.class, 1)), Continuation.end(),
                            received -> List.of(new Saw("starts=" + received.count(Started.class)
                                    + " initiating=" + received.initiating(Started.class).id()))))
                    .build();

            FlowState<AllOfEvent> afterStart = start(saga, new Started("j")).state();
            Saga.Step<FlowState<AllOfEvent>, AllOfCommand> fired = saga.step(afterStart, SagaInput.event(new Ready("j")));

            assertThat(fired.effects())
                    .as("the start event is outside even a first step's window, but initiating() still reaches it")
                    .containsExactly(SagaEffect.issue(new Saw("starts=0 initiating=j")));
        }

        @Test
        void the_recipes_collapsed_allOf_shape_still_needs_the_higher_count_and_keeps_first_appearance_order() {
            // The deprecated join collapsed two same-type expectations to the higher count before building its allOf tree
            // (StepBuilder.toConditions, removed with join in 0.34.0), since allOf itself refuses two children matching
            // the same events. The UpgradeToOccurrent_0_34 recipe reproduces that exact collapse when it rewrites a
            // provable join(List.of(Expectation.of(Ready.class, 1), Expectation.of(Note.class, 1), Expectation.of(Ready.class,
            // 3)), ...) call, so this declares the collapsed shape directly, first-appearance order included, and proves it
            // behaves the way the pre-collapse join did.
            Saga<AllOfEvent, FlowState<AllOfEvent>, AllOfCommand> saga = FlowSaga.<AllOfEvent, AllOfCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(AllOfEvent::id)
                    .step("wait", step -> step.on(
                            StepCondition.allOf(StepCondition.event(Ready.class, 3), StepCondition.event(Note.class, 1)),
                            Continuation.end()))
                    .build();

            FlowState<AllOfEvent> state = start(saga, new Started("j")).state();
            state = saga.evolve(state, SagaInput.event(new Note("j")));
            state = saga.evolve(state, SagaInput.event(new Ready("j")));
            state = saga.evolve(state, SagaInput.event(new Ready("j")));
            FlowState<AllOfEvent> afterTwoReady = state;
            FlowState<AllOfEvent> afterThirdReady = saga.evolve(afterTwoReady, SagaInput.event(new Ready("j")));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterTwoReady)).as("two Ready is short of the highest count asked for").isFalse(),
                    () -> assertThat(saga.isTerminal(afterThirdReady)).isTrue()
            );
        }

        @Test
        void the_recipes_collapsed_allOf_shape_for_one_type_still_needs_the_higher_count() {
            // Mirrors the recipe's collapse of join(List.of(Expectation.of(Ready.class, 2), Expectation.of(Ready.class, 3)),
            // ...), a join naming one type twice, to a single event(Ready.class, 3) leaf.
            Saga<AllOfEvent, FlowState<AllOfEvent>, AllOfCommand> saga = FlowSaga.<AllOfEvent, AllOfCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(AllOfEvent::id)
                    .step("wait", step -> step.on(StepCondition.allOf(StepCondition.event(Ready.class, 3)), Continuation.end()))
                    .build();

            FlowState<AllOfEvent> state = start(saga, new Started("j")).state();
            FlowState<AllOfEvent> afterFirst = saga.evolve(state, SagaInput.event(new Ready("j")));
            FlowState<AllOfEvent> afterSecond = saga.evolve(afterFirst, SagaInput.event(new Ready("j")));
            FlowState<AllOfEvent> afterThird = saga.evolve(afterSecond, SagaInput.event(new Ready("j")));

            assertAll(
                    () -> assertThat(saga.isTerminal(afterSecond)).as("two Ready is short of the higher count").isFalse(),
                    () -> assertThat(saga.isTerminal(afterThird)).as("the third fulfils it").isTrue()
            );
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

    @Nested
    class StepTimerName {

        @Test
        void a_steps_timer_is_named_inside_the_step_namespace() {
            assertThat(stepTimer("awaiting-payment")).isEqualTo(TimerName.of("step", "awaiting-payment"));
        }

        @Test
        void a_steps_timer_is_stored_under_the_step_name_behind_a_step_prefix() {
            assertThat(stepTimer("awaiting-payment").encode()).isEqualTo("step:awaiting-payment");
        }
    }

    @Nested
    class MissingStep {

        sealed interface FlowEvent permits Started, Progressed {
            String id();
        }

        record Started(String id) implements FlowEvent {
        }

        record Progressed(String id) implements FlowEvent {
        }

        record FlowCommand() {
        }

        // "second" is where an instance parks once "first" transitions it on. Building with a different name for that
        // same step is what a rename or a removal looks like to evolve, which is pure: two builds over one persisted
        // state, the same idiom StepConditions.ReconstructedState-style declaration-change tests already use.
        private static Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> flow(String secondStepName) {
            return FlowSaga.<FlowEvent, FlowCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(FlowEvent::id)
                    .step("first", step -> step.on(Progressed.class, Continuation.transitionTo(secondStepName)))
                    .step(secondStepName, step -> step
                            .on(Progressed.class, Continuation.end())
                            .timeout(Duration.ofMinutes(5), Continuation.end(), received -> List.of()))
                    .build();
        }

        @Test
        void an_event_delivered_to_a_step_that_has_been_renamed_or_removed_refuses_the_delivery() {
            Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> before = flow("second");
            Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> after = flow("renamed");
            FlowState<FlowEvent> opened = before.evolve(before.initialState(), SagaInput.event(new Started("c1")));
            FlowState<FlowEvent> parked = before.evolve(opened, SagaInput.event(new Progressed("c1")));

            assertThatThrownBy(() -> after.step(parked, SagaInput.event(new Progressed("c1"))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("step 'second'")
                    .hasMessageContaining("no longer exists");
        }

        @Test
        void a_due_timer_firing_into_a_step_that_has_been_renamed_or_removed_refuses_the_delivery() {
            Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> before = flow("second");
            Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> after = flow("renamed");
            FlowState<FlowEvent> opened = before.evolve(before.initialState(), SagaInput.event(new Started("c1")));
            FlowState<FlowEvent> parked = before.evolve(opened, SagaInput.event(new Progressed("c1")));

            assertThatThrownBy(() -> after.step(parked, SagaInput.timeout("c1", stepTimer("second"))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("step 'second'")
                    .hasMessageContaining("no longer exists");
        }

        @Test
        void a_step_still_present_but_with_fewer_branches_matches_nothing_rather_than_reading_a_stale_branch_index() {
            // The IndexOutOfBoundsException shape #748 also describes comes from reactToBranch reading a branch index
            // evolve computed against a step that has since lost that branch. "after" keeps "second" but drops the
            // branch "before" would have matched on Progressed, leaving only its timeout. saga.step(...) runs evolve
            // then react in the same call, and evolve re-evaluates branches fresh against "after"'s own (now smaller)
            // list every time, so it just finds no match instead of handing react a stale index. No branch removed from
            // a step that still exists can reach react with an index that step's own branches() does not have.
            Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> before = flow("second");
            Saga<FlowEvent, FlowState<FlowEvent>, FlowCommand> after = FlowSaga.<FlowEvent, FlowCommand>builder()
                    .startsOn(Started.class)
                    .correlateAll(FlowEvent::id)
                    .step("first", step -> step.on(Progressed.class, Continuation.transitionTo("second")))
                    .step("second", step -> step
                            .on(Started.class, Continuation.end())
                            .timeout(Duration.ofMinutes(5), Continuation.end(), received -> List.of()))
                    .build();
            FlowState<FlowEvent> opened = before.evolve(before.initialState(), SagaInput.event(new Started("c1")));
            FlowState<FlowEvent> parked = before.evolve(opened, SagaInput.event(new Progressed("c1")));

            Saga.Step<FlowState<FlowEvent>, FlowCommand> result = after.step(parked, SagaInput.event(new Progressed("c1")));

            assertAll(
                    () -> assertThat(result.state().currentStep()).as("no branch matched, so the instance stays parked").isEqualTo("second"),
                    () -> assertThat(after.isTerminal(result.state())).as("nothing transitioned it").isFalse(),
                    () -> assertThat(result.effects()).as("no branch fired, so no reaction ran").isEmpty()
            );
        }
    }
}
