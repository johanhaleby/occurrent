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

package org.occurrent.dsl.saga;

import org.junit.jupiter.api.*;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("Saga")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SagaTest {

    // --- A tiny order/payment domain, used across most nested test classes below ---

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved, PaymentFailed {
        String orderId();
    }

    record OrderPlaced(String orderId, int amount) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    record PaymentFailed(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ReservePayment, CancelOrder {
    }

    record ReservePayment(String orderId, int amount) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Paid, Cancelled {
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Paid(String orderId) implements OrderState {
    }

    record Cancelled(String orderId) implements OrderState {
    }

    private static final String PAYMENT_TIMER = "payment";

    /** The canonical saga used by most tests below: reserve payment on order placement, cancel on failure or timeout. */
    private static Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(
                        SagaEffect.issue(new ReservePayment(e.orderId(), e.amount())),
                        SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30))))
                .evolve(PaymentReserved.class, (state, e) -> new Paid(((AwaitingPayment) state).orderId()))
                .react(PaymentReserved.class, (state, e) -> List.of(SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                .evolve(PaymentFailed.class, (state, e) -> new Cancelled(((AwaitingPayment) state).orderId()))
                .react(PaymentFailed.class, (state, e) -> List.of(SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                .evolveOnTimeout(PAYMENT_TIMER, (state, t) -> new Cancelled(((AwaitingPayment) state).orderId()))
                .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                .isTerminal(state -> state instanceof Cancelled || state instanceof Paid)
                .build();
    }

    @Nested
    class StepOrdering {

        @Test
        void react_sees_the_state_produced_by_evolve_not_the_state_before_it() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            // PaymentReserved's react() only makes sense on the post-evolve Paid state (it reads nothing from the
            // event itself), so an implementation that fed react() the pre-evolve AwaitingPayment would still pass a
            // naive check; the assertion on the resulting state is what actually pins the ordering down.
            Saga.Step<OrderState, OrderCommand> step = saga.step(new AwaitingPayment("order-1"), SagaInput.event(new PaymentReserved("order-1")));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new Paid("order-1")),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.cancelTimeout(PAYMENT_TIMER))
            );
        }

        @Test
        void combines_evolve_and_react_for_the_start_event() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            Saga.Step<OrderState, OrderCommand> step = saga.step(null, SagaInput.event(new OrderPlaced("order-1", 100)));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new AwaitingPayment("order-1")),
                    () -> assertThat(step.effects()).containsExactly(
                            SagaEffect.issue(new ReservePayment("order-1", 100)),
                            SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)))
            );
        }
    }

    @Nested
    class StepConstruction {

        @Test
        void throws_NullPointerException_when_effects_is_null() {
            assertThatThrownBy(() -> new Saga.Step<OrderState, OrderCommand>(new AwaitingPayment("order-1"), null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_effects_contains_a_null_element() {
            List<SagaEffect<OrderCommand>> effects = new ArrayList<>();
            effects.add(null);

            assertThatThrownBy(() -> new Saga.Step<>(new AwaitingPayment("order-1"), effects))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void the_stored_effects_list_is_unmodifiable() {
            // Hand in a mutable list, otherwise this passes whether or not the constructor copies.
            List<SagaEffect<OrderCommand>> effects = new ArrayList<>();
            effects.add(SagaEffect.cancelTimeout(PAYMENT_TIMER));

            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), effects);

            assertThatThrownBy(() -> step.effects().add(SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void mutating_the_caller_list_afterwards_does_not_change_the_stored_effects() {
            List<SagaEffect<OrderCommand>> effects = new ArrayList<>();
            effects.add(SagaEffect.cancelTimeout(PAYMENT_TIMER));

            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), effects);
            effects.add(SagaEffect.issue(new ReservePayment("order-1", 100)));

            assertThat(step.effects()).containsExactly(SagaEffect.cancelTimeout(PAYMENT_TIMER));
        }
    }

    @Nested
    class IssuedCommands {

        @Test
        void is_empty_when_the_step_produced_no_effects() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of());

            assertThat(step.issuedCommands()).isEmpty();
        }

        @Test
        void is_empty_when_the_only_effect_is_a_timer_cancellation() {
            // The case the accessor exists for. Reacting to PaymentReserved issues nothing, but leaving the armed
            // timeout behind still contributes a CancelTimeout, so effects() is not empty and cannot answer
            // "did this reaction issue anything".
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            Saga.Step<OrderState, OrderCommand> step = saga.step(new AwaitingPayment("order-1"), SagaInput.event(new PaymentReserved("order-1")));

            assertAll(
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.cancelTimeout(PAYMENT_TIMER)),
                    () -> assertThat(step.issuedCommands()).isEmpty()
            );
        }

        @Test
        void ignores_all_three_kinds_of_timer_effect() {
            // StartTimeoutAt is the one no saga in this file produces, so build the step by hand to cover it.
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)),
                    SagaEffect.startTimeoutAt(PAYMENT_TIMER, Instant.parse("2026-07-28T12:00:00Z")),
                    SagaEffect.cancelTimeout(PAYMENT_TIMER)));

            assertThat(step.issuedCommands()).isEmpty();
        }

        @Test
        void returns_the_commands_in_effect_order_and_drops_the_timers() {
            // Interleaved rather than commands-then-timers, otherwise this passes even on an implementation that sorts
            // or partitions the list.
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.issue(new ReservePayment("order-1", 100)),
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)),
                    SagaEffect.issue(new CancelOrder("order-1")),
                    SagaEffect.cancelTimeout(PAYMENT_TIMER)));

            assertThat(step.issuedCommands()).containsExactly(new ReservePayment("order-1", 100), new CancelOrder("order-1"));
        }

        @Test
        void keeps_a_command_that_was_issued_twice() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.issue(new CancelOrder("order-1")),
                    SagaEffect.issue(new CancelOrder("order-1"))));

            assertThat(step.issuedCommands()).hasSize(2);
        }

        @Test
        void the_returned_list_is_unmodifiable() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"),
                    List.of(SagaEffect.issue(new CancelOrder("order-1"))));

            assertThatThrownBy(() -> step.issuedCommands().add(new CancelOrder("order-2")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void is_recomputed_on_each_call_rather_than_shared() {
            // Pins the documented per-call contract, so a later "cache it" change cannot start handing out one aliased
            // list. Needs a non-empty step, because List.copyOf of an empty list returns the shared List.of().
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"),
                    List.of(SagaEffect.issue(new CancelOrder("order-1"))));

            assertAll(
                    () -> assertThat(step.issuedCommands()).isEqualTo(step.issuedCommands()),
                    () -> assertThat(step.issuedCommands()).isNotSameAs(step.issuedCommands())
            );
        }

        @Test
        void does_not_take_part_in_equality_or_toString() {
            // The reason this is a derived accessor and not a third record component.
            List<SagaEffect<OrderCommand>> effects = List.of(SagaEffect.issue(new CancelOrder("order-1")));
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), effects);

            assertAll(
                    () -> assertThat(step).isEqualTo(new Saga.Step<>(new AwaitingPayment("order-1"), effects)),
                    () -> assertThat(step.toString()).doesNotContain("issuedCommands")
            );
        }

        @Test
        void reads_the_command_out_of_a_step_the_saga_itself_produced() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            Saga.Step<OrderState, OrderCommand> step = saga.step(null, SagaInput.event(new OrderPlaced("order-1", 100)));

            // effects() carries the armed timeout too, which is exactly what makes it the wrong thing to assert on here.
            assertAll(
                    () -> assertThat(step.effects()).hasSize(2),
                    () -> assertThat(step.issuedCommands()).containsExactly(new ReservePayment("order-1", 100))
            );
        }
    }

    @Nested
    class TimerEffects {

        @Test
        void is_empty_when_the_step_produced_no_effects() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of());

            assertThat(step.timerEffects()).isEmpty();
        }

        @Test
        void is_empty_when_the_only_effect_is_a_command() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"),
                    List.of(SagaEffect.issue(new CancelOrder("order-1"))));

            assertThat(step.timerEffects()).isEmpty();
        }

        @Test
        void isolates_the_timer_from_a_step_that_also_issued_a_command() {
            // The case the accessor exists for. On the mixed effects() list this assertion could only be contains(...),
            // which cannot show that no other timer was touched.
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.issue(new ReservePayment("order-1", 100)),
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30))));

            assertThat(step.timerEffects()).containsExactly(SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)));
        }

        @Test
        void keeps_all_three_kinds_of_timer_effect() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)),
                    SagaEffect.startTimeoutAt(PAYMENT_TIMER, Instant.parse("2026-07-28T12:00:00Z")),
                    SagaEffect.cancelTimeout(PAYMENT_TIMER)));

            assertThat(step.timerEffects()).hasSize(3);
        }

        @Test
        void returns_the_timers_in_effect_order_and_drops_the_commands() {
            // Interleaved on purpose, so an implementation that partitions or sorts the list would fail here.
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)),
                    SagaEffect.issue(new ReservePayment("order-1", 100)),
                    SagaEffect.cancelTimeout(PAYMENT_TIMER)));

            assertThat(step.timerEffects()).containsExactly(
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)),
                    SagaEffect.cancelTimeout(PAYMENT_TIMER));
        }

        @Test
        void partitions_effects_together_with_issuedCommands() {
            // The documented relationship between the two accessors, so neither can start dropping or duplicating.
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"), List.of(
                    SagaEffect.issue(new ReservePayment("order-1", 100)),
                    SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)),
                    SagaEffect.issue(new CancelOrder("order-1")),
                    SagaEffect.cancelTimeout(PAYMENT_TIMER)));

            assertThat(step.issuedCommands().size() + step.timerEffects().size()).isEqualTo(step.effects().size());
        }

        @Test
        void the_returned_list_is_unmodifiable() {
            Saga.Step<OrderState, OrderCommand> step = new Saga.Step<>(new AwaitingPayment("order-1"),
                    List.of(SagaEffect.cancelTimeout(PAYMENT_TIMER)));

            assertThatThrownBy(() -> step.timerEffects().add(SagaEffect.cancelTimeout("other")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class OnStart {

        @Test
        void defaults_to_no_effects() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            List<SagaEffect<OrderCommand>> effects = saga.onStart(new AwaitingPayment("order-1"), new OrderPlaced("order-1", 100));

            assertThat(effects).isEmpty();
        }

        @Test
        void exposes_a_builder_registered_handler() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .onStart((state, e) -> List.of(SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30))))
                    .build();

            List<SagaEffect<OrderCommand>> effects = saga.onStart(new AwaitingPayment("order-1"), new OrderPlaced("order-1", 100));

            assertThat(effects).containsExactly(SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)));
        }
    }

    @Nested
    class MetadataOverloads {

        private static EventMetadata metadata(String streamId, long streamVersion, long position) {
            return new EventMetadata(Map.of(
                    OccurrentCloudEventExtension.STREAM_ID, streamId,
                    OccurrentCloudEventExtension.STREAM_VERSION, streamVersion,
                    OccurrentCloudEventExtension.POSITION, position));
        }

        @Test
        void evolve_and_react_metadata_handlers_receive_the_delivered_events_metadata() {
            AtomicReference<EventMetadata> seenByEvolve = new AtomicReference<>();
            AtomicReference<EventMetadata> seenByReact = new AtomicReference<>();

            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, meta, e) -> {
                        seenByEvolve.set(meta);
                        return new AwaitingPayment(e.orderId());
                    })
                    .react(OrderPlaced.class, (state, meta, e) -> {
                        seenByReact.set(meta);
                        return List.of(SagaEffect.issue(new ReservePayment(e.orderId(), e.amount())));
                    })
                    .build();

            EventMetadata metadata = metadata("stream-1", 7L, 42L);
            Saga.Step<OrderState, OrderCommand> step = saga.step(null, SagaInput.event(new OrderPlaced("order-1", 100), metadata));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new AwaitingPayment("order-1")),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new ReservePayment("order-1", 100))),
                    () -> assertThat(seenByEvolve.get().getStreamId()).isEqualTo("stream-1"),
                    () -> assertThat(seenByEvolve.get().getStreamVersion()).isEqualTo(7L),
                    () -> assertThat(seenByEvolve.get().getPosition()).isEqualTo(42L),
                    () -> assertThat(seenByReact.get().getStreamId()).isEqualTo("stream-1"),
                    () -> assertThat(seenByReact.get().getStreamVersion()).isEqualTo(7L),
                    () -> assertThat(seenByReact.get().getPosition()).isEqualTo(42L)
            );
        }

        @Test
        void onStart_metadata_handler_receives_the_start_events_metadata() {
            AtomicReference<EventMetadata> seenByOnStart = new AtomicReference<>();

            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .onStart((state, meta, e) -> {
                        seenByOnStart.set(meta);
                        return List.of(SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)));
                    })
                    .build();

            EventMetadata metadata = metadata("stream-9", 3L, 11L);
            List<SagaEffect<OrderCommand>> effects = saga.onStart(new AwaitingPayment("order-1"), metadata, new OrderPlaced("order-1", 100));

            assertAll(
                    () -> assertThat(effects).containsExactly(SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30))),
                    () -> assertThat(seenByOnStart.get().getStreamId()).isEqualTo("stream-9"),
                    () -> assertThat(seenByOnStart.get().getStreamVersion()).isEqualTo(3L),
                    () -> assertThat(seenByOnStart.get().getPosition()).isEqualTo(11L)
            );
        }

        @Test
        void event_only_handlers_still_work_when_the_input_carries_metadata() {
            // The metadata-less builder overloads keep working unchanged. The metadata riding on the input is simply
            // ignored by a handler registered through the two-argument form.
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            Saga.Step<OrderState, OrderCommand> step = saga.step(null,
                    SagaInput.event(new OrderPlaced("order-1", 100), metadata("stream-1", 1L, 1L)));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new AwaitingPayment("order-1")),
                    () -> assertThat(step.effects()).containsExactly(
                            SagaEffect.issue(new ReservePayment("order-1", 100)),
                            SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)))
            );
        }
    }

    @Nested
    class IsTerminal {

        @Test
        void defaults_to_false() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .build();

            assertThat(saga.isTerminal(new Cancelled("order-1"))).isFalse();
        }

        @Test
        void reflects_a_registered_predicate() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            assertAll(
                    () -> assertThat(saga.isTerminal(new Cancelled("order-1"))).isTrue(),
                    () -> assertThat(saga.isTerminal(new Paid("order-1"))).isTrue(),
                    () -> assertThat(saga.isTerminal(new AwaitingPayment("order-1"))).isFalse()
            );
        }
    }

    @Nested
    class CorrelationId {

        @Test
        void a_per_type_correlate_wins_over_correlateAll() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlate(OrderPlaced.class, e -> "specific-" + e.orderId())
                    .correlateAll(e -> "fallback-" + e.orderId())
                    .startsOn(OrderPlaced.class)
                    .build();

            assertThat(saga.sagaId(new OrderPlaced("order-1", 100))).isEqualTo("specific-order-1");
        }

        @Test
        void falls_back_to_correlateAll_for_a_type_without_its_own_correlate() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlate(OrderPlaced.class, e -> "specific-" + e.orderId())
                    .correlateAll(e -> "fallback-" + e.orderId())
                    .startsOn(OrderPlaced.class)
                    .build();

            assertThat(saga.sagaId(new PaymentReserved("order-1"))).isEqualTo("fallback-order-1");
        }

        @Test
        void returns_null_for_an_unhandled_type_with_no_correlate_and_no_correlateAll() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlate(OrderPlaced.class, e -> "specific-" + e.orderId())
                    .startsOn(OrderPlaced.class)
                    .build();

            assertThat(saga.sagaId(new PaymentFailed("order-1"))).isNull();
        }

        @Test
        void resolves_a_correlate_registered_on_a_supertype_for_a_concrete_subtype() {
            // correlate() is registered on the OrderEvent interface itself, fed a concrete OrderPlaced.
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlate(OrderEvent.class, OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .build();

            assertThat(saga.sagaId(new OrderPlaced("order-1", 100))).isEqualTo("order-1");
        }
    }

    @Nested
    class EventTypesAndStartEventTypes {

        @Test
        void eventTypes_is_the_union_of_evolve_react_and_startsOn_registrations() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(PaymentReserved.class, (state, e) -> state)
                    .react(PaymentFailed.class, (state, e) -> List.of())
                    .build();

            assertThat(saga.eventTypes())
                    .containsExactlyInAnyOrder(OrderPlaced.class, PaymentReserved.class, PaymentFailed.class);
        }

        @Test
        void startEventTypes_is_exactly_the_startsOn_registrations() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(PaymentReserved.class, (state, e) -> state)
                    .react(PaymentFailed.class, (state, e) -> List.of())
                    .build();

            assertThat(saga.startEventTypes()).containsExactly(OrderPlaced.class);
        }

        @Test
        void eventTypes_defaults_to_empty_when_only_created_via_the_static_factory_without_types() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.create(
                    null,
                    OrderEvent::orderId,
                    Set.of(OrderPlaced.class),
                    Set.of(),
                    (state, input) -> state,
                    (state, input) -> List.of());

            assertThat(saga.eventTypes()).isEmpty();
        }

        @Test
        void create_unions_start_types_into_a_non_empty_eventTypes_so_a_start_type_is_never_filtered_off_the_subscription() {
            // A non-empty eventTypes that omits the start type would otherwise narrow the subscription so the start event
            // never reaches the saga and no instance could ever be created. create(...) must union it in, like builder().
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.create(
                    null,
                    OrderEvent::orderId,
                    Set.of(OrderPlaced.class),
                    Set.of(PaymentReserved.class),
                    (state, input) -> state,
                    (state, input) -> List.of());

            assertThat(saga.eventTypes()).containsExactlyInAnyOrder(OrderPlaced.class, PaymentReserved.class);
        }

        @Test
        void create_throws_IllegalArgumentException_when_startEventTypes_is_empty() {
            assertThatThrownBy(() -> Saga.<OrderEvent, OrderState, OrderCommand>create(
                    null,
                    OrderEvent::orderId,
                    Set.of(),
                    Set.of(),
                    (state, input) -> state,
                    (state, input) -> List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("start event type");
        }
    }

    @Nested
    class BuilderGuards {

        @Test
        void throws_IllegalStateException_on_duplicate_correlate_for_the_same_type() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlate(OrderPlaced.class, OrderEvent::orderId);

            assertThatThrownBy(() -> builder.correlate(OrderPlaced.class, OrderEvent::orderId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("correlate")
                    .hasMessageContaining("OrderPlaced");
        }

        @Test
        void throws_IllegalStateException_on_duplicate_evolve_for_the_same_type() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .evolve(OrderPlaced.class, (state, e) -> state);

            assertThatThrownBy(() -> builder.evolve(OrderPlaced.class, (state, e) -> state))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("evolve")
                    .hasMessageContaining("OrderPlaced");
        }

        @Test
        void throws_IllegalStateException_on_duplicate_react_for_the_same_type() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .react(OrderPlaced.class, (state, e) -> List.of());

            assertThatThrownBy(() -> builder.react(OrderPlaced.class, (state, e) -> List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("react")
                    .hasMessageContaining("OrderPlaced");
        }

        @Test
        void throws_IllegalStateException_on_duplicate_startsOn_for_the_same_type() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .startsOn(OrderPlaced.class);

            assertThatThrownBy(() -> builder.startsOn(OrderPlaced.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("startsOn")
                    .hasMessageContaining("OrderPlaced");
        }

        @Test
        void throws_IllegalStateException_on_duplicate_evolveOnTimeout_for_the_same_name() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .evolveOnTimeout(PAYMENT_TIMER, (state, t) -> state);

            assertThatThrownBy(() -> builder.evolveOnTimeout(PAYMENT_TIMER, (state, t) -> state))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("evolveOnTimeout")
                    .hasMessageContaining(PAYMENT_TIMER);
        }

        @Test
        void throws_IllegalStateException_on_duplicate_reactOnTimeout_for_the_same_name() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of());

            assertThatThrownBy(() -> builder.reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reactOnTimeout")
                    .hasMessageContaining(PAYMENT_TIMER);
        }

        @Test
        void throws_IllegalStateException_on_duplicate_evolveOnTimeout_registered_as_a_string_and_as_a_TimerName() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .evolveOnTimeout("a:b", (state, t) -> state);

            assertThatThrownBy(() -> builder.evolveOnTimeout(TimerName.of("a", "b"), (state, t) -> state))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("evolveOnTimeout")
                    .hasMessageContaining("a:b");
        }

        @Test
        void throws_IllegalStateException_on_duplicate_reactOnTimeout_registered_as_a_string_and_as_a_TimerName() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .reactOnTimeout("a:b", (state, t) -> List.of());

            assertThatThrownBy(() -> builder.reactOnTimeout(TimerName.of("a", "b"), (state, t) -> List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reactOnTimeout")
                    .hasMessageContaining("a:b");
        }

        @Test
        void throws_IllegalStateException_when_correlateAll_is_set_twice() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId);

            assertThatThrownBy(() -> builder.correlateAll(OrderEvent::orderId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("correlateAll");
        }

        @Test
        void throws_IllegalStateException_when_onStart_is_set_twice() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .onStart((state, e) -> List.of());

            assertThatThrownBy(() -> builder.onStart((state, e) -> List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("onStart");
        }

        @Test
        void throws_IllegalStateException_when_isTerminal_is_set_twice() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .isTerminal(state -> false);

            assertThatThrownBy(() -> builder.isTerminal(state -> true))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("isTerminal");
        }
    }

    @Nested
    class BuildValidation {

        @Test
        void throws_IllegalStateException_when_no_startsOn_type_was_registered() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("startsOn");
        }

        @Test
        void throws_IllegalStateException_naming_a_handled_type_with_no_correlation_and_no_correlateAll() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .startsOn(OrderPlaced.class)
                    .evolve(PaymentFailed.class, (state, e) -> state);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("PaymentFailed");
        }

        @Test
        void correlateAll_covers_every_handled_type_without_a_per_type_correlate() {
            Saga.Builder<OrderEvent, OrderState, OrderCommand> builder = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(PaymentReserved.class, (state, e) -> state)
                    .react(PaymentFailed.class, (state, e) -> List.of());

            Saga<OrderEvent, OrderState, OrderCommand> saga = builder.build();

            assertThat(saga).isNotNull();
        }
    }

    @Nested
    class TimeoutDispatch {

        @Test
        void dispatches_evolve_and_react_by_the_timer_name() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();

            Saga.Step<OrderState, OrderCommand> step = saga.step(new AwaitingPayment("order-1"),
                    SagaInput.timeout(new SagaTimeout("order-1", TimerName.parse(PAYMENT_TIMER))));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new Cancelled("order-1")),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new CancelOrder("order-1")))
            );
        }

        @Test
        void an_unregistered_timer_name_leaves_state_unchanged_and_produces_no_effects() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();
            OrderState state = new AwaitingPayment("order-1");

            Saga.Step<OrderState, OrderCommand> step = saga.step(state, SagaInput.timeout(new SagaTimeout("order-1", TimerName.parse("unknown-timer"))));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(state),
                    () -> assertThat(step.effects()).isEmpty()
            );
        }
    }

    @Nested
    class Adapt {

        private Saga<Object, OrderState, Object> widened() {
            Saga<OrderEvent, OrderState, OrderCommand> inner = orderFulfillment();
            return Saga.<Object, OrderState, Object, OrderEvent, OrderCommand>adapt(inner, OrderEvent.class);
        }

        @Test
        void a_foreign_event_leaves_state_unchanged_and_produces_no_effects() {
            Saga<Object, OrderState, Object> saga = widened();

            Saga.Step<OrderState, Object> step = saga.step(null, SagaInput.event("not-an-order-event"));

            assertAll(
                    () -> assertThat(step.state()).isNull(),
                    () -> assertThat(step.effects()).isEmpty()
            );
        }

        @Test
        void a_foreign_event_correlates_to_null() {
            Saga<Object, OrderState, Object> saga = widened();

            assertThat(saga.sagaId("not-an-order-event")).isNull();
        }

        @Test
        void a_SubE_event_delegates_to_the_inner_saga() {
            Saga<Object, OrderState, Object> saga = widened();

            Saga.Step<OrderState, Object> step = saga.step(null, SagaInput.event(new OrderPlaced("order-1", 100)));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new AwaitingPayment("order-1")),
                    () -> assertThat(step.effects()).containsExactly(
                            SagaEffect.issue(new ReservePayment("order-1", 100)),
                            SagaEffect.startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)))
            );
        }

        @Test
        void a_timeout_passes_through_to_the_inner_saga() {
            Saga<Object, OrderState, Object> saga = widened();

            Saga.Step<OrderState, Object> step = saga.step(new AwaitingPayment("order-1"),
                    SagaInput.timeout(new SagaTimeout("order-1", TimerName.parse(PAYMENT_TIMER))));

            assertAll(
                    () -> assertThat(step.state()).isEqualTo(new Cancelled("order-1")),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new CancelOrder("order-1")))
            );
        }

        @Test
        void issuedCommands_reads_through_the_widened_command_type() {
            // widen casts the effect list, so at runtime these are IssueCommand<OrderCommand> behind a static
            // IssueCommand<Object>. The accessor's type pattern is erased, so it matches either way, and this is the
            // test that proves that cast stays benign under the new accessor. Both paths through widen are covered:
            // the event path here and the timeout path below.
            Saga<Object, OrderState, Object> saga = widened();

            Saga.Step<OrderState, Object> fromEvent = saga.step(null, SagaInput.event(new OrderPlaced("order-1", 100)));
            Saga.Step<OrderState, Object> fromTimeout = saga.step(new AwaitingPayment("order-1"),
                    SagaInput.timeout(new SagaTimeout("order-1", TimerName.parse(PAYMENT_TIMER))));

            assertAll(
                    () -> assertThat(fromEvent.issuedCommands()).containsExactly(new ReservePayment("order-1", 100)),
                    () -> assertThat(fromTimeout.issuedCommands()).containsExactly(new CancelOrder("order-1"))
            );
        }

        @Test
        void preserves_startEventTypes_and_eventTypes() {
            Saga<Object, OrderState, Object> saga = widened();

            assertAll(
                    () -> assertThat(saga.startEventTypes()).containsExactly(OrderPlaced.class),
                    () -> assertThat(saga.eventTypes()).containsExactlyInAnyOrder(OrderPlaced.class, PaymentReserved.class, PaymentFailed.class)
            );
        }
    }

    @Nested
    class CreateFactory {

        @Test
        void produces_a_saga_that_delegates_to_the_supplied_functions() {
            BiFunction<OrderState, SagaInput<OrderEvent>, OrderState> evolve = (state, input) -> switch (input) {
                case SagaInput.Event<OrderEvent> ev when ev.event() instanceof OrderPlaced placed -> new AwaitingPayment(placed.orderId());
                default -> state;
            };
            BiFunction<OrderState, SagaInput<OrderEvent>, List<SagaEffect<OrderCommand>>> react = (state, input) -> switch (input) {
                case SagaInput.Event<OrderEvent> ev when ev.event() instanceof OrderPlaced placed ->
                        List.of(SagaEffect.issue(new ReservePayment(placed.orderId(), placed.amount())));
                default -> List.of();
            };
            Function<OrderEvent, String> sagaId = OrderEvent::orderId;

            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.create(
                    null,
                    sagaId,
                    Set.of(OrderPlaced.class),
                    Set.of(OrderPlaced.class),
                    evolve,
                    react);

            Saga.Step<OrderState, OrderCommand> step = saga.step(null, SagaInput.event(new OrderPlaced("order-1", 100)));

            assertAll(
                    () -> assertThat(saga.initialState()).isNull(),
                    () -> assertThat(step.state()).isEqualTo(new AwaitingPayment("order-1")),
                    () -> assertThat(step.effects()).containsExactly(SagaEffect.issue(new ReservePayment("order-1", 100))),
                    () -> assertThat(saga.sagaId(new OrderPlaced("order-1", 100))).isEqualTo("order-1"),
                    () -> assertThat(saga.startEventTypes()).containsExactly(OrderPlaced.class),
                    () -> assertThat(saga.eventTypes()).containsExactly(OrderPlaced.class)
            );
        }

        @Test
        void startEventTypes_is_an_immutable_copy() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.create(
                    null,
                    OrderEvent::orderId,
                    Set.of(OrderPlaced.class),
                    Set.of(),
                    (state, input) -> state,
                    (state, input) -> List.of());

            assertThatThrownBy(() -> saga.startEventTypes().add(PaymentReserved.class))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void expands_a_sealed_event_type_the_caller_declared() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.create(
                    null,
                    OrderEvent::orderId,
                    Set.of(OrderPlaced.class),
                    Set.of(OrderEvent.class),
                    (state, input) -> state,
                    (state, input) -> List.of());

            assertThat(saga.eventTypes()).contains(OrderEvent.class, OrderPlaced.class, PaymentReserved.class);
        }

        @Test
        void an_empty_eventTypes_stays_empty_and_is_not_refused() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.create(
                    null,
                    OrderEvent::orderId,
                    Set.of(OrderPlaced.class),
                    Set.of(),
                    (state, input) -> state,
                    (state, input) -> List.of());

            assertThat(saga.eventTypes()).isEmpty();
        }
    }

    @Nested
    class EffectsAndInputsAsData {

        @Test
        void reacting_twice_with_the_same_input_yields_equal_effect_lists() {
            // Proves the reaction reads no clock: SagaEffect.StartTimeout carries a relative Duration, never an Instant.now().
            Saga<OrderEvent, OrderState, OrderCommand> saga = orderFulfillment();
            OrderEvent event = new OrderPlaced("order-1", 100);

            List<SagaEffect<OrderCommand>> first = saga.react(new AwaitingPayment("order-1"), SagaInput.event(event));
            List<SagaEffect<OrderCommand>> second = saga.react(new AwaitingPayment("order-1"), SagaInput.event(event));

            assertThat(first).isEqualTo(second);
        }

        @Test
        void IssueCommand_instances_with_equal_commands_are_equal() {
            assertThat(SagaEffect.issue(new ReservePayment("order-1", 100)))
                    .isEqualTo(SagaEffect.issue(new ReservePayment("order-1", 100)));
        }

        @Test
        void throws_NullPointerException_when_IssueCommand_command_is_null() {
            assertThatThrownBy(() -> new SagaEffect.IssueCommand<OrderCommand>(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void StartTimeout_instances_with_equal_fields_are_equal() {
            assertThat(SagaEffect.<OrderCommand>startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)))
                    .isEqualTo(SagaEffect.<OrderCommand>startTimeout(PAYMENT_TIMER, Duration.ofMinutes(30)));
        }

        @Test
        void throws_NullPointerException_when_StartTimeout_timerName_is_null() {
            assertThatThrownBy(() -> new SagaEffect.StartTimeout<OrderCommand>(null, Duration.ofMinutes(30)))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_StartTimeout_duration_is_null() {
            assertThatThrownBy(() -> new SagaEffect.StartTimeout<OrderCommand>(TimerName.parse(PAYMENT_TIMER), null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void StartTimeoutAt_instances_with_equal_fields_are_equal() {
            Instant at = Instant.parse("2026-01-01T00:00:00Z");

            assertThat(SagaEffect.<OrderCommand>startTimeoutAt(PAYMENT_TIMER, at))
                    .isEqualTo(SagaEffect.<OrderCommand>startTimeoutAt(PAYMENT_TIMER, at));
        }

        @Test
        void throws_NullPointerException_when_StartTimeoutAt_timerName_is_null() {
            assertThatThrownBy(() -> new SagaEffect.StartTimeoutAt<OrderCommand>(null, Instant.now()))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_StartTimeoutAt_instant_is_null() {
            assertThatThrownBy(() -> new SagaEffect.StartTimeoutAt<OrderCommand>(TimerName.parse(PAYMENT_TIMER), null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void CancelTimeout_instances_with_equal_names_are_equal() {
            assertThat(SagaEffect.<OrderCommand>cancelTimeout(PAYMENT_TIMER))
                    .isEqualTo(SagaEffect.<OrderCommand>cancelTimeout(PAYMENT_TIMER));
        }

        @Test
        void throws_NullPointerException_when_CancelTimeout_timerName_is_null() {
            assertThatThrownBy(() -> new SagaEffect.CancelTimeout<OrderCommand>(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_SagaTimeout_sagaId_is_null() {
            assertThatThrownBy(() -> new SagaTimeout(null, TimerName.parse(PAYMENT_TIMER)))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_SagaTimeout_timerName_is_null() {
            assertThatThrownBy(() -> new SagaTimeout("order-1", null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_SagaInput_Event_wraps_null() {
            assertThatThrownBy(() -> SagaInput.<OrderEvent>event(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_SagaInput_Event_metadata_is_null() {
            assertThatThrownBy(() -> new SagaInput.Event<>(new OrderPlaced("order-1", 100), null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void throws_NullPointerException_when_SagaInput_Timeout_wraps_null() {
            assertThatThrownBy(() -> new SagaInput.Timeout<OrderEvent>(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class NoArgBuilder {

        @Test
        void builder_with_no_argument_starts_from_null_like_builder_of_null() {
            Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder()
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .build();

            assertThat(saga.initialState()).isNull();
        }
    }
}
