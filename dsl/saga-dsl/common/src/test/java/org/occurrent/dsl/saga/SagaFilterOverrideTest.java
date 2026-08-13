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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.filter.Filter;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("A saga given an explicit filter")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaFilterOverrideTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    /** Not sealed, so its concrete types cannot be enumerated and a derived filter would miss them. */
    interface OpenEvent {
        String orderId();
    }

    record OpenOrderPlaced(String orderId) implements OpenEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    private static final Filter SUBJECT_FILTER = Filter.subject("order-1");

    @Test
    void is_reported_by_the_core_builder() {
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .filter(SUBJECT_FILTER)
                .build();

        assertThat(saga.filter()).isSameAs(SUBJECT_FILTER);
    }

    @Test
    void is_reported_by_the_flow_builder() {
        Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .filter(SUBJECT_FILTER)
                .step("wait", step -> step.on(PaymentReserved.class, Continuation.end()))
                .build();

        assertThat(saga.filter()).isSameAs(SUBJECT_FILTER);
    }

    @Test
    void is_reported_by_the_create_factory() {
        Saga<OrderEvent, String, OrderCommand> saga = Saga.create(null, OrderEvent::orderId, Set.of(OrderPlaced.class),
                Set.of(OrderPlaced.class), (state, input) -> state, (state, input) -> List.of(), SUBJECT_FILTER);

        assertThat(saga.filter()).isSameAs(SUBJECT_FILTER);
    }

    @Test
    void is_absent_when_none_was_given() {
        Saga<OrderEvent, String, OrderCommand> built = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .build();
        Saga<OrderEvent, String, OrderCommand> created = Saga.create(null, OrderEvent::orderId,
                Set.of(OrderPlaced.class), Set.of(OrderPlaced.class), (state, input) -> state, (state, input) -> List.of());

        assertThat(built.filter()).isNull();
        assertThat(created.filter()).isNull();
    }

    @Test
    void builds_on_a_supertype_the_core_builder_would_otherwise_refuse() {
        Saga<OpenEvent, String, OrderCommand> saga = Saga.<OpenEvent, String, OrderCommand>builder(null)
                .correlateAll(OpenEvent::orderId)
                .startsOn(OpenEvent.class)
                .evolve(OpenEvent.class, (state, e) -> e.orderId())
                .filter(Filter.type("open-order-event"))
                .build();

        assertThat(saga.eventTypes()).containsExactly(OpenEvent.class);
    }

    @Test
    void builds_on_a_supertype_the_flow_builder_would_otherwise_refuse() {
        Saga<OpenEvent, FlowState<OpenEvent>, OrderCommand> saga = FlowSaga.<OpenEvent, OrderCommand>builder()
                .correlateAll(OpenEvent::orderId)
                .startsOn(OpenOrderPlaced.class)
                .filter(Filter.type("open-order-event"))
                .step("wait", step -> step.on(OpenEvent.class, Continuation.end()))
                .build();

        assertThat(saga.eventTypes()).contains(OpenEvent.class);
    }

    @Test
    void builds_on_a_supertype_the_create_factory_would_otherwise_refuse() {
        Saga<OpenEvent, String, OrderCommand> saga = Saga.create(null, OpenEvent::orderId, Set.of(OpenOrderPlaced.class),
                Set.of(OpenEvent.class), (state, input) -> state, (state, input) -> List.of(), Filter.type("open-order-event"));

        assertThat(saga.eventTypes()).contains(OpenEvent.class);
    }

    @Test
    void leaves_the_reported_event_types_exactly_as_they_are_without_one() {
        Saga<OrderEvent, String, OrderCommand> withFilter = sealedSaga(SUBJECT_FILTER);
        Saga<OrderEvent, String, OrderCommand> withoutFilter = sealedSaga(null);

        // A filter changes which events arrive, never which event types the saga says it handles.
        assertThat(withFilter.eventTypes()).isEqualTo(withoutFilter.eventTypes());
        assertThat(withFilter.eventTypes()).contains(OrderEvent.class, OrderPlaced.class, PaymentReserved.class);
    }

    @Test
    void reports_the_concrete_types_it_can_find_when_the_hierarchy_cannot_be_enumerated() {
        Saga<Object, String, OrderCommand> saga = Saga.<Object, String, OrderCommand>builder(null)
                .correlateAll(e -> "order-1")
                .startsOn(OrderPlaced.class)
                .evolve(OpenEvent.class, (state, e) -> state)
                .filter(SUBJECT_FILTER)
                .build();

        assertThat(saga.eventTypes()).contains(OpenEvent.class, OrderPlaced.class);
    }

    @Test
    void reports_event_types_in_declaration_order_and_unmodifiable() {
        Saga<OrderEvent, String, OrderCommand> saga = sealedSaga(SUBJECT_FILTER);
        Set<Class<? extends OrderEvent>> eventTypes = saga.eventTypes();

        assertThat(eventTypes).startsWith(OrderEvent.class);
        assertThatThrownBy(() -> eventTypes.add(OrderPlaced.class)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void leaves_the_start_event_types_alone() {
        Saga<OrderEvent, String, OrderCommand> withFilter = sealedSaga(SUBJECT_FILTER);
        Saga<OrderEvent, String, OrderCommand> withoutFilter = sealedSaga(null);

        assertThat(withFilter.startEventTypes()).isEqualTo(withoutFilter.startEventTypes());
        assertThat(withFilter.startEventTypes()).containsExactly(OrderPlaced.class);
    }

    @Test
    void survives_being_widened_by_adapt() {
        Saga<OrderEvent, String, OrderCommand> saga = sealedSaga(SUBJECT_FILTER);

        Saga<Object, String, OrderCommand> widened = Saga.adapt(saga, OrderEvent.class);

        assertThat(widened.filter()).isSameAs(SUBJECT_FILTER);
    }

    @Test
    void cannot_be_set_twice_on_the_core_builder() {
        Saga.Builder<OrderEvent, String, OrderCommand> builder = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .filter(SUBJECT_FILTER);

        assertThatThrownBy(() -> builder.filter(Filter.subject("order-2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("filter(...) has already been set");
    }

    @Test
    void cannot_be_set_twice_on_the_flow_builder() {
        FlowSaga.Builder<OrderEvent, OrderCommand> builder = FlowSaga.<OrderEvent, OrderCommand>builder()
                .filter(SUBJECT_FILTER);

        assertThatThrownBy(() -> builder.filter(Filter.subject("order-2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("filter(...) has already been set");
    }

    @Test
    void cannot_be_null() {
        assertThatThrownBy(() -> Saga.<OrderEvent, String, OrderCommand>builder(null).filter(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FlowSaga.<OrderEvent, OrderCommand>builder().filter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void does_not_excuse_a_declared_event_type_that_has_no_correlation() {
        assertThatThrownBy(() -> uncorrelatedSaga(SUBJECT_FILTER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has no correlation");
    }

    @Test
    void names_the_same_uncorrelated_event_type_a_saga_without_one_names() {
        // Which type the coverage exception names is a determinism property (ADR 124), so the two branches have to agree
        // rather than merely both failing.
        Throwable withFilter = catchIt(() -> uncorrelatedSaga(SUBJECT_FILTER));
        Throwable withoutFilter = catchIt(() -> uncorrelatedSaga(null));

        assertThat(withFilter).hasMessage(withoutFilter.getMessage());
    }

    @Test
    void is_offered_as_a_remedy_by_the_refusal_it_is_the_way_out_of() {
        assertThatThrownBy(() -> Saga.<OpenEvent, String, OrderCommand>builder(null)
                .correlateAll(OpenEvent::orderId)
                .startsOn(OpenEvent.class)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Declare the concrete event types instead")
                .hasMessageContaining("set an explicit filter");
    }

    @Test
    void does_not_excuse_an_array_event_type_either() {
        // An array is not an event type at all rather than a hierarchy that cannot be enumerated, so it stays refused
        // whether or not a filter is set. Without this the filter would silently turn off a diagnostic for a
        // declaration that is always a mistake, which is what the message above declines to point anyone at.
        assertThatThrownBy(() -> Saga.<Object, String, OrderCommand>builder(null)
                .correlateAll(e -> "order-1")
                .startsOn(OrderPlaced[].class)
                .filter(SUBJECT_FILTER)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support an array");
    }

    @Test
    void does_not_excuse_an_array_event_type_on_the_flow_builder_either() {
        assertThatThrownBy(() -> FlowSaga.<Object, OrderCommand>builder()
                .correlateAll(e -> "order-1")
                .startsOn(OrderPlaced[].class)
                .filter(SUBJECT_FILTER)
                .step("wait", step -> step.on(OrderPlaced.class, Continuation.end()))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support an array");
    }

    @Test
    void is_not_offered_as_a_remedy_for_an_array_event_type() {
        // An array is a mistake rather than a hierarchy that cannot be enumerated, so pointing at a filter would be
        // advice to keep the mistake.
        assertThatThrownBy(() -> Saga.<Object, String, OrderCommand>builder(null)
                .correlateAll(e -> "order-1")
                .startsOn(OrderPlaced[].class)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support an array")
                .hasMessageNotContaining("filter");
    }

    private static Saga<OrderEvent, String, OrderCommand> sealedSaga(Filter filter) {
        Saga.Builder<OrderEvent, String, OrderCommand> builder = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderEvent.class, (state, e) -> e.orderId());
        if (filter != null) {
            builder.filter(filter);
        }
        return builder.build();
    }

    private static Saga<OrderEvent, String, OrderCommand> uncorrelatedSaga(Filter filter) {
        Saga.Builder<OrderEvent, String, OrderCommand> builder = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlate(OrderPlaced.class, OrderPlaced::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderEvent.class, (state, e) -> e.orderId());
        if (filter != null) {
            builder.filter(filter);
        }
        return builder.build();
    }

    private static Throwable catchIt(Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("expected the saga to be refused");
        } catch (RuntimeException e) {
            return e;
        }
    }
}
