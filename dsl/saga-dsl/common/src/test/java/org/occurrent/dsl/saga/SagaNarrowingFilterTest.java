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

@DisplayName("A saga given a narrowing filter")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaNarrowingFilterTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    /** Sealed at the top but reopened one level down, so its concrete types cannot all be found. */
    sealed interface ReopenedEvent permits OpenBase {
        String orderId();
    }

    non-sealed static class OpenBase implements ReopenedEvent {
        @Override
        public String orderId() {
            return "order-1";
        }
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    private static final Filter SUBJECT = Filter.subject("order-1");

    @Test
    void is_reported_by_the_core_builder() {
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .narrowingFilter(SUBJECT)
                .build();

        assertThat(saga.narrowingFilter()).isSameAs(SUBJECT);
    }

    @Test
    void is_reported_by_the_flow_builder() {
        Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .narrowingFilter(SUBJECT)
                .step("wait", step -> step.on(PaymentReserved.class, Continuation.end()))
                .build();

        assertThat(saga.narrowingFilter()).isSameAs(SUBJECT);
    }

    @Test
    void survives_being_widened_by_adapt() {
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .narrowingFilter(SUBJECT)
                .build();

        Saga<Object, String, Object> widened = Saga.adapt(saga, OrderEvent.class);

        assertThat(widened.narrowingFilter()).isSameAs(SUBJECT);
    }

    @Test
    void is_absent_when_none_was_given() {
        Saga<OrderEvent, String, OrderCommand> built = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .build();
        Saga<OrderEvent, String, OrderCommand> created = Saga.create(null, OrderEvent::orderId,
                Set.of(OrderPlaced.class), Set.of(OrderPlaced.class), (state, input) -> state, (state, input) -> List.of());

        assertThat(built.narrowingFilter()).isNull();
        assertThat(created.narrowingFilter()).isNull();
    }

    @Test
    void does_not_stop_the_core_builder_refusing_a_hierarchy_it_cannot_enumerate() {
        Saga.Builder<ReopenedEvent, String, OrderCommand> builder = Saga.<ReopenedEvent, String, OrderCommand>builder(null)
                .correlateAll(ReopenedEvent::orderId)
                .startsOn(ReopenedEvent.class)
                .narrowingFilter(SUBJECT);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot all be enumerated");
    }

    @Test
    void does_not_stop_the_flow_builder_refusing_a_hierarchy_it_cannot_enumerate() {
        FlowSaga.Builder<ReopenedEvent, OrderCommand> builder = FlowSaga.<ReopenedEvent, OrderCommand>builder()
                .correlateAll(ReopenedEvent::orderId)
                .startsOn(ReopenedEvent.class)
                .narrowingFilter(SUBJECT)
                .step("wait", step -> step.on(OpenBase.class, Continuation.end()));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot all be enumerated");
    }

    @Test
    void leaves_a_hierarchy_accepted_when_a_replacement_is_set_too() {
        Saga<ReopenedEvent, String, OrderCommand> saga = Saga.<ReopenedEvent, String, OrderCommand>builder(null)
                .correlateAll(ReopenedEvent::orderId)
                .startsOn(ReopenedEvent.class)
                .narrowingFilter(SUBJECT)
                .replacementFilter(Filter.type("reopened-event"))
                .build();

        assertThat(saga.narrowingFilter()).isSameAs(SUBJECT);
        assertThat(saga.replacementFilter()).isEqualTo(Filter.type("reopened-event"));
    }

    @Test
    void leaves_the_reported_event_types_exactly_as_they_are() {
        Saga<OrderEvent, String, OrderCommand> without = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderEvent.class, (state, event) -> state)
                .build();
        Saga<OrderEvent, String, OrderCommand> with = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderEvent.class, (state, event) -> state)
                .narrowingFilter(SUBJECT)
                .build();

        assertThat(with.eventTypes()).containsExactlyElementsOf(without.eventTypes());
    }

    @Test
    void cannot_be_set_twice_on_either_builder() {
        Saga.Builder<OrderEvent, String, OrderCommand> core = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .narrowingFilter(SUBJECT);
        FlowSaga.Builder<OrderEvent, OrderCommand> flow = FlowSaga.<OrderEvent, OrderCommand>builder()
                .narrowingFilter(SUBJECT);

        assertThatThrownBy(() -> core.narrowingFilter(Filter.subject("order-2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("narrowingFilter(...) has already been set");
        assertThatThrownBy(() -> flow.narrowingFilter(Filter.subject("order-2")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("narrowingFilter(...) has already been set");
    }

    @Test
    void cannot_be_null() {
        assertThatThrownBy(() -> Saga.<OrderEvent, String, OrderCommand>builder(null).narrowingFilter(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FlowSaga.<OrderEvent, OrderCommand>builder().narrowingFilter(null))
                .isInstanceOf(NullPointerException.class);
    }
}
