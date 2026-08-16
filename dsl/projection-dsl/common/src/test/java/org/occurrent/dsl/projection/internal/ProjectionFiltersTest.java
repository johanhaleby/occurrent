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

package org.occurrent.dsl.projection.internal;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ProjectionFilters#filterFor(CloudEventConverter, Projection)} derives its filter through
 * {@code EventTypeExpansion.deriveFilter}, so a handler registered on a sealed supertype asks for every concrete type
 * it permits (ADR 126). These tests check the derivation directly, independent of any runner or subscription stack.
 */
@DisplayName("ProjectionFilters")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ProjectionFiltersTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
    }

    record OrderPlaced() implements OrderEvent {
    }

    record PaymentReserved() implements OrderEvent {
    }

    // Sealed above, reopened here, so nothing below this class can be found.
    sealed interface ReopenedEvent permits ReopenedBase {
    }

    abstract static non-sealed class ReopenedBase implements ReopenedEvent {
    }

    private static final CloudEventConverter<OrderEvent> CONVERTER = new PerTypeConverter<>();

    @Test
    void an_explicit_filter_wins_over_any_derivation() {
        Filter explicit = Filter.subject("premium");
        Projection<Boolean, OrderEvent, String> projection = Projection.<Boolean, OrderEvent, String>builder(false)
                .id(event -> "id")
                .on(OrderPlaced.class, (state, event) -> state)
                .filter(explicit)
                .build();

        Filter filter = ProjectionFilters.filterFor(CONVERTER, projection);

        assertThat(filter).isEqualTo(explicit);
    }

    @Test
    void no_registered_handler_derives_filter_all() {
        Projection<Boolean, OrderEvent, String> projection = Projection.<Boolean, OrderEvent, String>builder(false)
                .id(event -> "id")
                .build();

        Filter filter = ProjectionFilters.filterFor(CONVERTER, projection);

        assertThat(filter).isEqualTo(Filter.all());
    }

    @Test
    void a_handler_on_a_concrete_type_derives_a_single_type_condition() {
        Projection<Boolean, OrderEvent, String> projection = Projection.<Boolean, OrderEvent, String>builder(false)
                .id(event -> "id")
                .on(OrderPlaced.class, (state, event) -> state)
                .build();

        Filter filter = ProjectionFilters.filterFor(CONVERTER, projection);

        assertThat(filter).isEqualTo(Filter.type(Condition.eq("OrderPlaced")));
    }

    @Test
    void a_handler_on_a_sealed_supertype_asks_for_every_concrete_type_it_permits() {
        Projection<Integer, OrderEvent, String> projection = Projection.<Integer, OrderEvent, String>builder(0)
                .id(event -> "id")
                .on(OrderEvent.class, (state, event) -> state + 1)
                .build();

        Filter filter = ProjectionFilters.filterFor(CONVERTER, projection);

        assertThat(filter).isEqualTo(Filter.type(Condition.or(List.of(
                Condition.eq("OrderEvent"), Condition.eq("OrderPlaced"), Condition.eq("PaymentReserved")))));
    }

    @Test
    void a_handler_on_a_sealed_type_reopened_below_it_is_refused() {
        Projection<Boolean, ReopenedEvent, String> projection = Projection.<Boolean, ReopenedEvent, String>builder(false)
                .id(event -> "id")
                .on(ReopenedEvent.class, (state, event) -> state)
                .build();
        CloudEventConverter<ReopenedEvent> converter = new PerTypeConverter<>();

        assertThatThrownBy(() -> ProjectionFilters.filterFor(converter, projection))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ReopenedEvent.class.getName());
    }

    /** One CloudEvent type per concrete class, so a derived type filter can tell the types apart. */
    private static final class PerTypeConverter<E> implements CloudEventConverter<E> {

        @Override
        public CloudEvent toCloudEvent(E event) {
            return CloudEventBuilder.v1()
                    .withId("id")
                    .withSource(URI.create("urn:test"))
                    .withType(event.getClass().getSimpleName())
                    .build();
        }

        @Override
        public E toDomainEvent(CloudEvent cloudEvent) {
            throw new UnsupportedOperationException("not needed for these tests");
        }

        @Override
        public String getCloudEventType(Class<? extends E> type) {
            return type.getSimpleName();
        }
    }
}
