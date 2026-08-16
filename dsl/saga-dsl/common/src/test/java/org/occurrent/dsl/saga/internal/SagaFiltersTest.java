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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The composition rules {@link SagaFilters#filterFor(CloudEventConverter, Saga)} applies, independent of any
 * subscription stack. The blocking runner's subscription tests cover the same rules end to end through an actual
 * {@code Subscribable}; these pin the derivation and combination logic on its own.
 */
@DisplayName("SagaFilters")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaFiltersTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record PaymentReserved(String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ReservePayment {
    }

    record ReservePayment(String orderId) implements OrderCommand {
    }

    private static final Filter PREMIUM = Filter.subject("premium");
    private static final CloudEventConverter<OrderEvent> CONVERTER = new PerTypeConverter();

    @Test
    void derives_filter_all_when_the_saga_declares_no_event_types() {
        Saga<OrderEvent, String, OrderCommand> saga = sagaWithoutDeclaredTypes(null);

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isEqualTo(Filter.all());
    }

    @Test
    void derives_a_single_type_condition_for_one_declared_event_type() {
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedOnly(null);

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isEqualTo(Filter.type(Condition.eq(typeOf(OrderPlaced.class))));
    }

    @Test
    void derives_an_or_of_type_conditions_for_several_declared_event_types() {
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedAndPaymentReserved();

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isEqualTo(Filter.type(Condition.or(List.of(
                Condition.eq(typeOf(OrderPlaced.class)),
                Condition.eq(typeOf(PaymentReserved.class))))));
    }

    @Test
    void a_replacement_filter_is_used_as_the_base_instead_of_a_derived_one() {
        Filter replacement = Filter.subject("replacement");
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedOnly(replacement);

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isSameAs(replacement);
    }

    @Test
    void a_missing_narrowing_filter_leaves_the_base_filter_unchanged() {
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedOnly(null);

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isEqualTo(Filter.type(Condition.eq(typeOf(OrderPlaced.class))));
    }

    @Test
    void a_narrowing_filter_of_filter_all_leaves_the_base_filter_unchanged() {
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedOnly(null, Filter.all());

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isEqualTo(Filter.type(Condition.eq(typeOf(OrderPlaced.class))));
    }

    @Test
    void a_narrowing_filter_becomes_the_whole_selector_when_the_base_matches_everything() {
        Saga<OrderEvent, String, OrderCommand> saga = sagaWithoutDeclaredTypes(PREMIUM);

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isSameAs(PREMIUM);
    }

    @Test
    void a_narrowing_filter_is_anded_onto_a_non_all_base_with_the_narrowing_on_the_right() {
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedOnly(null, PREMIUM);
        Filter derived = Filter.type(Condition.eq(typeOf(OrderPlaced.class)));

        Filter filter = SagaFilters.filterFor(CONVERTER, saga);

        assertThat(filter).isInstanceOf(Filter.CompositionFilter.class);
        assertThat(((Filter.CompositionFilter) filter).filters()).containsExactly(derived, PREMIUM);
    }

    @Test
    void throws_NullPointerException_when_the_cloud_event_converter_is_null() {
        Saga<OrderEvent, String, OrderCommand> saga = orderPlacedOnly(null);

        assertThatThrownBy(() -> SagaFilters.filterFor(null, saga))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("cloudEventConverter cannot be null");
    }

    @Test
    void throws_NullPointerException_when_the_saga_is_null() {
        assertThatThrownBy(() -> SagaFilters.filterFor(CONVERTER, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("saga cannot be null");
    }

    private static Saga<OrderEvent, String, OrderCommand> orderPlacedOnly(@Nullable Filter replacementFilter) {
        return orderPlacedOnly(replacementFilter, null);
    }

    private static Saga<OrderEvent, String, OrderCommand> orderPlacedOnly(@Nullable Filter replacementFilter, @Nullable Filter narrowingFilter) {
        Saga.Builder<OrderEvent, String, OrderCommand> builder = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ReservePayment(e.orderId()))));
        if (replacementFilter != null) {
            builder = builder.replacementFilter(replacementFilter);
        }
        if (narrowingFilter != null) {
            builder = builder.narrowingFilter(narrowingFilter);
        }
        return builder.build();
    }

    private static Saga<OrderEvent, String, OrderCommand> orderPlacedAndPaymentReserved() {
        return Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ReservePayment(e.orderId()))))
                .react(PaymentReserved.class, (state, e) -> List.of())
                .build();
    }

    /** A saga declaring no event types at all, so its derived filter is {@link Filter#all()}. */
    private static Saga<OrderEvent, String, OrderCommand> sagaWithoutDeclaredTypes(Filter narrowingFilter) {
        return new Saga<>() {
            @Override
            public String initialState() {
                return "";
            }

            @Override
            public String evolve(String state, SagaInput<OrderEvent> input) {
                return state;
            }

            @Override
            public List<SagaEffect<OrderCommand>> react(String state, SagaInput<OrderEvent> input) {
                return List.of();
            }

            @Override
            public String sagaId(OrderEvent event) {
                return event.orderId();
            }

            @Override
            public Set<Class<? extends OrderEvent>> startEventTypes() {
                return Set.of(OrderPlaced.class);
            }

            @Override
            public Set<Class<? extends OrderEvent>> eventTypes() {
                return Set.of();
            }

            @Override
            public Filter narrowingFilter() {
                return narrowingFilter;
            }
        };
    }

    private static String typeOf(Class<? extends OrderEvent> type) {
        return type.getSimpleName();
    }

    /** One CloudEvent type per concrete class, so a derived type filter can tell the types apart. */
    private static final class PerTypeConverter implements CloudEventConverter<OrderEvent> {

        @Override
        public CloudEvent toCloudEvent(OrderEvent event) {
            return CloudEventBuilder.v1()
                    .withId(event.orderId())
                    .withSource(URI.create("urn:test"))
                    .withType(event.getClass().getSimpleName())
                    .withDataContentType("application/json")
                    .withData(event.orderId().getBytes())
                    .build();
        }

        @Override
        public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
            String orderId = new String(cloudEvent.getData().toBytes());
            return switch (cloudEvent.getType()) {
                case "OrderPlaced" -> new OrderPlaced(orderId);
                case "PaymentReserved" -> new PaymentReserved(orderId);
                default -> throw new IllegalArgumentException("unknown type " + cloudEvent.getType());
            };
        }

        @Override
        public String getCloudEventType(Class<? extends OrderEvent> type) {
            return type.getSimpleName();
        }
    }
}
