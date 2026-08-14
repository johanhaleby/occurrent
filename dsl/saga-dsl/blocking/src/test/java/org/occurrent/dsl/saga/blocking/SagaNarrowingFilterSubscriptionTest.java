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

package org.occurrent.dsl.saga.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInput;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A saga given a narrowing filter subscribes on that condition combined with the filter derived from its event types,
 * so it still asks for its own types and additionally requires the condition.
 */
@DisplayName("A saga subscribing on a narrowing filter")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaNarrowingFilterSubscriptionTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved, OrderArchived {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    /** Convertible, but never declared by the saga, so only the derived half of the selector keeps it out. */
    record OrderArchived(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    private static final Filter PREMIUM = Filter.subject("premium");

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new PerTypeConverter();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        subscriptionModel.shutdown();
    }

    @Test
    void delivers_only_events_matching_both_the_declared_types_and_the_condition() {
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        run("both-halves", shipOnPlaced(PREMIUM), issued);

        // A declared type outside the condition, and a type the saga never declared that does match it. Neither should
        // arrive. Only the third matches both.
        write("order1", "standard", new OrderPlaced("e1", "order1"));
        write("order2", "premium", new OrderArchived("e2", "order2"));
        write("order3", "premium", new OrderPlaced("e3", "order3"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order3")));
    }

    @Test
    void creates_no_instance_when_the_condition_excludes_the_start_event() {
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        run("starved-start", shipOnPlaced(Filter.subject("never-written")), issued);

        write("order1", "premium", new OrderPlaced("e1", "order1"));

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).isEmpty());
    }

    @Test
    void combines_a_replacement_with_a_narrowing_so_neither_ones_exclusions_get_through() {
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .replacementFilter(Filter.type(typeOf(OrderPlaced.class)))
                .narrowingFilter(PREMIUM)
                .build();

        run("replacement-and-narrowing", saga, issued);

        // Matches the narrowing but the replacement excludes its type, so reading the narrowing as the base would let
        // it through.
        write("order1", "premium", new OrderArchived("e1", "order1"));
        // Matches the replacement but not the narrowing.
        write("order2", "standard", new OrderPlaced("e2", "order2"));
        write("order3", "premium", new OrderPlaced("e3", "order3"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order3")));
    }

    @Test
    void puts_the_condition_after_the_derived_types_in_the_composed_filter() {
        Saga<OrderEvent, String, OrderCommand> saga = shipOnPlaced(PREMIUM);
        Filter derived = Filter.type(Condition.eq(typeOf(OrderPlaced.class)));

        Filter composed = SagaFilters.filterFor(converter, saga);

        assertThat(composed).isInstanceOf(Filter.CompositionFilter.class);
        assertThat(((Filter.CompositionFilter) composed).filters()).containsExactly(derived, PREMIUM);
    }

    @Test
    void is_the_whole_selector_when_the_saga_narrows_no_types() {
        Saga<OrderEvent, String, OrderCommand> saga = new Saga<>() {
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
            public Filter narrowingFilter() {
                return PREMIUM;
            }
        };

        assertThat(SagaFilters.filterFor(converter, saga)).isSameAs(PREMIUM);
    }

    @Test
    void leaves_the_derived_filter_alone_when_it_matches_everything() {
        Saga<OrderEvent, String, OrderCommand> saga = shipOnPlaced(Filter.all());

        Filter composed = SagaFilters.filterFor(converter, saga);

        assertThat(composed).isEqualTo(Filter.type(Condition.eq(typeOf(OrderPlaced.class))));
    }

    private Saga<OrderEvent, String, OrderCommand> shipOnPlaced(Filter narrowing) {
        return Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .narrowingFilter(narrowing)
                .build();
    }

    private <S> void run(String subscriptionId, Saga<OrderEvent, S, OrderCommand> saga, List<OrderCommand> issued) {
        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                .run(subscriptionId, saga, SagaStateStore.inMemory(), issued::add);
        subscriptionsToClose.add(subscription);
        subscription.waitUntilStarted(Duration.ofSeconds(5));
    }

    private void write(String streamId, String subject, OrderEvent event) {
        eventStore.write(streamId, List.of(((PerTypeConverter) converter).toCloudEvent(event, subject)));
    }

    private static String typeOf(Class<? extends OrderEvent> type) {
        return type.getSimpleName();
    }

    /** One CloudEvent type per concrete class, so a derived type filter can tell the types apart. */
    private static final class PerTypeConverter implements CloudEventConverter<OrderEvent> {

        @Override
        public CloudEvent toCloudEvent(OrderEvent event) {
            return toCloudEvent(event, null);
        }

        CloudEvent toCloudEvent(OrderEvent event, String subject) {
            CloudEventBuilder builder = CloudEventBuilder.v1()
                    .withId(event.eventId())
                    .withSource(URI.create("urn:test"))
                    .withType(event.getClass().getSimpleName())
                    .withDataContentType("application/json")
                    .withData("%s:%s".formatted(event.eventId(), event.orderId()).getBytes(StandardCharsets.UTF_8));
            return subject == null ? builder.build() : builder.withSubject(subject).build();
        }

        @Override
        public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
            String[] parts = new String(cloudEvent.getData().toBytes(), StandardCharsets.UTF_8).split(":");
            return switch (cloudEvent.getType()) {
                case "OrderPlaced" -> new OrderPlaced(parts[0], parts[1]);
                case "PaymentReserved" -> new PaymentReserved(parts[0], parts[1]);
                case "OrderArchived" -> new OrderArchived(parts[0], parts[1]);
                default -> throw new IllegalArgumentException("unknown type " + cloudEvent.getType());
            };
        }

        @Override
        public String getCloudEventType(Class<? extends OrderEvent> type) {
            return type.getSimpleName();
        }
    }
}
