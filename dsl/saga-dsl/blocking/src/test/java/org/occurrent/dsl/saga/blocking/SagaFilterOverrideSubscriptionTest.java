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
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.internal.SagaFilters;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A saga given an explicit filter subscribes on that filter and is built without deriving one, which is what lets a
 * genuinely open event hierarchy run under a {@code CloudEventTypeMapper} that collapses it onto one CloudEvent type
 * string. See <a href="https://github.com/johanhaleby/occurrent/issues/751">issue 751</a>.
 */
@DisplayName("A saga subscribing on an explicit filter")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaFilterOverrideSubscriptionTest {

    /**
     * Deliberately not sealed, and its subtypes are not reachable by reflection, so the expansion refuses it and the
     * only way to subscribe on it is an explicit filter.
     */
    interface OpenOrderEvent {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OpenOrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OpenOrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    /** Every type in the hierarchy maps onto this one string, which is what makes the declared supertype work. */
    private static final String COLLAPSED_TYPE = "open-order-event";

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OpenOrderEvent> converter;
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new CollapsingConverter();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        subscriptionModel.shutdown();
    }

    @Test
    void uses_that_filter_verbatim_instead_of_deriving_one_from_the_event_types() {
        Filter explicit = Filter.type(COLLAPSED_TYPE);
        Saga<OpenOrderEvent, String, OrderCommand> saga = shipOnEveryEvent(explicit);

        assertThat(SagaFilters.filterFor(converter, saga)).isSameAs(explicit);
    }

    @Test
    void runs_an_open_hierarchy_a_derived_filter_would_have_refused() {
        // Without the filter this saga does not build at all, which is the whole point of the escape hatch. The
        // collapsing converter then makes the one type string match every concrete event.
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        Saga<OpenOrderEvent, String, OrderCommand> saga = shipOnEveryEvent(Filter.type(COLLAPSED_TYPE));

        run("collapsing-mapper", saga, issued);
        write("order1", null, new OrderPlaced("e1", "order1"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order1")));
    }

    @Test
    void selects_on_something_no_derived_filter_could_express() {
        // Both events carry the one collapsed type, so a derived filter would have taken both and only the explicit
        // subject filter can tell them apart. If the override were ignored, two commands would arrive here, not one.
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        Saga<OpenOrderEvent, String, OrderCommand> saga = shipOnEveryEvent(
                Filter.type(COLLAPSED_TYPE).and(Filter.subject("premium")));

        run("subject-narrowed", saga, issued);
        write("order1", "standard", new OrderPlaced("e1", "order1"));
        write("order2", "premium", new OrderPlaced("e2", "order2"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order2")));
    }

    private Saga<OpenOrderEvent, String, OrderCommand> shipOnEveryEvent(Filter filter) {
        return Saga.<OpenOrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OpenOrderEvent::orderId)
                .startsOn(OpenOrderEvent.class)
                .evolve(OpenOrderEvent.class, (state, e) -> e.orderId())
                .react(OpenOrderEvent.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .replacementFilter(filter)
                .build();
    }

    private <S> void run(String subscriptionId, Saga<OpenOrderEvent, S, OrderCommand> saga, List<OrderCommand> issued) {
        SagaSubscription subscription = SagaRunner.<OpenOrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                .run(subscriptionId, saga, SagaStateStore.inMemory(), issued::add);
        subscriptionsToClose.add(subscription);
        subscription.waitUntilStarted(Duration.ofSeconds(5));
    }

    private void write(String streamId, String subject, OpenOrderEvent event) {
        eventStore.write(streamId, List.of(((CollapsingConverter) converter).toCloudEvent(event, subject)));
    }

    /**
     * Maps every event type in the hierarchy onto one CloudEvent type and carries the concrete type in the payload, the
     * shape a hand-written converter takes when the type string is not what identifies the event. Written by hand
     * rather than through a real serializer so the test asserts about filters and nothing else.
     */
    private static final class CollapsingConverter implements CloudEventConverter<OpenOrderEvent> {

        @Override
        public CloudEvent toCloudEvent(OpenOrderEvent event) {
            return toCloudEvent(event, null);
        }

        CloudEvent toCloudEvent(OpenOrderEvent event, String subject) {
            CloudEventBuilder builder = CloudEventBuilder.v1()
                    .withId(event.eventId())
                    .withSource(URI.create("urn:test"))
                    .withType(COLLAPSED_TYPE)
                    .withDataContentType("application/json")
                    .withData("%s:%s:%s".formatted(event.getClass().getSimpleName(), event.eventId(), event.orderId())
                            .getBytes(StandardCharsets.UTF_8));
            return subject == null ? builder.build() : builder.withSubject(subject).build();
        }

        @Override
        public OpenOrderEvent toDomainEvent(CloudEvent cloudEvent) {
            String[] parts = new String(requireData(cloudEvent), StandardCharsets.UTF_8).split(":");
            return switch (parts[0]) {
                case "OrderPlaced" -> new OrderPlaced(parts[1], parts[2]);
                case "PaymentReserved" -> new PaymentReserved(parts[1], parts[2]);
                default -> throw new IllegalArgumentException("unknown event " + parts[0]);
            };
        }

        @Override
        public String getCloudEventType(Class<? extends OpenOrderEvent> type) {
            return COLLAPSED_TYPE;
        }

        private static byte[] requireData(CloudEvent cloudEvent) {
            if (cloudEvent.getData() == null) {
                throw new IllegalArgumentException("cloud event " + cloudEvent.getId() + " carries no data");
            }
            return cloudEvent.getData().toBytes();
        }
    }
}
