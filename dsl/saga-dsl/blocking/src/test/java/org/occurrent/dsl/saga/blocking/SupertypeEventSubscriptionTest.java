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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.StepCondition;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * A saga that declares a sealed supertype receives the concrete events stored under it, and one that declares a
 * supertype whose subtypes cannot be found is refused when it is built. See <a
 * href="https://github.com/johanhaleby/occurrent/issues/743">issue 743</a>.
 */
@DisplayName("A saga declaring a supertype event")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SupertypeEventSubscriptionTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    interface OpenEvent {
        String eventId();

        String orderId();
    }

    record OpenOrderPlaced(String eventId, String orderId) implements OpenEvent {
    }

    sealed interface OrderCommand permits ShipOrder {
        String orderId();
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        subscriptionModel.shutdown();
    }

    @Test
    void subscribes_on_every_concrete_type_the_supertype_permits() {
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderEvent.class)
                .evolve(OrderEvent.class, (state, e) -> e.orderId())
                .build();

        assertThat(saga.eventTypes()).containsExactlyInAnyOrder(OrderEvent.class, OrderPlaced.class, PaymentReserved.class);
        assertThat(SagaFilters.filterFor(converter, saga).toString())
                .contains(converter.getCloudEventType(OrderPlaced.class))
                .contains(converter.getCloudEventType(PaymentReserved.class));
    }

    @Test
    void starts_an_instance_from_a_concrete_event_when_the_core_builder_declares_the_supertype() {
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        Saga<OrderEvent, String, OrderCommand> saga = Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderEvent.class)
                .evolve(OrderEvent.class, (state, e) -> e.orderId())
                .react(OrderEvent.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .build();

        run("core-supertype", saga, issued);
        write("order1", new OrderPlaced("e1", "order1"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order1")));
    }

    @Test
    void reaches_a_flow_step_declared_on_the_supertype() {
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .step("wait", step -> step.on(OrderEvent.class, Continuation.end(),
                        (OrderEvent e) -> List.of(new ShipOrder(e.orderId()))))
                .build();

        run("flow-supertype", saga, issued);
        write("order2", new OrderPlaced("e1", "order2"));
        write("order2", new PaymentReserved("e2", "order2"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order2")));
    }

    @Test
    void counts_concrete_events_towards_a_window_condition_declared_on_the_supertype() {
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> saga = FlowSaga.<OrderEvent, OrderCommand>builder()
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .step("wait", step -> step.on(StepCondition.event(OrderEvent.class, 2), Continuation.end(),
                        received -> List.of(new ShipOrder("order3"))))
                .build();

        run("window-supertype", saga, issued);
        write("order3", new OrderPlaced("e1", "order3"));
        write("order3", new PaymentReserved("e2", "order3"));
        write("order3", new PaymentReserved("e3", "order3"));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(issued).containsExactly(new ShipOrder("order3")));
    }

    @Test
    void is_refused_by_the_core_builder_when_the_supertype_is_not_sealed() {
        assertThatThrownBy(() -> Saga.<OpenEvent, String, OrderCommand>builder(null)
                .correlateAll(OpenEvent::orderId)
                .startsOn(OpenEvent.class)
                .evolve(OpenEvent.class, (state, e) -> e.orderId())
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OpenEvent.class.getName())
                .hasMessageContaining("Declare the concrete event types instead");
    }

    @Test
    void is_refused_by_the_core_builder_for_an_array_event_type_with_a_message_that_does_not_offer_sealing_it() {
        // An array can never be sealed or final in a way that fixes this, so it gets its own message rather than the
        // "cannot all be enumerated" one, which would tell a reader to do something impossible.
        assertThatThrownBy(() -> Saga.<Object, String, OrderCommand>builder(null)
                .correlateAll(e -> "id")
                .startsOn(OrderPlaced[].class)
                .evolve(OrderPlaced[].class, (state, e) -> state)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no event is ever stored as an array")
                .hasMessageNotContaining("cannot all be enumerated")
                .hasMessageNotContaining("final or sealed");
    }

    @Test
    void is_refused_by_the_flow_builder_when_a_step_declares_a_supertype_that_is_not_sealed() {
        assertThatThrownBy(() -> FlowSaga.<OpenEvent, OrderCommand>builder()
                .correlateAll(OpenEvent::orderId)
                .startsOn(OpenOrderPlaced.class)
                .step("wait", step -> step.on(OpenEvent.class, Continuation.end()))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OpenEvent.class.getName());
    }

    private <S> void run(String subscriptionId, Saga<OrderEvent, S, OrderCommand> saga, List<OrderCommand> issued) {
        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                .run(subscriptionId, saga, SagaStateStore.inMemory(), issued::add);
        subscriptionsToClose.add(subscription);
        subscription.waitUntilStarted(Duration.ofSeconds(5));
    }

    private void write(String orderId, OrderEvent... events) {
        eventStore.write(orderId, converter.toCloudEvents(List.of(events)));
    }
}
