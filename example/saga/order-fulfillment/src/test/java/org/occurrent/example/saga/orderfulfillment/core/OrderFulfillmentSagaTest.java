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

package org.occurrent.example.saga.orderfulfillment.core;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.command.CommandDispatchers;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.example.saga.orderfulfillment.*;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Runs {@link OrderFulfillmentSaga} through {@link SagaRunner}, showing the decider-free dispatch path first (a plain
 * lambda over an {@code ApplicationService}) and the decider adapter as an alternative.
 */
@DisplayName("Core order-fulfillment saga")
@DisplayNameGeneration(ReplaceUnderscores.class)
class OrderFulfillmentSagaTest {

    private static final Duration LONG_PAYMENT_TIMEOUT = Duration.ofMinutes(30);

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:example:saga:order-fulfillment"))
                .typeMapper(ReflectionCloudEventTypeMapper.simple(OrderEvent.class))
                .idMapper(event -> UUID.randomUUID().toString())
                .build();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        subscriptionModel.shutdown();
    }

    private SagaSubscription run(String subscriptionId, Saga<OrderEvent, OrderSagaState, OrderCommand> saga,
                                  SagaStateStore<OrderSagaState> stateStore, CommandDispatcher<OrderCommand> dispatcher) {
        SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.agnostic(subscriptionModel, converter);
        SagaSubscription subscription = runner.run(subscriptionId, saga, stateStore, dispatcher, null, SagaRunnerConfig.defaults());
        subscriptionsToClose.add(subscription);
        return subscription;
    }

    private void write(String orderId, OrderEvent... events) {
        eventStore.write(orderId, converter.toCloudEvents(List.of(events)));
    }

    /** What the decider-free and decider dispatchers below write instead of performing the command for real. */
    private static OrderEvent toCommandOutcomeEvent(OrderCommand command) {
        return switch (command) {
            case ReservePayment reservePayment -> new PaymentReservationRequested(reservePayment.orderId(), reservePayment.amount());
            case ShipOrder shipOrder -> new OrderShipped(shipOrder.orderId());
            case CancelOrder cancelOrder -> new OrderCancelled(cancelOrder.orderId(), cancelOrder.reason());
        };
    }

    @Nested
    class DeciderFreeDispatcher {

        @Test
        void a_plain_lambda_over_an_ApplicationService_dispatches_every_command_without_a_decider() {
            String orderId = "order-1";
            SagaStateStore<OrderSagaState> stateStore = SagaStateStore.inMemory();
            // A separate, unwired event store: writes here must never feed back into this saga's own subscription.
            InMemoryEventStore commandEventStore = new InMemoryEventStore();
            ApplicationService<OrderEvent> applicationService = new GenericApplicationService<>(commandEventStore, converter);
            CommandDispatcher<OrderCommand> dispatcher = cmd -> applicationService.execute(cmd.orderId(), events -> List.of(toCommandOutcomeEvent(cmd)));
            run("decider-free-dispatcher", OrderFulfillmentSaga.orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher).waitUntilStarted();

            write(orderId, new OrderPlaced(orderId, 42.0));

            await().untilAsserted(() -> assertThat(commandEvents(commandEventStore, orderId)).containsExactly(new PaymentReservationRequested(orderId, 42.0)));

            write(orderId, new PaymentReserved(orderId));

            await().untilAsserted(() -> assertThat(commandEvents(commandEventStore, orderId))
                    .containsExactly(new PaymentReservationRequested(orderId, 42.0), new OrderShipped(orderId)));
            SagaEnvelope<OrderSagaState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(envelope.status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(envelope.timers()).isEmpty()
            );
        }

        private List<OrderEvent> commandEvents(InMemoryEventStore commandEventStore, String orderId) {
            return commandEventStore.read(orderId).eventList().stream().map(converter::toDomainEvent).toList();
        }
    }

    @Nested
    class DeciderDispatcher {

        @Test
        void CommandDispatchers_decider_wires_a_decider_backed_ApplicationService_as_the_dispatcher() {
            String orderId = "order-2";
            SagaStateStore<OrderSagaState> stateStore = SagaStateStore.inMemory();
            InMemoryEventStore commandEventStore = new InMemoryEventStore();
            ApplicationService<OrderEvent> applicationService = new GenericApplicationService<>(commandEventStore, converter);
            DeciderApplicationService<OrderEvent> deciderApplicationService = new DeciderApplicationService<>(applicationService);
            Decider<OrderCommand, Void, OrderEvent> orderCommandDecider = Decider.create(
                    null,
                    (OrderCommand cmd, Void state) -> List.of(toCommandOutcomeEvent(cmd)),
                    (state, event) -> state
            );
            CommandDispatcher<OrderCommand> dispatcher = CommandDispatchers.decider(deciderApplicationService, orderCommandDecider, OrderCommand::orderId);
            run("decider-dispatcher", OrderFulfillmentSaga.orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher).waitUntilStarted();

            write(orderId, new OrderPlaced(orderId, 42.0));
            write(orderId, new PaymentReserved(orderId));

            await().untilAsserted(() -> {
                List<OrderEvent> events = commandEventStore.read(orderId).eventList().stream().map(converter::toDomainEvent).toList();
                assertThat(events).containsExactly(new PaymentReservationRequested(orderId, 42.0), new OrderShipped(orderId));
            });
        }
    }
}
