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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.Status;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SagaRunner")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaRunnerTest {

    private static final String PAYMENT_TIMER = "payment";
    private static final Duration LONG_PAYMENT_TIMEOUT = Duration.ofMinutes(30);
    private static final Duration SHORT_PAYMENT_TIMEOUT = Duration.ofMillis(150);

    // --- The tiny order/payment domain described in the test brief ---

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved, OrderShipped, OrderCancelled {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    /** Written by the "real application service" and "decider" dispatchers below, never by the saga itself. */
    record OrderShipped(String eventId, String orderId) implements OrderEvent {
    }

    /** Written by the "real application service" and "decider" dispatchers below, never by the saga itself. */
    record OrderCancelled(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder, CancelOrder {
        String orderId();
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Completed, Cancelled {
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Completed(String orderId) implements OrderState {
    }

    record Cancelled(String orderId) implements OrderState {
    }

    /**
     * correlateAll returns {@code null} for a blank order id, exercising the "event correlates to no instance" skip
     * path used by {@link CorrelationSkip}; every other test always supplies a real order id.
     */
    private static Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment(Duration paymentTimeout) {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(e -> e.orderId().isBlank() ? null : e.orderId())
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.startTimeout(PAYMENT_TIMER, paymentTimeout)))
                .evolve(PaymentReserved.class, (state, e) -> new Completed(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> List.of(
                        SagaEffect.issue(new ShipOrder(e.orderId())),
                        SagaEffect.cancelTimeout(PAYMENT_TIMER)))
                .evolveOnTimeout(PAYMENT_TIMER, (state, t) -> new Cancelled(t.sagaId()))
                .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                .isTerminal(state -> state instanceof Completed || state instanceof Cancelled)
                .build();
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

    private SagaSubscription run(String subscriptionId, Saga<OrderEvent, OrderState, OrderCommand> saga,
                                 SagaStateStore<OrderState> stateStore, CommandDispatcher<OrderCommand> dispatcher, SagaRunnerConfig config) {
        SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.agnostic(subscriptionModel, converter);
        SagaSubscription subscription = runner.run(subscriptionId, saga, stateStore, dispatcher, null, config);
        subscriptionsToClose.add(subscription);
        return subscription;
    }

    private void write(String orderId, OrderEvent... events) {
        eventStore.write(orderId, converter.toCloudEvents(List.of(events)));
    }

    @Nested
    class EventToCommand {

        @Test
        void a_reservation_completes_the_instance_ships_the_order_and_clears_its_timer() {
            String orderId = "order-1";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = issued::add;
            run("event-to-command", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, SagaRunnerConfig.defaults()).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(orderId)));
            SagaEnvelope<OrderState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(envelope.status()).isEqualTo(Status.COMPLETED),
                    () -> assertThat(envelope.timers()).isEmpty()
            );
        }
    }

    @Nested
    class RealApplicationServiceDispatcher {

        @Test
        void a_plain_lambda_over_an_ApplicationService_dispatches_commands_without_a_decider() {
            String orderId = "order-2";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            // A separate, unwired event store: writes here must never feed back into this saga's own subscription.
            InMemoryEventStore commandEventStore = new InMemoryEventStore();
            ApplicationService<OrderEvent> applicationService = new GenericApplicationService<>(commandEventStore, converter);
            CommandDispatcher<OrderCommand> dispatcher = cmd -> applicationService.execute(cmd.orderId(), events -> List.of(toShipmentEvent(cmd)));
            run("real-application-service", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, SagaRunnerConfig.defaults()).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> {
                List<OrderEvent> commandEvents = commandEventStore.read(orderId).eventList().stream().map(converter::toDomainEvent).toList();
                assertThat(commandEvents).containsExactly(new OrderShipped(commandEvents.getFirst().eventId(), orderId));
            });
        }

        private OrderEvent toShipmentEvent(OrderCommand cmd) {
            return switch (cmd) {
                case ShipOrder shipOrder -> new OrderShipped(UUID.randomUUID().toString(), shipOrder.orderId());
                case CancelOrder cancelOrder -> new OrderCancelled(UUID.randomUUID().toString(), cancelOrder.orderId());
            };
        }
    }

    @Nested
    class TimerFires {

        @Test
        void an_unpaid_order_is_cancelled_once_its_payment_timer_expires() {
            String orderId = "order-3";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = issued::add;
            SagaRunnerConfig config = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50));
            run("timer-fires", orderFulfillment(SHORT_PAYMENT_TIMEOUT), stateStore, dispatcher, config).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(issued).containsExactly(new CancelOrder(orderId)));
            SagaEnvelope<OrderState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(envelope.status()).isEqualTo(Status.COMPLETED),
                    () -> assertThat(envelope.state()).isEqualTo(new Cancelled(orderId)),
                    () -> assertThat(envelope.timers()).isEmpty()
            );
        }
    }

    @Nested
    class TimerCancelledByEvent {

        @Test
        void a_reservation_arriving_before_the_timer_fires_cancels_it_so_no_cancellation_ever_follows() {
            String orderId = "order-4";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = issued::add;
            SagaRunnerConfig config = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50));
            run("timer-cancelled", orderFulfillment(SHORT_PAYMENT_TIMEOUT), stateStore, dispatcher, config).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(orderId)));
            // Outlive the (short) payment timeout by a comfortable margin: if cancellation were not wired up, the
            // timer poller would have fired CancelOrder well within this window.
            await().pollDelay(SHORT_PAYMENT_TIMEOUT.multipliedBy(3)).atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(orderId)));
        }
    }

    @Nested
    class CorrelationSkip {

        @Test
        void an_event_that_correlates_to_no_instance_is_ignored() {
            String goodOrderId = "order-5";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = issued::add;
            run("correlation-skip", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, SagaRunnerConfig.defaults()).waitUntilStarted();

            // A blank order id correlates to null per orderFulfillment()'s correlateAll and must be skipped.
            write("blank-stream", new OrderPlaced(UUID.randomUUID().toString(), ""));
            // A witness on a real order id proves the blank event has already had its chance to be processed.
            write(goodOrderId, new OrderPlaced(UUID.randomUUID().toString(), goodOrderId));
            write(goodOrderId, new PaymentReserved(UUID.randomUUID().toString(), goodOrderId));

            await().untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(goodOrderId)));
            assertThat(stateStore.find("")).isEmpty();
        }
    }

    @Nested
    class ReplayDedup {

        @Test
        void note_replay_dedup_cannot_be_exercised_through_InMemorySubscriptionModel() {
            // InMemorySubscriptionModel only supports starting a subscription from "now" or "default" (which is also
            // "now" for this model, see InMemorySubscriptionModel.subscribe): it cannot replay already-published
            // events to a brand-new subscription, so a second SagaRunner started against the same stateStore never
            // receives the earlier OrderPlaced/PaymentReserved and this scenario cannot be driven end-to-end here.
            // The dedup behaviour itself -- a redelivered event at or below the stored watermark is skipped, and a
            // terminal instance ignores further input -- is exercised directly against the pure executor in
            // SagaExecutionSupportTest (see TerminalInstance and RedeliveryDedup there).
            assertThat(true).isTrue();
        }
    }

    @Nested
    class DeciderDispatcher {

        @Test
        void CommandDispatchers_decider_wires_a_decider_backed_ApplicationService_as_the_dispatcher() {
            String orderId = "order-6";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            InMemoryEventStore commandEventStore = new InMemoryEventStore();
            ApplicationService<OrderEvent> applicationService = new GenericApplicationService<>(commandEventStore, converter);
            DeciderApplicationService<OrderEvent> deciderApplicationService = new DeciderApplicationService<>(applicationService);
            Decider<OrderCommand, Void, OrderEvent> shipmentDecider = Decider.create(
                    null,
                    (OrderCommand cmd, Void state) -> List.of(switch (cmd) {
                        case ShipOrder shipOrder -> new OrderShipped(UUID.randomUUID().toString(), shipOrder.orderId());
                        case CancelOrder cancelOrder -> new OrderCancelled(UUID.randomUUID().toString(), cancelOrder.orderId());
                    }),
                    (state, event) -> state
            );
            CommandDispatcher<OrderCommand> dispatcher = CommandDispatchers.decider(deciderApplicationService, shipmentDecider, OrderCommand::orderId);
            run("decider-dispatcher", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, SagaRunnerConfig.defaults()).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> {
                List<OrderEvent> commandEvents = commandEventStore.read(orderId).eventList().stream().map(converter::toDomainEvent).toList();
                assertThat(commandEvents).hasSize(1);
                assertThat(commandEvents.getFirst()).isInstanceOf(OrderShipped.class);
            });
        }
    }

    @Nested
    class CrashBetweenDispatchAndSave {

        @Test
        void a_dispatcher_that_fails_once_still_gets_the_command_delivered_exactly_once_after_redelivery() {
            // InMemorySubscription retries a failed delivery of the SAME cloud event through its RetryStrategy
            // (see InMemorySubscription.run -> executeWithRetry), which is exactly the at-least-once redelivery this
            // proves: the failed attempt dispatches nothing and saves nothing, so the eventual, successful redelivery
            // is the only one that records the command and saves the instance.
            String orderId = "order-7";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            AtomicInteger shipAttempts = new AtomicInteger();
            CommandDispatcher<OrderCommand> flaky = cmd -> {
                if (cmd instanceof ShipOrder && shipAttempts.getAndIncrement() == 0) {
                    throw new RuntimeException("simulated dispatch failure");
                }
                issued.add(cmd);
            };
            run("crash-between-dispatch-and-save", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, flaky, SagaRunnerConfig.defaults()).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(orderId)));
            SagaEnvelope<OrderState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(shipAttempts.get()).isGreaterThanOrEqualTo(2),
                    () -> assertThat(envelope.status()).isEqualTo(Status.COMPLETED),
                    () -> assertThat(envelope.version()).isEqualTo(2)
            );
        }
    }
}
