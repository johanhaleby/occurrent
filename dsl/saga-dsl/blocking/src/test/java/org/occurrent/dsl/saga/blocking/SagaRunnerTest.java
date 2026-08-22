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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.command.CommandDispatchers;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.decider.DeciderApplicationService;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.flow.*;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntPredicate;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("SagaRunner")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaRunnerTest {

    private static final String PAYMENT_TIMER = "payment";
    private static final Duration LONG_PAYMENT_TIMEOUT = Duration.ofMinutes(30);
    private static final Duration SHORT_PAYMENT_TIMEOUT = Duration.ofMillis(150);
    // Every runner here polls fast, so a timer fires promptly and no test is slowed by the production default (15s). Set
    // the interval explicitly rather than leaning on SagaRunnerConfig.defaults(), so no test depends on that value.
    private static final SagaRunnerConfig FAST_POLL_CONFIG = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50));

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

    @Test
    void a_saga_run_without_waiting_returns_before_its_replay_has_folded() throws Exception {
        CountDownLatch foldReached = new CountDownLatch(1);
        CountDownLatch releaseFold = new CountDownLatch(1);
        CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
        PushSubscriptionModel pushModel = new PushSubscriptionModel();
        Saga<OrderEvent, OrderState, OrderCommand> saga = Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                .build();

        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(pushModel, converter)
                .run("no-wait", saga, SagaStateStore.inMemory(), command -> {
                    issued.add(command);
                    foldReached.countDown();
                    try {
                        assertThat(releaseFold.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }, null, FAST_POLL_CONFIG, () -> true, false);
        subscriptionsToClose.add(subscription);

        // The fold parks on another thread, so reaching this line at all is the point: waiting would keep the caller
        // inside run until the whole thing had folded.
        CompletableFuture<Void> pushed = CompletableFuture.runAsync(() -> pushModel.accept(
                CloudEventBuilder.v1(converter.toCloudEvent(new OrderPlaced("e1", "order-1")))
                        .withExtension(new OccurrentCloudEventExtension("order-1", 1L))
                        .build()));
        assertThat(foldReached.await(5, TimeUnit.SECONDS)).isTrue();

        releaseFold.countDown();
        pushed.get(5, TimeUnit.SECONDS);
        assertThat(issued).containsExactly(new ShipOrder("order-1"));
    }

    @Nested
    class EventToCommand {

        @Test
        void a_reservation_completes_the_instance_ships_the_order_and_clears_its_timer() {
            String orderId = "order-1";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = issued::add;
            run("event-to-command", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, FAST_POLL_CONFIG).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(orderId)));
            SagaEnvelope<OrderState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(envelope.status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(envelope.timers()).isEmpty()
            );
        }
    }

    @Nested
    class ProgrammaticObservation {

        /**
         * The observation facade is justified by serving the programmatic path as well as Spring's, but only the Spring
         * path had coverage: nothing invoked {@code SagaSubscription.instances()} or {@code SagaInstances.findByStatus}.
         * This drives a real saga through the runner and observes it through the handle {@code run(...)} hands back.
         */
        @Test
        void observes_an_instance_through_the_handle_the_runner_returns() {
            String orderId = "order-observed";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            SagaSubscription subscription = run("programmatic-observation", orderFulfillment(LONG_PAYMENT_TIMEOUT),
                    stateStore, command -> {
                    }, FAST_POLL_CONFIG);
            subscription.waitUntilStarted();
            SagaInstances instances = subscription.instances();
            Instant wellAfterNow = Instant.now().plusSeconds(60);

            assertThat(instances.find(orderId)).as("nothing is observable before the saga starts").isEmpty();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> assertThat(instances.find(orderId)).isPresent());
            SagaInstance active = instances.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(active.sagaId()).isEqualTo(orderId),
                    () -> assertThat(active.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(active.isCompleted()).isFalse(),
                    () -> assertThat(active.createdAt()).isNotNull(),
                    () -> assertThat(active.completedAt()).as("still running").isNull(),
                    () -> assertThat(active.nextTimerAt()).as("the payment timeout is pending").isNotNull(),
                    () -> assertThat(active.currentStep()).as("a core saga has no flow step").isNull(),
                    () -> assertThat(instances.findByStatus(SagaStatus.ACTIVE, wellAfterNow, 10))
                            .extracting(SagaInstance::sagaId).containsExactly(orderId),
                    () -> assertThat(instances.findByStatus(SagaStatus.COMPLETED, wellAfterNow, 10)).isEmpty()
            );

            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().untilAsserted(() -> assertThat(instances.find(orderId).orElseThrow().isCompleted()).isTrue());
            SagaInstance completed = instances.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(completed.status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(completed.completedAt()).isNotNull(),
                    () -> assertThat(completed.nextTimerAt()).as("a terminal instance holds no timers").isNull(),
                    () -> assertThat(instances.findByStatus(SagaStatus.COMPLETED, Instant.now().plusSeconds(60), 10))
                            .extracting(SagaInstance::sagaId).containsExactly(orderId),
                    () -> assertThat(instances.findByStatus(SagaStatus.ACTIVE, Instant.now().plusSeconds(60), 10)).isEmpty()
            );
        }
    }

    @Nested
    class CasContention {

        // Drives the compare-and-set retry with a scripted store: find() is always empty, so every attempt starts the
        // OrderPlaced instance fresh and produces an envelope to save, and compareAndSave decides whether that attempt wins.
        private final class ScriptedCasStore implements SagaStateStore<OrderState> {
            private final IntPredicate savedOnAttempt; // 1-based attempt number, did that save win?
            private final RuntimeException failWith;    // when set, compareAndSave throws instead of returning
            int saveAttempts = 0;

            ScriptedCasStore(IntPredicate savedOnAttempt, RuntimeException failWith) {
                this.savedOnAttempt = savedOnAttempt;
                this.failWith = failWith;
            }

            @Override
            public Optional<SagaEnvelope<OrderState>> find(@NonNull String sagaId) {
                return Optional.empty();
            }

            @Override
            public boolean compareAndSave(@NonNull String sagaId, @NonNull SagaEnvelope<OrderState> envelope, long expectedVersion) {
                saveAttempts++;
                if (failWith != null) {
                    throw failWith;
                }
                return savedOnAttempt.test(saveAttempts);
            }

            @Override
            public List<SagaEnvelope<OrderState>> findWithDueTimers(@NonNull Instant now, int limit) {
                return List.of();
            }


            @Override
            public void delete(@NonNull String sagaId) {
            }
        }

        // Carries the stream metadata a stored event always has, so these stay about what they are named for rather
        // than tripping the redelivery-detection refusal on the way in.
        private CloudEvent orderPlaced(String orderId) {
            CloudEvent event = converter.toCloudEvents(List.of(new OrderPlaced(UUID.randomUUID().toString(), orderId))).getFirst();
            return CloudEventBuilder.v1(event).withExtension(new OccurrentCloudEventExtension(orderId, 1L)).build();
        }

        private SagaExecution<OrderEvent, OrderState, OrderCommand> execution(SagaStateStore<OrderState> store, int maxCasAttempts) {
            SagaRunnerConfig config = new SagaRunnerConfig(Duration.ofMinutes(1), 100, maxCasAttempts);
            return new SagaExecution<>("cas-retry", orderFulfillment(LONG_PAYMENT_TIMEOUT), store, command -> {
            }, converter, config);
        }

        @Test
        void a_lost_compare_and_set_retries_until_it_wins() {
            ScriptedCasStore store = new ScriptedCasStore(attempt -> attempt >= 3, null);

            execution(store, 5).onCloudEvent(orderPlaced("order-cas-1"));

            assertThat(store.saveAttempts).isEqualTo(3);
        }

        @Test
        void exhausting_the_retries_raises_SagaConcurrencyException_after_exactly_maxCasAttempts_saves() {
            ScriptedCasStore store = new ScriptedCasStore(attempt -> false, null);

            assertThatThrownBy(() -> execution(store, 3).onCloudEvent(orderPlaced("order-cas-2")))
                    .isInstanceOf(SagaConcurrencyException.class)
                    .hasMessageContaining("after 3 attempts");
            assertThat(store.saveAttempts).isEqualTo(3);
        }

        @Test
        void a_non_concurrency_failure_propagates_on_the_first_attempt_without_retrying() {
            RuntimeException storeDown = new IllegalStateException("saga store is down");
            ScriptedCasStore store = new ScriptedCasStore(attempt -> false, storeDown);

            assertThatThrownBy(() -> execution(store, 5).onCloudEvent(orderPlaced("order-cas-3")))
                    .isSameAs(storeDown);
            assertThat(store.saveAttempts).isEqualTo(1);
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
            run("real-application-service", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, FAST_POLL_CONFIG).waitUntilStarted();

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
            run("timer-fires", orderFulfillment(SHORT_PAYMENT_TIMEOUT), stateStore, dispatcher, FAST_POLL_CONFIG).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(issued).containsExactly(new CancelOrder(orderId)));
            SagaEnvelope<OrderState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(envelope.status()).isEqualTo(SagaStatus.COMPLETED),
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
            run("timer-cancelled", orderFulfillment(SHORT_PAYMENT_TIMEOUT), stateStore, dispatcher, FAST_POLL_CONFIG).waitUntilStarted();

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
            run("correlation-skip", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, FAST_POLL_CONFIG).waitUntilStarted();

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

        // InMemorySubscriptionModel cannot replay to a new subscription, so these drive a PushSubscriptionModel
        // instead, which redelivers whatever you hand it twice. That is also the shape a broker listener has.
        // Ships on every OrderPlaced and never becomes terminal, so a second delivery is governed by dedup alone
        // rather than by the terminal-instance skip, which would hide the thing under test.
        private Saga<OrderEvent, OrderState, OrderCommand> shipsOnEveryPlaced() {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                    .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new ShipOrder(e.orderId()))))
                    .build();
        }

        private SagaSubscription runOn(PushSubscriptionModel pushModel, String subscriptionId,
                                       SagaStateStore<OrderState> stateStore, CommandDispatcher<OrderCommand> dispatcher) {
            return runOn(pushModel, subscriptionId, stateStore, dispatcher, FAST_POLL_CONFIG);
        }

        private SagaSubscription runOn(PushSubscriptionModel pushModel, String subscriptionId,
                                       SagaStateStore<OrderState> stateStore, CommandDispatcher<OrderCommand> dispatcher,
                                       SagaRunnerConfig config) {
            SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(pushModel, converter)
                    .run(subscriptionId, shipsOnEveryPlaced(), stateStore, dispatcher, null, config);
            subscriptionsToClose.add(subscription);
            return subscription;
        }

        private CloudEvent taggedEvent(OrderEvent event, String streamId, long streamVersion) {
            return CloudEventBuilder.v1(converter.toCloudEvent(event))
                    .withExtension(new OccurrentCloudEventExtension(streamId, streamVersion))
                    .build();
        }

        @Test
        void a_redelivered_event_carrying_its_stream_version_is_recognised_and_not_reacted_to_twice() {
            // Given a saga fed by a push model, as a broker listener would
            PushSubscriptionModel pushModel = new PushSubscriptionModel();
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            runOn(pushModel, "push-dedup", stateStore, issued::add);
            CloudEvent placed = taggedEvent(new OrderPlaced("e1", "order-1"), "order-1", 1L);

            // When the same event is delivered twice, which at-least-once delivery does
            pushModel.accept(placed);
            pushModel.accept(placed);

            // Then the reaction ran once
            assertThat(issued).containsExactly(new ShipOrder("order-1"));
        }

        @Test
        void an_event_carrying_no_stream_metadata_is_refused_by_default_and_no_command_is_issued() {
            // Given the same saga fed events with none of the Occurrent extensions, which is what a listener that
            // forwards a converter-produced CloudEvent delivers
            PushSubscriptionModel pushModel = new PushSubscriptionModel();
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            runOn(pushModel, "push-no-dedup", stateStore, issued::add);
            CloudEvent placed = converter.toCloudEvent(new OrderPlaced("e1", "order-2"));

            // When
            Throwable refusal = catchThrowable(() -> pushModel.accept(placed));

            // Then the refusal reaches the feed, so the event goes unacknowledged instead of the saga silently taking
            // on duplicate commands, and it names the way out
            assertAll(
                    () -> assertThat(refusal).isInstanceOf(SagaRedeliveryDetectionException.class)
                            .hasMessageContaining("push-no-dedup")
                            .hasMessageContaining("BEST_EFFORT"),
                    () -> assertThat(issued).isEmpty()
            );
        }

        @Test
        void an_event_carrying_no_stream_metadata_is_reacted_to_twice_under_best_effort() {
            // Given a saga that knowingly accepts a feed carrying none of the metadata, which is what another
            // application's broker delivers
            PushSubscriptionModel pushModel = new PushSubscriptionModel();
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            runOn(pushModel, "push-best-effort", stateStore, issued::add,
                    FAST_POLL_CONFIG.withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));
            CloudEvent placed = converter.toCloudEvent(new OrderPlaced("e1", "order-2"));

            // When
            pushModel.accept(placed);
            pushModel.accept(placed);

            // Then the command goes out twice, which is the cost BEST_EFFORT accepts. Pinned so the consequence of
            // opting out is written down as behaviour rather than only in prose.
            assertThat(issued).containsExactly(new ShipOrder("order-2"), new ShipOrder("order-2"));
        }

        @Test
        void an_event_that_has_been_through_cloud_events_json_still_deduplicates() {
            // Given the realistic broker shape: the stored event serialized out and rebuilt on the listener side,
            // which turns the streamversion extension into a string
            PushSubscriptionModel pushModel = new PushSubscriptionModel();
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            runOn(pushModel, "push-json-dedup", stateStore, issued::add);
            EventFormat json = requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE));
            CloudEvent roundTripped = json.deserialize(json.serialize(taggedEvent(new OrderPlaced("e1", "order-3"), "order-3", 1L)));

            // When
            pushModel.accept(roundTripped);
            pushModel.accept(roundTripped);

            // Then dedup still works, so the round trip did not cost the saga its redelivery protection
            assertThat(issued).containsExactly(new ShipOrder("order-3"));
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
            run("decider-dispatcher", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, FAST_POLL_CONFIG).waitUntilStarted();

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
            run("crash-between-dispatch-and-save", orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, flaky, FAST_POLL_CONFIG).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));
            write(orderId, new PaymentReserved(UUID.randomUUID().toString(), orderId));

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(issued).containsExactly(new ShipOrder(orderId)));
            SagaEnvelope<OrderState> envelope = stateStore.find(orderId).orElseThrow();
            assertAll(
                    () -> assertThat(shipAttempts.get()).isGreaterThanOrEqualTo(2),
                    () -> assertThat(envelope.status()).isEqualTo(SagaStatus.COMPLETED),
                    // Three saves rather than two, because the first dispatch failure writes the record that starts the
                    // quarantine budget before it rethrows. The successful redelivery then clears it again.
                    () -> assertThat(envelope.version()).isEqualTo(3),
                    () -> assertThat(envelope.failure()).isNull()
            );
        }
    }

    @Nested
    class DispatchAll {

        /** Reacts to OrderPlaced with two commands in one list, unlike orderFulfillment's single-command reactions. */
        private static Saga<OrderEvent, OrderState, OrderCommand> issuesTwoCommandsOnStart() {
            return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new Completed(e.orderId()))
                    .react(OrderPlaced.class, (state, e) -> List.of(
                            SagaEffect.issue(new ShipOrder(e.orderId())),
                            SagaEffect.issue(new CancelOrder(e.orderId()))))
                    .isTerminal(state -> true)
                    .build();
        }

        // Carries the stream metadata a stored event always has, so these stay about what they are named for rather
        // than tripping the redelivery-detection refusal on the way in.
        private CloudEvent orderPlaced(String orderId) {
            CloudEvent event = converter.toCloudEvents(List.of(new OrderPlaced(UUID.randomUUID().toString(), orderId))).getFirst();
            return CloudEventBuilder.v1(event).withExtension(new OccurrentCloudEventExtension(orderId, 1L)).build();
        }

        private SagaExecution<OrderEvent, OrderState, OrderCommand> execution(CommandDispatcher<OrderCommand> dispatcher) {
            return new SagaExecution<>("dispatch-all", issuesTwoCommandsOnStart(), SagaStateStore.inMemory(), dispatcher, converter, SagaRunnerConfig.defaults());
        }

        @Test
        void a_plain_lambda_dispatcher_still_receives_one_call_per_command_through_the_default() {
            String orderId = "order-dispatch-all-1";
            List<OrderCommand> received = new ArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = received::add; // a lambda only implements dispatch, so dispatchAll's default loop applies.

            execution(dispatcher).onCloudEvent(orderPlaced(orderId));

            assertThat(received).containsExactly(new ShipOrder(orderId), new CancelOrder(orderId));
        }

        @Test
        void a_dispatcher_overriding_dispatchAll_receives_the_reactions_whole_command_list_in_a_single_call() {
            String orderId = "order-dispatch-all-2";
            List<List<OrderCommand>> batches = new ArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = new CommandDispatcher<>() {
                @Override
                public void dispatch(OrderCommand command) {
                    throw new AssertionError("dispatch(C) must not be called once dispatchAll is overridden");
                }

                @Override
                public void dispatchAll(List<OrderCommand> commands) {
                    batches.add(commands);
                }
            };

            execution(dispatcher).onCloudEvent(orderPlaced(orderId));

            assertThat(batches).containsExactly(List.of(new ShipOrder(orderId), new CancelOrder(orderId)));
        }
    }

    @Nested
    class TimerLeaseGating {

        private static final Duration FAST_POLL = Duration.ofMillis(30);
        private static final SagaRunnerConfig FAST = new SagaRunnerConfig(FAST_POLL, 100, 50);

        // Records what the runner asked of the strategy and lets a test flip leadership on and off.
        private final class StubStrategy implements CompetingConsumerStrategy {
            private final AtomicBoolean locked;
            private final CopyOnWriteArrayList<String> registered = new CopyOnWriteArrayList<>();
            private final CopyOnWriteArrayList<String> unregistered = new CopyOnWriteArrayList<>();

            StubStrategy(boolean initiallyLocked) {
                this.locked = new AtomicBoolean(initiallyLocked);
            }

            @Override
            public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
                registered.add(subscriptionId + "|" + subscriberId);
                return locked.get();
            }

            @Override
            public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
                unregistered.add(subscriptionId + "|" + subscriberId);
            }

            @Override
            public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            }

            @Override
            public boolean hasLock(String subscriptionId, String subscriberId) {
                return locked.get();
            }

            @Override
            public void addListener(CompetingConsumerListener listenerConsumer) {
            }

            @Override
            public void removeListener(CompetingConsumerListener listenerConsumer) {
            }
        }

        // Wraps an in-memory store and counts the due-timer queries, the DB load this feature is meant to remove.
        private final class CountingStateStore implements SagaStateStore<OrderState> {
            private final SagaStateStore<OrderState> delegate = SagaStateStore.inMemory();
            private final AtomicInteger dueTimerQueries = new AtomicInteger();

            @Override
            public Optional<SagaEnvelope<OrderState>> find(@NonNull String sagaId) {
                return delegate.find(sagaId);
            }

            @Override
            public boolean compareAndSave(@NonNull String sagaId, @NonNull SagaEnvelope<OrderState> envelope, long expectedVersion) {
                return delegate.compareAndSave(sagaId, envelope, expectedVersion);
            }

            @Override
            public List<SagaEnvelope<OrderState>> findWithDueTimers(@NonNull Instant now, int limit) {
                dueTimerQueries.incrementAndGet();
                return delegate.findWithDueTimers(now, limit);
            }


            @Override
            public void delete(@NonNull String sagaId) {
                delegate.delete(sagaId);
            }
        }

        private SagaSubscription run(String subscriptionId, SagaStateStore<OrderState> stateStore,
                                    CommandDispatcher<OrderCommand> dispatcher, CompetingConsumerStrategy strategy) {
            SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter)
                    .competingConsumerStrategy(strategy);
            SagaSubscription subscription = runner.run(subscriptionId, orderFulfillment(LONG_PAYMENT_TIMEOUT), stateStore, dispatcher, null, FAST);
            subscriptionsToClose.add(subscription);
            return subscription;
        }

        @Test
        void competingConsumerStrategy_returns_a_distinct_runner_without_mutating_the_original() {
            SagaRunner<OrderEvent, OrderCommand> base = SagaRunner.agnostic(subscriptionModel, converter);
            SagaRunner<OrderEvent, OrderCommand> gated = base.competingConsumerStrategy(new StubStrategy(true));
            assertThat(gated).isNotSameAs(base);
        }

        @Test
        void competingConsumerStrategy_rejects_a_null_strategy() {
            SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.agnostic(subscriptionModel, converter);
            assertThatThrownBy(() -> runner.competingConsumerStrategy(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("competingConsumerStrategy");
        }

        @Test
        void a_standby_instance_never_queries_the_store_for_due_timers() {
            CountingStateStore store = new CountingStateStore();
            run("standby", store, cmd -> {
            }, new StubStrategy(false)).waitUntilStarted();

            // The poller ticks several times over this window, and a non-leader must not touch the store on any tick.
            await().during(Duration.ofMillis(250)).atMost(Duration.ofSeconds(1)).until(() -> store.dueTimerQueries.get() == 0);
        }

        @Test
        void the_leader_instance_polls_the_store_for_due_timers() {
            CountingStateStore store = new CountingStateStore();
            run("leader", store, cmd -> {
            }, new StubStrategy(true)).waitUntilStarted();

            await().atMost(Duration.ofSeconds(2)).until(() -> store.dueTimerQueries.get() > 0);
        }

        @Test
        void polling_starts_when_this_instance_wins_the_lease() {
            CountingStateStore store = new CountingStateStore();
            StubStrategy strategy = new StubStrategy(false);
            run("failover", store, cmd -> {
            }, strategy).waitUntilStarted();

            await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).until(() -> store.dueTimerQueries.get() == 0);
            strategy.locked.set(true);
            await().atMost(Duration.ofSeconds(2)).until(() -> store.dueTimerQueries.get() > 0);
        }

        @Test
        void the_poller_registers_and_releases_a_lease_keyed_apart_from_the_event_subscription() {
            String subscriptionId = "lease-key-order";
            StubStrategy strategy = new StubStrategy(true);
            SagaSubscription subscription = run(subscriptionId, new CountingStateStore(), cmd -> {
            }, strategy);
            subscription.waitUntilStarted();

            assertThat(strategy.registered).hasSize(1);
            String registration = strategy.registered.getFirst();
            String leaseKey = registration.substring(0, registration.indexOf('|'));
            assertAll(
                    () -> assertThat(leaseKey).isEqualTo(SagaRunner.timerLeaseKey(subscriptionId)),
                    () -> assertThat(leaseKey).isNotEqualTo(subscriptionId)
            );

            subscription.close();
            subscriptionsToClose.remove(subscription);
            assertThat(strategy.unregistered).containsExactly(registration);
        }

        @Test
        void without_a_strategy_the_poller_runs_on_every_instance() {
            CountingStateStore store = new CountingStateStore();
            SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.agnostic(subscriptionModel, converter);
            SagaSubscription subscription = runner.run("ungated", orderFulfillment(LONG_PAYMENT_TIMEOUT), store, cmd -> {
            }, null, FAST);
            subscriptionsToClose.add(subscription);
            subscription.waitUntilStarted();

            await().atMost(Duration.ofSeconds(2)).until(() -> store.dueTimerQueries.get() > 0);
        }
    }

    @Nested
    class TimersEnabledGating {

        private static final Duration FAST_POLL = Duration.ofMillis(30);
        private static final SagaRunnerConfig FAST = new SagaRunnerConfig(FAST_POLL, 100, 50);

        private SagaSubscription run(String subscriptionId, SagaStateStore<OrderState> stateStore,
                                    CommandDispatcher<OrderCommand> dispatcher, BooleanSupplier timersEnabled) {
            SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.agnostic(subscriptionModel, converter);
            SagaSubscription subscription = runner.run(subscriptionId, orderFulfillment(SHORT_PAYMENT_TIMEOUT), stateStore, dispatcher, null, FAST, timersEnabled);
            subscriptionsToClose.add(subscription);
            return subscription;
        }

        @Test
        void a_due_timer_is_not_dispatched_while_the_supplier_returns_false_and_fires_once_it_returns_true() {
            String orderId = "order-timers-enabled";
            SagaStateStore<OrderState> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<OrderCommand> issued = new CopyOnWriteArrayList<>();
            CommandDispatcher<OrderCommand> dispatcher = issued::add;
            AtomicBoolean timersEnabled = new AtomicBoolean(false);
            run("timers-enabled-gating", stateStore, dispatcher, timersEnabled::get).waitUntilStarted();

            write(orderId, new OrderPlaced(UUID.randomUUID().toString(), orderId));

            // The timeout is short and the poller ticks fast, so several polls elapse with the timer due; none of them
            // may dispatch while the supplier stays false.
            await().pollDelay(SHORT_PAYMENT_TIMEOUT.multipliedBy(3)).atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(issued).isEmpty());

            timersEnabled.set(true);

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(issued).containsExactly(new CancelOrder(orderId)));
        }
    }

    @Nested
    class FlowSagaConditionStep {

        // A single-leaf allOf window condition already gets runner coverage for free through InvocationSagaTest's
        // twoPaymentsFlow. A richer on(StepCondition) tree does not, so this drives one through the real executor,
        // subscription, timer poller and command dispatch together, not the pure evolve/react unit tests FlowSagaTest
        // and SagaFlowExtensionsTest cover.
        sealed interface ReviewEvent permits ReviewRequested, Approved, Escalated {
            String eventId();

            String reviewId();
        }

        record ReviewRequested(String eventId, String reviewId) implements ReviewEvent {
        }

        record Approved(String eventId, String reviewId, int score) implements ReviewEvent {
        }

        record Escalated(String eventId, String reviewId) implements ReviewEvent {
        }

        sealed interface ReviewCommand permits Publish {
        }

        record Publish(String reviewId) implements ReviewCommand {
        }

        private Saga<ReviewEvent, FlowState<ReviewEvent>, ReviewCommand> reviewSaga() {
            return FlowSaga.<ReviewEvent, ReviewCommand>builder()
                    .startsOn(ReviewRequested.class)
                    .correlateAll(ReviewEvent::reviewId)
                    .step("awaiting-approval", step -> step
                            .on(StepCondition.anyOf(
                                            StepCondition.event(Approved.class, (Approved a) -> a.score() >= 80),
                                            StepCondition.event(Escalated.class)),
                                    Continuation.end(),
                                    received -> List.of(new Publish(received.initiating(ReviewRequested.class).reviewId()))))
                    .build();
        }

        @Test
        void an_anyOf_predicate_condition_step_fires_through_the_real_executor() {
            String reviewId = "review-1";
            CloudEventConverter<ReviewEvent> reviewConverter =
                    new JacksonCloudEventConverter.Builder<ReviewEvent>(new ObjectMapper(), URI.create("urn:test"))
                            .idMapper(ReviewEvent::eventId).build();
            SagaStateStore<FlowState<ReviewEvent>> stateStore = SagaStateStore.inMemory();
            CopyOnWriteArrayList<ReviewCommand> issued = new CopyOnWriteArrayList<>();
            SagaRunner<ReviewEvent, ReviewCommand> runner = SagaRunner.agnostic(subscriptionModel, reviewConverter);
            SagaSubscription subscription = runner.run("condition-step", reviewSaga(), stateStore, issued::add, null, FAST_POLL_CONFIG);
            subscriptionsToClose.add(subscription);
            subscription.waitUntilStarted();

            eventStore.write(reviewId, reviewConverter.toCloudEvents(List.of(new ReviewRequested(UUID.randomUUID().toString(), reviewId))));
            // A low-scoring approval satisfies neither alternative in the anyOf, so the saga stays open on it alone.
            eventStore.write(reviewId, reviewConverter.toCloudEvents(List.of(new Approved(UUID.randomUUID().toString(), reviewId, 50))));
            eventStore.write(reviewId, reviewConverter.toCloudEvents(List.of(new Escalated(UUID.randomUUID().toString(), reviewId))));

            await().untilAsserted(() -> assertThat(issued).containsExactly(new Publish(reviewId)));
            SagaEnvelope<FlowState<ReviewEvent>> envelope = stateStore.find(reviewId).orElseThrow();
            assertThat(envelope.status()).isEqualTo(SagaStatus.COMPLETED);
        }
    }
}
