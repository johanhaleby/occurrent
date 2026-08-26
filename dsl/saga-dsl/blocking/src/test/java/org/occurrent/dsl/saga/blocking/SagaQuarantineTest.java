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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import io.cloudevents.core.builder.CloudEventBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The behaviour <a href="https://github.com/johanhaleby/occurrent/issues/818">#818</a> asked for: one saga instance
 * that cannot handle its event must not stop every other instance sharing the saga's single subscription.
 */
@DisplayName("A saga instance that keeps failing")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineTest {

    private static final String POISON = "poison";
    private static final String HEALTHY = "healthy";
    private static final String TICKING = "ticking";
    private static final String PAYMENT_TIMER = "payment";
    private static final Duration BUDGET = Duration.ofMillis(300);

    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults()
            .withTimerPollInterval(Duration.ofMillis(50))
            .withQuarantineAfter(BUDGET);

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder, CancelOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Shipped {
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Shipped(String orderId) implements OrderState {
    }

    // Reading this inside react keeps the saga a single definition rather than two that could drift apart.
    private volatile boolean reactionFails = true;

    // Long enough that a timer never fires during a test that is not about timers. The one that is shortens it.
    private volatile Duration paymentTimeout = Duration.ofMinutes(30);

    /** Reacting to {@link PaymentReserved} throws for {@link #POISON} and only for it, for as long as it is broken. */
    private Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.startTimeout(PAYMENT_TIMER, paymentTimeout)))
                .evolve(PaymentReserved.class, (state, e) -> new Shipped(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> {
                    if (e.orderId().equals(POISON) && reactionFails) {
                        throw new IllegalStateException("this instance can never handle its payment");
                    }
                    return List.of(SagaEffect.issue(new ShipOrder(e.orderId())), SagaEffect.cancelTimeout(PAYMENT_TIMER));
                })
                .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                .isTerminal(state -> state instanceof Shipped)
                .build();
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private SagaStateStore<OrderState> stateStore;
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
        stateStore = SagaStateStore.inMemory();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        subscriptionModel.shutdown();
    }

    private SagaSubscription run(SagaRunnerConfig config) {
        return run(subscriptionModel, config);
    }

    private SagaSubscription run(Subscribable model, SagaRunnerConfig config) {
        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(model, converter)
                .run("orders", orderFulfillment(), stateStore, dispatched::add, null, config);
        subscriptionsToClose.add(subscription);
        return subscription;
    }

    private void write(String orderId, OrderEvent... events) {
        eventStore.write(orderId, converter.toCloudEvents(List.of(events)));
    }

    @Nested
    class OnASubscriptionSharedWithOtherInstances {

        @Test
        void does_not_stop_the_other_instances_from_processing_the_events_queued_behind_it() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));

            // The failing event is pushed first, so the healthy instance's own event sits behind it in the
            // subscription's single ordered channel. Up to 0.33.0 it stayed there for good.
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        @Test
        void blocks_them_exactly_as_before_when_the_quarantine_budget_is_switched_off() throws Exception {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            run(model, CONFIG.withQuarantineAfter(null));
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            TimeUnit.SECONDS.sleep(2);

            // The healthy instance's own event is still stuck behind the failing one, which is the bug #818 describes.
            assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY));
        }

        @Test
        void blocks_them_exactly_as_before_on_a_model_that_could_never_replay_the_event_it_stopped_on() throws Exception {
            // InMemorySubscriptionModel implements no RepositionableSubscriptions, so returning normally would
            // acknowledge an event nothing could ever hand back. The runner switches the budget off rather than
            // quarantine into that, which is ADR 134's ruling on a source that cannot replay.
            SagaSubscription subscription = run(subscriptionModel, CONFIG);
            write(POISON, new OrderPlaced("1", POISON));
            write(HEALTHY, new OrderPlaced("2", HEALTHY));
            write(POISON, new PaymentReserved("3", POISON));
            write(HEALTHY, new PaymentReserved("4", HEALTHY));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull(),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }

        @Test
        void blocks_them_exactly_as_before_on_a_wrapper_that_advertises_repositioning_it_cannot_honour() throws Exception {
            // A wrapper forwards the reposition to the model it wraps, so implementing RepositionableSubscriptions says
            // nothing about whether the reposition works. CatchupSubscriptionModel is the real case, implementing it
            // unconditionally and throwing UnsupportedOperationException when its delegate cannot reposition.
            SagaSubscription subscription = run(new ForwardingWrapper(subscriptionModel), CONFIG);
            write(POISON, new OrderPlaced("1", POISON));
            write(HEALTHY, new OrderPlaced("2", HEALTHY));
            write(POISON, new PaymentReserved("3", POISON));
            write(HEALTHY, new PaymentReserved("4", HEALTHY));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull(),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }

        @Test
        void are_isolated_from_it_on_a_wrapper_whose_delegate_can_really_be_repositioned() {
            ReplayableSubscriptionModel repositionable = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new ForwardingWrapper(repositionable), CONFIG);
            repositionable.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            repositionable.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            repositionable.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            repositionable.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }
    }

    @Nested
    class WhenItIsQuarantined {

        @Test
        void records_where_it_stopped_and_what_it_was_failing_with() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                SagaFailure failure = subscription.instances().find(POISON).orElseThrow().failure();
                assertAll(
                        () -> assertThat(failure).isNotNull(),
                        () -> assertThat(failure.failureType()).isEqualTo(IllegalStateException.class.getName()),
                        () -> assertThat(failure.failureMessage()).isEqualTo("this instance can never handle its payment"),
                        // The second event pushed onto this feed, so global position 2.
                        () -> assertThat(failure.position()).isEqualTo(2)
                );
            });
        }

        @Test
        void is_found_by_enumerating_the_quarantined_status_rather_than_the_active_one() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().findByStatus(SagaStatus.QUARANTINED, farFuture(), 10))
                            .extracting(SagaInstance::sagaId).containsExactly(POISON),
                    () -> assertThat(subscription.instances().findByStatus(SagaStatus.ACTIVE, farFuture(), 10)).isEmpty()
            ));
        }

        @Test
        void fires_none_of_its_own_timers_while_the_poller_keeps_firing_everyone_else_s() {
            // Comfortably longer than the quarantine budget, so the poison instance is already quarantined by the time
            // its own timer comes due, which is the state this test is about.
            paymentTimeout = Duration.ofMillis(800);
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(TICKING, 1, new OrderPlaced("3", TICKING)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            // The ticking instance's timeout proves the poller is alive and firing, so the quarantined instance's own
            // armed-and-overdue timer staying silent is the quarantine and not a stalled poller.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(dispatched).contains(new CancelOrder(TICKING)));
            assertThat(dispatched).doesNotContain(new CancelOrder(POISON));
        }
    }

    /**
     * Advertises {@link RepositionableSubscriptions} while forwarding the reposition to the model it wraps, which is
     * what {@code CatchupSubscriptionModel} does. Whether a reposition works is therefore a question about the
     * delegate rather than about this class.
     */
    private record ForwardingWrapper(SubscriptionModel delegate)
            implements Subscribable, SubscriptionModelWrapper, RepositionableSubscriptions {

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return RepositionableSubscriptions.findIn(delegate)
                    .orElseThrow(() -> new UnsupportedOperationException(delegate.getClass().getSimpleName() + " is not repositionable"))
                    .resumeSubscription(subscriptionId, startAt);
        }
    }

    private static Instant farFuture() {
        return Instant.now().plus(Duration.ofDays(1));
    }

    // The stream and position extensions an event store writes, added by hand because this feed is a plain list. Without
    // them the runner refuses the event outright, since it could not tell a redelivery from a new event.
    private CloudEvent cloudEvent(String streamId, long streamVersion, OrderEvent event) {
        CloudEvent withStream = CloudEventBuilder.v1(converter.toCloudEvent(event))
                .withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersion))
                .build();
        return OccurrentCloudEventExtension.withPosition(withStream, position.incrementAndGet());
    }

    private final java.util.concurrent.atomic.AtomicLong position = new java.util.concurrent.atomic.AtomicLong();

    /**
     * A subscription model that keeps every event it is handed and can be resumed at a chosen position, which is what
     * the runner requires before it turns quarantine on and which {@link InMemorySubscriptionModel} does not have.
     * Deliberately minimal, meaning one subscription, one delivery thread, and a retry loop that keeps re-offering an
     * event whose handler threw, which is what makes a time budget reachable at all.
     */
    private final class ReplayableSubscriptionModel implements SubscriptionModel, RepositionableSubscriptions {

        private final List<CloudEvent> log = new CopyOnWriteArrayList<>();
        private volatile @Nullable Consumer<CloudEvent> action;
        private volatile @Nullable String subscriptionId;
        private volatile boolean running;
        // The 0-based index of the next event to deliver, which is the 1-based position of the previous one.
        private volatile int nextIndex;


        void push(CloudEvent event) {
            log.add(event);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this.subscriptionId = subscriptionId;
            this.action = action;
            this.running = true;
            Thread deliverer = new Thread(this::deliver, "replayable-" + subscriptionId);
            deliverer.setDaemon(true);
            deliverer.start();
            return new ReplayableSubscription(subscriptionId);
        }

        private void deliver() {
            while (!Thread.currentThread().isInterrupted()) {
                Consumer<CloudEvent> current = action;
                if (!running || current == null || nextIndex >= log.size()) {
                    sleepBriefly();
                    continue;
                }
                try {
                    current.accept(log.get(nextIndex));
                    nextIndex++;
                } catch (RuntimeException e) {
                    // Left where it is, so the same event is offered again. That is what every transport this design
                    // works on does, and it is what lets a failure last long enough to reach the budget.
                    sleepBriefly();
                }
            }
        }

        private void sleepBriefly() {
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            nextIndex = (int) GlobalCheckpoint.positionOf(((StartAt.StartAtCheckpoint) startAt).checkpoint);
            running = true;
            return new ReplayableSubscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            running = true;
            return new ReplayableSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running = false;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running = false;
            action = null;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running && subscriptionId.equals(this.subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !isRunning(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        private record ReplayableSubscription(String id) implements Subscription {
            @Override
            public boolean waitUntilStarted(Duration timeout) {
                return true;
            }
        }
    }
}
