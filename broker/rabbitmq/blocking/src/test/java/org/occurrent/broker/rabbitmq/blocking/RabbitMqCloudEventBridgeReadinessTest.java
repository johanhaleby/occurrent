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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@link PushSubscriptionModel#acceptRedeliverable(CloudEvent)} reporting {@link RoutingOutcome#DEFERRED} is what
 * closes the catch-up acknowledgement hole on its own, with no {@code readinessSource} configured at all.
 * {@link RabbitMqCloudEventBridge.Builder#readinessSource(java.util.function.Predicate)} is a pacing hint on top of
 * that, proven here against a real broker rather than against {@code CatchupThenPushSubscriptionModel} itself,
 * which {@code CatchupThenPushSubscriptionModelTest} already covers for {@code isReadyForLiveDelivery(String)} in
 * isolation. A {@code readinessSource} answering {@code false} keeps this bridge from pulling a matching message
 * off the queue at all, cutting down on how often the refuse-and-redeliver round trip below happens, but is never
 * what makes that round trip safe. Also covers this bridge's own bounded pacing, which keeps a redelivered
 * {@code DEFERRED} message from driving it into a tight loop even with no {@code readinessSource} configured at
 * all.
 */
class RabbitMqCloudEventBridgeReadinessTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Test
    void a_readiness_source_answering_false_keeps_a_matching_message_on_the_queue_and_never_invokes_the_handler() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .readinessSource(subscriptionId -> false)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            // Long enough for several poll intervals to pass, so this is a genuine "never consumed" assertion, not
            // a race against the bridge's own poll cadence.
            Thread.sleep(500);

            assertThat(handled).isEmpty();
            assertThat(queueMessageCount(queue)).isEqualTo(1);
        }
    }

    @Test
    void flipping_readiness_to_true_drains_and_acknowledges_a_message_that_arrived_while_not_ready() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        AtomicBoolean ready = new AtomicBoolean(false);
        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .readinessSource(subscriptionId -> ready.get())
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");
            Thread.sleep(200);
            assertThat(handled).isEmpty();

            ready.set(true);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
    }

    @Test
    void no_readiness_source_configured_consumes_immediately_the_same_as_before_this_capability_existed() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> handled.add(cloudEvent));

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
    }

    /**
     * The dec-0009 falsifying race, closed by construction this time rather than by narrowing a timing window: a
     * {@link CatchupThenPushSubscriptionModel} still replaying refuses a live delivery outright
     * ({@code RoutingOutcome.DEFERRED}) instead of buffering it and reporting {@code DELIVERED} ahead of the fold.
     * No {@code readinessSource} is configured here at all, proving {@code DEFERRED} alone, not the pacing hint, is
     * what keeps this bridge correct.
     */
    @Test
    void a_message_that_arrives_during_a_catch_up_replay_is_never_acknowledged_and_is_delivered_once_live_with_no_readiness_source_configured() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("historical", OrderPlaced.class.getName())));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        List<String> folded = new CopyOnWriteArrayList<>();
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            folded.add(ce.getId());
            if (ce.getId().equals("historical")) {
                replayEntered.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).as("the replay reached its one historical event").isTrue();

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            // Long enough for many refuse-and-redeliver round trips against DEFERRED, so this is a genuine
            // "never folded" assertion, not a race against the bridge's own retry pace. The queue's own message
            // count is not asserted here: a tight refuse-and-requeue loop leaves the message alternating between
            // ready and momentarily unacked, which queueMessageCount(..) cannot sample reliably.
            Thread.sleep(500);

            assertThat(folded).as("refused, never buffered or acknowledged, while the replay is still blocked").doesNotContain("id-1");

            releaseReplay.countDown();

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(folded).contains("id-1"));
        }
        assertAcknowledged(queue);
    }

    /**
     * {@code RoutingOutcome.DEFERRED} bypasses {@link DeliveryFailurePolicy} entirely, including {@code PARK}: a
     * message that only needs the replay to catch up must never be republished to a parking destination, since
     * nothing about it is broken.
     */
    @Test
    void park_configured_never_parks_a_deferred_message() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        String parkingQueue = "parking-queue-" + UUID.randomUUID();
        String parkingExchange = "parking-exchange-" + UUID.randomUUID();
        adminChannel.exchangeDeclare(parkingExchange, "topic", false, true, null);
        adminChannel.queueDeclare(parkingQueue, false, false, false, null);
        adminChannel.queueBind(parkingQueue, parkingExchange, "#");

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("historical", OrderPlaced.class.getName())));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            if (ce.getId().equals("historical")) {
                replayEntered.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(parkingExchange, "parked"))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            Thread.sleep(500);

            assertThat(queueMessageCount(parkingQueue)).as("DEFERRED bypasses PARK entirely").isZero();
        } finally {
            releaseReplay.countDown();
        }
    }

    /**
     * The pacing a Copilot review of this PR asked for: with no {@code readinessSource} configured, every
     * {@code DEFERRED} delivery used to be immediately nacked and requeued with nothing pacing the resulting loop,
     * which a real broker turns into thousands of refuse-and-redeliver round trips over a long-running replay. Two
     * earlier fixes both still flaked on CI: an outright poll-interval self-cancel only halved the churn rather
     * than bounding it (a real broker's round trip is fast enough to pack dozens of nack-and-redeliver cycles into
     * the single poll interval before that cancel takes effect), and asserting a quiet streak against that same
     * mechanism just moved the same round-trip-speed dependency to a different number. Holding the {@code DEFERRED}
     * delivery unacked instead of nacking it, released by the poll thread rather than by the delivery callback,
     * removes the dependency outright: with {@link RabbitMqCloudEventBridge.Builder#prefetchCount(int)} left at its
     * default of one, the broker sends nothing further on this consumer while a delivery is held, so exactly one
     * redelivery happens per poll interval by construction, not "however many fit." That makes the bound here
     * exact rather than statistical: {@code deferredCount} after {@code N} poll intervals is at most {@code N} plus
     * a small constant for the delivery already in flight when the window started and the scheduler's own first
     * tick, never a number that depends on how fast the broker itself round-trips a nack.
     */
    @Test
    void a_deferred_delivery_with_no_readiness_source_is_redelivered_at_most_once_per_poll_interval() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        AtomicInteger deferredCount = new AtomicInteger();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel((cloudEvent, outcome) -> {
            if (outcome == RoutingOutcome.DEFERRED) {
                deferredCount.incrementAndGet();
            }
        });
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("historical", OrderPlaced.class.getName())));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            if (ce.getId().equals("historical")) {
                replayEntered.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            int pollIntervalsToObserve = 10;
            Thread.sleep(POLL_INTERVAL.multipliedBy(pollIntervalsToObserve).toMillis());

            // The mechanism engaged at all, so this is not vacuously true against a bridge that, say, never
            // consumes the message in the first place.
            assertThat(deferredCount.get()).as("at least one DEFERRED redelivery happened").isGreaterThan(0);

            // The exact structural bound: with prefetchCount left at its default of one, the broker cannot deliver
            // a second message on this consumer while the first is still held, so at most one redelivery happens
            // per reconcileConsumption pass. Over pollIntervalsToObserve poll intervals of wall-clock time that
            // bounds the count at pollIntervalsToObserve plus a small constant, room for the scheduler's own first
            // tick (which runs immediately, before the first poll interval has elapsed) and the delivery already
            // in flight when this window started, never a number tied to how fast the broker round-trips a nack.
            assertThat(deferredCount.get()).as("at most one redelivery per poll interval, by construction, not a "
                            + "raw count that depends on how fast the broker round-trips a nack")
                    .isLessThanOrEqualTo(pollIntervalsToObserve + 3);
        } finally {
            releaseReplay.countDown();
        }
    }

    /**
     * A {@code DEFERRED} delivery this bridge is holding unacked when {@code close()} runs must not hang it and
     * must not be lost. {@code close()} releases (nacks with requeue) any held tag itself, and even without that,
     * a channel closed with an unacked delivery on it is requeued by the broker on its own, so this covers both:
     * that {@code close()} returns promptly, and that the message is still on the queue afterwards.
     */
    @Test
    void close_while_a_deferred_delivery_is_held_completes_promptly_and_does_not_lose_the_message() throws Exception {
        String queue = declareAndBindQueue(OrderPlaced.class.getName());
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel((cloudEvent, outcome) -> {
        });
        PushSubscriptionModel liveFeed = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        InMemoryEventStore store = new InMemoryEventStore();
        store.write("s1", List.of(cloudEvent("historical", OrderPlaced.class.getName())));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, liveFeed, null);

        CountDownLatch replayEntered = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> {
            if (ce.getId().equals("historical")) {
                replayEntered.countDown();
                awaitLatch(releaseReplay);
            }
        });
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection(), liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build();
        try {
            publish(OrderPlaced.class.getName(), "id-1");
            // Long enough that the bridge has fetched the message and is holding it unacked (DEFERRED, the replay
            // still parked) by the time close() runs on top of it.
            Thread.sleep(POLL_INTERVAL.toMillis() * 2);

            long start = System.nanoTime();
            bridge.close();
            long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();
            assertThat(elapsedMs).as("close() must not hang on a held DEFERRED delivery").isLessThan(5000);
        } finally {
            releaseReplay.countDown();
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(1));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static CloudEvent cloudEvent(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:occurrent:test"))
                .withType(type)
                .withExtension("streamid", "s1")
                .build();
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, routingKey);
        return queue;
    }

    private void publish(String type, String id) throws Exception {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(type)
                .withExtension("streamid", "stream-1")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        adminChannel.basicPublish(exchange, type, properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private long queueMessageCount(String queue) throws Exception {
        return adminChannel.queueDeclarePassive(queue).getMessageCount();
    }

    private void assertAcknowledged(String queue) throws Exception {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    private static final class OrderPlaced {
    }
}
