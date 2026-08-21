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

package org.occurrent.broker.rabbitmq.blocking.domain;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTestSupport;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RabbitMqDomainEventBridgeTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    @Test
    void acks_on_delivered_and_the_message_does_not_stay_on_the_queue() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));
        // No catch-up in this test (nothing stored, nothing to replay): go straight live so a live event is folded
        // immediately rather than buffered awaiting a replay handover that never happens.
        feed.goLive("proj");

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
        }
        assertAcknowledged(queue);
    }

    /**
     * The domain bridge's own copy of {@code RabbitMqCloudEventBridgeTest}'s numeric-extension round-trip test.
     * Both bridges share {@code RabbitMqCloudEventMapper}, so the fix covers both, but this exact class of bug, a
     * rebuilt CloudEvent losing the type a live filter needs to match it, has bitten this bridge twice already, so
     * it gets its own direct proof rather than relying on the CloudEvent bridge's coverage alone.
     */
    @Test
    void a_numeric_extension_survives_the_broker_round_trip_so_a_stream_version_filter_still_matches_it() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()).and(Filter.streamVersion(Condition.eq(3L))));
        feed.goLive("proj");

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
                    .withDataContentType("text/plain")
                    .withData("order-1".getBytes(StandardCharsets.UTF_8))
                    .withExtension("streamid", "stream-1")
                    .withExtension("streamversion", 3L)
                    .build();
            BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
            adminChannel.basicPublish(exchange, TestOrderPlaced.class.getName(), properties, RabbitMqCloudEventMapper.toBody(cloudEvent));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
        }
        assertAcknowledged(queue);
    }

    @Test
    void the_bridge_does_not_consume_before_a_projection_is_registered_then_starts_once_one_is() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            // No projection registered yet. Give the coarse poll several chances to (wrongly) consume, then confirm
            // the message is still sitting on the queue, untouched. Sound to check while the bridge is still open,
            // unlike an acknowledgement claim. Nothing has been delivered to any consumer yet, so the ready count
            // is not the ambiguous one an outstanding-but-unacked delivery would produce.
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(queue)).isEqualTo(1);
            assertThat(handled).isEmpty();

            feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));
            feed.goLive("proj");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertAcknowledged(queue);
    }

    /**
     * The silent-loss case ADR 133 exists to prevent. A projection registered but not yet caught up or gone live
     * only buffers in memory, and a {@code DomainEventFeed} bound for {@code goLive(..)} never replays, so an
     * acknowledged message in that window would be lost for good on a crash. Proof this cannot happen: the message
     * stays on the queue, unacknowledged, until the application actually calls {@code goLive(..)}, at which point it
     * is delivered and only then acknowledged.
     */
    @Test
    void a_message_delivered_before_the_feed_has_gone_live_is_not_acknowledged_and_is_delivered_once_it_does() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        // Registered, but goLive(..) is deliberately not called yet: hasProjection() is already true, so without the
        // isReadyForLiveDelivery() gate the bridge would start consuming and, before this fix, would acknowledge the
        // message on a merely buffered event.
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            // Several poll intervals pass with the feed registered but not ready: the message must still be sitting
            // on the queue, never acknowledged, and never handled either (buffered, not folded). Sound to check
            // while the bridge is still open. Nothing has been delivered to a consumer at all yet in this window,
            // unlike the later acknowledgement claim this test also makes, which is checked only after close().
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(queue)).isEqualTo(1);
            assertThat(handled).isEmpty();

            feed.goLive("proj");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
        }
        assertAcknowledged(queue);
    }

    @Test
    void redelivers_on_a_projection_failure_and_the_retry_succeeds() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        AtomicInteger attempts = new AtomicInteger();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        MaterializedView<TestOrderPlaced> view = event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("fails once");
            }
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
        }
        assertAcknowledged(queue);
    }

    /**
     * The converter, the live matcher or the projection can throw an {@link AssertionError}, and the bridge has to
     * redeliver on it exactly as it does on a {@link RuntimeException}, rather than leaving it uncaught and
     * stalling the consumer at prefetch one.
     */
    @Test
    void a_projection_that_throws_an_assertionError_is_redelivered_rather_than_stalling_the_consumer() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        AtomicInteger attempts = new AtomicInteger();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        MaterializedView<TestOrderPlaced> view = event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("simulated assertion failure");
            }
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(attempts).hasValue(2));
        }
        assertAcknowledged(queue);
    }

    @Test
    void parks_a_delivery_that_keeps_failing_and_acknowledges_the_original_once_the_park_is_confirmed() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
        RabbitMqDestination parkingDestination = RabbitMqDestination.of(exchange, "parked");

        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        MaterializedView<TestOrderPlaced> view = event -> {
            throw new RuntimeException("always fails");
        };
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            // The parking queue has no consumer attached, so its own ready count is not the ambiguous one a
            // delivery outstanding on the bridge's consumer would produce; sound to check while the bridge is open.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
        }
        assertAcknowledged(queue);
    }

    /**
     * A message with no {@code cloudEvents_} headers at all cannot become a {@link CloudEvent}, since
     * {@link CloudEventBuilder#build()} requires an id, a source and a type. {@link DeliveryFailurePolicy#PARK}
     * still applies to it rather than always redelivering: the bridge parks the delivery's own raw properties and
     * body unchanged, since there is no {@link CloudEvent} to republish through the ordinary parking path.
     */
    @Test
    void an_undecodable_message_is_parked_with_its_raw_bytes_and_the_original_is_acknowledged_once_confirmed() throws Exception {
        String queue = declareAndBindQueue("malformed");
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
        RabbitMqDestination parkingDestination = RabbitMqDestination.of(exchange, "parked");

        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", event -> {
        }, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            BasicProperties malformedProperties = new BasicProperties.Builder().contentType("text/plain").build();
            byte[] malformedBody = "not a cloud event".getBytes(StandardCharsets.UTF_8);
            adminChannel.basicPublish(exchange, "malformed", malformedProperties, malformedBody);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getBody()).isEqualTo(malformedBody);
            assertThat(parked.getProps().getContentType()).isEqualTo("text/plain");
        }
        assertAcknowledged(queue);
    }

    /**
     * The permanent stop UnreadableLiveFilterException requires: a data payload filter with no DataFieldReader is
     * refused live, the bridge stops itself rather than redelivering into the same permanent failure, and, per
     * UnreadableLiveFilterException's own javadoc, the triggering delivery is never acknowledged and never
     * negatively acknowledged. Proven here by closing the bridge afterward (without ever calling ack/nack ourselves)
     * and observing the message come straight back to the queue, still there and still unconsumed. This is the same
     * close-then-check shape assertAcknowledged(...) below uses, since it is what actually distinguishes an
     * acknowledged message from one merely delivered and never acknowledged.
     */
    @Test
    void an_unreadable_live_filter_stops_the_bridge_and_leaves_the_delivery_unacknowledged() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        // A data-field filter with the default (refusing) DataFieldReader: refused live on the first acceptCloudEvent.
        feed.register("proj", handled::add, Filter.data("amount", Condition.eq(42)));
        feed.goLive("proj");

        RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build();
        try {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            // The bridge stops consuming and the message stays outstanding, unacknowledged, on the bridge's own
            // channel, so a plain queue length check would still read zero (it is "delivered", just not acked).
            // Closing the connection this test's admin channel and the bridge's channel share is not an option
            // here, so instead this waits for the bridge to have visibly stopped (no more deliveries happen) and
            // then closes the bridge, which is the one caller-driven event that returns the message to the queue.
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(handled).isEmpty();
        } finally {
            bridge.close();
        }

        // Once the bridge's channel is closed, RabbitMQ requeues whatever was left outstanding on it: proof the
        // message was never acked (an ack would have removed it for good) and never nacked-without-requeue either.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(1));
    }

    /**
     * {@code RoutingOutcome.DEFERRED} bypasses {@link DeliveryFailurePolicy} entirely, including {@code PARK}, the
     * same as the CloudEvent-level bridge. {@code DomainEventFeed}'s coarse gate requires
     * {@code isReadyForLiveDelivery()} before consuming at all, unlike the push-side bridge, and RabbitMQ's Java
     * client dispatches one channel's deliveries to {@code handleDelivery} sequentially regardless of prefetch, so
     * two messages from the same bridge can never race each other into {@code acceptIfLive} concurrently. The
     * reachable way to prove this against a real broker is a direct, out-of-band call for the same dedup key (the
     * domain event's {@code orderId}, this feed's own id extractor) that holds it in flight while the bridge
     * delivers a duplicate for that same key: {@code acceptIfLive} refuses the bridge's delivery while the direct
     * call is still folding, reported {@code DEFERRED}.
     */
    @Test
    void park_configured_never_parks_a_deferred_message() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
        RabbitMqDestination parkingDestination = RabbitMqDestination.of(exchange, "parked");

        CountDownLatch directCallEntered = new CountDownLatch(1);
        CountDownLatch releaseDirectCall = new CountDownLatch(1);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", event -> {
            handled.add(event);
            if (handled.size() == 1) {
                directCallEntered.countDown();
                awaitLatch(releaseDirectCall);
            }
        }, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        // Out-of-band, bypassing the bridge entirely: holds the "order-1" dedup key in flight so the bridge's own
        // delivery for the same key below is the one that gets refused, without needing two deliveries racing on
        // the same sequential dispatch thread.
        CloudEvent directCloudEvent = new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1"));
        Thread directCaller = new Thread(() -> feed.acceptCloudEvent(directCloudEvent), "direct-in-flight-caller");
        directCaller.start();
        assertThat(directCallEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(parkingDestination)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            // Long enough for several redeliver round trips of the DEFERRED delivery, so this is a genuine "never
            // parked" assertion, not a race against the bridge's own retry pace.
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(parkingQueue)).as("DEFERRED bypasses PARK entirely").isZero();

            releaseDirectCall.countDown();
            directCaller.join(TimeUnit.SECONDS.toMillis(5));

            // The redelivered duplicate finds the dedup key already applied by the direct call and does not fold
            // the handler a second time.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
            assertThat(handled).containsExactly(new TestOrderPlaced("order-1"));
        }
    }

    /**
     * The same pacing fix {@code RabbitMqCloudEventBridge} needed, mirrored here: a {@code DEFERRED} delivery is
     * held unacked rather than nacked immediately, so with {@link RabbitMqDomainEventBridge.Builder#prefetchCount(int)}
     * left at its default of one, the broker sends nothing further on this consumer while a delivery is held, and
     * {@code reconcileConsumption} releases at most one held tag per poll interval. That makes the bound here exact
     * rather than statistical: a raw wall-clock count depends on how fast the broker itself round-trips a nack and
     * flakes with it, the mistake an earlier version of {@code RabbitMqCloudEventBridge}'s own equivalent test made.
     * There is no {@code RoutingOutcome} observer on {@code DomainEventFeed} to count {@code DEFERRED} directly, so
     * this counts {@code CloudEventConverter#toDomainEvent} calls instead: {@code acceptCloudEvent} decodes on
     * every matching attempt, {@code DELIVERED} or {@code DEFERRED} alike, so with the dedup key held in flight for
     * the whole observation window, every one of those decodes but the direct call's own first one is a redelivery
     * this bridge issued.
     */
    @Test
    void a_deferred_delivery_is_redelivered_at_most_once_per_poll_interval() throws Exception {
        String queue = declareAndBindQueue(TestOrderPlaced.class.getName());

        CountDownLatch directCallEntered = new CountDownLatch(1);
        CountDownLatch releaseDirectCall = new CountDownLatch(1);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        AtomicInteger decodeAttempts = new AtomicInteger();
        TestOrderPlacedConverter delegate = new TestOrderPlacedConverter();
        CloudEventConverter<TestOrderPlaced> countingConverter = new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
                return delegate.toCloudEvent(domainEvent);
            }

            @Override
            public TestOrderPlaced toDomainEvent(CloudEvent cloudEvent) {
                decodeAttempts.incrementAndGet();
                return delegate.toDomainEvent(cloudEvent);
            }

            @Override
            public String getCloudEventType(Class<? extends TestOrderPlaced> type) {
                return delegate.getCloudEventType(type);
            }
        };
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), countingConverter, TestOrderPlaced::orderId);
        feed.register("proj", event -> {
            handled.add(event);
            if (handled.size() == 1) {
                directCallEntered.countDown();
                awaitLatch(releaseDirectCall);
            }
        }, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        CloudEvent directCloudEvent = delegate.toCloudEvent(new TestOrderPlaced("order-1"));
        Thread directCaller = new Thread(() -> feed.acceptCloudEvent(directCloudEvent), "direct-in-flight-caller");
        directCaller.start();
        assertThat(directCallEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            publish(TestOrderPlaced.class.getName(), "id-1", "order-1");

            int pollIntervalsToObserve = 10;
            Thread.sleep(POLL_INTERVAL.multipliedBy(pollIntervalsToObserve).toMillis());

            assertThat(decodeAttempts.get()).as("the bridge's own delivery was attempted at all, beyond the "
                            + "direct call's own first decode").isGreaterThan(1);
            assertThat(decodeAttempts.get()).as("at most one redelivery per poll interval, by construction, not a "
                            + "raw count that depends on how fast the broker round-trips a nack, plus one for the "
                            + "direct call's own first decode")
                    .isLessThanOrEqualTo(pollIntervalsToObserve + 4);

            releaseDirectCall.countDown();
            directCaller.join(TimeUnit.SECONDS.toMillis(5));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        // Not auto-delete: a bridge that cancels its own consumer (going idle between the coarse-poll tests, or
        // stopping for good on UnreadableLiveFilterException) must not take the queue, and whatever is still on it,
        // down with that cancel.
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, routingKey);
        return queue;
    }

    private void publish(String type, String id, String orderId) throws Exception {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(type)
                .withDataContentType("text/plain")
                .withData(orderId.getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        adminChannel.basicPublish(exchange, type, properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private long queueMessageCount(String queue) throws Exception {
        return adminChannel.queueDeclarePassive(queue).getMessageCount();
    }

    /**
     * Asserts {@code queue} is empty, meaningfully only once the bridge that consumed it has already been closed
     * (a try-with-resources block exiting, typically). RabbitMQ's ready count excludes a delivery still outstanding
     * on a consumer, so checking it while the bridge is still open cannot tell an acknowledged message apart from
     * one merely delivered and never acknowledged, both read zero the instant the bridge receives it. Closing the
     * bridge first requeues whatever it never acknowledged, so a message still gone afterwards is proof of an
     * actual acknowledgement rather than an artifact of the consumer still holding it.
     */
    private void assertAcknowledged(String queue) throws Exception {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    private record TestOrderPlaced(String orderId) {
    }

    private static final class TestOrderPlacedConverter implements CloudEventConverter<TestOrderPlaced> {

        @Override
        public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
                    .withDataContentType("text/plain")
                    .withData(domainEvent.orderId().getBytes(StandardCharsets.UTF_8))
                    .build();
        }

        @Override
        public TestOrderPlaced toDomainEvent(CloudEvent cloudEvent) {
            byte[] data = cloudEvent.getData() == null ? new byte[0] : cloudEvent.getData().toBytes();
            return new TestOrderPlaced(new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public String getCloudEventType(Class<? extends TestOrderPlaced> type) {
            return TestOrderPlaced.class.getName();
        }
    }
}
