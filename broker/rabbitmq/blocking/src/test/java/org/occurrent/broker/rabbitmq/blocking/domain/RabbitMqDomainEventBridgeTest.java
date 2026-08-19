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
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
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

            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(queue)).isEqualTo(1);
            assertThat(handled).isEmpty();

            feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));
            feed.goLive("proj");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
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
            // on the queue, never acknowledged, and never handled either (buffered, not folded).
            Thread.sleep(POLL_INTERVAL.toMillis() * 6);
            assertThat(queueMessageCount(queue)).isEqualTo(1);
            assertThat(handled).isEmpty();

            feed.goLive("proj");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(new TestOrderPlaced("order-1")));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
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
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
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
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }
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

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
        }
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
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());

            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getBody()).isEqualTo(malformedBody);
            assertThat(parked.getProps().getContentType()).isEqualTo("text/plain");
        }
    }

    /**
     * The permanent stop UnreadableLiveFilterException requires: a data payload filter with no DataFieldReader is
     * refused live, the bridge stops itself rather than redelivering into the same permanent failure, and, per
     * UnreadableLiveFilterException's own javadoc, the triggering delivery is never acknowledged and never
     * negatively acknowledged. Proven here by closing the bridge afterward (without ever calling ack/nack ourselves)
     * and observing the message come straight back to the queue, still there and still unconsumed.
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
