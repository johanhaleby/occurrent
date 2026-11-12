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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.NoOpMetricsCollector;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class RabbitMqCloudEventSourceInboundMapperTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
    private static final String ROUTING_KEY = "orders.placed";

    /**
     * Issue #1433: another publisher sends a JSON body with the stream id and version as application headers, one of
     * them a number, and no {@code cloudEvents_} headers. The mapper builds the event, so the subscriber gets it
     * with the {@code streamid} and {@code streamversion} extensions set.
     */
    @Test
    void a_message_from_a_plain_publisher_reaches_the_subscriber_with_the_stream_id_and_version_a_custom_inbound_mapper_read_from_its_headers() throws Exception {
        String queue = declareAndBindQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .inboundMapper(plainPublisherMapper())
                .build()) {
            publishPlainMessage("order-7", 3);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            CloudEvent delivered = handled.get(0);
            assertThat(delivered.getExtension("streamid")).isEqualTo("order-7");
            assertThat(delivered.getExtension("streamversion")).isEqualTo(3L);
            assertThat(delivered.getType()).isEqualTo(OrderPlaced.class.getName());
            assertThat(delivered.getData().toBytes()).isEqualTo("{\"orderId\":\"order-7\"}".getBytes(StandardCharsets.UTF_8));
        }
        assertAcknowledged(queue);
    }

    @Test
    void the_mapper_is_given_the_properties_and_the_body_of_the_delivered_message() throws Exception {
        String queue = declareAndBindQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);
        Map<String, Object> seen = new HashMap<>();
        RabbitMqInboundMapper mapper = (properties, body) -> {
            seen.put("messageId", properties.getMessageId());
            seen.put("body", new String(body, StandardCharsets.UTF_8));
            return plainPublisherMapper().toCloudEvent(properties, body);
        };

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .inboundMapper(mapper)
                .build()) {
            publishPlainMessage("order-7", 3);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }
        assertThat(seen).containsEntry("messageId", "message-order-7").containsEntry("body", "{\"orderId\":\"order-7\"}");
    }

    @Test
    void a_mapper_that_throws_sends_the_message_to_the_parking_destination_and_acknowledges_the_original() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);
        RabbitMqInboundMapper mapper = (properties, body) -> {
            throw new IllegalArgumentException("cannot map this message");
        };

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(exchange, "parked"))
                .inboundMapper(mapper)
                .build()) {
            publishDefaultMappableMessage("order-7");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));
            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getBody()).isEqualTo("{\"orderId\":\"order-7\"}".getBytes(StandardCharsets.UTF_8));
            assertThat(handled).isEmpty();
        }
        assertAcknowledged(queue);
    }

    @Test
    void a_mapper_that_returns_null_sends_the_message_to_the_parking_destination_and_acknowledges_the_original() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(exchange, "parked"))
                .inboundMapper((properties, body) -> null)
                .build()) {
            publishDefaultMappableMessage("order-7");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));
            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getBody()).isEqualTo("{\"orderId\":\"order-7\"}".getBytes(StandardCharsets.UTF_8));
            assertThat(handled).isEmpty();
        }
        assertAcknowledged(queue);
    }

    /**
     * Under {@link DeliveryFailurePolicy#REDELIVER} a mapper that fails is offered the same message again, so the
     * second attempt can succeed. The first attempt throws, the second maps, and the subscriber sees the event once.
     */
    @Test
    void a_mapper_that_throws_once_under_REDELIVER_gets_the_message_again_and_the_subscriber_sees_it_once() throws Exception {
        String queue = declareAndBindQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);
        AtomicInteger attempts = new AtomicInteger();
        RabbitMqInboundMapper mapper = (properties, body) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("first attempt fails");
            }
            return plainPublisherMapper().toCloudEvent(properties, body);
        };

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.REDELIVER)
                .inboundMapper(mapper)
                .build()) {
            publishPlainMessage("order-7", 3);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            assertThat(attempts).hasValue(2);
        }
        assertAcknowledged(queue);
    }

    @Test
    void a_source_with_no_inbound_mapper_still_refuses_a_plain_message_to_the_failure_policy() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(exchange, "parked"))
                .build()) {
            publishPlainMessage("order-7", 3);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));
            assertThat(handled).isEmpty();
        }
    }

    @Test
    void a_source_with_no_inbound_mapper_reads_what_the_default_outbound_mapper_wrote() throws Exception {
        String queue = declareAndBindQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);
        CloudEvent event = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .withDataContentType("application/json")
                .withData("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .withExtension("streamversion", 3L)
                .build();

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .build()) {
            RabbitMqMessage message = RabbitMqOutboundMapper.cloudEventHeaders().toMessage(event, RabbitMqDestination.of(exchange, ROUTING_KEY));
            adminChannel.basicPublish(exchange, ROUTING_KEY, message.properties(), message.body());

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).containsExactly(event));
        }
        assertAcknowledged(queue);
    }

    @Test
    void a_mapper_that_sneaky_throws_a_checked_exception_parks_the_message_acknowledges_the_original_and_the_source_keeps_consuming() throws Exception {
        assertSourceKeepsConsumingAfterMapperFailure(() -> InboundMapperTestSupport.<RuntimeException>sneakyThrow(new IOException("cannot parse the body")));
    }

    @Test
    void a_mapper_that_throws_an_AssertionError_parks_the_message_acknowledges_the_original_and_the_source_keeps_consuming() throws Exception {
        assertSourceKeepsConsumingAfterMapperFailure(() -> {
            throw new AssertionError("mapper invariant violated");
        });
    }

    private void assertSourceKeepsConsumingAfterMapperFailure(Runnable failure) throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);
        RabbitMqInboundMapper mapper = (properties, body) -> {
            if ("message-order-bad".equals(properties.getMessageId())) {
                failure.run();
            }
            return plainPublisherMapper().toCloudEvent(properties, body);
        };

        try (RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(exchange, "parked"))
                .inboundMapper(mapper)
                .build()) {
            publishPlainMessage("order-bad", 1);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));

            publishPlainMessage("order-8", 2);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            assertThat(handled.get(0).getExtension("streamid")).isEqualTo("order-8");
        }
        assertAcknowledged(queue);
    }

    /**
     * Only close() interrupts the worker thread, and a mapper that blocks and is interrupted throws an
     * InterruptedException after the thread's interrupt flag is cleared. The source puts the flag back before its
     * failure policy runs. The park publish is the first thing the policy does, so the connection's MetricsCollector
     * sees the flag on the worker thread there. Whether the publish confirm then notices the interrupt depends on
     * whether the confirm had already arrived, so the test does not assert what happens to the original delivery.
     * The parked copy is written before the confirm wait either way, so the test does check that it reaches the
     * parking queue.
     */
    @Test
    void a_mapper_that_sneaky_throws_an_InterruptedException_leaves_the_worker_interrupted_when_the_failure_policy_runs() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> {
        });
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<Thread> mapperThread = new AtomicReference<>();
        RabbitMqInboundMapper mapper = (properties, body) -> {
            if (attempts.incrementAndGet() == 1) {
                mapperThread.set(Thread.currentThread());
                InboundMapperTestSupport.<RuntimeException>sneakyThrow(new InterruptedException("interrupted while mapping"));
            }
            return plainPublisherMapper().toCloudEvent(properties, body);
        };
        PublishRecorder publishes = new PublishRecorder();
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(amqpUrl());
        connectionFactory.setMetricsCollector(publishes);

        try (Connection sourceConnection = connectionFactory.newConnection();
             RabbitMqCloudEventSource source = RabbitMqCloudEventSource.builder(sourceConnection, model, queue)
                     .declareTopology(false)
                     .pollInterval(POLL_INTERVAL)
                     .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                     .parkingDestination(RabbitMqDestination.of(exchange, "parked"))
                     .inboundMapper(mapper)
                     .build()) {
            publishPlainMessage("order-7", 3);

            await().atMost(Duration.ofSeconds(5)).until(() -> !publishes.publishes.isEmpty());
            PublishRecorder.Publish park = publishes.publishes.get(0);
            assertThat(park.thread()).isSameAs(mapperThread.get());
            assertThat(park.interrupted()).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(parkingQueue)).isEqualTo(1));
            GetResponse parked = adminChannel.basicGet(parkingQueue, true);
            assertThat(parked).isNotNull();
            assertThat(parked.getBody()).isEqualTo("{\"orderId\":\"order-7\"}".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void inboundMapper_refuses_null() {
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        RabbitMqCloudEventSource.Builder builder = RabbitMqCloudEventSource.builder(connection(), model, "queue");

        assertThatThrownBy(() -> builder.inboundMapper(null)).isInstanceOf(NullPointerException.class);
    }

    /**
     * Builds the event from a JSON body and the {@code streamId} and {@code streamVersion} application headers of a
     * publisher that knows nothing about CloudEvents. The version arrives as an {@link Integer}, since that is what
     * the AMQP client hands back for a number header.
     */
    static RabbitMqInboundMapper plainPublisherMapper() {
        return (properties, body) -> CloudEventBuilder.v1()
                .withId(properties.getMessageId())
                .withSource(URI.create("urn:plain-publisher"))
                .withType(OrderPlaced.class.getName())
                .withDataContentType("application/json")
                .withData(body)
                .withExtension("streamid", String.valueOf(properties.getHeaders().get("streamId")))
                .withExtension("streamversion", ((Number) properties.getHeaders().get("streamVersion")).longValue())
                .build();
    }

    private void publishDefaultMappableMessage(String streamId) throws Exception {
        CloudEvent event = CloudEventBuilder.v1()
                .withId("id-" + streamId)
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .withDataContentType("application/json")
                .withData(("{\"orderId\":\"" + streamId + "\"}").getBytes(StandardCharsets.UTF_8))
                .build();
        RabbitMqMessage message = RabbitMqOutboundMapper.cloudEventHeaders().toMessage(event, RabbitMqDestination.of(exchange, ROUTING_KEY));
        assertThat(RabbitMqInboundMapper.cloudEventHeaders().toCloudEvent(message.properties(), message.body())).as("the default mapper accepts this message").isEqualTo(event);
        adminChannel.basicPublish(exchange, ROUTING_KEY, message.properties(), message.body());
    }

    private void publishPlainMessage(String streamId, int streamVersion) throws Exception {
        Map<String, Object> headers = new HashMap<>();
        headers.put("streamId", streamId);
        headers.put("streamVersion", streamVersion);
        BasicProperties properties = new BasicProperties.Builder()
                .contentType("application/json")
                .messageId("message-" + streamId)
                .headers(headers)
                .build();
        adminChannel.basicPublish(exchange, ROUTING_KEY, properties, ("{\"orderId\":\"" + streamId + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    private String declareAndBindQueue() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, ROUTING_KEY);
        return queue;
    }

    private String declareParkingQueue() throws Exception {
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
        return parkingQueue;
    }

    private long queueMessageCount(String queue) throws Exception {
        return adminChannel.queueDeclarePassive(queue).getMessageCount();
    }

    private void assertAcknowledged(String queue) {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    private static final class OrderPlaced {
    }

    private static final class InboundMapperTestSupport {
        @SuppressWarnings("unchecked")
        static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
            throw (T) throwable;
        }
    }

    /**
     * Records, for each publish on the connection, the thread that made it and whether that thread was interrupted.
     */
    private static final class PublishRecorder extends NoOpMetricsCollector {

        private final List<Publish> publishes = new CopyOnWriteArrayList<>();

        @Override
        public void basicPublish(Channel channel) {
            publishes.add(new Publish(Thread.currentThread(), Thread.currentThread().isInterrupted()));
        }

        private record Publish(Thread thread, boolean interrupted) {
        }
    }
}
