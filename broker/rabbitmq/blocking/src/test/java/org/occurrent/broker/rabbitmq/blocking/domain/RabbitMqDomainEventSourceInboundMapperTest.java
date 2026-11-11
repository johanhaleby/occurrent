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
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqInboundMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqMessage;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqOutboundMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTestSupport;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class RabbitMqDomainEventSourceInboundMapperTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
    private static final String ROUTING_KEY = "orders.placed";

    /**
     * Issue #1433 at the domain level. The handler reads the {@link EventMetadata} {@code @StreamId} and
     * {@code @StreamVersion} are resolved from, so this proves the extensions the mapper set reach it. The converter
     * resolves the {@code type} the mapper sets.
     */
    @Test
    void a_message_from_a_plain_publisher_reaches_the_handler_with_the_stream_id_and_version_a_custom_inbound_mapper_read_from_its_headers() throws Exception {
        String queue = declareAndBindQueue();
        List<RecordedEvent> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = feedWith(new RecordingView(handled));

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = RabbitMqDomainEventSource.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .inboundMapper(plainPublisherMapper())
                .build()) {
            publishPlainMessage("order-7", 3);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            RecordedEvent recorded = handled.get(0);
            assertThat(recorded.event()).isEqualTo(new TestOrderPlaced("order-7"));
            assertThat(recorded.streamId()).isEqualTo("order-7");
            assertThat(recorded.streamVersion()).isEqualTo(3L);
        }
        assertAcknowledged(queue);
    }

    @Test
    void a_mapper_that_throws_sends_the_message_to_the_parking_destination_and_acknowledges_the_original() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        List<RecordedEvent> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = feedWith(new RecordingView(handled));
        RabbitMqInboundMapper mapper = (properties, body) -> {
            throw new IllegalArgumentException("cannot map this message");
        };

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = RabbitMqDomainEventSource.builder(connection(), feed, queue)
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
            assertThat(parked.getBody()).isEqualTo("order-7".getBytes(StandardCharsets.UTF_8));
            assertThat(handled).isEmpty();
        }
        assertAcknowledged(queue);
    }

    @Test
    void a_mapper_that_returns_null_sends_the_message_to_the_parking_destination_and_acknowledges_the_original() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        List<RecordedEvent> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = feedWith(new RecordingView(handled));

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = RabbitMqDomainEventSource.builder(connection(), feed, queue)
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
            assertThat(parked.getBody()).isEqualTo("order-7".getBytes(StandardCharsets.UTF_8));
            assertThat(handled).isEmpty();
        }
        assertAcknowledged(queue);
    }

    @Test
    void a_source_with_no_inbound_mapper_still_sends_a_plain_message_to_the_failure_policy() throws Exception {
        String queue = declareAndBindQueue();
        String parkingQueue = declareParkingQueue();
        List<RecordedEvent> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = feedWith(new RecordingView(handled));

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = RabbitMqDomainEventSource.builder(connection(), feed, queue)
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
        List<RecordedEvent> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = feedWith(new RecordingView(handled));
        RabbitMqInboundMapper mapper = (properties, body) -> {
            if ("message-order-bad".equals(properties.getMessageId())) {
                failure.run();
            }
            return plainPublisherMapper().toCloudEvent(properties, body);
        };

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = RabbitMqDomainEventSource.builder(connection(), feed, queue)
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
            assertThat(handled.get(0).streamId()).isEqualTo("order-8");
        }
        assertAcknowledged(queue);
    }

    @Test
    void inboundMapper_refuses_null() {
        DomainEventFeed<TestOrderPlaced> feed = feedWith(new RecordingView(new CopyOnWriteArrayList<>()));
        RabbitMqDomainEventSource.Builder<TestOrderPlaced> builder = RabbitMqDomainEventSource.builder(connection(), feed, "queue");

        assertThatThrownBy(() -> builder.inboundMapper(null)).isInstanceOf(NullPointerException.class);
    }

    private static RabbitMqInboundMapper plainPublisherMapper() {
        return (properties, body) -> CloudEventBuilder.v1()
                .withId(properties.getMessageId())
                .withSource(URI.create("urn:plain-publisher"))
                .withType(TestOrderPlaced.class.getName())
                .withDataContentType("text/plain")
                .withData(body)
                .withExtension("streamid", String.valueOf(properties.getHeaders().get("streamId")))
                .withExtension("streamversion", ((Number) properties.getHeaders().get("streamVersion")).longValue())
                .build();
    }

    private static DomainEventFeed<TestOrderPlaced> feedWith(MaterializedView<TestOrderPlaced> view) {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");
        return feed;
    }

    private void publishDefaultMappableMessage(String orderId) throws Exception {
        CloudEvent event = new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced(orderId));
        RabbitMqMessage message = RabbitMqOutboundMapper.cloudEventHeaders().toMessage(event, RabbitMqDestination.of(exchange, ROUTING_KEY));
        assertThat(RabbitMqInboundMapper.cloudEventHeaders().toCloudEvent(message.properties(), message.body())).as("the default mapper accepts this message").isEqualTo(event);
        adminChannel.basicPublish(exchange, ROUTING_KEY, message.properties(), message.body());
    }

    private void publishPlainMessage(String streamId, int streamVersion) throws Exception {
        Map<String, Object> headers = new HashMap<>();
        headers.put("streamId", streamId);
        headers.put("streamVersion", streamVersion);
        BasicProperties properties = new BasicProperties.Builder()
                .contentType("text/plain")
                .messageId("message-" + streamId)
                .headers(headers)
                .build();
        adminChannel.basicPublish(exchange, ROUTING_KEY, properties, streamId.getBytes(StandardCharsets.UTF_8));
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

    private static final class InboundMapperTestSupport {
        @SuppressWarnings("unchecked")
        static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
            throw (T) throwable;
        }
    }

    private record TestOrderPlaced(String orderId) {
    }

    private record RecordedEvent(TestOrderPlaced event, String streamId, long streamVersion) {
    }

    private static final class RecordingView implements MaterializedView<TestOrderPlaced> {
        private final List<RecordedEvent> recorded;

        private RecordingView(List<RecordedEvent> recorded) {
            this.recorded = recorded;
        }

        @Override
        public void update(TestOrderPlaced event) {
            update(EventMetadata.empty(), event);
        }

        @Override
        public void update(EventMetadata metadata, TestOrderPlaced event) {
            recorded.add(new RecordedEvent(event, metadata.getStreamId(), metadata.getStreamVersion()));
        }
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
