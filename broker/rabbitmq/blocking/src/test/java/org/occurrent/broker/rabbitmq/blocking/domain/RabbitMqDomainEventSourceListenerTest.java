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
import com.rabbitmq.client.Channel;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.ParkingFailureActionWithMockedPublisher;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqInboundMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqMessage;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqOutboundMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceListener;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceListenerCalls;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceStatus;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTestSupport;
import org.occurrent.broker.rabbitmq.blocking.RecordingRabbitMqSourceListener;
import org.occurrent.broker.rabbitmq.blocking.ThrowingRabbitMqSourceListener;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.RoutingOutcome;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * What {@link RabbitMqDomainEventSource} tells its {@link RabbitMqSourceListener} for each delivery, against a real
 * broker. The twin of {@code RabbitMqCloudEventSourceListenerTest}, with the same cases, since the two sources
 * report through the same code.
 */
class RabbitMqDomainEventSourceListenerTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
    private static final String ROUTING_KEY = "orders.placed";

    @Test
    void a_consumed_event_tells_the_listener_delivered_for_the_queue() throws Exception {
        String queue = declareAndBindQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(handled::add), queue, listener).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.calls()).hasSize(1));
        }

        assertThat(handled).hasSize(1);
        assertThat(listener.calls()).containsExactly(new RecordingRabbitMqSourceListener.Call("delivered", queue, null));
    }

    @Test
    void a_filtered_out_event_tells_the_listener_delivered_since_the_source_acknowledged_it() throws Exception {
        String queue = declareAndBindQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(handled::add), queue, listener).build()) {
            publishMessage(CloudEventBuilder.v1().withId("id-1").withSource(URI.create("urn:test")).withType("com.acme.SomethingElse").build());

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.calls()).hasSize(1));
        }

        assertThat(handled).isEmpty();
        assertThat(listener.calls()).containsExactly(new RecordingRabbitMqSourceListener.Call("delivered", queue, null));
    }

    @Test
    void a_handler_that_throws_tells_the_listener_failed_with_that_exception_and_then_parked_under_PARK() throws Exception {
        String queue = declareAndBindQueue();
        declareParkingQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        IllegalStateException failure = new IllegalStateException("handler failed");

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(event -> {
            throw failure;
        }), queue, listener).onDeliveryFailure(DeliveryFailurePolicy.PARK).parkingDestination(RabbitMqDestination.of(exchange, "parked")).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.callbacks()).contains("parked"));
        }

        assertThat(listener.calls()).containsExactly(
                new RecordingRabbitMqSourceListener.Call("failed", queue, failure),
                new RecordingRabbitMqSourceListener.Call("parked", queue, null));
    }

    @Test
    void an_inbound_mapper_that_throws_tells_the_listener_failed_with_that_exception_and_then_parked_under_PARK() throws Exception {
        String queue = declareAndBindQueue();
        declareParkingQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        IllegalArgumentException failure = new IllegalArgumentException("cannot map this message");
        RabbitMqInboundMapper mapper = (properties, body) -> {
            throw failure;
        };

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(event -> { }), queue, listener).inboundMapper(mapper)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK).parkingDestination(RabbitMqDestination.of(exchange, "parked")).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.callbacks()).contains("parked"));
        }

        assertThat(listener.calls()).containsExactly(
                new RecordingRabbitMqSourceListener.Call("failed", queue, failure),
                new RecordingRabbitMqSourceListener.Call("parked", queue, null));
    }

    @Test
    void a_failure_under_REDELIVER_tells_the_listener_failed_then_redelivered_and_the_redelivery_is_delivered() throws Exception {
        String queue = declareAndBindQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("handler fails the first time");

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(event -> {
            if (attempts.incrementAndGet() == 1) {
                throw failure;
            }
        }), queue, listener).onDeliveryFailure(DeliveryFailurePolicy.REDELIVER).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.callbacks()).contains("delivered"));
        }

        assertThat(listener.calls()).containsExactly(
                new RecordingRabbitMqSourceListener.Call("failed", queue, failure),
                new RecordingRabbitMqSourceListener.Call("redelivered", queue, null),
                new RecordingRabbitMqSourceListener.Call("delivered", queue, null));
    }

    @Test
    void a_listener_that_throws_an_exception_from_delivered_does_not_stop_delivery() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqSourceListener listener = new RabbitMqSourceListener() {
            @Override
            public void delivered(String queue) {
                throw new IllegalStateException("broken metrics binding");
            }
        };
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(handled::add), queue, listener).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-2")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(2));
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    @Test
    void a_listener_that_throws_an_AssertionError_from_delivered_does_not_stop_the_source() throws Exception {
        String queue = declareAndBindQueue();
        ThrowingRabbitMqSourceListener listener = new ThrowingRabbitMqSourceListener(() -> new AssertionError("broken metrics binding"), "delivered");
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(handled::add), queue, listener).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-2")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(2));
            assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.OPEN);
        }

        assertThat(listener.callbacks()).containsExactly("delivered", "delivered");
    }

    @Test
    void a_listener_that_throws_an_Error_from_redelivered_does_not_stop_the_source_under_REDELIVER() throws Exception {
        String queue = declareAndBindQueue();
        ThrowingRabbitMqSourceListener listener = new ThrowingRabbitMqSourceListener(() -> new NoSuchMethodError("broken metrics binding"), "redelivered");
        AtomicInteger attempts = new AtomicInteger();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("handler fails the first time");
            }
            handled.add(event);
        }), queue, listener).onDeliveryFailure(DeliveryFailurePolicy.REDELIVER).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.OPEN);
        }

        assertThat(listener.callbacks()).containsSubsequence("failed", "redelivered", "delivered");
    }

    @Test
    void a_listener_that_throws_an_Error_from_every_callback_does_not_stop_a_redelivered_failure() throws Exception {
        String queue = declareAndBindQueue();
        ThrowingRabbitMqSourceListener listener = ThrowingRabbitMqSourceListener.fromEveryCallback(() -> new AssertionError("broken metrics binding"));
        AtomicInteger attempts = new AtomicInteger();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("handler fails the first time");
            }
            handled.add(event);
        }), queue, listener).onDeliveryFailure(DeliveryFailurePolicy.REDELIVER).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }

        assertThat(listener.callbacks()).containsExactly("failed", "redelivered", "delivered");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    @Test
    void a_listener_that_throws_an_Error_from_every_callback_does_not_stop_a_parked_failure_or_the_next_delivery() throws Exception {
        String queue = declareAndBindQueue();
        declareParkingQueue();
        ThrowingRabbitMqSourceListener listener = ThrowingRabbitMqSourceListener.fromEveryCallback(() -> new AssertionError("broken metrics binding"));
        AtomicInteger attempts = new AtomicInteger();
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();

        try (RabbitMqDomainEventSource<TestOrderPlaced> source = sourceBuilder(feedWith(event -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("handler fails the first event");
            }
            handled.add(event);
        }), queue, listener).onDeliveryFailure(DeliveryFailurePolicy.PARK).parkingDestination(RabbitMqDestination.of(exchange, "parked")).build()) {
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-1")));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.callbacks()).contains("parked"));
            publishMessage(new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced("order-2")));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }

        assertThat(listener.callbacks()).containsExactly("failed", "parked", "delivered");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    /**
     * DomainEventFeed does not report NOT_DELIVERABLE today, so this drives the outcome into route(..) the way
     * RabbitMqDomainEventSourceOutcomeRoutingTest does, with a mocked channel and parking publisher.
     */
    @Test
    void a_NOT_DELIVERABLE_outcome_tells_the_listener_failed_with_no_failure_and_then_parked_under_PARK() {
        Channel channel = mock(Channel.class);
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        RabbitMqSourceListenerCalls calls = new RabbitMqSourceListenerCalls(listener, "queue", LoggerFactory.getLogger(getClass()));
        RabbitMqDeliveryFailureAction failureAction = ParkingFailureActionWithMockedPublisher.over(channel, calls);
        RabbitMqDomainEventSource<Object> source = new RabbitMqDomainEventSource<>(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction,
                Duration.ofSeconds(1), RabbitMqInboundMapper.cloudEventHeaders(), calls);

        source.route(RoutingOutcome.NOT_DELIVERABLE, 42L, new BasicProperties(), new byte[0]);

        assertThat(listener.calls()).containsExactly(
                new RecordingRabbitMqSourceListener.Call("failed", "queue", null),
                new RecordingRabbitMqSourceListener.Call("parked", "queue", null));
    }

    private RabbitMqDomainEventSource.Builder<TestOrderPlaced> sourceBuilder(DomainEventFeed<TestOrderPlaced> feed, String queue, RabbitMqSourceListener listener) {
        return RabbitMqDomainEventSource.builder(connection(), feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .listener(listener);
    }

    private static DomainEventFeed<TestOrderPlaced> feedWith(Consumer<TestOrderPlaced> handler) {
        MaterializedView<TestOrderPlaced> view = handler::accept;
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", view, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");
        return feed;
    }

    private void publishMessage(CloudEvent event) throws Exception {
        RabbitMqMessage message = RabbitMqOutboundMapper.cloudEventHeaders().toMessage(event, RabbitMqDestination.of(exchange, ROUTING_KEY));
        adminChannel.basicPublish(exchange, ROUTING_KEY, message.properties(), message.body());
    }

    private String declareAndBindQueue() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, ROUTING_KEY);
        return queue;
    }

    private void declareParkingQueue() throws Exception {
        String parkingQueue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(parkingQueue, exchange, "parked");
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
