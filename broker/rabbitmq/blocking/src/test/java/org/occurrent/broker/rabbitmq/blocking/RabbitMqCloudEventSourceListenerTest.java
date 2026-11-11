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

import com.rabbitmq.client.Channel;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * What {@link RabbitMqCloudEventSource} tells its {@link RabbitMqSourceListener} for each delivery, against a real
 * broker. The callback table is in ADR 171. A twin of this class for {@code RabbitMqDomainEventSource} lives in the
 * {@code domain} package and covers the same cases.
 */
class RabbitMqCloudEventSourceListenerTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
    private static final String ROUTING_KEY = "orders.placed";

    @Test
    void a_consumed_event_tells_the_listener_delivered_for_the_queue() throws Exception {
        String queue = declareAndBindQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).build()) {
            publishMessage("id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.calls()).hasSize(1));
        }

        assertThat(handled).hasSize(1);
        assertThat(listener.calls()).containsExactly(new RecordingRabbitMqSourceListener.Call("delivered", queue, null));
    }

    @Test
    void a_filtered_out_event_tells_the_listener_delivered_since_the_source_acknowledged_it() throws Exception {
        String queue = declareAndBindQueue();
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", StreamSubscriptionFilter.filter(Filter.type(SomethingElse.class.getName())), handled::add);

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).build()) {
            publishMessage("id-1");

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
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        IllegalStateException failure = new IllegalStateException("handler failed");
        model.subscribe("sub", cloudEvent -> {
            throw failure;
        });

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK).parkingDestination(RabbitMqDestination.of(exchange, "parked")).build()) {
            publishMessage("id-1");

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
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> { });
        IllegalArgumentException failure = new IllegalArgumentException("cannot map this message");
        RabbitMqInboundMapper mapper = (properties, body) -> {
            throw failure;
        };

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).inboundMapper(mapper)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK).parkingDestination(RabbitMqDestination.of(exchange, "parked")).build()) {
            publishMessage("id-1");

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
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("handler fails the first time");
        model.subscribe("sub", cloudEvent -> {
            if (attempts.incrementAndGet() == 1) {
                throw failure;
            }
        });

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).onDeliveryFailure(DeliveryFailurePolicy.REDELIVER).build()) {
            publishMessage("id-1");

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
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).build()) {
            publishMessage("id-1");
            publishMessage("id-2");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(2));
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    @Test
    void a_listener_that_throws_an_AssertionError_from_delivered_does_not_stop_the_source() throws Exception {
        String queue = declareAndBindQueue();
        ThrowingRabbitMqSourceListener listener = new ThrowingRabbitMqSourceListener(() -> new AssertionError("broken metrics binding"), "delivered");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", handled::add);

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).build()) {
            publishMessage("id-1");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            publishMessage("id-2");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(2));
            assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.CONSUMING);
        }

        assertThat(listener.callbacks()).containsExactly("delivered", "delivered");
    }

    @Test
    void a_listener_that_throws_an_Error_from_redelivered_does_not_stop_the_source_under_REDELIVER() throws Exception {
        String queue = declareAndBindQueue();
        ThrowingRabbitMqSourceListener listener = new ThrowingRabbitMqSourceListener(() -> new NoSuchMethodError("broken metrics binding"), "redelivered");
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        AtomicInteger attempts = new AtomicInteger();
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("handler fails the first time");
            }
            handled.add(cloudEvent);
        });

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).onDeliveryFailure(DeliveryFailurePolicy.REDELIVER).build()) {
            publishMessage("id-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
            assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.CONSUMING);
        }

        assertThat(listener.callbacks()).containsSubsequence("failed", "redelivered", "delivered");
    }

    @Test
    void a_listener_that_throws_an_Error_from_every_callback_does_not_stop_a_redelivered_failure() throws Exception {
        String queue = declareAndBindQueue();
        ThrowingRabbitMqSourceListener listener = ThrowingRabbitMqSourceListener.fromEveryCallback(() -> new AssertionError("broken metrics binding"));
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        AtomicInteger attempts = new AtomicInteger();
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("handler fails the first time");
            }
            handled.add(cloudEvent);
        });

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener).onDeliveryFailure(DeliveryFailurePolicy.REDELIVER).build()) {
            publishMessage("id-1");

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
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        model.subscribe("sub", cloudEvent -> {
            if (cloudEvent.getId().equals("id-1")) {
                throw new IllegalStateException("handler fails the first event");
            }
            handled.add(cloudEvent);
        });

        try (RabbitMqCloudEventSource source = sourceBuilder(model, queue, listener)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK).parkingDestination(RabbitMqDestination.of(exchange, "parked")).build()) {
            publishMessage("id-1");
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(listener.callbacks()).contains("parked"));
            publishMessage("id-2");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handled).hasSize(1));
        }

        assertThat(listener.callbacks()).containsExactly("failed", "parked", "delivered");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    /**
     * No shipped model reports NOT_DELIVERABLE for a live delivery, so this drives the outcome into route(..) the
     * way RabbitMqCloudEventSourceOutcomeRoutingTest does, with a mocked channel and parking publisher.
     */
    @Test
    void a_NOT_DELIVERABLE_outcome_tells_the_listener_failed_with_no_failure_and_then_parked_under_PARK() {
        Channel channel = mock(Channel.class);
        RecordingRabbitMqSourceListener listener = new RecordingRabbitMqSourceListener();
        RabbitMqSourceListenerCalls calls = new RabbitMqSourceListenerCalls(listener, "queue", LoggerFactory.getLogger(getClass()));
        RabbitMqDeliveryFailureAction failureAction = ParkingFailureActionWithMockedPublisher.over(channel, calls);
        RabbitMqCloudEventSource source = new RabbitMqCloudEventSource(null, channel, "queue", 1, Duration.ofSeconds(1), failureAction, null,
                Duration.ofSeconds(1), RabbitMqInboundMapper.cloudEventHeaders(), calls);

        source.route(RoutingOutcome.NOT_DELIVERABLE, 42L, new com.rabbitmq.client.AMQP.BasicProperties(), new byte[0]);

        assertThat(listener.calls()).containsExactly(
                new RecordingRabbitMqSourceListener.Call("failed", "queue", null),
                new RecordingRabbitMqSourceListener.Call("parked", "queue", null));
    }

    private RabbitMqCloudEventSource.Builder sourceBuilder(PushSubscriptionModel model, String queue, RabbitMqSourceListener listener) {
        return RabbitMqCloudEventSource.builder(connection(), model, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL)
                .listener(listener);
    }

    private void publishMessage(String id) throws Exception {
        CloudEvent event = CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType(OrderPlaced.class.getName()).build();
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

    private static final class OrderPlaced {
    }

    private static final class SomethingElse {
    }
}
