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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTestSupport;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The domain-level twin of {@code RabbitMqCloudEventBridgeWorkerThreadTest}, on a connection whose consumer callbacks
 * all run on one shared thread.
 */
class RabbitMqDomainEventBridgeWorkerThreadTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

    private ExecutorService sharedConsumerExecutor;
    private Connection singleThreadedConnection;

    @BeforeEach
    void openConnectionWithOneSharedConsumerThread() throws Exception {
        sharedConsumerExecutor = Executors.newFixedThreadPool(1);
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(amqpUrl());
        connectionFactory.setSharedExecutor(sharedConsumerExecutor);
        singleThreadedConnection = connectionFactory.newConnection();
    }

    @AfterEach
    void closeConnectionWithOneSharedConsumerThread() throws Exception {
        singleThreadedConnection.close();
        sharedConsumerExecutor.shutdownNow();
    }

    @Test
    void a_projection_blocked_in_one_bridge_does_not_delay_a_delivery_to_another_bridge_on_the_same_connection() throws Exception {
        String blockedQueue = declareAndBindQueue("blocked");
        String healthyQueue = declareAndBindQueue("healthy");
        CountDownLatch blockedProjectionEntered = new CountDownLatch(1);
        CountDownLatch releaseBlockedProjection = new CountDownLatch(1);
        List<TestOrderPlaced> handledByHealthyBridge = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> blockedFeed = liveFeed(event -> {
            blockedProjectionEntered.countDown();
            awaitQuietly(releaseBlockedProjection);
        });
        DomainEventFeed<TestOrderPlaced> healthyFeed = liveFeed(handledByHealthyBridge::add);

        try (RabbitMqDomainEventBridge<TestOrderPlaced> blockedBridge = bridge(blockedFeed, blockedQueue).build();
             RabbitMqDomainEventBridge<TestOrderPlaced> healthyBridge = bridge(healthyFeed, healthyQueue).build()) {
            publish("blocked", "order-1");
            assertThat(blockedProjectionEntered.await(5, TimeUnit.SECONDS)).as("the blocked projection started").isTrue();

            publish("healthy", "order-2");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handledByHealthyBridge).containsExactly(new TestOrderPlaced("order-2")));
            assertThat(releaseBlockedProjection.getCount()).as("the blocked projection is still blocked").isOne();
            // Released here rather than only in finally, so closing the blocked bridge does not wait out its close timeout.
            releaseBlockedProjection.countDown();
        } finally {
            releaseBlockedProjection.countDown();
        }
    }

    /**
     * The domain twin of {@code RabbitMqCloudEventBridgeWorkerThreadTest}'s own {@code Error} test. Before each bridge
     * had a thread of its own the RabbitMQ client closed the channel for anything escaping its callback, which put the
     * delivery back on the queue.
     */
    @Test
    void an_error_from_a_projection_stops_the_bridge_and_puts_its_delivery_back_on_the_queue() throws Exception {
        String queue = declareAndBindQueue("erroring");
        AtomicBoolean firstCall = new AtomicBoolean(true);
        CountDownLatch projectionFailed = new CountDownLatch(1);
        DomainEventFeed<TestOrderPlaced> feed = liveFeed(event -> {
            if (firstCall.compareAndSet(true, false)) {
                projectionFailed.countDown();
                throw new Error("boom");
            }
        });

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = bridge(feed, queue).build()) {
            publish("erroring", "order-1");
            publish("erroring", "order-2");

            // Waits for the failure itself before reading the queue, since both messages are ready on it until the
            // bridge takes the first one, and a count of two taken then would be the state before anything happened
            // rather than the requeue this asserts.
            assertThat(projectionFailed.await(5, TimeUnit.SECONDS)).as("the projection failed").isTrue();
            // Both back on the queue, which only a closed channel does. The bridge acknowledged neither, and with
            // prefetch one the second was never even sent to it.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(2));
        }
    }

    /**
     * A projection written in Kotlin has no checked exception to declare, so it throws one straight through the Java
     * interface this bridge calls it behind, which is what {@link #sneakyThrow(Throwable)} below reproduces from Java.
     * The delivery failure policy covers a projection's own {@code RuntimeException} and {@code AssertionError}, so a
     * checked exception belongs on the stopping path with an {@code Error}. Against a catch of
     * {@code RuntimeException | Error} it escapes the worker's task instead, the worker thread dies, and the delivery
     * stays unacknowledged on a bridge that still says it is consuming.
     */
    @Test
    void a_checked_exception_from_a_projection_stops_the_bridge_and_puts_its_delivery_back_on_the_queue() throws Exception {
        String queue = declareAndBindQueue("checked");
        AtomicBoolean firstCall = new AtomicBoolean(true);
        CountDownLatch projectionFailed = new CountDownLatch(1);
        DomainEventFeed<TestOrderPlaced> feed = liveFeed(event -> {
            if (firstCall.compareAndSet(true, false)) {
                projectionFailed.countDown();
                sneakyThrow(new IOException("the view this projection writes to is down"));
            }
        });

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = bridge(feed, queue).build()) {
            publish("checked", "order-1");
            publish("checked", "order-2");

            // Waits for the failure itself before reading the queue, since both messages are ready on it until the
            // bridge takes the first one, and a count of two taken then would be the state before anything happened
            // rather than the requeue this asserts.
            assertThat(projectionFailed.await(5, TimeUnit.SECONDS)).as("the projection failed").isTrue();
            // Both back on the queue, which only a closed channel does. The bridge acknowledged neither, and with
            // prefetch one the second was never even sent to it.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(2));
        }
    }

    @Test
    void close_puts_every_unfinished_delivery_back_on_the_queue_once_the_close_timeout_runs_out() throws Exception {
        String queue = declareAndBindQueue("stuck");
        CountDownLatch projectionEntered = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        List<TestOrderPlaced> started = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = liveFeed(event -> {
            started.add(event);
            projectionEntered.countDown();
            try {
                neverReleased.await();
            } catch (InterruptedException e) {
                // Failing rather than returning, since a projection that returns has finished and is acknowledged.
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted", e);
            }
        });

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = bridge(feed, queue).prefetchCount(3).closeTimeout(Duration.ofMillis(200)).build()) {
            publish("stuck", "order-1");
            publish("stuck", "order-2");
            publish("stuck", "order-3");
            assertThat(projectionEntered.await(5, TimeUnit.SECONDS)).isTrue();
            // All three are with the bridge once none of them is ready on the queue any more.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }

        assertThat(started).containsExactly(new TestOrderPlaced("order-1"));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(3));
    }

    private static DomainEventFeed<TestOrderPlaced> liveFeed(MaterializedView<TestOrderPlaced> projection) {
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", projection, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");
        return feed;
    }

    private RabbitMqDomainEventBridge.Builder<TestOrderPlaced> bridge(DomainEventFeed<TestOrderPlaced> feed, String queue) {
        return RabbitMqDomainEventBridge.builder(singleThreadedConnection, feed, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL);
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, routingKey);
        return queue;
    }

    private void publish(String routingKey, String orderId) throws Exception {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:test"))
                .withType(TestOrderPlaced.class.getName())
                .withDataContentType("text/plain")
                .withData(orderId.getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        adminChannel.basicPublish(exchange, routingKey, properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private long queueMessageCount(String queue) throws Exception {
        return adminChannel.queueDeclarePassive(queue).getMessageCount();
    }

    // Throws a checked exception the way a Kotlin projection does, without declaring it, since the interface the
    // bridge calls the projection behind declares none.
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
