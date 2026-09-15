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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Every test here builds its bridges on a connection whose consumer callbacks all run on one shared thread, the
 * pool the RabbitMQ client gives a connection on a machine with one available processor. A bridge that handled a
 * delivery on that thread would stop every other bridge on the connection for as long as its handler ran.
 */
class RabbitMqCloudEventBridgeWorkerThreadTest extends RabbitMqTestSupport {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
    private static final String SHARED_THREAD_NAME = "shared-consumer-thread";

    private ExecutorService sharedConsumerExecutor;
    private Connection singleThreadedConnection;

    @BeforeEach
    void openConnectionWithOneSharedConsumerThread() throws Exception {
        sharedConsumerExecutor = Executors.newFixedThreadPool(1, runnable -> new Thread(runnable, SHARED_THREAD_NAME));
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
    void a_handler_blocked_in_one_bridge_does_not_delay_a_delivery_to_another_bridge_on_the_same_connection() throws Exception {
        String blockedQueue = declareAndBindQueue("blocked");
        String healthyQueue = declareAndBindQueue("healthy");
        CountDownLatch blockedHandlerEntered = new CountDownLatch(1);
        CountDownLatch releaseBlockedHandler = new CountDownLatch(1);
        List<CloudEvent> handledByHealthyBridge = new CopyOnWriteArrayList<>();

        RoutingOutcomeChannel blockedOutcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel blockedModel = new PushSubscriptionModel(DataFieldReader.refusing(), blockedOutcomeChannel);
        blockedModel.subscribe("blocked", cloudEvent -> {
            blockedHandlerEntered.countDown();
            awaitQuietly(releaseBlockedHandler);
        });
        RoutingOutcomeChannel healthyOutcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel healthyModel = new PushSubscriptionModel(DataFieldReader.refusing(), healthyOutcomeChannel);
        healthyModel.subscribe("healthy", handledByHealthyBridge::add);

        try (RabbitMqCloudEventBridge blockedBridge = bridge(blockedModel, blockedOutcomeChannel, blockedQueue).build();
             RabbitMqCloudEventBridge healthyBridge = bridge(healthyModel, healthyOutcomeChannel, healthyQueue).build()) {
            publish("blocked", "blocked-1");
            assertThat(blockedHandlerEntered.await(5, TimeUnit.SECONDS)).as("the blocked handler started").isTrue();

            publish("healthy", "healthy-1");

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handledByHealthyBridge).extracting(CloudEvent::getId).containsExactly("healthy-1"));
            assertThat(releaseBlockedHandler.getCount()).as("the blocked handler is still blocked").isOne();
            // Released here rather than only in finally, so closing the blocked bridge does not wait out its close timeout.
            releaseBlockedHandler.countDown();
        } finally {
            releaseBlockedHandler.countDown();
        }
    }

    @Test
    void deliveries_are_handled_one_at_a_time_in_publish_order_on_one_thread_of_the_bridges_own_and_acknowledged() throws Exception {
        String queue = declareAndBindQueue("ordered");
        List<String> handledIds = new CopyOnWriteArrayList<>();
        Set<String> handlerThreadNames = ConcurrentHashMap.newKeySet();
        AtomicInteger handlersRunning = new AtomicInteger();
        AtomicBoolean overlapped = new AtomicBoolean();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("ordered", cloudEvent -> {
            if (handlersRunning.incrementAndGet() > 1) {
                overlapped.set(true);
            }
            handlerThreadNames.add(Thread.currentThread().getName());
            sleepQuietly(5);
            handledIds.add(cloudEvent.getId());
            handlersRunning.decrementAndGet();
        });
        List<String> publishedIds = IntStream.range(0, 20).mapToObj(i -> "id-" + i).toList();

        try (RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).prefetchCount(10).build()) {
            for (String id : publishedIds) {
                publish("ordered", id);
            }

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handledIds).hasSize(publishedIds.size()));
        }
        assertThat(handledIds).containsExactlyElementsOf(publishedIds);
        assertThat(overlapped).isFalse();
        assertThat(handlerThreadNames).hasSize(1).doesNotContain(SHARED_THREAD_NAME);
        assertAcknowledged(queue);
    }

    @Test
    void close_waits_for_the_delivery_being_handled_and_acknowledges_it() throws Exception {
        String queue = declareAndBindQueue("closing");
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("closing", cloudEvent -> {
            handlerEntered.countDown();
            awaitQuietly(releaseHandler);
            handled.add(cloudEvent);
        });
        RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).build();
        publish("closing", "id-1");
        assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Void> closing = CompletableFuture.runAsync(bridge::close);
        Thread.sleep(300);
        assertThat(closing).as("close() waits while the handler is still running").isNotDone();
        releaseHandler.countDown();

        closing.get(5, TimeUnit.SECONDS);
        assertThat(handled).extracting(CloudEvent::getId).containsExactly("id-1");
        assertAcknowledged(queue);
    }

    @Test
    void close_puts_every_unfinished_delivery_back_on_the_queue_once_the_close_timeout_runs_out() throws Exception {
        String queue = declareAndBindQueue("stuck");
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        AtomicBoolean handlerInterrupted = new AtomicBoolean();
        List<String> startedIds = new CopyOnWriteArrayList<>();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("stuck", cloudEvent -> {
            startedIds.add(cloudEvent.getId());
            handlerEntered.countDown();
            try {
                neverReleased.await();
            } catch (InterruptedException e) {
                // Failing rather than returning, since a handler that returns has finished and is acknowledged.
                handlerInterrupted.set(true);
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted", e);
            }
        });

        try (RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).prefetchCount(3).closeTimeout(Duration.ofMillis(200)).build()) {
            publish("stuck", "id-1");
            publish("stuck", "id-2");
            publish("stuck", "id-3");
            assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();
            // All three are with the bridge once none of them is ready on the queue any more.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
        }

        // close() interrupts the handler without waiting for it to notice.
        await().atMost(Duration.ofSeconds(5)).untilTrue(handlerInterrupted);
        assertThat(startedIds).containsExactly("id-1");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(3));
    }

    @Test
    void close_neither_parks_nor_acknowledges_a_delivery_whose_handler_it_interrupted() throws Exception {
        String queue = declareAndBindQueue("parking-at-close");
        String parkingQueue = declareAndBindQueue("parked");
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("parking-at-close", cloudEvent -> {
            handlerEntered.countDown();
            try {
                neverReleased.await();
            } catch (InterruptedException e) {
                // Failing, which under PARK would park the delivery if close() still let it.
                throw new IllegalStateException("interrupted", e);
            }
        });

        try (RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK)
                .parkingDestination(RabbitMqDestination.of(exchange, "parked"))
                .closeTimeout(Duration.ofMillis(200))
                .build()) {
            publish("parking-at-close", "id-1");
            assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();
        }

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isOne());
        // Long enough for a park the handler's failure started to have reached the parking queue.
        Thread.sleep(500);
        assertThat(queueMessageCount(parkingQueue)).isZero();
    }

    private RabbitMqCloudEventBridge.Builder bridge(PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, String queue) {
        return RabbitMqCloudEventBridge.builder(singleThreadedConnection, model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(POLL_INTERVAL);
    }

    private String declareAndBindQueue(String routingKey) throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, routingKey);
        return queue;
    }

    private void publish(String routingKey, String id) throws Exception {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType("OrderPlaced")
                .withExtension("streamid", "stream-1")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        adminChannel.basicPublish(exchange, routingKey, properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private long queueMessageCount(String queue) throws Exception {
        return adminChannel.queueDeclarePassive(queue).getMessageCount();
    }

    // Meaningful only once the bridge is closed, since closing requeues whatever it never acknowledged.
    private void assertAcknowledged(String queue) throws Exception {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isZero());
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
