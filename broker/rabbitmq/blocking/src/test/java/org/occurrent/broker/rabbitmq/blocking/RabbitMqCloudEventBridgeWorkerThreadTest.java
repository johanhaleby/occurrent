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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    /**
     * An {@code Error} is not a handler failure the delivery failure policy covers, and before each bridge had a
     * thread of its own the RabbitMQ client closed the channel for anything escaping its callback, which put the
     * delivery back on the queue. Left to itself the worker thread would die with the delivery unacknowledged on a
     * consumer the broker sends nothing further to, so the message would stay invisible until someone closed the
     * bridge.
     */
    @Test
    void an_error_from_a_handler_stops_the_bridge_and_puts_its_delivery_back_on_the_queue() throws Exception {
        String queue = declareAndBindQueue("erroring");
        AtomicBoolean firstCall = new AtomicBoolean(true);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("erroring", cloudEvent -> {
            if (firstCall.compareAndSet(true, false)) {
                throw new Error("boom");
            }
        });

        try (RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).build()) {
            publish("erroring", "id-1");
            publish("erroring", "id-2");

            // Both back on the queue, which only a closed channel does. The bridge acknowledged neither, and with
            // prefetch one the second was never even sent to it.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(2));
        }
    }

    /**
     * A handler written in Kotlin has no checked exception to declare, so it throws one straight through the Java
     * interface this bridge calls it behind, which is what {@link #sneakyThrow(Throwable)} below reproduces from Java.
     * The delivery failure policy covers a handler's own {@code RuntimeException} and {@code AssertionError}, so a
     * checked exception belongs on the stopping path with an {@code Error}. Against a catch of
     * {@code RuntimeException | Error} it escapes the worker's task instead, the worker thread dies, and the delivery
     * stays unacknowledged on a bridge that still says it is consuming.
     */
    @Test
    void a_checked_exception_from_a_handler_stops_the_bridge_and_puts_its_delivery_back_on_the_queue() throws Exception {
        String queue = declareAndBindQueue("checked");
        AtomicBoolean firstCall = new AtomicBoolean(true);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("checked", cloudEvent -> {
            if (firstCall.compareAndSet(true, false)) {
                sneakyThrow(new IOException("the store this handler writes to is down"));
            }
        });

        try (RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).build()) {
            publish("checked", "id-1");
            publish("checked", "id-2");

            // Both back on the queue, which only a closed channel does. The bridge acknowledged neither, and with
            // prefetch one the second was never even sent to it.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isEqualTo(2));
        }
    }

    /**
     * RabbitMQ applies the prefetch count to each consumer separately, so a consumer started again while the handler
     * is still blocked would be sent a delivery of its own, which would wait behind the blocked one.
     */
    @Test
    void a_consumer_is_not_started_again_while_the_worker_still_has_a_delivery_from_the_previous_one() throws Exception {
        String queue = declareAndBindQueue("restarted");
        AtomicBoolean ready = new AtomicBoolean(true);
        CountDownLatch firstCallEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        List<String> handledIds = new CopyOnWriteArrayList<>();
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("restarted", cloudEvent -> {
            handledIds.add(cloudEvent.getId());
            if (handledIds.size() == 1) {
                firstCallEntered.countDown();
                awaitQuietly(releaseFirstCall);
            }
        });

        try (RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).readinessSource(subscriptionId -> ready.get()).build()) {
            publish("restarted", "id-1");
            assertThat(firstCallEntered.await(5, TimeUnit.SECONDS)).isTrue();

            ready.set(false);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(adminChannel.queueDeclarePassive(queue).getConsumerCount()).isZero());
            ready.set(true);
            publish("restarted", "id-2");
            // Several polls, each of which would start a consumer if nothing held it back.
            Thread.sleep(POLL_INTERVAL.toMillis() * 10);

            assertThat(queueMessageCount(queue)).as("id-2 is still ready on the queue").isOne();
            releaseFirstCall.countDown();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(handledIds).containsExactly("id-1", "id-2"));
        } finally {
            releaseFirstCall.countDown();
        }
        assertAcknowledged(queue);
    }

    /**
     * A second {@code close()}, from a shutdown hook after a try-with-resources say, must not move the deadline the
     * first one already passed back into the future. A handler that ignored its interrupt would otherwise be allowed
     * to acknowledge, and would try it on the channel the first close already shut, which fails and is logged as this
     * bridge failing. The log is the only place that is visible, since the acknowledgement cannot succeed either way.
     */
    @Test
    void a_second_close_does_not_let_a_handler_that_ignored_its_interrupt_through_the_deadline() throws Exception {
        String queue = declareAndBindQueue("double-close");
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        model.subscribe("double-close", cloudEvent -> {
            handlerEntered.countDown();
            boolean released = false;
            while (!released) {
                try {
                    released = releaseHandler.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // Ignoring the interrupt on purpose, which is what this test is about.
                }
            }
        });
        ch.qos.logback.classic.Logger bridgeLog = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqCloudEventBridge.class);
        ListAppender<ILoggingEvent> logged = new ListAppender<>();
        logged.start();
        bridgeLog.addAppender(logged);

        RabbitMqCloudEventBridge bridge = bridge(model, outcomeChannel, queue).closeTimeout(Duration.ofMillis(200)).build();
        try {
            publish("double-close", "id-1");
            assertThat(handlerEntered.await(5, TimeUnit.SECONDS)).isTrue();

            bridge.close();
            // The second close() waits its own close timeout, and the handler returns inside that window, which is
            // where a deadline moved back into the future would let it through.
            CompletableFuture<Void> secondClose = CompletableFuture.runAsync(bridge::close);
            Thread.sleep(50);
            releaseHandler.countDown();
            secondClose.get(10, TimeUnit.SECONDS);
            // Long enough for the handler to reach the acknowledgement it must not make.
            Thread.sleep(500);

            assertThat(logged.list).extracting(ILoggingEvent::getFormattedMessage)
                    .noneMatch(message -> message.contains("failed outside this bridge's delivery failure policy"));
        } finally {
            releaseHandler.countDown();
            bridgeLog.detachAppender(logged);
        }
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(queueMessageCount(queue)).isOne());
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

    // Throws a checked exception the way a Kotlin handler does, without declaring it, since the interface the bridge
    // calls the handler behind declares none.
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
