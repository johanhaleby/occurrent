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
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@link RabbitMqCloudEventBridge} keeps consuming after its connection has recovered automatically. Both tests
 * hold a {@code DEFERRED} delivery (a live message arriving while a catch-up replay is parked), force the
 * underlying TCP connection closed from the broker side with {@code rabbitmqctl close_all_connections}, and then
 * release the replay and assert the message still reaches the projection.
 * <p>
 * The second test also delays every recovery listener on the connection past the point where the recovered
 * consumer has already been handed the requeued message. The RabbitMQ client re-issues {@code basic.consume} while
 * it recovers topology, before it notifies any recovery listener, so a bridge that decided a delivery's fate from
 * what a recovery listener had told it would leave that first redelivery unacknowledged and, at the default
 * prefetch of one, never receive anything again. See <a
 * href="https://github.com/johanhaleby/occurrent/issues/922">occurrent#922</a>.
 * <p>
 * Both fail outright, rather than skipping quietly, when {@code close_all_connections} reports a non-zero exit
 * code. A test that can pass without ever forcing the recovery it exists to exercise is worse than no test at all.
 */
@Testcontainers
class RabbitMqCloudEventBridgeConnectionRecoveryTest {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion());

    private Connection connection;
    private Channel adminChannel;
    private String exchange;

    @BeforeEach
    void openConnectionAndScratchExchange() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setNetworkRecoveryInterval(500);
        connection = connectionFactory.newConnection();
        adminChannel = connection.createChannel();
        exchange = "test-exchange-" + UUID.randomUUID();
        adminChannel.exchangeDeclare(exchange, "topic", false, true, null);
    }

    @AfterEach
    void closeConnection() throws Exception {
        connection.close();
    }

    @Test
    void a_held_deferred_delivery_tag_across_a_forced_connection_recovery() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

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
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection, liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(Duration.ofSeconds(2))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");

            // Long enough for the bridge to have fetched the message and be holding it unacked (DEFERRED, the
            // replay still parked), well before the 2-second pollInterval could release it.
            Thread.sleep(500);

            // Force the TCP connection closed from the broker side. The client's automatic recovery (enabled
            // above) reconnects on a fresh channel with delivery tags restarting at 1.
            forceCloseAllConnectionsOrFail();

            // Wait for the client to report the connection open again (automatic recovery completed).
            await().atMost(Duration.ofSeconds(15)).until(() -> connection.isOpen());
            // Give the consumer's own topology/consumer recovery a moment to finish resubscribing on the fresh
            // channel before releasing the replay.
            Thread.sleep(1000);

            releaseReplay.countDown();

            // The held message must still reach the projection once catch-up finishes and the bridge's own
            // held-tag release runs.
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(folded).contains("id-1"));
        }
    }

    @Test
    void a_redelivery_arriving_before_the_connections_recovery_listeners_run_is_still_consumed() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

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
        assertThat(replayEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // Registered on the connection before the bridge is built, and recovery listeners run in registration
        // order, so this one holds every later listener back for two seconds after the recovered consumer has
        // already been handed the requeued message. Two seconds rather than none, because a listener that returned
        // at once could bump a generation counter before the redelivery had read it, which is the ordering that
        // made the first test flaky rather than failing.
        CountDownLatch recoveryComplete = new CountDownLatch(1);
        ((Recoverable) connection).addRecoveryListener(new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                sleep(Duration.ofSeconds(2));
                recoveryComplete.countDown();
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
            }
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection, liveFeed, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(Duration.ofSeconds(2))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");
            sleep(Duration.ofMillis(500));

            forceCloseAllConnectionsOrFail();
            // Waits for the listener above rather than for a fixed delay, so the replay is released only once the
            // redelivery has been handed to the recovered consumer with the replay still parked, which is the
            // ordering this test exists for.
            assertThat(recoveryComplete.await(30, TimeUnit.SECONDS))
                    .as("the connection's recovery listeners must have run")
                    .isTrue();

            releaseReplay.countDown();

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(folded).contains("id-1"));
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Closes every connection currently open on the broker via {@code rabbitmqctl close_all_connections}, which
     * forces this test's own AMQP connection to drop and trigger the client's automatic recovery. Fails the test
     * outright, rather than skipping the assertion this exists for, when the exec itself reports a non-zero exit
     * code.
     */
    private void forceCloseAllConnectionsOrFail() throws Exception {
        Container.ExecResult closeResult = rabbitMQContainer.execInContainer(
                "rabbitmqctl", "close_all_connections", "forced-by-connection-recovery-test");
        assertThat(closeResult.getExitCode())
                .as("rabbitmqctl close_all_connections must succeed for this test to force the recovery it exists "
                        + "to exercise; stdout: %s, stderr: %s", closeResult.getStdout(), closeResult.getStderr())
                .isZero();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
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

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }

    private static final class OrderPlaced {
    }
}
