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
import org.slf4j.LoggerFactory;
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
import java.util.concurrent.atomic.AtomicInteger;

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
            // above) reconnects, resubscribes the consumer, and offsets the recovered channel's delivery tags past
            // every tag the dead channel issued.
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

    /**
     * A handler blocked across two recoveries finds the message waiting for it a second time from each recovered
     * channel unless the bridge drops what the dead channel left behind. With prefetch one the message is handled
     * twice here, once by the blocked call and once by the copy the last recovered channel delivered.
     */
    @Test
    void a_handler_blocked_across_two_recoveries_handles_the_message_once_more_rather_than_once_per_recovery() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());
        AtomicInteger recoveries = new AtomicInteger();
        ((Recoverable) connection).addRecoveryListener(new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                recoveries.incrementAndGet();
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
            }
        });

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        CountDownLatch firstCallEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        List<String> handled = new CopyOnWriteArrayList<>();
        model.subscribe("proj", ce -> {
            handled.add(ce.getId());
            if (handled.size() == 1) {
                firstCallEntered.countDown();
                try {
                    releaseFirstCall.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, queue)
                .declareTopology(false)
                .pollInterval(Duration.ofMillis(200))
                .build()) {
            publish(OrderPlaced.class.getName(), "id-1");
            assertThat(firstCallEntered.await(5, TimeUnit.SECONDS)).isTrue();

            for (int recovery = 1; recovery <= 2; recovery++) {
                int expectedRecoveries = recovery;
                forceCloseAllConnectionsOrFail();
                await().atMost(Duration.ofSeconds(30)).until(() -> recoveries.get() == expectedRecoveries);
                // The recovered consumer has taken the requeued copy once the queue has nothing ready on it.
                await().atMost(Duration.ofSeconds(10)).ignoreExceptions()
                        .untilAsserted(() -> assertThat(adminChannel.queueDeclarePassive(queue).getMessageCount()).isZero());
            }
            releaseFirstCall.countDown();

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handled).hasSize(2));
            sleep(Duration.ofSeconds(1));
            assertThat(handled).containsExactly("id-1", "id-1");
        } finally {
            releaseFirstCall.countDown();
        }
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(adminChannel.queueDeclarePassive(queue).getMessageCount()).isZero());
    }

    /**
     * A handler that finishes after the connection has dropped but before its recovery has started acknowledges on a
     * closed channel, which throws. The other tests here release their handler only once recovery has finished, when
     * the acknowledgement goes to the replacement channel and the client skips it without a word. The connection gets
     * a recovery interval of five seconds so the handler can finish inside that gap. The test then waits for the
     * bridge to log what it did with the failed acknowledgement, and checks that the recovery had not started by
     * then, so the acknowledgement really did go to the closed channel.
     */
    @Test
    void a_handler_finishing_before_recovery_starts_does_not_stop_the_bridge_from_consuming_after_recovery() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

        ConnectionFactory slowRecoveryFactory = new ConnectionFactory();
        slowRecoveryFactory.setUri(rabbitMQContainer.getAmqpUrl());
        slowRecoveryFactory.setAutomaticRecoveryEnabled(true);
        slowRecoveryFactory.setNetworkRecoveryInterval(5000);
        try (Connection slowRecoveryConnection = slowRecoveryFactory.newConnection()) {
            CountDownLatch recoveryStarted = new CountDownLatch(1);
            CountDownLatch recoveryComplete = new CountDownLatch(1);
            ((Recoverable) slowRecoveryConnection).addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                    recoveryStarted.countDown();
                }

                @Override
                public void handleRecovery(Recoverable recoverable) {
                    recoveryComplete.countDown();
                }
            });

            RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
            PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
            CountDownLatch firstCallEntered = new CountDownLatch(1);
            CountDownLatch releaseFirstCall = new CountDownLatch(1);
            List<String> handled = new CopyOnWriteArrayList<>();
            model.subscribe("proj", ce -> {
                handled.add(ce.getId());
                if (handled.size() == 1) {
                    firstCallEntered.countDown();
                    awaitLatch(releaseFirstCall);
                }
            });

            ch.qos.logback.classic.Logger bridgeLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqCloudEventBridge.class);
            ListAppender<ILoggingEvent> bridgeLog = new ListAppender<>();
            bridgeLog.start();
            bridgeLogger.addAppender(bridgeLog);

            try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(slowRecoveryConnection, model, outcomeChannel, queue)
                    .declareTopology(false)
                    .pollInterval(Duration.ofMillis(200))
                    .build()) {
                publish(OrderPlaced.class.getName(), "id-1");
                assertThat(firstCallEntered.await(5, TimeUnit.SECONDS)).isTrue();

                forceCloseAllConnectionsOrFail();
                await().atMost(Duration.ofSeconds(5)).until(() -> !slowRecoveryConnection.isOpen());
                releaseFirstCall.countDown();
                // Waits for the bridge's own decision about the failed acknowledgement, rather than for a fixed
                // moment, so this test is in the window it exists for whichever decision the bridge makes. The
                // acknowledgement it logs is the one that went to the closed channel, since the assertion below
                // shows the recovery had not started, and therefore no replacement channel existed yet.
                await().atMost(Duration.ofSeconds(4)).until(() -> bridgeLog.list.stream().anyMatch(event ->
                        event.getFormattedMessage().contains("dropped before delivery tag")
                                || event.getFormattedMessage().contains("failed outside this bridge's delivery failure policy")));
                assertThat(recoveryStarted.getCount())
                        .as("the handler must have tried to acknowledge before the connection's recovery started")
                        .isOne();

                assertThat(recoveryComplete.await(30, TimeUnit.SECONDS)).isTrue();
                publish(OrderPlaced.class.getName(), "id-2");

                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(handled).contains("id-2"));
            } finally {
                releaseFirstCall.countDown();
            }
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
