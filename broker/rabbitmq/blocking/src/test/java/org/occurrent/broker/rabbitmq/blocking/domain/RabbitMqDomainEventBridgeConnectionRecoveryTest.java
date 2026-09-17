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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@link RabbitMqDomainEventBridge} keeps consuming after its connection has recovered automatically. The
 * RabbitMQ client re-issues {@code basic.consume} while it recovers topology, before it notifies any recovery
 * listener, so a bridge that decided a delivery's fate from what a recovery listener had told it would leave the
 * first delivery after a recovery unacknowledged and, at the default prefetch of one, never receive anything
 * again. This is the domain-feed twin of
 * {@code RabbitMqCloudEventBridgeConnectionRecoveryTest}, which the cloud-event bridge has for the same reason. It
 * runs against a real broker, because what makes the fix correct is the client's own handling of a delivery tag
 * from the channel that died, which no stub can stand in for. See <a
 * href="https://github.com/johanhaleby/occurrent/issues/922">occurrent#922</a>.
 * <p>
 * Its own container and its own connection, rather than the one {@code RabbitMqTestSupport} shares, because
 * {@code rabbitmqctl close_all_connections} closes every connection on the broker and would take other tests'
 * connections down with it. Fails outright, rather than skipping quietly, when that command reports a non-zero
 * exit code.
 */
@Testcontainers
class RabbitMqDomainEventBridgeConnectionRecoveryTest {

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
    void a_delivery_arriving_before_the_connections_recovery_listeners_run_is_still_consumed() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, TestOrderPlaced.class.getName());

        // The two halves of a handshake between the recovered consumer and the connection's recovery listeners,
        // so neither side depends on how fast the other one got there. The order-2 delivery announces itself, the
        // listeners run only then, and the delivery finishes only once they have. A bridge that invalidated a
        // delivery tag from a recovery listener would therefore always decide order-2's fate after the
        // invalidation, and drop it.
        CountDownLatch order2Started = new CountDownLatch(1);
        CountDownLatch recoveryComplete = new CountDownLatch(1);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", event -> {
            handled.add(event);
            if (event.orderId().equals("order-2")) {
                order2Started.countDown();
                awaitLatch(recoveryComplete);
            }
        }, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        // Registered before the bridge is built, and recovery listeners run in registration order, so this one
        // holds every later listener back until order-2 is being handled.
        ((Recoverable) connection).addRecoveryListener(new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                awaitLatch(order2Started);
                recoveryComplete.countDown();
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
            }
        });

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection, feed, queue)
                .declareTopology(false)
                .pollInterval(Duration.ofSeconds(2))
                .build()) {
            publish("order-1");
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-1")));

            forceCloseAllConnectionsOrFail();
            await().atMost(Duration.ofSeconds(20)).until(() -> connection.isOpen());

            // Published after the connection is back, so both land on the recovered consumer. The handshake above
            // is what orders them against the recovery listeners, not this call's own timing.
            publish("order-2");
            publish("order-3");

            // order-3 is what this test is really after. At the default prefetch of one, an order-2 left
            // unacknowledged means order-3 is never delivered at all.
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-3")));
        }
    }

    /**
     * A projection that finishes after the connection has dropped but before its recovery has started is acknowledged
     * on a closed channel, which throws. The connection gets a recovery interval of five seconds so the projection can
     * finish inside that gap. The test then waits for the bridge to log what it did with the failed acknowledgement,
     * and checks that the recovery had not started by then, so the acknowledgement really did go to the closed
     * channel.
     */
    @Test
    void a_projection_finishing_before_recovery_starts_does_not_stop_the_bridge_from_consuming_after_recovery() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, TestOrderPlaced.class.getName());

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

            CountDownLatch order1Started = new CountDownLatch(1);
            CountDownLatch releaseOrder1 = new CountDownLatch(1);
            List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
            DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
            feed.register("proj", event -> {
                handled.add(event);
                if (handled.size() == 1) {
                    order1Started.countDown();
                    awaitLatch(releaseOrder1);
                }
            }, Filter.type(TestOrderPlaced.class.getName()));
            feed.goLive("proj");

            ch.qos.logback.classic.Logger bridgeLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RabbitMqDomainEventBridge.class);
            ListAppender<ILoggingEvent> bridgeLog = new ListAppender<>();
            bridgeLog.start();
            bridgeLogger.addAppender(bridgeLog);

            try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(slowRecoveryConnection, feed, queue)
                    .declareTopology(false)
                    .pollInterval(Duration.ofMillis(200))
                    .build()) {
                publish("order-1");
                assertThat(order1Started.await(15, TimeUnit.SECONDS)).isTrue();

                forceCloseAllConnectionsOrFail();
                await().atMost(Duration.ofSeconds(5)).until(() -> !slowRecoveryConnection.isOpen());
                releaseOrder1.countDown();
                // Waits for the bridge's own decision about the failed acknowledgement, rather than for a fixed
                // moment, so this test is in the window it exists for whichever decision the bridge makes. The
                // acknowledgement it logs is the one that went to the closed channel, since the assertion below
                // shows the recovery had not started, and therefore no replacement channel existed yet.
                await().atMost(Duration.ofSeconds(4)).until(() -> bridgeLog.list.stream().anyMatch(event ->
                        event.getFormattedMessage().contains("dropped before delivery tag")
                                || event.getFormattedMessage().contains("failed outside this bridge's delivery failure policy")));
                assertThat(recoveryStarted.getCount())
                        .as("the projection must have finished and its acknowledgement been tried before the recovery started")
                        .isOne();

                assertThat(recoveryComplete.await(30, TimeUnit.SECONDS)).isTrue();
                publish("order-2");

                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-2")));
            } finally {
                releaseOrder1.countDown();
                bridgeLogger.detachAppender(bridgeLog);
            }
        }
    }

    private void forceCloseAllConnectionsOrFail() throws Exception {
        Container.ExecResult closeResult = rabbitMQContainer.execInContainer(
                "rabbitmqctl", "close_all_connections", "forced-by-connection-recovery-test");
        assertThat(closeResult.getExitCode())
                .as("rabbitmqctl close_all_connections must succeed for this test to force the recovery it exists "
                        + "to exercise; stdout: %s, stderr: %s", closeResult.getStdout(), closeResult.getStderr())
                .isZero();
    }

    private void publish(String orderId) throws Exception {
        CloudEvent cloudEvent = new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced(orderId));
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        adminChannel.basicPublish(exchange, TestOrderPlaced.class.getName(), properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch reached within the timeout").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
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
