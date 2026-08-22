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

        CountDownLatch recoveryComplete = new CountDownLatch(1);
        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", event -> {
            handled.add(event);
            if (event.orderId().equals("order-2")) {
                // Held until the recovery listener below has finished, so the delivery this projection is handling
                // is still in flight when the last recovery listener runs. A bridge that invalidated a delivery
                // tag from a recovery listener would decide this delivery's fate after that, and drop it.
                awaitLatch(recoveryComplete);
            }
        }, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        // Registered before the bridge is built, and recovery listeners run in registration order, so this one
        // holds every later listener back for two seconds after the recovered consumer has already been handed the
        // next message.
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

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection, feed, queue)
                .declareTopology(false)
                .pollInterval(Duration.ofSeconds(2))
                .build()) {
            publish("order-1");
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-1")));

            forceCloseAllConnectionsOrFail();
            await().atMost(Duration.ofSeconds(20)).until(() -> connection.isOpen());

            publish("order-2");
            publish("order-3");

            // order-3 is what this test is really after. At the default prefetch of one, an order-2 left
            // unacknowledged means order-3 is never delivered at all.
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-3")));
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

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
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
