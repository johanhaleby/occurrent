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
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@link RabbitMqDomainEventSource} consumes again after its connection has recovered automatically, whether or not
 * the client's topology recovery is turned on. With it turned off the client does not register the consumer on the
 * recovered channel, so the source has to register a new one itself. With it turned on the client does, and the source
 * must not add a second one next to it. This is the domain-feed twin of
 * {@code RabbitMqCloudEventSourceConsumerRecoveryTest}.
 * <p>
 * Its own container, because {@code rabbitmqctl close_all_connections} closes every connection on the broker. The
 * source's connection is the only one that recovers. The test publishes and inspects the queue on short-lived
 * connections of its own, so none of that depends on the timing of a recovery. Fails outright, rather than skipping
 * quietly, when the close command reports a non-zero exit code.
 */
@Testcontainers
class RabbitMqDomainEventSourceConsumerRecoveryTest {

    @org.testcontainers.junit.jupiter.Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion());

    private String exchange;
    private String queue;

    @BeforeEach
    void declareScratchExchangeAndQueue() throws Exception {
        exchange = "test-exchange-" + UUID.randomUUID();
        queue = "test-queue-" + UUID.randomUUID();
        try (Connection admin = plainConnectionFactory().newConnection(); Channel channel = admin.createChannel()) {
            channel.exchangeDeclare(exchange, "topic", false, true, null);
            channel.queueDeclare(queue, false, false, false, null);
            channel.queueBind(queue, exchange, TestOrderPlaced.class.getName());
        }
    }

    @Test
    void a_source_registers_a_new_consumer_after_a_recovery_that_did_not_recover_topology() throws Exception {
        consumesAgainAfterForcedRecovery(false);
    }

    @Test
    void a_source_keeps_the_consumer_the_client_recovered_and_registers_no_second_one_when_topology_recovery_is_on() throws Exception {
        consumesAgainAfterForcedRecovery(true);
    }

    private void consumesAgainAfterForcedRecovery(boolean topologyRecovery) throws Exception {
        ConnectionFactory sourceConnectionFactory = plainConnectionFactory();
        sourceConnectionFactory.setAutomaticRecoveryEnabled(true);
        sourceConnectionFactory.setTopologyRecoveryEnabled(topologyRecovery);
        sourceConnectionFactory.setNetworkRecoveryInterval(500);

        List<TestOrderPlaced> handled = new CopyOnWriteArrayList<>();
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        feed.register("proj", handled::add, Filter.type(TestOrderPlaced.class.getName()));
        feed.goLive("proj");

        try (Connection sourceConnection = sourceConnectionFactory.newConnection();
             RabbitMqDomainEventSource<TestOrderPlaced> ignored = RabbitMqDomainEventSource.builder(sourceConnection, feed, queue)
                     .declareTopology(false)
                     .pollInterval(Duration.ofMillis(200))
                     .build()) {
            // Registered after the source, so it runs after the source's own listener has judged the recovery.
            CountDownLatch recovered = new CountDownLatch(1);
            ((Recoverable) sourceConnection).addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    recovered.countDown();
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                }
            });
            publish("order-1");
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-1")));

            forceCloseAllConnectionsOrFail();
            assertThat(recovered.await(30, TimeUnit.SECONDS)).as("connection recovered").isTrue();
            publish("order-2");

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(handled).contains(new TestOrderPlaced("order-2")));
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(consumerCount()).isOne());
            // A record left behind would hold this source's callbacks and be registered again by a later recovery
            assertThat(((AutorecoveringConnection) sourceConnection).getRecordedConsumers()).hasSize(1);
        }
    }

    private int consumerCount() throws Exception {
        try (Connection admin = plainConnectionFactory().newConnection(); Channel channel = admin.createChannel()) {
            return channel.queueDeclarePassive(queue).getConsumerCount();
        }
    }

    private void publish(String orderId) throws Exception {
        CloudEvent cloudEvent = new TestOrderPlacedConverter().toCloudEvent(new TestOrderPlaced(orderId));
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        try (Connection admin = plainConnectionFactory().newConnection(); Channel channel = admin.createChannel()) {
            channel.basicPublish(exchange, TestOrderPlaced.class.getName(), properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
        }
    }

    private void forceCloseAllConnectionsOrFail() throws Exception {
        Container.ExecResult closeResult = rabbitMQContainer.execInContainer(
                "rabbitmqctl", "close_all_connections", "forced-by-consumer-recovery-test");
        assertThat(closeResult.getExitCode())
                .as("rabbitmqctl close_all_connections must succeed for this test to force the recovery it exists "
                        + "to exercise; stdout: %s, stderr: %s", closeResult.getStdout(), closeResult.getStderr())
                .isZero();
    }

    private static ConnectionFactory plainConnectionFactory() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        connectionFactory.setAutomaticRecoveryEnabled(false);
        return connectionFactory;
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
