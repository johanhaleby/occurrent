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
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.RecordedConsumer;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryFilter;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.StartAt;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@link RabbitMqCloudEventSource} consumes again after its connection has recovered automatically, whether or not
 * the client's topology recovery is turned on. With it turned off the client does not register the consumer on the
 * recovered channel, so the source has to register a new one itself. With it turned on the client does, and the source
 * must not add a second one next to it.
 * <p>
 * Its own container, because {@code rabbitmqctl close_all_connections} closes every connection on the broker. The
 * source's connection is the only one that recovers. The test publishes and inspects the queue on short-lived
 * connections of its own, so none of that depends on the timing of a recovery. Fails outright, rather than skipping
 * quietly, when the close command reports a non-zero exit code.
 */
@Testcontainers
class RabbitMqCloudEventSourceConsumerRecoveryTest {

    private static final String TYPE = "org.occurrent.test.OrderPlaced";

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
            channel.queueBind(queue, exchange, TYPE);
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

        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<String> received = new CopyOnWriteArrayList<>();
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> received.add(ce.getId()));

        try (Connection sourceConnection = sourceConnectionFactory.newConnection();
             RabbitMqCloudEventSource ignored = RabbitMqCloudEventSource.builder(sourceConnection, model, queue)
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
            publish("before");
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(received).contains("before"));

            forceCloseAllConnectionsOrFail();
            assertThat(recovered.await(30, TimeUnit.SECONDS)).as("connection recovered").isTrue();
            publish("after");

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).contains("after"));
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(consumerCount()).isOne());
            // A record left behind would hold this source's callbacks and be registered again by a later recovery
            assertThat(((AutorecoveringConnection) sourceConnection).getRecordedConsumers()).hasSize(1);
        }
    }

    /**
     * Topology recovery registers every consumer it found recorded when it started, so a consumer this source cancels
     * after that point comes back. A filter that holds the recovery right after it has read the records is what lets
     * a poll that pauses the source run in exactly that window, every time.
     */
    @Test
    void a_source_paused_while_topology_recovery_registers_its_consumer_again_cancels_that_consumer_and_registers_one_when_it_resumes() throws Exception {
        AtomicBoolean holdConsumerRecovery = new AtomicBoolean();
        CountDownLatch consumerRecoveryHeld = new CountDownLatch(1);
        CountDownLatch releaseConsumerRecovery = new CountDownLatch(1);
        ConnectionFactory sourceConnectionFactory = plainConnectionFactory();
        sourceConnectionFactory.setAutomaticRecoveryEnabled(true);
        sourceConnectionFactory.setTopologyRecoveryEnabled(true);
        sourceConnectionFactory.setNetworkRecoveryInterval(500);
        sourceConnectionFactory.setTopologyRecoveryFilter(new TopologyRecoveryFilter() {
            @Override
            public boolean filterConsumer(RecordedConsumer recordedConsumer) {
                if (holdConsumerRecovery.get()) {
                    consumerRecoveryHeld.countDown();
                    awaitUninterruptibly(releaseConsumerRecovery);
                }
                return true;
            }
        });

        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        List<String> received = new CopyOnWriteArrayList<>();
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> received.add(ce.getId()));
        AtomicBoolean ready = new AtomicBoolean(true);
        AtomicInteger readinessChecks = new AtomicInteger();

        try (Connection sourceConnection = sourceConnectionFactory.newConnection();
             RabbitMqCloudEventSource ignored = RabbitMqCloudEventSource.builder(sourceConnection, model, queue)
                     .declareTopology(false)
                     .pollInterval(Duration.ofMillis(100))
                     .readinessSource(subscriptionId -> {
                         readinessChecks.incrementAndGet();
                         return ready.get();
                     })
                     .build()) {
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
            publish("before");
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(received).contains("before"));

            holdConsumerRecovery.set(true);
            forceCloseAllConnectionsOrFail();
            assertThat(consumerRecoveryHeld.await(30, TimeUnit.SECONDS)).as("topology recovery reached the consumer").isTrue();
            ready.set(false);
            int checksBeforePause = readinessChecks.get();
            // The polls run one after the other, so the second one to start after the pause means the first has finished
            await().atMost(Duration.ofSeconds(10)).until(() -> readinessChecks.get() >= checksBeforePause + 2);
            releaseConsumerRecovery.countDown();
            assertThat(recovered.await(30, TimeUnit.SECONDS)).as("connection recovered").isTrue();

            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(consumerCount()).as("consumers while paused").isZero());
            ready.set(true);
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(consumerCount()).as("consumers after resuming").isOne());
        } finally {
            releaseConsumerRecovery.countDown();
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }


    private int consumerCount() throws Exception {
        try (Connection admin = plainConnectionFactory().newConnection(); Channel channel = admin.createChannel()) {
            return channel.queueDeclarePassive(queue).getConsumerCount();
        }
    }

    private void publish(String id) throws Exception {
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(TYPE)
                .withExtension("streamid", "stream-1")
                .build();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, Map.of());
        try (Connection admin = plainConnectionFactory().newConnection(); Channel channel = admin.createChannel()) {
            channel.basicPublish(exchange, TYPE, properties, RabbitMqCloudEventMapper.toBody(cloudEvent));
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
}
