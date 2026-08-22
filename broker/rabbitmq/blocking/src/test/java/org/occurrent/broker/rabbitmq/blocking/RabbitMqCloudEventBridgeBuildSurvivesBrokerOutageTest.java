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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The scenario #867 is about: a broker that is briefly unreachable while {@link RabbitMqCloudEventBridge.Builder#build()}
 * runs must not fail startup outright. {@code RabbitMqCloudEventBridgeBuildRetryTest} proves the retry's bound and
 * its transient-versus-permanent predicate precisely, against a mocked {@link Connection}, since a real broker
 * cannot be made to fail channel creation an exact, chosen number of times. This file proves the same retry end to
 * end against a real broker: an already-established {@link Connection} whose broker becomes briefly unreachable
 * exactly while {@code build()} is running, the same technique and the same {@code rabbitmqctl close_all_connections}
 * {@code RabbitMqCloudEventBridgeConnectionRecoveryTest} uses to force a drop, timed here to land before
 * {@code build()} starts instead of after it finishes, so the very first attempt is guaranteed to see a connection
 * that is not currently open.
 */
@Testcontainers
class RabbitMqCloudEventBridgeBuildSurvivesBrokerOutageTest {

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
    void build_survives_a_broker_that_is_down_when_it_starts_and_comes_up_while_it_retries() throws Exception {
        String queue = "test-queue-" + UUID.randomUUID();
        adminChannel.queueDeclare(queue, false, false, false, null);
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        List<String> received = new CopyOnWriteArrayList<>();
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), ce -> received.add(ce.getId()));

        // Force the connection down before build() ever runs, then confirm the client has actually noticed, so
        // the first attempt is guaranteed to see a connection that is not currently open rather than racing a
        // close signal that has not arrived yet.
        forceCloseAllConnectionsOrFail();
        await().atMost(Duration.ofSeconds(15)).until(() -> !connection.isOpen());

        // build() runs in the foreground, on the connection this test just forced down. Its default retry (100 ms
        // up to 2 seconds backoff, ten attempts) outlasts the 500 ms automatic recovery interval configured above,
        // so a later attempt lands once the client has reconnected to this same, never-stopped container, and
        // build() returns normally instead of throwing.
        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, queue)
                .declareTopology(false)
                .build()) {
            assertThat(bridge).isNotNull();

            publish(OrderPlaced.class.getName(), "id-1");

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).contains("id-1"));
        }
    }

    private void forceCloseAllConnectionsOrFail() throws Exception {
        Container.ExecResult closeResult = rabbitMQContainer.execInContainer(
                "rabbitmqctl", "close_all_connections", "forced-by-build-survives-broker-outage-test");
        assertThat(closeResult.getExitCode())
                .as("rabbitmqctl close_all_connections must succeed for this test to force the outage it exists "
                        + "to exercise; stdout: %s, stderr: %s", closeResult.getStdout(), closeResult.getStderr())
                .isZero();
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
