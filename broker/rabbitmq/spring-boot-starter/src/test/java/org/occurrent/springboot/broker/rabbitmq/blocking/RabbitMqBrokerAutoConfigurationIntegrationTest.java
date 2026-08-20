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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the auto-configured sink and a factory-built bridge round-trip a real event through a real RabbitMQ,
 * the same loop {@code RabbitMqCloudEventLevelBrokerExampleTest} proves for the hand-wired bootstrap, driven here
 * by properties and the auto-configuration beans instead of manual builder calls.
 */
@Testcontainers
class RabbitMqBrokerAutoConfigurationIntegrationTest {

    @Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer("rabbitmq:" + rabbitMqVersion()).withReuse(true);

    private Connection connection;
    private String exchange;

    @BeforeEach
    void openConnectionAndScratchExchange() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQContainer.getAmqpUrl());
        connection = connectionFactory.newConnection();
        exchange = "test-exchange-" + UUID.randomUUID();
        Channel channel = connection.createChannel();
        try {
            channel.exchangeDeclare(exchange, "topic", false, true, null);
        } finally {
            channel.close();
        }
    }

    @AfterEach
    void closeConnection() throws Exception {
        connection.close();
    }

    @Test
    void cloud_event_level_round_trip_through_the_auto_configured_sink_and_bridge() throws Exception {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                // No inferred destroy method: com.rabbitmq.client.Connection is itself Closeable, and this
                // connection is this test's own, torn down in closeConnection() below, not the context's to close
                // when the ApplicationContextRunner tears itself down at the end of run(...).
                .withBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.exchange=" + exchange,
                        "occurrent.broker.rabbitmq.bridge.poll-interval=100ms"
                )
                .run(context -> {
                    RabbitMqCloudEventSink sink = context.getBean(RabbitMqCloudEventSink.class);
                    RabbitMqCloudEventBridgeFactory bridgeFactory = context.getBean(RabbitMqCloudEventBridgeFactory.class);

                    BlockingQueue<CloudEvent> received = new LinkedBlockingQueue<>();
                    RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
                    PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
                    model.subscribe("test-subscription", received::add);

                    RabbitMqCloudEventBridge bridge = bridgeFactory.forQueue("test-queue-" + UUID.randomUUID(), model, outcomeChannel).build();
                    try {
                        // The type has to be a resolvable fully qualified class name, not an arbitrary string:
                        // ReflectionCloudEventTypeMapper.qualified() round-trips it through Class.forName(...) on
                        // every publish, and a name it cannot resolve throws, which the sink's default retry
                        // strategy then retries forever rather than failing this test outright.
                        CloudEvent event = CloudEventBuilder.v1()
                                .withId(UUID.randomUUID().toString())
                                .withSource(URI.create("urn:occurrent:test"))
                                .withType(TestEvent.class.getName())
                                .build();
                        sink.publish(event);

                        CloudEvent delivered = received.poll(15, TimeUnit.SECONDS);
                        assertThat(delivered).isNotNull();
                        assertThat(delivered.getId()).isEqualTo(event.getId());
                        assertThat(delivered.getType()).isEqualTo(event.getType());
                    } finally {
                        bridge.close();
                        model.shutdown();
                    }
                });
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }

    /** Its fully qualified name is the event type published above, see the comment there. */
    private record TestEvent() {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }
}
