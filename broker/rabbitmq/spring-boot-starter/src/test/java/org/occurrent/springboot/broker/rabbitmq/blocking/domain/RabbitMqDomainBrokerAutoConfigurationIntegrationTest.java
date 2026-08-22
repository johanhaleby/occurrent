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

package org.occurrent.springboot.broker.rabbitmq.blocking.domain;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridge;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.springboot.broker.rabbitmq.blocking.EnableOccurrentRabbitMqBroker;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the domain-level half of the RabbitMQ broker auto-configuration against a real RabbitMQ. An
 * auto-configured {@code RabbitMqDomainEventSink} publishes a domain event, and a factory-built
 * {@code RabbitMqDomainEventBridge} feeds it into a {@code DomainEventFeed} projection.
 */
@Testcontainers
class RabbitMqDomainBrokerAutoConfigurationIntegrationTest {

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
    @SuppressWarnings("unchecked")
    void domain_level_round_trip_through_the_auto_configured_sink_and_bridge() throws Exception {
        CloudEventConverter<TestOrderPlaced> converter = new TestOrderPlacedConverter();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        org.occurrent.springboot.broker.rabbitmq.blocking.OccurrentRabbitMqAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                // No inferred destroy method: com.rabbitmq.client.Connection is itself Closeable, and this
                // connection is this test's own, torn down in closeConnection() below, not the context's to close
                // when the ApplicationContextRunner tears itself down at the end of run(...).
                .withBean(Connection.class, () -> connection, bd -> bd.setDestroyMethodName(""))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withBean(CloudEventConverter.class, () -> converter)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.exchange=" + exchange,
                        "occurrent.broker.rabbitmq.bridge.poll-interval=100ms"
                )
                .run(context -> {
                    DomainEventSink<TestOrderPlaced> sink = context.getBean(DomainEventSink.class);
                    RabbitMqDomainEventBridgeFactory bridgeFactory = context.getBean(RabbitMqDomainEventBridgeFactory.class);

                    Map<String, String> orderStatusViews = new ConcurrentHashMap<>();
                    DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), converter, TestOrderPlaced::orderId);
                    feed.register("test-domain-projection",
                            Projection.<String, TestOrderPlaced, String>builder(null)
                                    .id(TestOrderPlaced::orderId)
                                    .on(TestOrderPlaced.class, (state, metadata, event) -> "PLACED")
                                    .build(),
                            ViewStateRepository.create(orderStatusViews::get, orderStatusViews::put));

                    RabbitMqDomainEventBridge<TestOrderPlaced> bridge = bridgeFactory.forQueue("test-domain-queue-" + UUID.randomUUID(), feed).build();
                    try {
                        // register(...) alone does not reach live. isReadyForLiveDelivery() (what the bridge's
                        // coarse lifecycle gate polls) only answers true once the one-time catch-up below has
                        // actually completed, the same order RabbitMqDomainEventLevelBootstrap follows.
                        feed.catchUp("test-domain-projection");

                        String orderId = "order-" + UUID.randomUUID();
                        sink.publish(new TestOrderPlaced(orderId));

                        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                                assertThat(orderStatusViews.get(orderId)).isEqualTo("PLACED"));
                    } finally {
                        bridge.close();
                    }
                });
    }

    private static String rabbitMqVersion() {
        String version = System.getProperty("test.rabbitmq.version");
        return version == null || version.isBlank() ? "4.1" : version.trim();
    }

    record TestOrderPlaced(String orderId) {
    }

    private static final class TestOrderPlacedConverter implements CloudEventConverter<TestOrderPlaced> {
        @Override
        public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(getCloudEventType(TestOrderPlaced.class))
                    .withData(domainEvent.orderId().getBytes())
                    .build();
        }

        @Override
        public TestOrderPlaced toDomainEvent(CloudEvent cloudEvent) {
            byte[] data = cloudEvent.getData() == null ? new byte[0] : cloudEvent.getData().toBytes();
            return new TestOrderPlaced(new String(data));
        }

        // The fully qualified class name, not an arbitrary string. ReflectionCloudEventTypeMapper.qualified(),
        // configured on the auto-configured resolver this sink publishes through, round-trips this type through
        // Class.forName(...) on every publish, and a name it cannot resolve throws immediately rather than
        // delivering the event.
        @Override
        public String getCloudEventType(Class<? extends TestOrderPlaced> type) {
            return TestOrderPlaced.class.getName();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }
}
