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

package org.occurrent.springboot.broker.kafka.blocking.domain;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.broker.kafka.blocking.domain.KafkaDomainEventBridge;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.springboot.broker.kafka.blocking.EnableOccurrentKafkaBroker;
import org.occurrent.springboot.broker.kafka.blocking.OccurrentKafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the domain-level half of the Kafka broker auto-configuration against a real Kafka. An auto-configured
 * {@code KafkaDomainEventSink} publishes a domain event, and a factory-built {@code KafkaDomainEventBridge} feeds
 * it into a {@code DomainEventFeed} projection.
 */
@Testcontainers
class KafkaDomainBrokerAutoConfigurationIntegrationTest {

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:" + kafkaVersion()).withReuse(true);

    @Test
    @SuppressWarnings("unchecked")
    void domain_level_round_trip_through_the_auto_configured_sink_and_bridge() throws Exception {
        String topic = "test-topic-" + UUID.randomUUID();
        CloudEventConverter<TestOrderPlaced> converter = new TestOrderPlacedConverter();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentKafkaAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
                .withBean(CloudEventConverter.class, () -> converter)
                .withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=" + kafkaContainer.getBootstrapServers(),
                        "occurrent.broker.kafka.topic=" + topic,
                        "occurrent.broker.kafka.bridge.poll-timeout=100ms",
                        // This test's consumer group is a fresh UUID with no committed offset, and the publish
                        // below can land before the bridge's own subscription finishes joining the group. Kafka's
                        // own default, auto.offset.reset=latest, would then skip straight past that already
                        // published record instead of ever seeing it.
                        "occurrent.broker.kafka.consumer.additional-properties[auto.offset.reset]=earliest"
                )
                .run(context -> {
                    DomainEventSink<TestOrderPlaced> sink = context.getBean(DomainEventSink.class);
                    KafkaDomainEventBridgeFactory bridgeFactory = context.getBean(KafkaDomainEventBridgeFactory.class);

                    Map<String, String> orderStatusViews = new ConcurrentHashMap<>();
                    DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), converter, TestOrderPlaced::orderId);
                    feed.register("test-domain-projection",
                            Projection.<String, TestOrderPlaced, String>builder(null)
                                    .id(TestOrderPlaced::orderId)
                                    .on(TestOrderPlaced.class, (state, metadata, event) -> "PLACED")
                                    .build(),
                            ViewStateRepository.create(orderStatusViews::get, orderStatusViews::put));

                    KafkaDomainEventBridge<TestOrderPlaced> bridge = bridgeFactory.forGroup("test-group-" + UUID.randomUUID(), feed).build();
                    try {
                        // register(...) alone does not reach live. isReadyForLiveDelivery() (what the bridge's
                        // coarse lifecycle gate polls) only answers true once the one-time catch-up below has
                        // actually completed.
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

    private static String kafkaVersion() {
        String version = System.getProperty("test.kafka.version");
        return version == null || version.isBlank() ? "4.1.0" : version.trim();
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

        @Override
        public String getCloudEventType(Class<? extends TestOrderPlaced> type) {
            return "test.order.placed";
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentKafkaBroker
    static class EnabledConfiguration {
    }
}
