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

package org.occurrent.springboot.broker.kafka.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventBridge;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventSink;
import org.occurrent.broker.kafka.blocking.RoutingOutcomeChannel;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the auto-configured sink and a factory-built bridge round-trip a real event through a real Kafka, the
 * same loop the RabbitMQ starter's own integration test proves, driven here by properties and the
 * auto-configuration beans instead of manual builder calls.
 */
@Testcontainers
class KafkaBrokerAutoConfigurationIntegrationTest {

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:" + kafkaVersion()).withReuse(true);

    @Test
    void cloud_event_level_round_trip_through_the_auto_configured_sink_and_bridge() {
        String topic = "test-topic-" + UUID.randomUUID();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OccurrentKafkaAutoConfiguration.class))
                .withUserConfiguration(EnabledConfiguration.class)
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
                    KafkaCloudEventSink sink = context.getBean(KafkaCloudEventSink.class);
                    KafkaCloudEventBridgeFactory bridgeFactory = context.getBean(KafkaCloudEventBridgeFactory.class);

                    BlockingQueue<CloudEvent> received = new LinkedBlockingQueue<>();
                    RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
                    PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
                    model.subscribe("test-subscription", received::add);

                    KafkaCloudEventBridge bridge = bridgeFactory.forGroup("test-group-" + UUID.randomUUID(), model, outcomeChannel).build();
                    try {
                        CloudEvent event = CloudEventBuilder.v1()
                                .withId(UUID.randomUUID().toString())
                                .withSource(URI.create("urn:occurrent:test"))
                                .withType("test.event")
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

    private static String kafkaVersion() {
        String version = System.getProperty("test.kafka.version");
        return version == null || version.isBlank() ? "4.1.0" : version.trim();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentKafkaBroker
    static class EnabledConfiguration {
    }
}
