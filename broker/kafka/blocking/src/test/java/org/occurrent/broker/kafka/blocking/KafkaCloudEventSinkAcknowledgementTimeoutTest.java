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

package org.occurrent.broker.kafka.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link KafkaCloudEventSink#publish(CloudEvent)} has two distinct ways to fail on an unreachable broker, and only
 * one of them is {@link KafkaPublishTimeoutException}. A broker unreachable from a cold producer fails {@code send}
 * itself, synchronously, with Kafka's own {@code org.apache.kafka.common.errors.TimeoutException} while waiting for
 * topic metadata, which {@link KafkaCloudEventSink} maps to a plain {@link KafkaPublishException} since it is
 * usually transient and the default retry strategy is meant to absorb it. Reaching the acknowledgement-wait timeout
 * this class actually tests needs the send itself to succeed first, meaning the producer already holds the topic's
 * metadata, so the broker is warmed up with one successful publish and only then stopped.
 * <p>
 * This container is a JUnit-managed, per-method instance field rather than the shared static one
 * {@link KafkaTestSupport} declares, since it is stopped mid-test and must not affect any other test.
 */
@Testcontainers
class KafkaCloudEventSinkAcknowledgementTimeoutTest {

    // Deliberately the literal "4.1.0" rather than the test.kafka.version system property every other test in this
    // module reads (see KafkaTestSupport.kafkaVersion()). On this machine, resolving the tag through any method
    // call in this field's own initializer, whether on KafkaTestSupport or on an identical duplicate kept local to
    // this class, was observed to make the container it built intermittently unreachable from a freshly built
    // producer, for an hour of debugging with no exception this test could catch to explain why, while the literal
    // was reliable every time. If integration-tests.kafka.version moves in pom.xml, bump this literal too.
    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:4.1.0");

    @Test
    @Timeout(60)
    void an_expired_acknowledgement_wait_fails_promptly_and_is_excluded_from_the_default_retry() throws Exception {
        String topic = "test-topic";
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()))) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }

        KafkaDestination destination = KafkaDestination.of(topic);
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                // Bounds how long the initial metadata fetch and the client's own internal retries can take, so a
                // broker that is genuinely unreachable fails this test with a clear timeout well inside the 60
                // second budget above, instead of silently eating into it one default-60-second block at a time.
                ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000",
                // Bounds how long the Kafka client itself keeps retrying a batch once the broker is gone, so this
                // sink's close() below (which waits for outstanding sends to resolve) returns promptly instead of
                // waiting out the default two-minute delivery timeout.
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000",
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");

        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, new KafkaCloudEventSinkTest.FixedDestinationResolver(destination))
                .acknowledgementTimeout(Duration.ofSeconds(1))
                .build()) {
            // Warms the producer's metadata cache for this topic while the broker is still up, so the second
            // publish's send() enqueues immediately instead of failing synchronously in the metadata lookup.
            sink.publish(orderPlaced("id-1"));

            kafkaContainer.stop();

            Instant before = Instant.now();
            assertThatThrownBy(() -> sink.publish(orderPlaced("id-2")))
                    .isInstanceOf(KafkaPublishTimeoutException.class);
            Duration elapsed = Duration.between(before, Instant.now());
            assertThat(elapsed)
                    .as("KafkaPublishTimeoutException is excluded from the default retry, so this should return " +
                            "close to the one second acknowledgementTimeout rather than after several backed-off attempts")
                    .isLessThan(Duration.ofSeconds(4));
        }
    }

    private static CloudEvent orderPlaced(String id) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .build();
    }

    private static final class OrderPlaced {
    }
}
