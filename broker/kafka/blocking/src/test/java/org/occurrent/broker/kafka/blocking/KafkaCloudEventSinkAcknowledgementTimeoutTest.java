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
 * {@link KafkaCloudEventSink#publish(CloudEvent)} has two distinct ways to be unable to confirm a send against a
 * broker that has gone away, and both are meant to report {@link KafkaPublishTimeoutException} within the
 * configured acknowledgement timeout, not just one of them. A broker that goes away between two publishes on the
 * same producer, the case this test forces by warming the producer up with one successful publish and only then
 * stopping the container, makes the second {@code send} itself block waiting for a fresh view of the cluster,
 * which is exactly the case {@link KafkaCloudEventSink.Builder#build()}'s forced {@code max.block.ms} exists to
 * bound. Before that fix this test hung for a full minute on CI, the {@code send} call blocking on Kafka's own
 * default well past the one second {@code acknowledgementTimeout} configured below, and on every retry after that.
 * <p>
 * This container is a JUnit-managed, per-method instance field rather than the shared static one
 * {@link KafkaTestSupport} declares, since it is stopped mid-test and must not affect any other test.
 */
@Testcontainers
class KafkaCloudEventSinkAcknowledgementTimeoutTest {

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:" + KafkaTestSupport.kafkaVersion());

    @Test
    @Timeout(20)
    void an_expired_acknowledgement_wait_fails_promptly_and_is_excluded_from_the_default_retry() throws Exception {
        String topic = "test-topic";
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()))) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }

        KafkaDestination destination = KafkaDestination.of(topic);
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                // Bounds how long the Kafka client itself keeps retrying a batch once the broker is gone, so this
                // sink's close() below returns promptly instead of waiting out the default two-minute delivery
                // timeout. max.block.ms is deliberately not set here, since Builder.build() is the thing under
                // test for forcing it down to acknowledgementTimeout.
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
                    .as("KafkaPublishTimeoutException is excluded from the default retry, and the forced " +
                            "max.block.ms bounds send() itself the same way, so this should return close to the " +
                            "one second acknowledgementTimeout rather than after several backed-off attempts or a " +
                            "send() blocked on Kafka's own much longer default")
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
