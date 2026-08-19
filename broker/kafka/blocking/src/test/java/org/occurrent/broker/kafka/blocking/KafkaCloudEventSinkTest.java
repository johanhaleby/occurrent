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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaCloudEventSinkTest extends KafkaTestSupport {

    /**
     * The invariant that matters: {@code publish} only returns once the broker has acknowledged the send, so by the
     * time it returns the record is already fetchable from the topic, with no polling needed.
     */
    @Test
    void publish_waits_for_the_brokers_acknowledgement_and_the_message_is_already_on_the_topic_when_it_returns() {
        KafkaDestination destination = KafkaDestination.of(topic, "stream-1");
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(destination)).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType(OrderPlaced.class.getName())
                    .withDataContentType("application/json")
                    .withData("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                    .withExtension("streamid", "stream-1")
                    .build();

            sink.publish(cloudEvent);

            ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
            assertThat(new String(record.value(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":42}");
            assertThat(record.key()).isEqualTo("stream-1");
            assertThat(headerValue(record, "content-type")).isEqualTo("application/json");
            assertThat(headerValue(record, "ce_streamid")).isEqualTo("stream-1");
            assertThat(headerValue(record, "ce_id")).isEqualTo("id-1");
            assertThat(headerValue(record, "ce_type")).isEqualTo(cloudEvent.getType());
        }
    }

    @Test
    void publish_merges_application_headers_from_the_destination_alongside_the_cloudevents_ones() {
        KafkaDestination destination = KafkaDestination.of(topic, "stream-1").withHeaders(Map.of("tenant", "acme"));
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(destination)).build()) {
            CloudEvent cloudEvent = orderPlaced("id-2");

            sink.publish(cloudEvent);

            ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
            assertThat(headerValue(record, "tenant")).isEqualTo("acme");
            assertThat(headerValue(record, "ce_id")).isEqualTo("id-2");
        }
    }

    /**
     * The topic name {@link KafkaTopicPerTypeDestinationResolver} derives for a legal cloud event type is not just
     * something this module's own regex accepts, a real broker accepts it too, both to create ahead of time through
     * {@code AdminClient} and to publish a record onto through this sink.
     */
    @Test
    void a_legal_derived_topic_name_is_accepted_by_a_real_broker() throws Exception {
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic + "-", ReflectionCloudEventTypeMapper.qualified());
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId("id-3")
                .withSource(URI.create("urn:test"))
                .withType(EventA.class.getName())
                .build();
        String derivedTopic = resolver.destinationFor(cloudEvent).topic();

        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            adminClient.createTopics(List.of(new NewTopic(derivedTopic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }

        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, resolver).build()) {
            sink.publish(cloudEvent);

            ConsumerRecord<String, byte[]> record = consumeOneRecord(derivedTopic);
            assertThat(headerValue(record, "ce_id")).isEqualTo("id-3");
        }
    }

    /**
     * A record too large for {@code max.request.size} is a permanent failure, since resending the exact same
     * record can never make it smaller. The default {@link org.occurrent.retry.RetryStrategy} must never retry it,
     * or {@link KafkaCloudEventSink#publish(CloudEvent)} would keep retrying into the same failure and never
     * return, which is checked here by asserting the call fails promptly rather than after the default's
     * exponential backoff would have piled up.
     */
    @Test
    void a_permanent_broker_side_failure_is_not_retried_and_publish_returns_promptly() {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100");
        KafkaDestination destination = KafkaDestination.of(topic);
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(destination)).build()) {
            CloudEvent oversizedEvent = CloudEventBuilder.v1()
                    .withId("id-oversized")
                    .withSource(URI.create("urn:test"))
                    .withType(OrderPlaced.class.getName())
                    .withData(new byte[10_000])
                    .build();

            Instant before = Instant.now();
            assertThatThrownBy(() -> sink.publish(oversizedEvent))
                    .isInstanceOf(KafkaPublishException.class)
                    .isNotInstanceOf(KafkaPublishTimeoutException.class);
            Duration elapsed = Duration.between(before, Instant.now());
            assertThat(elapsed)
                    .as("a record too large for max.request.size is never retriable, so this must fail on the " +
                            "first attempt instead of retrying into the default's exponential backoff")
                    .isLessThan(Duration.ofSeconds(1));
        }
    }

    @Test
    void acknowledgementTimeout_refuses_a_duration_that_truncates_to_zero_milliseconds() {
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic, ReflectionCloudEventTypeMapper.qualified());
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        KafkaCloudEventSink.Builder builder = KafkaCloudEventSink.builder(producerConfig, resolver);

        assertThatThrownBy(() -> builder.acknowledgementTimeout(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.acknowledgementTimeout(Duration.ofNanos(500))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void build_refuses_when_acks_is_configured_below_all() {
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic, ReflectionCloudEventTypeMapper.qualified());
        Map<String, Object> producerConfigWithAcksOne = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "1");
        Map<String, Object> producerConfigWithAcksZero = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "0");

        assertThatThrownBy(() -> KafkaCloudEventSink.builder(producerConfigWithAcksOne, resolver).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks");
        assertThatThrownBy(() -> KafkaCloudEventSink.builder(producerConfigWithAcksZero, resolver).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks");
    }

    @Test
    void build_accepts_acks_explicitly_set_to_all_or_its_numeric_equivalent() {
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic, ReflectionCloudEventTypeMapper.qualified());
        Map<String, Object> producerConfigWithAcksAll = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");
        Map<String, Object> producerConfigWithAcksMinusOne = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "-1");

        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfigWithAcksAll, resolver).build()) {
            assertThat(sink).isNotNull();
        }
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfigWithAcksMinusOne, resolver).build()) {
            assertThat(sink).isNotNull();
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

    /**
     * Points every publish at one predetermined destination, so a test can control exactly which topic a record
     * lands on without going through {@link KafkaTopicPerTypeDestinationResolver}'s type-mapper round trip.
     * {@code destinationsFor} and {@code catchAllDestination} are unused by {@link KafkaCloudEventSink}, which only
     * ever calls {@code destinationFor}. Package-private rather than private, since
     * {@code KafkaCloudEventSinkAcknowledgementTimeoutTest} reuses it too.
     */
    record FixedDestinationResolver(KafkaDestination destination) implements DestinationResolver<KafkaDestination> {
        @Override
        public KafkaDestination destinationFor(CloudEvent cloudEvent) {
            return destination;
        }

        @Override
        public Optional<Set<KafkaDestination>> destinationsFor(SubscriptionFilter filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public KafkaDestination catchAllDestination() {
            throw new UnsupportedOperationException();
        }
    }
}
