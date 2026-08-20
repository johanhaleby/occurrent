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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.SubscriptionFilter;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
     * The property {@link KafkaSharedTopicDestinationResolver} exists to deliver. Two different event types of the
     * same stream, keyed identically because they share a {@code streamid}, land on the exact same partition of
     * the one shared topic, in the order they were published. Created with several partitions so this is not true
     * by accident, the way it would be on {@link KafkaTestSupport}'s own single-partition scratch topic.
     * {@link KafkaTopicPerTypeDestinationResolver} could never make this claim, since two types never even share a
     * topic to begin with, let alone a partition.
     */
    @Test
    void two_event_types_of_the_same_stream_land_on_one_partition_in_publish_order() throws Exception {
        String sharedTopic = topic + "-shared";
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            adminClient.createTopics(List.of(new NewTopic(sharedTopic, 6, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
        KafkaSharedTopicDestinationResolver resolver = new KafkaSharedTopicDestinationResolver(sharedTopic);
        CloudEvent first = CloudEventBuilder.v1()
                .withId("id-first")
                .withSource(URI.create("urn:test"))
                .withType(EventA.class.getName())
                .withExtension("streamid", "stream-1")
                .build();
        CloudEvent second = CloudEventBuilder.v1()
                .withId("id-second")
                .withSource(URI.create("urn:test"))
                .withType(EventB.class.getName())
                .withExtension("streamid", "stream-1")
                .build();

        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, resolver).build()) {
            sink.publish(first);
            sink.publish(second);
        }

        List<ConsumerRecord<String, byte[]>> records = consumeRecords(sharedTopic, 2);
        assertThat(records.get(0).partition())
                .as("both events share a streamid, so they must land on the same partition of the shared topic")
                .isEqualTo(records.get(1).partition());
        assertThat(headerValue(records.get(0), "ce_id")).isEqualTo("id-first");
        assertThat(headerValue(records.get(1), "ce_id")).isEqualTo("id-second");
        assertThat(records.get(0).offset())
                .as("the first published event must have the lower offset on that shared partition")
                .isLessThan(records.get(1).offset());
    }

    /**
     * A record too large for {@code max.request.size} is a permanent failure, since resending the exact same
     * record can never make it smaller, proved here against the real client, that it comes back as
     * {@link KafkaPublishException} rather than a timeout. Whether the default {@link org.occurrent.retry.RetryStrategy}
     * actually honours that classification and never retries it is proved deterministically instead, by counting
     * {@code send} invocations against a mocked {@link org.apache.kafka.clients.producer.Producer}, in
     * {@code KafkaCloudEventSinkRetryTest.a_permanent_failure_is_not_retried}. A real broker's own attempt latency
     * varies with load, so timing this end to end could only ever prove absence of a retry by a wall-clock margin
     * wide enough to swallow that variance, which is what flaked under CI load rather than what this test is
     * actually about.
     */
    @Test
    void a_record_too_large_for_max_request_size_is_reported_as_a_permanent_failure() {
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

            assertThatThrownBy(() -> sink.publish(oversizedEvent))
                    .isInstanceOf(KafkaPublishException.class)
                    .isNotInstanceOf(KafkaPublishTimeoutException.class);
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

    /**
     * A transactional id puts the producer in transactional mode, which requires {@code initTransactions()} and
     * the rest of that lifecycle before any {@code send()}. This sink never calls it, so every publish would be
     * rejected or withheld indefinitely by a producer configured this way, and unlike
     * {@code partitioner.ignore.keys} there is no legitimate reason to combine the two.
     */
    @Test
    void build_refuses_when_producerConfig_sets_a_transactional_id() {
        KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic, ReflectionCloudEventTypeMapper.qualified());
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some-transactional-id");

        assertThatThrownBy(() -> KafkaCloudEventSink.builder(producerConfig, resolver).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
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

    /**
     * {@link KafkaOrderingPrerequisites} is the pure predicate, exhaustively tested on its own in
     * {@code KafkaOrderingPrerequisitesTest} including per-leg mutation coverage. This class only proves the
     * wiring, that {@link KafkaCloudEventSink.Builder#build()} actually calls the predicate against the
     * {@code producerConfig} it was given and logs a warning rather than refusing to build, the way it refuses a
     * weaker {@code acks}.
     */
    @Nested
    class OrderingPrerequisiteWarning {

        private ListAppender<ILoggingEvent> appender;
        private ch.qos.logback.classic.Logger logger;

        @BeforeEach
        void attachAppender() {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            logger = context.getLogger(KafkaCloudEventSink.class);
            appender = new ListAppender<>();
            appender.start();
            logger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            logger.detachAppender(appender);
        }

        @Test
        void build_warns_when_producerConfig_breaks_an_ordering_prerequisite() {
            KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic, ReflectionCloudEventTypeMapper.qualified());
            Map<String, Object> producerConfig = Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                    "partitioner.ignore.keys", "true");

            try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, resolver).build()) {
                assertThat(sink).isNotNull();
            }

            assertThat(appender.list)
                    .filteredOn(event -> event.getLevel() == Level.WARN)
                    .anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("partitioner.ignore.keys"));
        }

        @Test
        void build_does_not_warn_when_producerConfig_keeps_both_ordering_prerequisites() {
            KafkaTopicPerTypeDestinationResolver resolver = new KafkaTopicPerTypeDestinationResolver(topic, ReflectionCloudEventTypeMapper.qualified());
            Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());

            try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, resolver).build()) {
                assertThat(sink).isNotNull();
            }

            assertThat(appender.list).noneSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.WARN));
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
