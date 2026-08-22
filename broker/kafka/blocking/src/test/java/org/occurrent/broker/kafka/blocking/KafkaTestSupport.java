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
import io.cloudevents.kafka.KafkaMessageFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A single-node Kafka container, the native KRaft {@code apache/kafka} image
 * ({@link org.testcontainers.kafka.KafkaContainer}, not the legacy Confluent-based one), plus a fresh scratch topic
 * for each test method, torn down when the test ends. Shared by the sink tests in this package and in
 * {@code .domain} rather than duplicated in each of them, which is why this and its members are
 * {@code public}/{@code protected} rather than package-private.
 */
@Testcontainers
public abstract class KafkaTestSupport {

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:" + kafkaVersion()).withReuse(true);

    private AdminClient adminClient;

    protected String topic;

    @BeforeEach
    protected void createScratchTopic() throws Exception {
        adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()));
        topic = "test-topic-" + UUID.randomUUID();
        adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }

    @AfterEach
    protected void deleteScratchTopicAndCloseAdminClient() throws Exception {
        adminClient.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        adminClient.close();
    }

    protected String bootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    /**
     * Polls {@code recordTopic} with a fresh, uniquely-grouped consumer starting from the earliest offset, and
     * asserts exactly one record is already there. Used to prove a publish did not return before the record was
     * durably on the topic, with no polling loop needed on the caller's side.
     */
    protected ConsumerRecord<String, byte[]> consumeOneRecord(String recordTopic) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + recordTopic,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(recordTopic));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).as("exactly one record should already be on \"" + recordTopic + "\"").isEqualTo(1);
            return records.iterator().next();
        }
    }

    protected static String headerValue(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertThat(header).as("header \"" + key + "\"").isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Polls {@code recordTopic} with a fresh, uniquely-grouped consumer starting from the earliest offset, and
     * asserts exactly {@code count} records are already there, in the order the broker returns them. Unlike
     * {@link #consumeOneRecord(String)}, meant for a topic more than one record can land on, {@code count} greater
     * than one included.
     */
    protected List<ConsumerRecord<String, byte[]>> consumeRecords(String recordTopic, int count) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + recordTopic,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(recordTopic));
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).as("exactly " + count + " record(s) should already be on \"" + recordTopic + "\"").isEqualTo(count);
            List<ConsumerRecord<String, byte[]>> result = new ArrayList<>();
            records.forEach(result::add);
            return result;
        }
    }

    /**
     * The {@code test.kafka.version} system property Surefire is configured to pass, the same way
     * {@code test.rabbitmq.version} already works for the RabbitMQ container. Falls back to a literal for an IDE
     * run, where nothing sets it.
     */
    protected static String kafkaVersion() {
        String version = System.getProperty("test.kafka.version");
        return version == null || version.isBlank() ? "4.1.0" : version.trim();
    }

    /**
     * Creates an additional topic with {@code partitionCount} partitions, beyond the one-partition scratch topic
     * {@link #topic} already is, for a test proving cross-partition independence. Not torn down automatically.
     * Callers that need cleanup do it themselves, mirroring how {@link #topic} is the only topic this class manages
     * the lifecycle of.
     */
    protected String createTopic(int partitionCount) throws Exception {
        String name = "test-topic-" + UUID.randomUUID();
        createNamedTopic(name, partitionCount);
        return name;
    }

    /**
     * As {@link #createTopic(int)}, with an explicit {@code name} rather than a generated one, for a test that
     * needs control over the name itself, a shared prefix for a topic-per-type resolver's pattern subscription
     * among them.
     */
    protected void createNamedTopic(String name, int partitionCount) throws Exception {
        adminClient.createTopics(List.of(new NewTopic(name, partitionCount, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }

    /**
     * Deletes a topic {@link #createTopic(int)} or {@link #createNamedTopic(String, int)} created, for a test that
     * needs more than the one scratch topic {@link #topic} already is (which {@link #deleteScratchTopicAndCloseAdminClient()}
     * tears down on its own). Best effort. A failure here is swallowed rather than failing the test, since it only
     * ever runs during cleanup.
     */
    protected void deleteTopic(String name) {
        try {
            adminClient.deleteTopics(List.of(name)).all().get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
    }

    /**
     * Reads back the offset {@code groupId} has committed for {@code topicPartition}, through
     * {@link AdminClient#listConsumerGroupOffsets(String)}, which reads a group's committed offsets without
     * joining it. {@code null} when nothing has been committed yet. This is how every commit claim in the bridge
     * tests is proven. A committed offset read back from the broker, never the absence of a record arriving
     * somewhere else, which on Kafka proves nothing on its own unless bounded by something observable.
     * <p>
     * Deliberately not a throwaway {@code KafkaConsumer.committed(...)} call built with its own {@code group.id}.
     * That reads the offsets of whatever group the consumer itself belongs to, not an arbitrary group passed as an
     * argument, so a consumer configured with a synthetic id of its own would silently read the wrong group's
     * offsets, or none at all, rather than {@code groupId}'s.
     */
    protected @Nullable Long committedOffset(String groupId, TopicPartition topicPartition) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = adminClient.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
        OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
        return offsetAndMetadata == null ? null : offsetAndMetadata.offset();
    }

    /**
     * The number of members currently in consumer group {@code groupId}, read through {@link AdminClient#describeConsumerGroups}.
     * Used to prove a bridge's permanent stop leaves the group immediately by closing its own {@code Consumer},
     * rather than waiting to be evicted at {@code max.poll.interval.ms}.
     */
    protected int consumerGroupMemberCount(String groupId) throws Exception {
        ConsumerGroupDescription description = adminClient.describeConsumerGroups(List.of(groupId))
                .describedGroups().get(groupId).get(30, TimeUnit.SECONDS);
        return description.members().size();
    }

    /**
     * Publishes {@code cloudEvent} to {@code recordTopic} in the CloudEvents Kafka binary content mode, keyed by
     * {@code key}, waiting for the broker's acknowledgement before returning. A lean direct producer rather than
     * going through {@link KafkaCloudEventSink}, the same directness {@code RabbitMqCloudEventBridgeTest.publish(...)}
     * uses on the RabbitMQ side, so a bridge test controls exactly what lands on the wire without a sink's own
     * retry and validation machinery in the way.
     */
    protected void publishCloudEvent(String recordTopic, @Nullable String key, CloudEvent cloudEvent) {
        publishCloudEvent(recordTopic, null, key, cloudEvent);
    }

    /**
     * As {@link #publishCloudEvent(String, String, CloudEvent)}, pinned to an explicit {@code partition} rather
     * than letting the producer's own partitioner place it by key, for a test proving cross-partition independence
     * where which partition an event lands on has to be controlled precisely.
     */
    protected void publishCloudEvent(String recordTopic, @Nullable Integer partition, @Nullable String key, CloudEvent cloudEvent) {
        publishCloudEvent(recordTopic, partition, key, cloudEvent, Map.of());
    }

    /**
     * As {@link #publishCloudEvent(String, Integer, String, CloudEvent)}, with {@code extraHeaders} added on top of
     * whatever headers {@link KafkaMessageFactory} writes for {@code cloudEvent} itself, for a test proving a
     * header outside the CloudEvents mapping survives wherever the record it rode in on ends up.
     */
    protected void publishCloudEvent(String recordTopic, @Nullable Integer partition, @Nullable String key, CloudEvent cloudEvent,
                                      Map<String, String> extraHeaders) {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfig, new StringSerializer(), new ByteArraySerializer())) {
            ProducerRecord<String, byte[]> record = KafkaMessageFactory
                    .<String>createWriter(recordTopic, partition, null, key)
                    .writeBinary(cloudEvent);
            extraHeaders.forEach((headerKey, value) -> record.headers().add(headerKey, value.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish a CloudEvent to \"" + recordTopic + "\" for a test", e);
        }
    }

    /**
     * Publishes {@code body} to {@code recordTopic} unchanged, with no CloudEvents headers at all, for a test
     * proving how a bridge handles a record it cannot rebuild as a {@link CloudEvent}.
     */
    protected void publishRaw(String recordTopic, @Nullable String key, byte[] body) {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerConfig, new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(recordTopic, key, body)).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish a raw record to \"" + recordTopic + "\" for a test", e);
        }
    }
}
