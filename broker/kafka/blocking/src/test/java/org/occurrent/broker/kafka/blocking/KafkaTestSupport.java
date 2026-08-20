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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
}
