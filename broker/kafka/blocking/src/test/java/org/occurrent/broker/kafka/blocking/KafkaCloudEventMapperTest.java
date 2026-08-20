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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link KafkaCloudEventMapper}'s two corrections on top of {@code cloudevents-kafka}'s own reader: {@code streamversion}
 * and {@code position} come back {@code Long}, and data that is present but empty survives distinctly from no data
 * at all. Most cases build a {@link ConsumerRecord} by hand, exactly matching what {@code cloudevents-kafka}'s
 * binary writer produces, so no broker is needed to prove the reader-side fix. One test anchors the round trip
 * against a record actually produced by {@link KafkaCloudEventSink}, so the write side and the read side are proven
 * against each other rather than only against this test's own idea of what the wire format looks like.
 */
class KafkaCloudEventMapperTest extends KafkaTestSupport {

    private static ConsumerRecord<String, byte[]> record(byte[] value, Map<String, String> ceHeaders) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> header : ceHeaders.entrySet()) {
            headers.add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
        }
        return new ConsumerRecord<>("some-topic", 0, 0L, ConsumerRecord.NO_TIMESTAMP, TimestampType.CREATE_TIME,
                ConsumerRecord.NULL_SIZE, value == null ? ConsumerRecord.NULL_SIZE : value.length,
                "stream-1", value, headers, Optional.empty());
    }

    @Test
    void ordinary_attributes_and_extensions_come_back_as_strings() {
        ConsumerRecord<String, byte[]> record = record("{\"amount\":42}".getBytes(StandardCharsets.UTF_8), Map.of(
                "ce_specversion", "1.0",
                "ce_id", "id-1",
                "ce_source", "urn:test",
                "ce_type", "com.acme.OrderPlaced",
                "ce_streamid", "stream-1"));

        CloudEvent cloudEvent = KafkaCloudEventMapper.toCloudEvent(record);

        assertThat(cloudEvent.getId()).isEqualTo("id-1");
        assertThat(cloudEvent.getSource()).isEqualTo(URI.create("urn:test"));
        assertThat(cloudEvent.getType()).isEqualTo("com.acme.OrderPlaced");
        assertThat(cloudEvent.getDataContentType()).isEqualTo("application/json");
        assertThat(cloudEvent.getExtension("streamid")).isEqualTo("stream-1").isInstanceOf(String.class);
        assertThat(cloudEvent.getData().toBytes()).isEqualTo("{\"amount\":42}".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void streamversion_and_position_come_back_as_longs_so_a_numeric_filter_still_matches() {
        ConsumerRecord<String, byte[]> record = record(new byte[0], Map.of(
                "ce_specversion", "1.0",
                "ce_id", "id-1",
                "ce_source", "urn:test",
                "ce_type", "com.acme.OrderPlaced",
                "ce_streamversion", "3",
                "ce_position", "42"));

        CloudEvent cloudEvent = KafkaCloudEventMapper.toCloudEvent(record);

        assertThat(cloudEvent.getExtension("streamversion")).isEqualTo(3L).isInstanceOf(Long.class);
        assertThat(cloudEvent.getExtension("position")).isEqualTo(42L).isInstanceOf(Long.class);
    }

    @Test
    void a_null_record_value_round_trips_to_no_data_at_all() {
        ConsumerRecord<String, byte[]> record = record(null, Map.of(
                "ce_specversion", "1.0", "ce_id", "id-1", "ce_source", "urn:test", "ce_type", "com.acme.OrderPlaced"));

        CloudEvent cloudEvent = KafkaCloudEventMapper.toCloudEvent(record);

        assertThat(cloudEvent.getData()).isNull();
    }

    @Test
    void a_zero_length_record_value_round_trips_to_data_present_but_empty_distinct_from_no_data_at_all() {
        ConsumerRecord<String, byte[]> record = record(new byte[0], Map.of(
                "ce_specversion", "1.0", "ce_id", "id-1", "ce_source", "urn:test", "ce_type", "com.acme.OrderPlaced"));

        CloudEvent cloudEvent = KafkaCloudEventMapper.toCloudEvent(record);

        assertThat(cloudEvent.getData()).isNotNull();
        assertThat(cloudEvent.getData().toBytes()).isEmpty();
    }

    @Test
    void a_record_the_cloudevents_reader_cannot_parse_throws() {
        // No ce_specversion, ce_id, ce_source or ce_type at all: the reader has nothing to build a CloudEvent from.
        ConsumerRecord<String, byte[]> record = record(new byte[0], Map.of());

        assertThatThrownBy(() -> KafkaCloudEventMapper.toCloudEvent(record)).isInstanceOf(RuntimeException.class);
    }

    /**
     * Anchors the round trip against a record {@link KafkaCloudEventSink} actually produced, through a real broker,
     * rather than only against records this test builds by hand. Proves both halves at once: the sink's own
     * present-but-empty write (see its own tests for the plain byte-content proof) and this mapper's read-side fix
     * agree with each other on the same wire record.
     */
    @Test
    void round_trips_against_a_record_a_real_KafkaCloudEventSink_produced() {
        KafkaDestination destination = KafkaDestination.of(topic, "stream-1");
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        CloudEvent published;
        try (KafkaCloudEventSink sink = KafkaCloudEventSink.builder(producerConfig, new KafkaCloudEventSinkTest.FixedDestinationResolver(destination)).build()) {
            published = CloudEventBuilder.v1()
                    .withId("id-real-broker")
                    .withSource(URI.create("urn:test"))
                    .withType("com.acme.OrderPlaced")
                    .withTime(OffsetDateTime.parse("2026-08-20T10:00:00Z"))
                    .withExtension("streamid", "stream-1")
                    .withExtension("streamversion", 7L)
                    .withData(new byte[0])
                    .build();
            sink.publish(published);
        }

        ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
        CloudEvent rebuilt = KafkaCloudEventMapper.toCloudEvent(record);

        assertThat(rebuilt.getId()).isEqualTo("id-real-broker");
        assertThat(rebuilt.getType()).isEqualTo("com.acme.OrderPlaced");
        assertThat(rebuilt.getExtension("streamversion")).isEqualTo(7L).isInstanceOf(Long.class);
        assertThat(rebuilt.getData()).isNotNull();
        assertThat(rebuilt.getData().toBytes()).isEmpty();
    }
}
