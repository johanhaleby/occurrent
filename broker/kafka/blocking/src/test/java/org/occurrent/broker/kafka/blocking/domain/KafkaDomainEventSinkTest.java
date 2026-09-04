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

package org.occurrent.broker.kafka.blocking.domain;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventSink;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.KafkaTestSupport;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.SubscriptionFilter;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaDomainEventSinkTest extends KafkaTestSupport {

    private final TestOrderPlacedConverter converter = new TestOrderPlacedConverter();

    @Test
    void publish_converts_the_domain_event_and_delegates_to_the_cloudEvent_sink() {
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink cloudEventSink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(KafkaDestination.of(topic))).build()) {
            KafkaDomainEventSink<TestOrderPlaced> domainEventSink = KafkaDomainEventSink.using(cloudEventSink, converter);

            domainEventSink.publish(new TestOrderPlaced("order-1"));

            ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
            assertThat(new String(record.value(), StandardCharsets.UTF_8)).isEqualTo("order-1");
        }
    }

    @Test
    void publish_without_metadata_strips_every_extension_the_converter_set_so_the_consumer_sees_empty_metadata() {
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink cloudEventSink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(KafkaDestination.of(topic))).build()) {
            KafkaDomainEventSink<TestOrderPlaced> domainEventSink = KafkaDomainEventSink.using(cloudEventSink, converter);

            domainEventSink.publish(new TestOrderPlaced("order-4"));

            ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
            assertThat(record.headers().lastHeader("ce_streamid")).isNull();
        }
    }

    @Test
    void publish_with_metadata_stamps_the_supplied_extensions_onto_the_published_event() {
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink cloudEventSink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(KafkaDestination.of(topic))).build()) {
            KafkaDomainEventSink<TestOrderPlaced> domainEventSink = KafkaDomainEventSink.using(cloudEventSink, converter);
            EventMetadata metadata = new EventMetadata(Map.of("streamid", "stream-42", "streamversion", 7L));

            domainEventSink.publish(metadata, new TestOrderPlaced("order-2"));

            ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
            assertThat(headerValue(record, "ce_streamid")).isEqualTo("stream-42");
            assertThat(headerValue(record, "ce_streamversion")).isEqualTo("7");
        }
    }

    @Test
    void publish_with_metadata_drops_a_null_valued_extension_the_converter_already_set() {
        Map<String, Object> producerConfig = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (KafkaCloudEventSink cloudEventSink = KafkaCloudEventSink.builder(producerConfig, new FixedDestinationResolver(KafkaDestination.of(topic))).build()) {
            KafkaDomainEventSink<TestOrderPlaced> domainEventSink = KafkaDomainEventSink.using(cloudEventSink, converter);
            Map<String, Object> data = new HashMap<>();
            data.put("streamid", null);
            EventMetadata metadata = new EventMetadata(data);

            domainEventSink.publish(metadata, new TestOrderPlaced("order-3"));

            ConsumerRecord<String, byte[]> record = consumeOneRecord(topic);
            assertThat(record.headers().lastHeader("ce_streamid")).isNull();
        }
    }

    private record TestOrderPlaced(String orderId) {
    }

    private static final class TestOrderPlacedConverter implements CloudEventConverter<TestOrderPlaced> {

        @Override
        public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
                    .withDataContentType("text/plain")
                    .withData(domainEvent.orderId().getBytes(StandardCharsets.UTF_8))
                    .withExtension("streamid", "stream-from-converter")
                    .build();
        }

        @Override
        public TestOrderPlaced toDomainEvent(CloudEvent cloudEvent) {
            byte[] data = cloudEvent.getData() == null ? new byte[0] : cloudEvent.getData().toBytes();
            return new TestOrderPlaced(new String(data, StandardCharsets.UTF_8));
        }

        @Override
        public String getCloudEventType(Class<? extends TestOrderPlaced> type) {
            return TestOrderPlaced.class.getName();
        }
    }

    /**
     * Points every publish at one predetermined destination, the pre-created scratch topic, so this test does not
     * depend on broker auto-topic-creation. Mirrors {@code KafkaCloudEventSinkTest.FixedDestinationResolver}, kept
     * separate since that one is package-private to a different package.
     */
    private record FixedDestinationResolver(KafkaDestination destination) implements DestinationResolver<KafkaDestination> {
        @Override
        public KafkaDestination destinationFor(CloudEvent cloudEvent) {
            return destination;
        }

        @Override
        public Optional<Set<KafkaDestination>> destinationsFor(Filter filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public KafkaDestination catchAllDestination() {
            throw new UnsupportedOperationException();
        }
    }
}
