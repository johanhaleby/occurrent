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

package org.occurrent.broker.rabbitmq.blocking.domain;

import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTestSupport;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.cloudevents.EventMetadata;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitMqDomainEventSinkTest extends RabbitMqTestSupport {

    private final TestOrderPlacedConverter converter = new TestOrderPlacedConverter();

    @Test
    void publish_converts_the_domain_event_and_delegates_to_the_cloudEvent_sink() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, TestOrderPlaced.class.getName());

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink cloudEventSink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            RabbitMqDomainEventSink<TestOrderPlaced> domainEventSink = RabbitMqDomainEventSink.using(cloudEventSink, converter);

            domainEventSink.publish(new TestOrderPlaced("order-1"));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("message should already be on the queue once publish() returns").isNotNull();
            assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("order-1");
        }
    }

    @Test
    void publish_with_metadata_stamps_the_supplied_extensions_onto_the_published_event() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, TestOrderPlaced.class.getName());

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink cloudEventSink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            RabbitMqDomainEventSink<TestOrderPlaced> domainEventSink = RabbitMqDomainEventSink.using(cloudEventSink, converter);
            EventMetadata metadata = new EventMetadata(Map.of("streamid", "stream-42", "streamversion", 7L));

            domainEventSink.publish(metadata, new TestOrderPlaced("order-2"));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).isNotNull();
            assertThat(response.getProps().getHeaders().get("cloudEvents_streamid")).hasToString("stream-42");
            assertThat(response.getProps().getHeaders().get("cloudEvents_streamversion")).hasToString("7");
        }
    }

    @Test
    void publish_with_metadata_drops_a_null_valued_extension_the_converter_already_set() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, TestOrderPlaced.class.getName());

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink cloudEventSink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            RabbitMqDomainEventSink<TestOrderPlaced> domainEventSink = RabbitMqDomainEventSink.using(cloudEventSink, converter);
            Map<String, Object> data = new HashMap<>();
            data.put("streamid", null);
            EventMetadata metadata = new EventMetadata(data);

            domainEventSink.publish(metadata, new TestOrderPlaced("order-3"));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).isNotNull();
            assertThat(response.getProps().getHeaders()).doesNotContainKey("cloudEvents_streamid");
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
}
