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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitMqCloudEventSinkOutboundMapperTest extends RabbitMqTestSupport {

    /**
     * Issue #1434: a consumer that is not Occurrent expects its own body, a number header and a {@code messageId}
     * property. It reads the message with a plain {@code basicGet}, with no Occurrent code involved.
     */
    @Test
    void a_plain_consumer_reads_the_body_the_number_header_and_the_messageId_a_custom_outbound_mapper_wrote() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            Map<String, Object> headers = new HashMap<>();
            headers.put("orderId", "order-1");
            headers.put("quantity", 3);
            BasicProperties properties = new BasicProperties.Builder()
                    .contentType("application/json")
                    .messageId(cloudEvent.getId())
                    .headers(headers)
                    .build();
            return new RabbitMqMessage(properties, "{\"orderId\":\"order-1\",\"quantity\":3}".getBytes(StandardCharsets.UTF_8));
        };

        try (RabbitMqCloudEventSink sink = sink(mapper)) {
            sink.publish(event("id-1"));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("message should be on the queue once publish() returns").isNotNull();
            assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"orderId\":\"order-1\",\"quantity\":3}");
            assertThat(response.getProps().getContentType()).isEqualTo("application/json");
            assertThat(response.getProps().getMessageId()).isEqualTo("id-1");
            assertThat(response.getProps().getHeaders().get("orderId")).hasToString("order-1");
            assertThat(response.getProps().getHeaders().get("quantity")).isEqualTo(3);
            assertThat(response.getProps().getHeaders()).doesNotContainKey("cloudEvents_id");
        }
    }

    @Test
    void the_mapper_receives_the_event_and_the_destination_the_resolver_chose() throws Exception {
        declareAndBindQueue();
        Map<String, Object> seen = new HashMap<>();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            seen.put("id", cloudEvent.getId());
            seen.put("destination", destination);
            return RabbitMqOutboundMapper.cloudEventHeaders().toMessage(cloudEvent, destination);
        };

        try (RabbitMqCloudEventSink sink = sink(mapper)) {
            sink.publish(event("id-1"));
        }

        assertThat(seen).containsEntry("id", "id-1").containsEntry("destination", RabbitMqDestination.of(exchange, OrderPlaced.class.getName()));
    }

    @Test
    void the_sink_makes_the_message_persistent_when_the_mapper_leaves_deliveryMode_unset() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) ->
                new RabbitMqMessage(new BasicProperties.Builder().messageId(cloudEvent.getId()).build(), new byte[]{1});

        try (RabbitMqCloudEventSink sink = sink(mapper)) {
            sink.publish(event("id-1"));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).isNotNull();
            assertThat(response.getProps().getDeliveryMode()).isEqualTo(2);
            assertThat(response.getProps().getMessageId()).as("the rest of the mapper's properties are kept").isEqualTo("id-1");
        }
    }

    @Test
    void the_sink_keeps_a_deliveryMode_the_mapper_sets() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) ->
                new RabbitMqMessage(new BasicProperties.Builder().deliveryMode(1).build(), new byte[]{1});

        try (RabbitMqCloudEventSink sink = sink(mapper)) {
            sink.publish(event("id-1"));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).isNotNull();
            assertThat(response.getProps().getDeliveryMode()).isEqualTo(1);
        }
    }

    @Test
    void publish_throws_an_IllegalStateException_naming_the_event_id_and_type_when_the_mapper_returns_null_and_does_not_retry() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            calls.incrementAndGet();
            return null;
        };

        try (RabbitMqCloudEventSink sink = sink(mapper)) {
            assertThatThrownBy(() -> sink.publish(event("id-null")))
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("id-null")
                    .hasMessageContaining(OrderPlaced.class.getName());
            assertThat(calls).hasValue(1);
        }
    }

    @Test
    void an_exception_from_the_mapper_reaches_the_caller_of_publish_unchanged_and_is_not_retried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        IllegalArgumentException failure = new IllegalArgumentException("cannot map this event");
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            calls.incrementAndGet();
            throw failure;
        };

        try (RabbitMqCloudEventSink sink = sink(mapper)) {
            assertThatThrownBy(() -> sink.publish(event("id-throws"))).isSameAs(failure);
            assertThat(calls).hasValue(1);
        }
    }

    @Test
    void a_sink_with_no_outbound_mapper_publishes_the_cloudEvents_headers_a_default_inbound_mapper_reads_back() throws Exception {
        String queue = declareAndBindQueue();
        CloudEvent event = CloudEventBuilder.v1()
                .withId("id-1")
                .withSource(URI.create("urn:test"))
                .withType(OrderPlaced.class.getName())
                .withDataContentType("application/json")
                .withData("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                .withExtension("streamid", "stream-1")
                .withExtension("streamversion", 3L)
                .build();

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            sink.publish(event);

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).isNotNull();
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
            assertThat(response.getProps().getDeliveryMode()).isEqualTo(2);
            assertThat(RabbitMqInboundMapper.cloudEventHeaders().toCloudEvent(response.getProps(), response.getBody())).isEqualTo(event);
        }
    }

    @Test
    void outboundMapper_refuses_null() {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RabbitMqCloudEventSink.Builder builder = RabbitMqCloudEventSink.builder(connection(), resolver);

        assertThatThrownBy(() -> builder.outboundMapper(null)).isInstanceOf(NullPointerException.class);
    }

    private RabbitMqCloudEventSink sink(RabbitMqOutboundMapper mapper) {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        return RabbitMqCloudEventSink.builder(connection(), resolver).outboundMapper(mapper).build();
    }

    private String declareAndBindQueue() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());
        return queue;
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType(OrderPlaced.class.getName()).build();
    }

    private static final class OrderPlaced {
    }
}
