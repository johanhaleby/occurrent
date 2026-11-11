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
import org.occurrent.retry.RetryStrategy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
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

    /**
     * The AMQP client throws an IllegalArgumentException from basicPublish for a header value it cannot encode, after
     * it has counted the publish as awaiting a confirm the broker never sends. Without retiring the channel, the next
     * valid publish waited out the whole acknowledgement timeout and failed although its message was delivered.
     */
    @Test
    void a_header_value_the_client_cannot_encode_fails_that_publish_with_an_IllegalArgumentException_and_the_next_valid_publish_succeeds_within_the_acknowledgement_timeout() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            if ("id-bad".equals(cloudEvent.getId())) {
                Map<String, Object> headers = new HashMap<>();
                headers.put("orderId", UUID.randomUUID());
                return new RabbitMqMessage(new BasicProperties.Builder().headers(headers).build(), new byte[]{1});
            }
            return RabbitMqOutboundMapper.cloudEventHeaders().toMessage(cloudEvent, destination);
        };

        try (RabbitMqCloudEventSink sink = sinkWithAcknowledgementTimeout(mapper, Duration.ofSeconds(2))) {
            assertThatThrownBy(() -> sink.publish(event("id-bad"))).isExactlyInstanceOf(IllegalArgumentException.class);

            long start = System.nanoTime();
            sink.publish(event("id-good"));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the valid publish must not wait out the acknowledgement timeout").isLessThan(Duration.ofSeconds(1));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("the valid message should be on the queue").isNotNull();
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-good");
            assertThat(adminChannel.basicGet(queue, true)).as("the message that could not be encoded was not published").isNull();
        }
    }

    @Test
    void a_messageId_over_255_bytes_fails_that_publish_with_an_IllegalArgumentException_and_the_next_valid_publish_succeeds_within_the_acknowledgement_timeout() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            if ("id-bad".equals(cloudEvent.getId())) {
                return new RabbitMqMessage(new BasicProperties.Builder().messageId("x".repeat(300)).build(), new byte[]{1});
            }
            return RabbitMqOutboundMapper.cloudEventHeaders().toMessage(cloudEvent, destination);
        };

        try (RabbitMqCloudEventSink sink = sinkWithAcknowledgementTimeout(mapper, Duration.ofSeconds(2))) {
            assertThatThrownBy(() -> sink.publish(event("id-bad"))).isExactlyInstanceOf(IllegalArgumentException.class);

            long start = System.nanoTime();
            sink.publish(event("id-good"));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the valid publish must not wait out the acknowledgement timeout").isLessThan(Duration.ofSeconds(1));

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("the valid message should be on the queue").isNotNull();
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-good");
        }
    }

    @Test
    void the_mapper_is_called_again_for_each_retried_publish_attempt() throws Exception {
        String queue = declareAndBindQueue();
        AtomicInteger calls = new AtomicInteger();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("first attempt fails");
            }
            return RabbitMqOutboundMapper.cloudEventHeaders().toMessage(cloudEvent, destination);
        };
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());

        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver)
                .retryStrategy(RetryStrategy.fixed(10).maxAttempts(3)).outboundMapper(mapper).build()) {
            sink.publish(event("id-1"));

            assertThat(calls).hasValue(2);
            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).isNotNull();
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
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

    private RabbitMqCloudEventSink sinkWithAcknowledgementTimeout(RabbitMqOutboundMapper mapper, Duration acknowledgementTimeout) {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        return RabbitMqCloudEventSink.builder(connection(), resolver).acknowledgementTimeout(acknowledgementTimeout).outboundMapper(mapper).build();
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
