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

import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.retry.RetryStrategy;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitMqCloudEventSinkTest extends RabbitMqTestSupport {

    /**
     * The invariant that matters: {@code publish} only returns once the broker has both confirmed the message and
     * routed it, so by the time it returns the message is already visible to a plain {@code basicGet} on a queue
     * bound to it, with no polling needed.
     */
    @Test
    void publish_waits_for_the_brokers_confirmation_and_the_message_is_already_on_the_bound_queue_when_it_returns() throws Exception {
        String queue = adminChannel.queueDeclare().getQueue();
        adminChannel.queueBind(queue, exchange, OrderPlaced.class.getName());

        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-1")
                    .withSource(URI.create("urn:test"))
                    .withType(OrderPlaced.class.getName())
                    .withDataContentType("application/json")
                    .withData("{\"amount\":42}".getBytes(StandardCharsets.UTF_8))
                    .withExtension("streamid", "stream-1")
                    .build();

            sink.publish(cloudEvent);

            GetResponse response = adminChannel.basicGet(queue, true);
            assertThat(response).as("message should already be on the queue once publish() returns").isNotNull();
            assertThat(new String(response.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"amount\":42}");
            assertThat(response.getProps().getContentType()).isEqualTo("application/json");
            assertThat(response.getProps().getHeaders().get("cloudEvents_streamid")).hasToString("stream-1");
            assertThat(response.getProps().getHeaders().get("cloudEvents_id")).hasToString("id-1");
            assertThat(response.getProps().getHeaders().get("cloudEvents_type")).hasToString(OrderPlaced.class.getName());
        }
    }

    /**
     * The other half of the same invariant. A confirm alone is not proof of delivery, so a publish the broker could
     * not route (nothing bound to the routing key) has to fail even though RabbitMQ confirms it before discarding
     * it. Uses the builder's default {@link RetryStrategy}, which excludes
     * {@link RabbitMqUnroutableEventException} from its retries, since it is a configuration bug rather than a
     * transient failure, so this also proves the publish fails promptly instead of retrying into the same
     * permanent failure forever.
     */
    @Test
    void publish_throws_when_the_broker_returns_the_message_as_unroutable() throws Exception {
        // No queue is bound to this routing key on this exchange.
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        try (RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(connection(), resolver).build()) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId("id-2")
                    .withSource(URI.create("urn:test"))
                    .withType(NobodyListens.class.getName())
                    .build();

            assertThatThrownBy(() -> sink.publish(cloudEvent)).isInstanceOf(RabbitMqUnroutableEventException.class);
        }
    }

    private static final class OrderPlaced {
    }

    private static final class NobodyListens {
    }
}
