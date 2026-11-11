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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What {@link RabbitMqCloudEventSink} tells its {@link RabbitMqSinkListener} for each publish attempt, against a real
 * broker, and what {@link RabbitMqCloudEventSink#isOpen()} says. The callback table is in ADR 171.
 */
class RabbitMqCloudEventSinkListenerTest extends RabbitMqTestSupport {

    @Test
    void a_confirmed_publish_tells_the_listener_published_once_with_the_destination_and_a_positive_publish_time() throws Exception {
        declareAndBindQueue();
        RecordingRabbitMqSinkListener listener = new RecordingRabbitMqSinkListener();

        try (RabbitMqCloudEventSink sink = sinkBuilder().listener(listener).build()) {
            sink.publish(event("id-1"));
        }

        assertThat(listener.calls()).singleElement().satisfies(call -> {
            assertThat(call.callback()).isEqualTo("published");
            assertThat(call.destination()).isEqualTo(RabbitMqDestination.of(exchange, OrderPlaced.class.getName()));
            assertThat(call.publishTime()).isPositive();
        });
    }

    @ParameterizedTest
    @EnumSource(RabbitMqUnroutablePolicy.class)
    void an_unroutable_publish_tells_the_listener_unroutable_and_never_failed_under_either_policy(RabbitMqUnroutablePolicy policy) throws Exception {
        RecordingRabbitMqSinkListener listener = new RecordingRabbitMqSinkListener();

        try (RabbitMqCloudEventSink sink = sinkBuilder().onUnroutable(policy).listener(listener).build()) {
            if (policy == RabbitMqUnroutablePolicy.FAIL) {
                assertThatThrownBy(() -> sink.publish(event("id-1"))).isExactlyInstanceOf(RabbitMqUnroutableEventException.class);
            } else {
                assertThatCode(() -> sink.publish(event("id-1"))).doesNotThrowAnyException();
            }
        }

        assertThat(listener.calls()).singleElement().satisfies(call -> {
            assertThat(call.callback()).isEqualTo("unroutable");
            assertThat(call.destination()).isEqualTo(RabbitMqDestination.of(exchange, OrderPlaced.class.getName()));
        });
    }

    @Test
    void a_mapper_that_throws_tells_the_listener_failed_with_that_exception_and_the_caller_gets_the_same_exception() throws Exception {
        declareAndBindQueue();
        RecordingRabbitMqSinkListener listener = new RecordingRabbitMqSinkListener();
        IllegalArgumentException failure = new IllegalArgumentException("cannot map this event");
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> {
            throw failure;
        };

        try (RabbitMqCloudEventSink sink = sinkBuilder().outboundMapper(mapper).listener(listener).build()) {
            assertThatThrownBy(() -> sink.publish(event("id-1"))).isSameAs(failure);
        }

        assertThat(listener.calls()).singleElement().satisfies(call -> {
            assertThat(call.callback()).isEqualTo("failed");
            assertThat(call.destination()).isEqualTo(RabbitMqDestination.of(exchange, OrderPlaced.class.getName()));
            assertThat(call.failure()).isSameAs(failure);
        });
    }

    @Test
    void a_mapper_that_returns_null_tells_the_listener_failed_with_the_exception_the_caller_gets() throws Exception {
        declareAndBindQueue();
        RecordingRabbitMqSinkListener listener = new RecordingRabbitMqSinkListener();
        RabbitMqOutboundMapper mapper = (cloudEvent, destination) -> null;

        try (RabbitMqCloudEventSink sink = sinkBuilder().outboundMapper(mapper).listener(listener).build()) {
            Throwable thrown = catchThrowable(() -> sink.publish(event("id-null")));

            assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class).hasMessageContaining("id-null");
            assertThat(listener.calls()).singleElement().satisfies(call -> {
                assertThat(call.callback()).isEqualTo("failed");
                assertThat(call.failure()).isSameAs(thrown);
            });
        }
    }

    @Test
    void a_listener_that_throws_from_published_does_not_fail_the_publish() throws Exception {
        String queue = declareAndBindQueue();
        RabbitMqSinkListener listener = new RabbitMqSinkListener() {
            @Override
            public void published(RabbitMqDestination destination, Duration publishTime) {
                throw new IllegalStateException("broken metrics binding");
            }
        };

        try (RabbitMqCloudEventSink sink = sinkBuilder().listener(listener).build()) {
            assertThatCode(() -> sink.publish(event("id-1"))).doesNotThrowAnyException();
        }

        GetResponse response = adminChannel.basicGet(queue, true);
        assertThat(response).as("the event was published").isNotNull();
    }

    @Test
    void a_listener_that_throws_from_failed_leaves_the_publish_failure_unchanged() throws Exception {
        IllegalArgumentException failure = new IllegalArgumentException("cannot map this event");
        RabbitMqSinkListener listener = new RabbitMqSinkListener() {
            @Override
            public void failed(RabbitMqDestination destination, Throwable failure) {
                throw new IllegalStateException("broken metrics binding");
            }
        };

        try (RabbitMqCloudEventSink sink = sinkBuilder().outboundMapper((cloudEvent, destination) -> {
            throw failure;
        }).listener(listener).build()) {
            assertThatThrownBy(() -> sink.publish(event("id-1"))).isSameAs(failure);
        }
    }

    @Test
    void isOpen_is_true_after_build() throws Exception {
        try (RabbitMqCloudEventSink sink = sinkBuilder().build()) {
            assertThat(sink.isOpen()).isTrue();
        }
    }

    @Test
    void isOpen_is_false_after_close() throws Exception {
        RabbitMqCloudEventSink sink = sinkBuilder().build();

        sink.close();

        assertThat(sink.isOpen()).isFalse();
    }

    @Test
    void isOpen_is_false_once_the_connection_is_closed_although_the_sink_is_not() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(amqpUrl());
        Connection ownConnection = connectionFactory.newConnection();
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        RabbitMqCloudEventSink sink = RabbitMqCloudEventSink.builder(ownConnection, resolver).build();

        ownConnection.close();

        assertThat(sink.isOpen()).isFalse();
    }

    private RabbitMqCloudEventSink.Builder sinkBuilder() {
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(exchange, ReflectionCloudEventTypeMapper.qualified());
        return RabbitMqCloudEventSink.builder(connection(), resolver);
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
