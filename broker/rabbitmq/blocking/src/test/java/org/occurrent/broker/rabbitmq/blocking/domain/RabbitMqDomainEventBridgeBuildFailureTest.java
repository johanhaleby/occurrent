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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBridgeException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The domain bridge's twin of {@code RabbitMqCloudEventBridgeBuildFailureTest}: a failed
 * {@link RabbitMqDomainEventBridge.Builder#build()} must not leave a {@link Channel} open behind it.
 */
class RabbitMqDomainEventBridgeBuildFailureTest {

    private static final String EXCHANGE = "test-exchange";

    @Test
    void onDeliveryFailure_PARK_without_a_parkingDestination_is_refused_before_any_channel_is_opened() throws Exception {
        Connection connection = mock(Connection.class);
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        RabbitMqDomainEventBridge.Builder<TestOrderPlaced> builder = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK);

        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);

        verify(connection, never()).openChannel();
    }

    @Test
    void an_explicit_empty_bindings_set_is_refused_before_any_channel_is_opened() throws Exception {
        Connection connection = mock(Connection.class);
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        RabbitMqDomainEventBridge.Builder<TestOrderPlaced> builder = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .bindings(Set.of());

        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);

        verify(connection, never()).openChannel();
    }

    @Test
    void a_topology_declaration_failure_closes_the_channel_it_already_opened() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        when(channel.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any()))
                .thenThrow(new IOException("queue declare failed"));
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(EXCHANGE, ReflectionCloudEventTypeMapper.qualified());

        RabbitMqDomainEventBridge.Builder<TestOrderPlaced> builder = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .resolver(resolver);

        assertThatThrownBy(builder::build).isInstanceOf(RabbitMqBridgeException.class);

        verify(channel).close();
    }

    private record TestOrderPlaced(String orderId) {
    }

    private static final class TestOrderPlacedConverter implements CloudEventConverter<TestOrderPlaced> {

        @Override
        public CloudEvent toCloudEvent(TestOrderPlaced domainEvent) {
            return CloudEventBuilder.v1()
                    .withId("id")
                    .withSource(URI.create("urn:test"))
                    .withType(TestOrderPlaced.class.getName())
                    .withData(domainEvent.orderId().getBytes(StandardCharsets.UTF_8))
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
