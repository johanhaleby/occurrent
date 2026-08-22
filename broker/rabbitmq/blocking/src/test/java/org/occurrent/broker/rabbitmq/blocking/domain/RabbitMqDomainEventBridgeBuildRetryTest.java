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
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBridgeException;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.retry.RetryStrategy;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The domain bridge's twin of {@code RabbitMqCloudEventBridgeBuildRetryTest}: {@link RabbitMqDomainEventBridge.Builder#build()}'s
 * retry, exercised against a mocked {@link Connection} and {@link Channel} instead of a real broker.
 */
class RabbitMqDomainEventBridgeBuildRetryTest {

    @Test
    void a_broker_communication_failure_is_retried_with_the_default_strategy_and_build_eventually_succeeds() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel())
                .thenThrow(new IOException("expected, simulates a broker briefly unreachable"))
                .thenThrow(new IOException("expected, simulates a broker briefly unreachable"))
                .thenReturn(Optional.of(channel));
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        try (RabbitMqDomainEventBridge<TestOrderPlaced> bridge = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .build()) {
            assertThat(bridge).isNotNull();
        }

        verify(connection, times(3)).openChannel();
    }

    @Test
    void retries_are_exhausted_and_the_last_failure_is_thrown_after_the_configured_bound() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.openChannel()).thenThrow(new IOException("expected, simulates a broker that never comes back"));
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        RabbitMqDomainEventBridge.Builder<TestOrderPlaced> builder = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false)
                .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(1))
                        .maxAttempts(3)
                        .retryIf(throwable -> throwable instanceof RabbitMqBridgeException));

        assertThatThrownBy(builder::build).isInstanceOf(RabbitMqBridgeException.class);

        verify(connection, times(3)).openChannel();
    }

    @Test
    void a_bug_shaped_runtime_exception_is_never_retried() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel()).thenReturn(Optional.of(channel));
        doThrow(new RuntimeException("expected, simulates a bug, not a broker failure"))
                .when(channel).basicQos(1);
        DomainEventFeed<TestOrderPlaced> feed = new DomainEventFeed<>(new InMemoryEventStore(), new TestOrderPlacedConverter(), TestOrderPlaced::orderId);

        RabbitMqDomainEventBridge.Builder<TestOrderPlaced> builder = RabbitMqDomainEventBridge.builder(connection, feed, "queue")
                .declareTopology(false);

        assertThatThrownBy(builder::build)
                .isInstanceOf(RuntimeException.class)
                .isNotInstanceOf(RabbitMqBridgeException.class)
                .hasMessage("expected, simulates a bug, not a broker failure");

        verify(connection, times(1)).openChannel();
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
