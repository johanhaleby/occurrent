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
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceStatus;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTestSupport;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The twin of {@code RabbitMqCloudEventSourceStatusTest} for {@link RabbitMqDomainEventSource}, with the same cases.
 */
class RabbitMqDomainEventSourceStatusTest extends RabbitMqTestSupport {

    @Test
    void a_source_is_OPEN_and_connected_after_build() throws Exception {
        try (RabbitMqDomainEventSource<Object> source = builderOn(connection()).build()) {
            assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.OPEN);
            assertThat(source.isConnected()).isTrue();
        }
    }

    @Test
    void a_source_is_CLOSED_after_close() throws Exception {
        RabbitMqDomainEventSource<Object> source = builderOn(connection()).build();

        source.close();

        assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.CLOSED);
    }

    @Test
    void a_source_is_not_connected_once_its_connection_is_closed_and_is_still_OPEN_until_it_stops() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(amqpUrl());
        Connection ownConnection = connectionFactory.newConnection();
        RabbitMqDomainEventSource<Object> source = builderOn(ownConnection).pollInterval(Duration.ofMinutes(1)).build();
        try {
            ownConnection.close();

            assertThat(source.isConnected()).isFalse();
            assertThat(source.status()).as("status() says whether the source stopped, isConnected() says whether the connection is open")
                    .isEqualTo(RabbitMqSourceStatus.OPEN);
        } finally {
            source.close();
        }
    }

    @Test
    void a_source_is_STOPPED_after_a_channel_close_that_ends_it_for_good_and_the_next_poll_has_stopped_it() throws Exception {
        Channel channel = mock(Channel.class);
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(getClass()));
        RabbitMqDomainEventSource<Object> source = new RabbitMqDomainEventSource<>(feed(), channel, "queue-" + UUID.randomUUID(), 1,
                Duration.ofMinutes(1), failureAction, Duration.ofSeconds(1));
        assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.OPEN);

        ArgumentCaptor<ShutdownListener> listeners = ArgumentCaptor.forClass(ShutdownListener.class);
        verify(channel, atLeastOnce()).addShutdownListener(listeners.capture());
        listeners.getAllValues().forEach(listener -> listener.shutdownCompleted(new ShutdownSignalException(false, false, null, mock(Channel.class))));
        Method reconcileConsumption = RabbitMqDomainEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);

        assertThat(source.status()).isEqualTo(RabbitMqSourceStatus.STOPPED);
    }

    private RabbitMqDomainEventSource.Builder<Object> builderOn(Connection connection) {
        return RabbitMqDomainEventSource.builder(connection, feed(), "test-queue-" + UUID.randomUUID()).declareTopology(false).pollInterval(Duration.ofMillis(50));
    }

    @SuppressWarnings("unchecked")
    private static DomainEventFeed<Object> feed() {
        DomainEventFeed<Object> feed = mock(DomainEventFeed.class);
        when(feed.hasProjection()).thenReturn(true);
        when(feed.isReadyForLiveDelivery()).thenReturn(true);
        return feed;
    }
}
