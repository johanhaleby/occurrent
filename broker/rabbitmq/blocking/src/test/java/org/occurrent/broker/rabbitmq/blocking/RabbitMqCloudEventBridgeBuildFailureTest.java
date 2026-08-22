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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.io.IOException;
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
 * A failed {@link RabbitMqCloudEventBridge.Builder#build()} must not leave a {@link Channel} or a parking sink open
 * behind it, so these two failure paths are exercised against a mocked {@link Connection} and {@link Channel}
 * instead of a real broker, which has no way to force a mid-construction failure on demand.
 */
class RabbitMqCloudEventBridgeBuildFailureTest {

    private static final String EXCHANGE = "test-exchange";

    @Test
    void onDeliveryFailure_PARK_without_a_parkingDestination_is_refused_before_any_channel_is_opened() throws Exception {
        Connection connection = mock(Connection.class);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        RabbitMqCloudEventBridge.Builder builder = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, "queue")
                .declareTopology(false)
                .onDeliveryFailure(DeliveryFailurePolicy.PARK);

        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);

        verify(connection, never()).openChannel();
    }

    @Test
    void an_explicit_empty_bindings_set_is_refused_before_any_channel_is_opened() throws Exception {
        Connection connection = mock(Connection.class);
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        RabbitMqCloudEventBridge.Builder builder = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, "queue")
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
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);
        RabbitMqTopicExchangeDestinationResolver resolver = new RabbitMqTopicExchangeDestinationResolver(EXCHANGE, ReflectionCloudEventTypeMapper.qualified());

        // retryStrategy(none()): this test is about the unwind on ONE failed attempt, not about retrying, and a
        // queue declare failure wrapped as RabbitMqBridgeException is retried by default (see
        // RabbitMqCloudEventBridgeBuildRetryTest), which would both slow this down and call close() more than once.
        RabbitMqCloudEventBridge.Builder builder = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, "queue")
                .resolver(resolver)
                .retryStrategy(RetryStrategy.none());

        assertThatThrownBy(builder::build).isInstanceOf(RabbitMqBridgeException.class);

        verify(channel).close();
    }
}
