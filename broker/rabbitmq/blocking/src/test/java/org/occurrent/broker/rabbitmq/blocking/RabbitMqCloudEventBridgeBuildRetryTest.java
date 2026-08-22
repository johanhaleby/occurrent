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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

import java.io.IOException;
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
 * {@link RabbitMqCloudEventBridge.Builder#build()}'s retry, exercised against a mocked {@link Connection} and
 * {@link Channel} instead of a real broker, which has no way to force a channel open to fail on demand the exact
 * number of times a bound needs proving. {@code RabbitMqCloudEventBridgeConnectionRecoveryTest} proves the same
 * retry against a real broker end to end; this file proves the bound and the transient-versus-permanent predicate
 * precisely and fast.
 */
class RabbitMqCloudEventBridgeBuildRetryTest {

    @Test
    void a_broker_communication_failure_is_retried_with_the_default_strategy_and_build_eventually_succeeds() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.openChannel())
                .thenThrow(new IOException("expected, simulates a broker briefly unreachable"))
                .thenThrow(new IOException("expected, simulates a broker briefly unreachable"))
                .thenReturn(Optional.of(channel));
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        // declareTopology(false): this test is about the retry around openChannel(), not about topology, so no
        // resolver or queueDeclare/queueBind mocking is needed to reach a successful build().
        try (RabbitMqCloudEventBridge bridge = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, "queue")
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
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        // A fast, 3-attempt strategy in place of the (slower, 10-attempt) default: this test proves the bound gives
        // up and rethrows rather than hanging forever, not the default's own specific numbers.
        RabbitMqCloudEventBridge.Builder builder = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, "queue")
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
        RoutingOutcomeChannel outcomeChannel = new RoutingOutcomeChannel();
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing(), outcomeChannel);

        RabbitMqCloudEventBridge.Builder builder = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, "queue")
                .declareTopology(false);

        assertThatThrownBy(builder::build)
                .isInstanceOf(RuntimeException.class)
                .isNotInstanceOf(RabbitMqBridgeException.class)
                .hasMessage("expected, simulates a bug, not a broker failure");

        verify(connection, times(1)).openChannel();
    }
}
