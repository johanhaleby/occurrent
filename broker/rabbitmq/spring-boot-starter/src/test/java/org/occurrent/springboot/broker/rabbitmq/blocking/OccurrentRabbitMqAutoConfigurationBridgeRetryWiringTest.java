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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBridgeException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unlike {@link OccurrentRabbitMqAutoConfigurationWiringTest}, which never exercises channel opening, this proves
 * {@code occurrent.broker.rabbitmq.bridge.retry.*} actually reaches the bridge factory's {@code retryStrategy(...)}
 * call rather than being a property nobody reads. A {@code max-attempts} of 1 (retry effectively off) against a
 * {@link Connection} whose {@code openChannel()} always fails must fail {@code build()} after exactly one attempt.
 * A real broker cannot be made to fail channel creation a chosen number of times on demand, which is why this
 * stays a mocked {@link Connection} test rather than a {@code RabbitMqBrokerAutoConfigurationIntegrationTest} one.
 */
class OccurrentRabbitMqAutoConfigurationBridgeRetryWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
            .withUserConfiguration(OccurrentRabbitMqAutoConfigurationWiringTest.EnabledConfiguration.class);

    @Test
    void a_max_attempts_of_one_from_properties_means_build_never_retries() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.openChannel()).thenThrow(new IOException("expected, simulates a broker that never comes back"));

        contextRunner
                .withBean(Connection.class, () -> connection)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.bridge.declare-topology=false",
                        "occurrent.broker.rabbitmq.bridge.retry.max-attempts=1")
                .run(context -> {
                    RabbitMqCloudEventBridgeFactory factory = context.getBean(RabbitMqCloudEventBridgeFactory.class);
                    RabbitMqCloudEventBridge.Builder builder = factory.forQueue("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel());

                    assertThatThrownBy(builder::build).isInstanceOf(RabbitMqBridgeException.class);
                });

        verify(connection, times(1)).openChannel();
    }

    @Test
    void a_max_attempts_of_three_from_properties_means_build_retries_exactly_twice() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.openChannel()).thenThrow(new IOException("expected, simulates a broker that never comes back"));

        contextRunner
                .withBean(Connection.class, () -> connection)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.bridge.declare-topology=false",
                        "occurrent.broker.rabbitmq.bridge.retry.initial=1ms",
                        "occurrent.broker.rabbitmq.bridge.retry.max=1ms",
                        "occurrent.broker.rabbitmq.bridge.retry.max-attempts=3")
                .run(context -> {
                    RabbitMqCloudEventBridgeFactory factory = context.getBean(RabbitMqCloudEventBridgeFactory.class);
                    RabbitMqCloudEventBridge.Builder builder = factory.forQueue("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel());

                    assertThatThrownBy(builder::build).isInstanceOf(RabbitMqBridgeException.class);
                });

        verify(connection, times(3)).openChannel();
    }
}
