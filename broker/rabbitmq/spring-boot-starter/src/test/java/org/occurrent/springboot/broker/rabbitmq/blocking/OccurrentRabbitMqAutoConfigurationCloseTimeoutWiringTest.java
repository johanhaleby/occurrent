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
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.springboot.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridgeFactory;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * {@code occurrent.broker.rabbitmq.bridge.close-timeout} reaches the builder each bridge factory hands out. The two
 * factories read the property separately, so each gets its own test. The builder has no getter for it, so the tests
 * read the builder's own {@code closeTimeout} field, and a value other than the thirty-second default is what tells
 * a factory that set it apart from one that did not.
 */
class OccurrentRabbitMqAutoConfigurationCloseTimeoutWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
            .withUserConfiguration(OccurrentRabbitMqAutoConfigurationWiringTest.EnabledConfiguration.class)
            .withBean(Connection.class, () -> mock(Connection.class))
            .withPropertyValues("occurrent.broker.rabbitmq.bridge.close-timeout=7s");

    @Test
    void close_timeout_from_properties_reaches_the_cloud_event_bridge_builder() {
        contextRunner.run(context -> {
            RabbitMqCloudEventBridgeFactory factory = context.getBean(RabbitMqCloudEventBridgeFactory.class);

            assertThat(factory.forQueue("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel()))
                    .extracting("closeTimeout")
                    .isEqualTo(Duration.ofSeconds(7));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void close_timeout_from_properties_reaches_the_domain_event_bridge_builder() {
        contextRunner.run(context -> {
            RabbitMqDomainEventBridgeFactory factory = context.getBean(RabbitMqDomainEventBridgeFactory.class);

            assertThat(factory.forQueue("orders-projection", mock(DomainEventFeed.class)))
                    .extracting("closeTimeout")
                    .isEqualTo(Duration.ofSeconds(7));
        });
    }
}
