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
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.beans.factory.UnsatisfiedDependencyException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Proves the wiring rules from the plan gate without a real broker. That covers what activates the whole
 * configuration, what stays absent until its own prerequisite is met, and that {@code @Fallback} defers to an
 * application's own bean. The sink and the bridge factory's channel-opening behavior is never exercised here, so a
 * bare, unstubbed {@link Connection} mock is safe throughout. None of these tests ever call it. That behavior is
 * proved for real against a Testcontainers broker in {@link RabbitMqBrokerAutoConfigurationIntegrationTest}.
 */
class OccurrentRabbitMqAutoConfigurationWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentRabbitMqAutoConfiguration.class))
            .withUserConfiguration(EnabledConfiguration.class);

    @Test
    void nothing_is_registered_without_a_connection_bean() {
        contextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(RabbitMqCloudEventSink.class);
            assertThat(context).doesNotHaveBean(RabbitMqCloudEventBridgeFactory.class);
            assertThat(context).doesNotHaveBean(RabbitMqTopicExchangeDestinationResolver.class);
        });
    }

    @Test
    void bridge_factory_activates_without_an_exchange_but_the_resolver_bean_does_not() {
        contextRunner.withBean(Connection.class, () -> mock(Connection.class)).run(context -> {
            assertThat(context).hasSingleBean(RabbitMqCloudEventBridgeFactory.class);
            assertThat(context).doesNotHaveBean(RabbitMqTopicExchangeDestinationResolver.class);
        });
    }

    @Test
    void resolver_bean_activates_once_the_exchange_property_and_a_type_mapper_are_present() {
        contextRunner
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues("occurrent.broker.rabbitmq.exchange=orders")
                .run(context -> {
                    assertThat(context).hasSingleBean(RabbitMqTopicExchangeDestinationResolver.class);
                    assertThat(context.getBean(DestinationResolver.class)).isInstanceOf(RabbitMqTopicExchangeDestinationResolver.class);
                });
    }

    /**
     * A plain {@code @ConditionalOnProperty} treats the literal value {@code false} as absent, even though
     * {@code false} is a legal RabbitMQ exchange name. {@link RabbitMqExchangeConfiguredCondition} activates on
     * any nonblank value instead.
     */
    @Test
    void resolver_bean_activates_for_an_exchange_literally_named_false() {
        contextRunner
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues("occurrent.broker.rabbitmq.exchange=false")
                .run(context -> assertThat(context).hasSingleBean(RabbitMqTopicExchangeDestinationResolver.class));
    }

    /**
     * A plain {@code @ConditionalOnProperty} treats an empty value as present, which would build a resolver for a
     * blank exchange name here, one that later fails when a bridge tries to bind it.
     * {@link RabbitMqExchangeConfiguredCondition} requires a nonblank value.
     */
    @Test
    void resolver_bean_does_not_activate_for_a_blank_exchange() {
        contextRunner
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues("occurrent.broker.rabbitmq.exchange=")
                .run(context -> assertThat(context).doesNotHaveBean(RabbitMqTopicExchangeDestinationResolver.class));
    }

    /**
     * The resolver bean's own constructor needs a {@link CloudEventTypeMapper}, so if it were not {@code @Lazy},
     * Spring would try to build it during singleton pre-instantiation whenever {@code exchange} is set, whether or
     * not anything ever asks for a {@code CloudEventSink} or a resolver, and fail here where no type mapper bean is
     * supplied.
     */
    @Test
    void resolver_bean_stays_lazy_so_a_missing_type_mapper_does_not_break_startup() {
        contextRunner
                .withBean(Connection.class, () -> mock(Connection.class))
                .withPropertyValues("occurrent.broker.rabbitmq.exchange=orders")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void requesting_the_sink_without_a_resolver_fails_loud_naming_the_missing_bean() {
        contextRunner.withBean(Connection.class, () -> mock(Connection.class)).run(context ->
                assertThatThrownBy(() -> context.getBean(RabbitMqCloudEventSink.class))
                        .isInstanceOf(UnsatisfiedDependencyException.class));
    }

    @Test
    void a_user_supplied_cloud_event_sink_takes_precedence_over_the_fallback() {
        CloudEventSink userSink = mock(CloudEventSink.class);
        contextRunner
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues("occurrent.broker.rabbitmq.exchange=orders")
                .withBean(CloudEventSink.class, () -> userSink)
                .run(context -> {
                    // Both bean definitions genuinely exist, the user's and the @Fallback one, so hasSingleBean(...)
                    // would fail here on bean count alone. getBeanNamesForType(...) lists both without
                    // instantiating either, unlike assertThat(context).hasBean(name), which resolves the bean by
                    // name and would force the @Lazy fallback to build a channel against the unstubbed Connection
                    // mock. @Fallback only changes which one getBean(Class) resolves to, the same distinction
                    // OccurrentMongoAutoConfigurationStarterValidationTest draws for its own CloudEventConverter
                    // fallback.
                    assertThat(context.getBeanNamesForType(CloudEventSink.class))
                            .containsExactlyInAnyOrder("cloudEventSink", "occurrentRabbitMqCloudEventSink");
                    assertThat(context.getBean(CloudEventSink.class)).isSameAs(userSink);
                });
    }

    @Test
    void bridge_declaring_topology_without_a_resolver_or_explicit_bindings_refuses_at_build() {
        contextRunner.withBean(Connection.class, () -> mock(Connection.class)).run(context -> {
            RabbitMqCloudEventBridgeFactory factory = context.getBean(RabbitMqCloudEventBridgeFactory.class);
            RabbitMqCloudEventBridge.Builder builder = factory.forQueue("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel());
            assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("resolver");
        });
    }

    @Test
    void bridge_configured_to_park_without_a_parking_destination_refuses_at_build() {
        contextRunner
                .withBean(Connection.class, () -> mock(Connection.class))
                .withBean(CloudEventTypeMapper.class, ReflectionCloudEventTypeMapper::qualified)
                .withPropertyValues(
                        "occurrent.broker.rabbitmq.exchange=orders",
                        "occurrent.broker.rabbitmq.bridge.on-delivery-failure=PARK"
                )
                .run(context -> {
                    RabbitMqCloudEventBridgeFactory factory = context.getBean(RabbitMqCloudEventBridgeFactory.class);
                    RabbitMqCloudEventBridge.Builder builder = factory.forQueue("orders-projection", new PushSubscriptionModel(), new RoutingOutcomeChannel());
                    assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class).hasMessageContaining("parkingDestination");
                });
    }

    /**
     * A plain {@code @Import(OccurrentRabbitMqAutoConfiguration.class)} would let
     * {@code @ConditionalOnBean(Connection.class)} run before {@link ConnectionSupplyingConfiguration}, declared
     * after {@link EnabledConfiguration} here on purpose, registers its own {@code Connection} bean, making the
     * whole starter silently disappear. This does not go through {@code AutoConfigurations.of(...)} the way the
     * tests above do, since that helper's own import mechanism is not what a real application's
     * {@code @EnableOccurrentRabbitMqBroker} goes through, only {@link OccurrentRabbitMqBrokerImportSelector} is.
     */
    @Test
    void the_starter_activates_even_when_the_connection_bean_is_declared_after_the_enabling_configuration() {
        new ApplicationContextRunner()
                .withUserConfiguration(EnabledConfiguration.class, ConnectionSupplyingConfiguration.class)
                .run(context -> assertThat(context).hasSingleBean(RabbitMqCloudEventBridgeFactory.class));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class ConnectionSupplyingConfiguration {
        @Bean
        Connection connection() {
            return mock(Connection.class);
        }
    }
}
