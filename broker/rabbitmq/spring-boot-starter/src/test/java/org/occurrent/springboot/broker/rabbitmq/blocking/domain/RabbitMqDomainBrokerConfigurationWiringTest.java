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

package org.occurrent.springboot.broker.rabbitmq.blocking.domain;

import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.springboot.broker.rabbitmq.blocking.EnableOccurrentRabbitMqBroker;
import org.occurrent.springboot.broker.rabbitmq.blocking.RabbitMqBrokerProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A plain {@code @Import(OccurrentRabbitMqAutoConfiguration.class)} would let this class's own
 * {@code @ConditionalOnBean(CloudEventConverter.class)} run before {@link PrerequisiteSupplyingConfiguration},
 * declared after {@link EnabledConfiguration} here on purpose, registers its own {@code CloudEventConverter} and
 * {@code Connection} beans, making the domain sink silently absent. This relies on
 * {@link EnableOccurrentRabbitMqBroker} activating through {@code OccurrentRabbitMqBrokerImportSelector}, see that
 * selector's own javadoc.
 */
class RabbitMqDomainBrokerConfigurationWiringTest {

    @Test
    @SuppressWarnings("unchecked")
    void the_domain_sink_activates_even_when_the_converter_bean_is_declared_after_the_enabling_configuration() {
        new ApplicationContextRunner()
                .withUserConfiguration(EnabledConfiguration.class, PrerequisiteSupplyingConfiguration.class)
                .run(context -> assertThat(context.getBeanNamesForType(DomainEventSink.class)).isNotEmpty());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentRabbitMqBroker
    static class EnabledConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class PrerequisiteSupplyingConfiguration {
        @Bean
        Connection connection() {
            return mock(Connection.class);
        }

        @Bean
        CloudEventConverter<Object> cloudEventConverter() {
            return mock(CloudEventConverter.class);
        }
    }

    /**
     * Targets {@link RabbitMqDomainBrokerConfiguration} directly rather than the full auto-configuration, so
     * resolving the two competing {@code CloudEventSink} beans below never has to open a real RabbitMQ channel the
     * way building the auto-configured sink otherwise would.
     */
    @Test
    @SuppressWarnings("unchecked")
    void the_domain_sink_binds_to_its_own_cloud_event_sink_even_when_a_competing_fallback_exists() {
        new ApplicationContextRunner()
                .withUserConfiguration(PrerequisiteSupplyingConfiguration.class, TwoFallbackCloudEventSinksConfiguration.class, RabbitMqDomainBrokerConfiguration.class)
                .run(context -> assertThat(context.getBean(DomainEventSink.class)).isNotNull());
    }

    /**
     * Two {@code @Fallback} {@code CloudEventSink} beans, standing in for the RabbitMQ and the Kafka starter's own
     * sink beans, the situation an application enabling both broker starters together ends up in. Built directly
     * here rather than through {@code OccurrentRabbitMqAutoConfiguration}, so this test needs neither a real
     * RabbitMQ channel nor a real Kafka connection to reach the same ambiguity.
     */
    @Configuration(proxyBeanMethods = false)
    static class TwoFallbackCloudEventSinksConfiguration {
        @Bean
        RabbitMqBrokerProperties rabbitMqBrokerProperties() {
            return new RabbitMqBrokerProperties();
        }

        @Bean(name = "occurrentRabbitMqCloudEventSink")
        @Fallback
        CloudEventSink correctCloudEventSink() {
            return mock(CloudEventSink.class);
        }

        @Bean
        @Fallback
        CloudEventSink competingCloudEventSink() {
            return mock(CloudEventSink.class);
        }
    }
}
