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

package org.occurrent.springboot.broker.kafka.blocking.domain;

import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.springboot.broker.kafka.blocking.EnableOccurrentKafkaBroker;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A plain {@code @Import(OccurrentKafkaAutoConfiguration.class)} would let this class's own
 * {@code @ConditionalOnBean(CloudEventConverter.class)} run before {@link CloudEventConverterSupplyingConfiguration},
 * declared after {@link EnabledConfiguration} here on purpose, registers its own {@code CloudEventConverter}
 * bean, making the domain sink silently absent. This does not stub the resolver or {@code bootstrap-servers}
 * requirement, since {@link EnableOccurrentKafkaBroker} activates through
 * {@code OccurrentKafkaBrokerImportSelector} regardless of those, see that selector's own javadoc.
 */
class KafkaDomainBrokerConfigurationWiringTest {

    @Test
    @SuppressWarnings("unchecked")
    void the_domain_sink_activates_even_when_the_converter_bean_is_declared_after_the_enabling_configuration() {
        new ApplicationContextRunner()
                .withPropertyValues(
                        "occurrent.broker.kafka.bootstrap-servers=localhost:19092",
                        "occurrent.broker.kafka.topic=orders")
                .withUserConfiguration(EnabledConfiguration.class, CloudEventConverterSupplyingConfiguration.class)
                .run(context -> assertThat(context.getBeanNamesForType(DomainEventSink.class)).isNotEmpty());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrentKafkaBroker
    static class EnabledConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class CloudEventConverterSupplyingConfiguration {
        @Bean
        CloudEventConverter<Object> cloudEventConverter() {
            return mock(CloudEventConverter.class);
        }
    }
}
