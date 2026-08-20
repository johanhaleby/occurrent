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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.springboot.broker.kafka.blocking.EnableOccurrentKafkaBroker;
import org.occurrent.springboot.broker.kafka.blocking.KafkaBrokerProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * A plain {@code @Import(OccurrentKafkaAutoConfiguration.class)} would let this class's own
 * {@code @ConditionalOnBean(CloudEventConverter.class)} run before {@link CloudEventConverterSupplyingConfiguration},
 * declared after {@link EnabledConfiguration} here on purpose, registers its own {@code CloudEventConverter}
 * bean, making the domain sink silently absent. This does not stub the resolver or {@code bootstrap-servers}
 * requirement, since {@link EnableOccurrentKafkaBroker} activates through
 * {@code OccurrentKafkaBrokerImportSelector} regardless of those, see that selector's own javadoc.
 * <p>
 * Every other test in this class asserts at the delegate level, which mock's {@code publish(CloudEvent)} actually
 * received the call, not merely that a {@code DomainEventSink} bean exists. {@code context.getBean(CloudEventSink.class)}
 * alone cannot tell the two apart: {@link KafkaDomainBrokerConfiguration#occurrentKafkaDomainEventSink} once
 * resolved its delegate through a literal bean-name lookup, which activates the wrong sink without any error the
 * plain injection point would have surfaced.
 */
class KafkaDomainBrokerConfigurationWiringTest {

    private static final CloudEvent CONVERTED_EVENT = CloudEventBuilder.v1()
            .withId("test-id")
            .withSource(URI.create("urn:occurrent:test"))
            .withType("test.event")
            .build();

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
        @SuppressWarnings("unchecked")
        CloudEventConverter<Object> cloudEventConverter() {
            CloudEventConverter<Object> converter = mock(CloudEventConverter.class);
            when(converter.toCloudEvent(any())).thenReturn(CONVERTED_EVENT);
            return converter;
        }
    }

    /**
     * Targets {@link KafkaDomainBrokerConfiguration} directly rather than the full auto-configuration, so this
     * test controls exactly which {@code CloudEventSink} beans are in play.
     */
    @Test
    @SuppressWarnings("unchecked")
    void a_user_supplied_cloud_event_sink_wins_for_the_domain_sink() {
        CloudEventSink userSink = mock(CloudEventSink.class);
        new ApplicationContextRunner()
                .withUserConfiguration(CloudEventConverterSupplyingConfiguration.class, OwnFallbackCloudEventSinkConfiguration.class, KafkaDomainBrokerConfiguration.class)
                .withBean("userCloudEventSink", CloudEventSink.class, () -> userSink)
                .run(context -> {
                    DomainEventSink<Object> domainSink = context.getBean(DomainEventSink.class);
                    domainSink.publish(new Object());
                    CloudEventSink ownFallbackSink = context.getBean("occurrentKafkaCloudEventSink", CloudEventSink.class);
                    verify(userSink).publish(CONVERTED_EVENT);
                    verifyNoInteractions(ownFallbackSink);
                });
    }

    /**
     * The same as {@link #a_user_supplied_cloud_event_sink_wins_for_the_domain_sink}, but with the RabbitMQ
     * starter's own competing {@code @Fallback} sink also present, the exact situation an application enabling
     * both broker starters together ends up in. A user override must win here too, not just fall through to
     * whichever transport-named bean {@link KafkaDomainBrokerConfiguration#resolveCloudEventSink} would otherwise
     * pick.
     */
    @Test
    @SuppressWarnings("unchecked")
    void a_user_supplied_cloud_event_sink_wins_even_with_both_starters_present() {
        CloudEventSink userSink = mock(CloudEventSink.class);
        new ApplicationContextRunner()
                .withUserConfiguration(CloudEventConverterSupplyingConfiguration.class, TwoFallbackCloudEventSinksConfiguration.class, KafkaDomainBrokerConfiguration.class)
                .withBean("userCloudEventSink", CloudEventSink.class, () -> userSink)
                .run(context -> {
                    DomainEventSink<Object> domainSink = context.getBean(DomainEventSink.class);
                    domainSink.publish(new Object());
                    verify(userSink).publish(CONVERTED_EVENT);
                });
    }

    /**
     * With no user override and both starters' {@code @Fallback} sinks present, type resolution is genuinely
     * ambiguous, so {@link KafkaDomainBrokerConfiguration#occurrentKafkaDomainEventSink} falls back to the bean
     * literally named {@code occurrentKafkaCloudEventSink} rather than throwing
     * {@link org.springframework.beans.factory.NoUniqueBeanDefinitionException}.
     */
    @Test
    @SuppressWarnings("unchecked")
    void the_domain_sink_binds_to_its_own_cloud_event_sink_even_when_a_competing_fallback_exists() {
        new ApplicationContextRunner()
                .withUserConfiguration(CloudEventConverterSupplyingConfiguration.class, TwoFallbackCloudEventSinksConfiguration.class, KafkaDomainBrokerConfiguration.class)
                .run(context -> {
                    DomainEventSink<Object> domainSink = context.getBean(DomainEventSink.class);
                    domainSink.publish(new Object());
                    CloudEventSink ownSink = context.getBean("occurrentKafkaCloudEventSink", CloudEventSink.class);
                    CloudEventSink competingSink = context.getBean("competingCloudEventSink", CloudEventSink.class);
                    verify(ownSink).publish(CONVERTED_EVENT);
                    verifyNoInteractions(competingSink);
                });
    }

    /**
     * The single-starter baseline, no user override and no competing sink from another starter, exactly what an
     * application with only the Kafka starter enabled has today. Type resolution finds the one candidate
     * directly, without ever reaching {@link KafkaDomainBrokerConfiguration#resolveCloudEventSink}'s ambiguity
     * fallback.
     */
    @Test
    @SuppressWarnings("unchecked")
    void the_domain_sink_delegates_to_the_only_cloud_event_sink_present() {
        new ApplicationContextRunner()
                .withUserConfiguration(CloudEventConverterSupplyingConfiguration.class, OwnFallbackCloudEventSinkConfiguration.class, KafkaDomainBrokerConfiguration.class)
                .run(context -> {
                    DomainEventSink<Object> domainSink = context.getBean(DomainEventSink.class);
                    domainSink.publish(new Object());
                    CloudEventSink ownSink = context.getBean("occurrentKafkaCloudEventSink", CloudEventSink.class);
                    verify(ownSink).publish(CONVERTED_EVENT);
                });
    }

    /**
     * Stands in for {@code OccurrentKafkaAutoConfiguration}'s own {@code occurrentKafkaCloudEventSink} bean, built
     * directly here rather than through the auto-configuration so this test needs no real Kafka producer.
     */
    @Configuration(proxyBeanMethods = false)
    static class OwnFallbackCloudEventSinkConfiguration {
        @Bean
        KafkaBrokerProperties kafkaBrokerProperties() {
            return new KafkaBrokerProperties();
        }

        @Bean(name = "occurrentKafkaCloudEventSink")
        @Fallback
        CloudEventSink ownCloudEventSink() {
            return mock(CloudEventSink.class);
        }
    }

    /**
     * Two {@code @Fallback} {@code CloudEventSink} beans, standing in for the Kafka and the RabbitMQ starter's own
     * sink beans, the situation an application enabling both broker starters together ends up in. Built directly
     * here rather than through {@code OccurrentKafkaAutoConfiguration}, so this test needs neither a real Kafka
     * producer nor a real RabbitMQ channel to reach the same ambiguity.
     */
    @Configuration(proxyBeanMethods = false)
    static class TwoFallbackCloudEventSinksConfiguration {
        @Bean
        KafkaBrokerProperties kafkaBrokerProperties() {
            return new KafkaBrokerProperties();
        }

        @Bean(name = "occurrentKafkaCloudEventSink")
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
