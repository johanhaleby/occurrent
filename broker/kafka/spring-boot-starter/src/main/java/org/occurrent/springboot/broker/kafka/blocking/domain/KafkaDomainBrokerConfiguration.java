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

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.domain.KafkaDomainEventSink;
import org.occurrent.springboot.broker.kafka.blocking.KafkaBrokerProperties;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Lazy;

/**
 * The domain-level half of the Kafka broker auto-configuration, in its own package the way
 * {@code org.occurrent.broker.kafka.blocking.domain} sits beside the CloudEvent-level classes it wraps.
 * <p>
 * <strong>No fallback {@link CloudEventConverter} is provided here.</strong> {@code framework/spring-boot-autoconfigure/common}
 * already ships one ({@code Jackson3CloudEventConverterConfiguration}), but importing it would pull
 * {@code OccurrentProperties} and its MongoDB-flavored keys into a broker-only application, the same known gap
 * ADR 72 documents for that properties class. The domain level exists for an application that already has its own
 * converter, per ADR 133 decision 3, so this configuration requires one rather than inventing a second, narrower
 * fallback.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaDomainBrokerConfiguration {

    /**
     * {@code @Lazy}: instantiating this pulls in the {@code CloudEventSink} bean as a dependency, and that bean is
     * itself {@code @Lazy} for the same "do not force a resolver requirement on a deployment that does not need
     * it" reason. Requesting the domain sink is what should trigger both, not merely being on the classpath.
     * <p>
     * The {@code CloudEventSink} resolves by type first, through {@code cloudEventSinkProvider}, exactly what an
     * unqualified injection point would do. That lets an application's own non-fallback {@code CloudEventSink}
     * win here the same way it wins everywhere else, single starter or both. Only when type resolution is
     * genuinely ambiguous, both the Kafka and the RabbitMQ starter's own {@code @Fallback} sink present and
     * nothing else to break the tie, does {@link #resolveCloudEventSink} fall back to
     * {@code ownCloudEventSinkProvider}, which names this starter's own bean directly. An earlier version named
     * that bean unconditionally, which fixed the two-starter ambiguity but silently shadowed a genuine application
     * override, since a literal bean-name lookup never runs {@code @Fallback} scoring at all.
     */
    @Bean
    @Lazy
    @Fallback
    @ConditionalOnBean(CloudEventConverter.class)
    <E> DomainEventSink<E> occurrentKafkaDomainEventSink(ObjectProvider<CloudEventSink> cloudEventSinkProvider,
                                                          @Qualifier("occurrentKafkaCloudEventSink") ObjectProvider<CloudEventSink> ownCloudEventSinkProvider,
                                                          CloudEventConverter<E> converter) {
        return KafkaDomainEventSink.using(resolveCloudEventSink(cloudEventSinkProvider, ownCloudEventSinkProvider), converter);
    }

    private static CloudEventSink resolveCloudEventSink(ObjectProvider<CloudEventSink> cloudEventSinkProvider,
                                                          ObjectProvider<CloudEventSink> ownCloudEventSinkProvider) {
        try {
            return cloudEventSinkProvider.getObject();
        } catch (NoUniqueBeanDefinitionException ambiguousBetweenBrokerStarters) {
            return ownCloudEventSinkProvider.getObject();
        }
    }

    @Bean
    KafkaDomainEventBridgeFactory occurrentKafkaDomainEventBridgeFactory(KafkaBrokerProperties properties, ObjectProvider<DestinationResolver<KafkaDestination>> resolver) {
        return new DefaultKafkaDomainEventBridgeFactory(properties, resolver);
    }
}
