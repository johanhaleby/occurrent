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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.api.blocking.DomainEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventSink;
import org.occurrent.springboot.broker.rabbitmq.blocking.RabbitMqBrokerProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Lazy;

/**
 * The domain-level half of the RabbitMQ broker auto-configuration, in its own package the way
 * {@code org.occurrent.broker.rabbitmq.blocking.domain} sits beside the CloudEvent-level classes it wraps.
 * <p>
 * <strong>No fallback {@link CloudEventConverter} is provided here.</strong> {@code framework/spring-boot-autoconfigure/common}
 * already ships one ({@code Jackson3CloudEventConverterConfiguration}), but importing it would pull
 * {@code OccurrentProperties} and its MongoDB-flavored keys into a broker-only application, the same known gap
 * ADR 72 documents for that properties class. The domain level exists for an application that already has its own
 * converter, per ADR 133 decision 3, so this configuration requires one rather than inventing a second, narrower
 * fallback.
 */
@Configuration(proxyBeanMethods = false)
public class RabbitMqDomainBrokerConfiguration {

    /**
     * {@code @Lazy}: instantiating this pulls in the {@code CloudEventSink} bean as a dependency, and that bean is
     * itself {@code @Lazy} for the same "do not force a resolver requirement on a deployment that does not need
     * it" reason. Requesting the domain sink is what should trigger both, not merely being on the classpath.
     */
    @Bean
    @Lazy
    @Fallback
    @ConditionalOnBean(CloudEventConverter.class)
    <E> DomainEventSink<E> occurrentRabbitMqDomainEventSink(CloudEventSink cloudEventSink, CloudEventConverter<E> converter) {
        return RabbitMqDomainEventSink.using(cloudEventSink, converter);
    }

    @Bean
    RabbitMqDomainEventBridgeFactory occurrentRabbitMqDomainEventBridgeFactory(Connection connection, RabbitMqBrokerProperties properties,
                                                                                ObjectProvider<DestinationResolver<RabbitMqDestination>> resolver) {
        return new DefaultRabbitMqDomainEventBridgeFactory(connection, properties, resolver);
    }
}
