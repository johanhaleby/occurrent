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
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Lazy;

import java.util.Set;

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
     * The two bean names the {@code @Fallback} {@code CloudEventSink} this starter and the Kafka starter each
     * register under, the only ambiguity {@link #resolveCloudEventSink} treats as expected rather than as a
     * genuine application misconfiguration.
     */
    private static final Set<String> STARTER_FALLBACK_SINK_BEAN_NAMES = Set.of("occurrentKafkaCloudEventSink", "occurrentRabbitMqCloudEventSink");

    /**
     * {@code @Lazy}: instantiating this pulls in the {@code CloudEventSink} bean as a dependency, and that bean is
     * itself {@code @Lazy} for the same "do not force a resolver requirement on a deployment that does not need
     * it" reason. Requesting the domain sink is what should trigger both, not merely being on the classpath.
     * <p>
     * The {@code CloudEventSink} resolves by type first, through {@code cloudEventSinkProvider}, exactly what an
     * unqualified injection point would do. That lets an application's own non-fallback {@code CloudEventSink}
     * win here the same way it wins everywhere else, single starter or both. Only when type resolution is
     * genuinely ambiguous, both the RabbitMQ and the Kafka starter's own {@code @Fallback} sink present and
     * nothing else to break the tie, does {@link #resolveCloudEventSink} fall back to
     * {@code ownCloudEventSinkProvider}, which names this starter's own bean directly. An earlier version named
     * that bean unconditionally, which fixed the two-starter ambiguity but silently shadowed a genuine application
     * override, since a literal bean-name lookup never runs {@code @Fallback} scoring at all.
     * <p>
     * Declared as returning {@link RabbitMqDomainEventSink}, not the {@link DomainEventSink} interface it
     * implements. Spring's pre-instantiation type matching goes by the declared return type, so a plain
     * {@code DomainEventSink<E>} return here would hide which transport built this bean, leaving a caller in a
     * dual-starter context no way to select it other than by the same internal bean name
     * {@link #resolveCloudEventSink} uses. Injecting {@code RabbitMqDomainEventSink<E>} directly is unambiguous
     * even with both starters present, since only this bean produces that concrete type.
     * {@code @Fallback} still applies through the implemented interface, for the unqualified
     * {@code DomainEventSink<E>} injection point everything above this paragraph describes.
     */
    @Bean
    @Lazy
    @Fallback
    @ConditionalOnBean(CloudEventConverter.class)
    <E> RabbitMqDomainEventSink<E> occurrentRabbitMqDomainEventSink(ObjectProvider<CloudEventSink> cloudEventSinkProvider,
                                                                     @Qualifier("occurrentRabbitMqCloudEventSink") ObjectProvider<CloudEventSink> ownCloudEventSinkProvider,
                                                                     CloudEventConverter<E> converter) {
        return RabbitMqDomainEventSink.using(resolveCloudEventSink(cloudEventSinkProvider, ownCloudEventSinkProvider), converter);
    }

    /**
     * An application declaring two non-fallback {@code CloudEventSink} beans with no {@code @Primary} is a
     * genuine configuration error, one Spring already rejects loudly through {@code cloudEventSinkProvider}. The
     * only ambiguity this method absorbs instead of rethrowing is the one both broker starters cause on purpose,
     * so a check against the exact pair of bean names {@link #STARTER_FALLBACK_SINK_BEAN_NAMES} distinguishes the
     * two, catching every {@code NoUniqueBeanDefinitionException} regardless of which beans caused it would route
     * a genuine user mistake through this starter's own sink silently instead, the exact wrong-sink failure mode
     * fixing the two-starter case was meant to eliminate.
     */
    private static CloudEventSink resolveCloudEventSink(ObjectProvider<CloudEventSink> cloudEventSinkProvider,
                                                          ObjectProvider<CloudEventSink> ownCloudEventSinkProvider) {
        try {
            return cloudEventSinkProvider.getObject();
        } catch (NoUniqueBeanDefinitionException ambiguous) {
            // getBeanNamesFound() can itself be null (the constructor overload taking a bare count rather than a
            // name collection leaves it that way), which Set.copyOf(...) would NPE on rather than simply not
            // matching. Not reachable through ObjectProvider.getObject() today, but checked rather than assumed.
            if (ambiguous.getBeanNamesFound() != null && Set.copyOf(ambiguous.getBeanNamesFound()).equals(STARTER_FALLBACK_SINK_BEAN_NAMES)) {
                return ownCloudEventSinkProvider.getObject();
            }
            throw ambiguous;
        }
    }

    @Bean
    RabbitMqDomainEventBridgeFactory occurrentRabbitMqDomainEventBridgeFactory(Connection connection, RabbitMqBrokerProperties properties,
                                                                                ObjectProvider<DestinationResolver<RabbitMqDestination>> resolver) {
        return new DefaultRabbitMqDomainEventBridgeFactory(connection, properties, resolver);
    }
}
