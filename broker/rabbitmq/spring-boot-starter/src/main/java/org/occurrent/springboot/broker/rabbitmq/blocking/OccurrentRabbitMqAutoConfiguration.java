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
import com.rabbitmq.client.ShutdownSignalException;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSink;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqPublishException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqPublishTimeoutException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopicExchangeDestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqUnroutableEventException;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.springboot.broker.rabbitmq.blocking.domain.RabbitMqDomainBrokerConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

/**
 * Property-driven construction of the RabbitMQ broker sink and consume bridges, per
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md">ADR 133</a>.
 * Enabled with {@link EnableOccurrentRabbitMqBroker}, an {@code @Import}-based activation like the MongoDB
 * starter's {@code EnableOccurrent}, rather than Spring Boot's automatic classpath scanning, but reached through
 * {@link OccurrentRabbitMqBrokerImportSelector} instead of a plain class import, so this class's own
 * {@code @ConditionalOnBean(Connection.class)} sees the application's {@code Connection} bean, see that
 * selector's own javadoc for why a plain import cannot guarantee that.
 * <p>
 * <strong>The {@link Connection} is the application's to supply, never this starter's to construct.</strong> The
 * hand-wired bootstrap this auto-configuration is modeled on already treats connection setup, host, port,
 * credentials, TLS, as external to the broker modules, and duplicating that here would mean reimplementing what
 * {@code spring-boot-starter-amqp} already owns for a problem this auto-configuration was not asked to solve. This
 * whole configuration activates only once a {@link Connection} bean exists.
 */
@AutoConfiguration
@ConditionalOnClass(RabbitMqCloudEventSink.class)
@ConditionalOnBean(Connection.class)
@EnableConfigurationProperties(RabbitMqBrokerProperties.class)
@Import(RabbitMqDomainBrokerConfiguration.class)
public class OccurrentRabbitMqAutoConfiguration {

    /**
     * The zero-config {@link DestinationResolver} a sink or a bridge factory falls back to when the application
     * declares none, active once {@code occurrent.broker.rabbitmq.exchange} is set. {@code @Fallback} rather than
     * {@code @ConditionalOnMissingBean}: this configuration is activated by {@link EnableOccurrentRabbitMqBroker}'s
     * plain {@code @Import}, so a {@code @ConditionalOnMissingBean} condition can be evaluated before the
     * application's own resolver bean is registered and let both through, the same import-ordering gap ADR 72
     * documents for the MongoDB starter's own {@code Default*Provider} beans. A {@code @Fallback} bean is excluded at
     * dependency-resolution time instead, which registration order cannot affect.
     */
    @Bean
    @Fallback
    @ConditionalOnProperty(prefix = "occurrent.broker.rabbitmq", name = "exchange")
    RabbitMqTopicExchangeDestinationResolver occurrentRabbitMqDestinationResolver(RabbitMqBrokerProperties properties, CloudEventTypeMapper<?> typeMapper) {
        return new RabbitMqTopicExchangeDestinationResolver(properties.getExchange(), typeMapper);
    }

    /**
     * The zero-config {@code CloudEventSink}. {@code @Lazy} so a consume-only deployment, one that never publishes
     * and so never needs {@code occurrent.broker.rabbitmq.exchange} configured, is never forced to satisfy this
     * bean's {@link DestinationResolver} requirement just because the starter is on the classpath. Instantiated,
     * and so validated, only the first time something actually asks for a {@code CloudEventSink}.
     */
    @Bean
    @Lazy
    @Fallback
    RabbitMqCloudEventSink occurrentRabbitMqCloudEventSink(Connection connection, DestinationResolver<RabbitMqDestination> resolver, RabbitMqBrokerProperties properties) {
        RabbitMqBrokerProperties.Retry retry = properties.getSink().getRetry();
        return RabbitMqCloudEventSink.builder(connection, resolver)
                .acknowledgementTimeout(properties.getSink().getAcknowledgementTimeout())
                .retryStrategy(publishRetryStrategy(retry))
                .build();
    }

    /**
     * The same predicate {@code RabbitMqCloudEventSink.Builder}'s own default retry strategy uses. Calling
     * {@code retryStrategy(...)} replaces that default entirely, so applying the property-driven timing without
     * this predicate would retry a permanent publish failure, an unroutable event or a channel this client has
     * already closed, forever instead of surfacing it.
     */
    private static RetryStrategy.Retry publishRetryStrategy(RabbitMqBrokerProperties.Retry retry) {
        return RetryStrategy.exponentialBackoff(retry.getInitial(), retry.getMax(), retry.getMultiplier())
                .retryIf(throwable -> throwable instanceof RabbitMqPublishException publishException
                        && !(publishException instanceof RabbitMqUnroutableEventException)
                        && !(publishException instanceof RabbitMqPublishTimeoutException)
                        && !(publishException.getCause() instanceof ShutdownSignalException)
                        && !(publishException.getCause() instanceof InterruptedException));
    }

    /**
     * Pre-seeds {@link RabbitMqCloudEventBridgeFactory} with the connection, the resolver bean if one exists (a
     * bridge whose {@code occurrent.broker.rabbitmq.bridge.declare-topology} is {@code false}, or whose caller
     * supplies its own bindings, needs none), and every {@code occurrent.broker.rabbitmq.bridge.*} default. Not
     * conditioned on a resolver, and not {@code @Lazy}, since building this factory does nothing but capture
     * configuration. Nothing about it opens a channel or requires a resolver until a consumer actually calls
     * {@link RabbitMqCloudEventBridgeFactory#forQueue}.
     */
    @Bean
    RabbitMqCloudEventBridgeFactory occurrentRabbitMqCloudEventBridgeFactory(Connection connection, RabbitMqBrokerProperties properties,
                                                                              ObjectProvider<DestinationResolver<RabbitMqDestination>> resolver) {
        return new DefaultRabbitMqCloudEventBridgeFactory(connection, properties, resolver);
    }
}
