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
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBuildFailureClassifier;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.domain.RabbitMqDomainEventBridge;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.springboot.broker.rabbitmq.blocking.RabbitMqBrokerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

import static java.util.Objects.requireNonNull;

class DefaultRabbitMqDomainEventBridgeFactory implements RabbitMqDomainEventBridgeFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultRabbitMqDomainEventBridgeFactory.class);

    private final Connection connection;
    private final RabbitMqBrokerProperties properties;
    private final ObjectProvider<DestinationResolver<RabbitMqDestination>> resolverProvider;

    DefaultRabbitMqDomainEventBridgeFactory(Connection connection, RabbitMqBrokerProperties properties,
                                             ObjectProvider<DestinationResolver<RabbitMqDestination>> resolverProvider) {
        this.connection = requireNonNull(connection);
        this.properties = requireNonNull(properties);
        this.resolverProvider = requireNonNull(resolverProvider);
    }

    @Override
    public <E> RabbitMqDomainEventBridge.Builder<E> forQueue(String queue, DomainEventFeed<E> feed) {
        RabbitMqBrokerProperties.Bridge bridgeProperties = properties.getBridge();
        RabbitMqDomainEventBridge.Builder<E> builder = RabbitMqDomainEventBridge.builder(connection, feed, queue)
                .declareTopology(bridgeProperties.isDeclareTopology())
                .onDeliveryFailure(bridgeProperties.getOnDeliveryFailure())
                .pollInterval(bridgeProperties.getPollInterval())
                .prefetchCount(bridgeProperties.getPrefetchCount())
                .retryStrategy(buildRetryStrategy(queue, bridgeProperties.getRetry()));
        DestinationResolver<RabbitMqDestination> resolver = resolverProvider.getIfAvailable();
        if (resolver != null) {
            builder.resolver(resolver);
        }
        bridgeProperties.getParkingDestination().toDestination().ifPresent(builder::parkingDestination);
        return builder;
    }

    /**
     * The same classification {@link RabbitMqBuildFailureClassifier} gives {@code RabbitMqDomainEventBridge.Builder}'s
     * own default {@code build()} retry strategy, including its {@code onBeforeRetry} logging. Calling
     * {@code retryStrategy(...)} replaces that default entirely, so applying the property-driven timing without
     * either would retry an {@code IllegalStateException} from a missing resolver or parking destination forever
     * instead of failing on the first attempt, and leave a retrying startup logging nothing to tell it apart from
     * a hung one.
     */
    private static RetryStrategy buildRetryStrategy(String queue, RabbitMqBrokerProperties.BridgeRetry retry) {
        return RetryStrategy.exponentialBackoff(retry.getInitial(), retry.getMax(), retry.getMultiplier())
                .maxAttempts(retry.getMaxAttempts())
                .retryIf(RabbitMqBuildFailureClassifier::isTransient)
                .onBeforeRetry((info, throwable) -> log.warn(
                        "Attempt {} of {} to build the RabbitMQ domain event bridge for queue \"{}\" failed. Retrying in {}.",
                        info.getAttemptNumber(), info.getMaxAttempts(), queue, info.getBackoff(), throwable));
    }
}
