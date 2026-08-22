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
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBuildFailureClassifier;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RoutingOutcomeChannel;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import static java.util.Objects.requireNonNull;

class DefaultRabbitMqCloudEventBridgeFactory implements RabbitMqCloudEventBridgeFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultRabbitMqCloudEventBridgeFactory.class);

    private final Connection connection;
    private final RabbitMqBrokerProperties properties;
    private final ObjectProvider<DestinationResolver<RabbitMqDestination>> resolverProvider;
    private final ApplicationContext applicationContext;

    DefaultRabbitMqCloudEventBridgeFactory(Connection connection, RabbitMqBrokerProperties properties,
                                            ObjectProvider<DestinationResolver<RabbitMqDestination>> resolverProvider,
                                            ApplicationContext applicationContext) {
        this.connection = requireNonNull(connection);
        this.properties = requireNonNull(properties);
        this.resolverProvider = requireNonNull(resolverProvider);
        this.applicationContext = requireNonNull(applicationContext);
    }

    @Override
    public RabbitMqCloudEventBridge.Builder forQueue(String queue, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel) {
        RabbitMqBrokerProperties.Bridge bridgeProperties = properties.getBridge();
        RabbitMqCloudEventBridge.Builder builder = RabbitMqCloudEventBridge.builder(connection, model, outcomeChannel, queue)
                .declareTopology(bridgeProperties.isDeclareTopology())
                .onDeliveryFailure(bridgeProperties.getOnDeliveryFailure())
                .pollInterval(bridgeProperties.getPollInterval())
                .prefetchCount(bridgeProperties.getPrefetchCount())
                .retryStrategy(buildRetryStrategy(queue, bridgeProperties.getRetry()))
                .readinessSource(CatchupThenPushReadiness.memoized(applicationContext, model));
        DestinationResolver<RabbitMqDestination> resolver = resolverProvider.getIfAvailable();
        if (resolver != null) {
            builder.resolver(resolver);
        }
        bridgeProperties.getParkingDestination().toDestination().ifPresent(builder::parkingDestination);
        return builder;
    }

    /**
     * The same classification {@link RabbitMqBuildFailureClassifier} gives {@code RabbitMqCloudEventBridge.Builder}'s
     * own default {@code build()} retry strategy, including its {@code onRetryableError} logging. Calling
     * {@code retryStrategy(...)} replaces that default entirely, so applying the property-driven timing without
     * either would retry an {@code IllegalStateException} from a missing resolver or parking destination forever
     * instead of failing on the first attempt, and leave a retrying startup logging nothing to tell it apart from
     * a hung one.
     */
    private static RetryStrategy buildRetryStrategy(String queue, RabbitMqBrokerProperties.BridgeRetry retry) {
        return RetryStrategy.exponentialBackoff(retry.getInitial(), retry.getMax(), retry.getMultiplier())
                .maxAttempts(retry.getMaxAttempts())
                .retryIf(RabbitMqBuildFailureClassifier::isTransient)
                .onRetryableError((info, throwable) -> log.warn(
                        "Attempt {} of {} to build the RabbitMQ bridge for queue \"{}\" failed. Retrying in {}.",
                        info.getAttemptNumber(), info.getMaxAttempts(), queue, info.getBackoffBeforeNextRetryAttempt(), throwable));
    }
}
