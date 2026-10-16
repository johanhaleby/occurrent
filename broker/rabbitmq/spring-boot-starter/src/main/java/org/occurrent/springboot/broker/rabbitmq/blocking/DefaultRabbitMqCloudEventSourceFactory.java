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
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import static java.util.Objects.requireNonNull;

class DefaultRabbitMqCloudEventSourceFactory implements RabbitMqCloudEventSourceFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultRabbitMqCloudEventSourceFactory.class);

    private final Connection connection;
    private final RabbitMqBrokerProperties properties;
    private final ObjectProvider<DestinationResolver<RabbitMqDestination>> resolverProvider;
    private final ApplicationContext applicationContext;

    DefaultRabbitMqCloudEventSourceFactory(Connection connection, RabbitMqBrokerProperties properties,
                                            ObjectProvider<DestinationResolver<RabbitMqDestination>> resolverProvider,
                                            ApplicationContext applicationContext) {
        this.connection = requireNonNull(connection);
        this.properties = requireNonNull(properties);
        this.resolverProvider = requireNonNull(resolverProvider);
        this.applicationContext = requireNonNull(applicationContext);
    }

    @Override
    public RabbitMqCloudEventSource.Builder forQueue(String queue, PushSubscriptionModel model) {
        RabbitMqBrokerProperties.Source sourceProperties = properties.getSource();
        RabbitMqCloudEventSource.Builder builder = RabbitMqCloudEventSource.builder(connection, model, queue)
                .declareTopology(sourceProperties.isDeclareTopology())
                .onDeliveryFailure(sourceProperties.getOnDeliveryFailure())
                .pollInterval(sourceProperties.getPollInterval())
                .prefetchCount(sourceProperties.getPrefetchCount())
                .closeTimeout(sourceProperties.getCloseTimeout())
                .retryStrategy(buildRetryStrategy(queue, sourceProperties.getRetry()))
                .readinessSource(CatchupThenPushReadiness.memoized(applicationContext, model));
        DestinationResolver<RabbitMqDestination> resolver = resolverProvider.getIfAvailable();
        if (resolver != null) {
            builder.resolver(resolver);
        }
        sourceProperties.getParkingDestination().toDestination().ifPresent(builder::parkingDestination);
        return builder;
    }

    /**
     * The same classification {@link RabbitMqBuildFailureClassifier} gives {@code RabbitMqCloudEventSource.Builder}'s
     * own default {@code build()} retry strategy, including its {@code onRetryableError} logging. Calling
     * {@code retryStrategy(...)} replaces that default entirely, so applying the property-driven timing without
     * either would retry an {@code IllegalStateException} from a missing resolver or parking destination forever
     * instead of failing on the first attempt, and leave a retrying startup logging nothing to tell it apart from
     * a hung one.
     */
    private static RetryStrategy buildRetryStrategy(String queue, RabbitMqBrokerProperties.SourceRetry retry) {
        return RetryStrategy.exponentialBackoff(retry.getInitial(), retry.getMax(), retry.getMultiplier())
                .maxAttempts(retry.getMaxAttempts())
                .retryIf(RabbitMqBuildFailureClassifier::isTransient)
                .onRetryableError((info, throwable) -> log.warn(
                        "Attempt {} of {} to build the RabbitMQ source for queue \"{}\" failed. Retrying in {}.",
                        info.getAttemptNumber(), info.getMaxAttempts(), queue, info.getBackoffBeforeNextRetryAttempt(), throwable));
    }
}
