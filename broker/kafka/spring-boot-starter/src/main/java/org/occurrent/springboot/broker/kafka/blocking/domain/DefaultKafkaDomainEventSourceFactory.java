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

import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.domain.KafkaDomainEventSource;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.springboot.broker.kafka.blocking.KafkaBrokerProperties;
import org.occurrent.springboot.broker.kafka.blocking.KafkaClientConfigs;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;

import static java.util.Objects.requireNonNull;

class DefaultKafkaDomainEventSourceFactory implements KafkaDomainEventSourceFactory {

    private final KafkaBrokerProperties properties;
    private final ObjectProvider<DestinationResolver<KafkaDestination>> resolverProvider;

    DefaultKafkaDomainEventSourceFactory(KafkaBrokerProperties properties, ObjectProvider<DestinationResolver<KafkaDestination>> resolverProvider) {
        this.properties = requireNonNull(properties);
        this.resolverProvider = requireNonNull(resolverProvider);
    }

    @Override
    public <E> KafkaDomainEventSource.Builder<E> forGroup(String groupId, DomainEventFeed<E> feed) {
        KafkaBrokerProperties.Source sourceProperties = properties.getSource();
        Map<String, Object> consumerConfig = KafkaClientConfigs.consumerConfig(properties, groupId);
        KafkaDomainEventSource.Builder<E> builder = KafkaDomainEventSource.builder(consumerConfig, feed)
                .onDeliveryFailure(sourceProperties.getOnDeliveryFailure())
                .pollTimeout(sourceProperties.getPollTimeout())
                .closeTimeout(sourceProperties.getCloseTimeout())
                .commitRetryStrategy(KafkaClientConfigs.commitRetryStrategy(sourceProperties.getCommitRetry()));
        DestinationResolver<KafkaDestination> resolver = resolverProvider.getIfAvailable();
        if (resolver != null) {
            builder.resolver(resolver);
        }
        sourceProperties.getParkingDestination().toDestination().ifPresent(builder::parkingDestination);
        return builder;
    }
}
