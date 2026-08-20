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
import org.occurrent.broker.kafka.blocking.domain.KafkaDomainEventBridge;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.springboot.broker.kafka.blocking.KafkaBrokerProperties;
import org.occurrent.springboot.broker.kafka.blocking.KafkaClientConfigs;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;

import static java.util.Objects.requireNonNull;

class DefaultKafkaDomainEventBridgeFactory implements KafkaDomainEventBridgeFactory {

    private final KafkaBrokerProperties properties;
    private final ObjectProvider<DestinationResolver<KafkaDestination>> resolverProvider;

    DefaultKafkaDomainEventBridgeFactory(KafkaBrokerProperties properties, ObjectProvider<DestinationResolver<KafkaDestination>> resolverProvider) {
        this.properties = requireNonNull(properties);
        this.resolverProvider = requireNonNull(resolverProvider);
    }

    @Override
    public <E> KafkaDomainEventBridge.Builder<E> forGroup(String groupId, DomainEventFeed<E> feed) {
        KafkaBrokerProperties.Bridge bridgeProperties = properties.getBridge();
        Map<String, Object> consumerConfig = KafkaClientConfigs.consumerConfig(properties, groupId);
        KafkaDomainEventBridge.Builder<E> builder = KafkaDomainEventBridge.builder(consumerConfig, feed)
                .onDeliveryFailure(bridgeProperties.getOnDeliveryFailure())
                .pollTimeout(bridgeProperties.getPollTimeout())
                .closeTimeout(bridgeProperties.getCloseTimeout())
                .commitRetryStrategy(KafkaClientConfigs.commitRetryStrategy(bridgeProperties.getCommitRetry()));
        DestinationResolver<KafkaDestination> resolver = resolverProvider.getIfAvailable();
        if (resolver != null) {
            builder.resolver(resolver);
        }
        bridgeProperties.getParkingDestination().toDestination().ifPresent(builder::parkingDestination);
        return builder;
    }
}
