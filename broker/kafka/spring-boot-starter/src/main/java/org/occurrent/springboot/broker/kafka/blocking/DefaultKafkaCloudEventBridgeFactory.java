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

package org.occurrent.springboot.broker.kafka.blocking;

import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.kafka.blocking.KafkaCloudEventBridge;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.RoutingOutcomeChannel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import java.util.Map;

import static java.util.Objects.requireNonNull;

class DefaultKafkaCloudEventBridgeFactory implements KafkaCloudEventBridgeFactory {

    private final KafkaBrokerProperties properties;
    private final ObjectProvider<DestinationResolver<KafkaDestination>> resolverProvider;
    private final ApplicationContext applicationContext;

    DefaultKafkaCloudEventBridgeFactory(KafkaBrokerProperties properties, ObjectProvider<DestinationResolver<KafkaDestination>> resolverProvider,
                                         ApplicationContext applicationContext) {
        this.properties = requireNonNull(properties);
        this.resolverProvider = requireNonNull(resolverProvider);
        this.applicationContext = requireNonNull(applicationContext);
    }

    @Override
    public KafkaCloudEventBridge.Builder forGroup(String groupId, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel) {
        KafkaBrokerProperties.Bridge bridgeProperties = properties.getBridge();
        Map<String, Object> consumerConfig = KafkaClientConfigs.consumerConfig(properties, groupId);
        KafkaCloudEventBridge.Builder builder = KafkaCloudEventBridge.builder(consumerConfig, model, outcomeChannel)
                .onDeliveryFailure(bridgeProperties.getOnDeliveryFailure())
                .pollTimeout(bridgeProperties.getPollTimeout())
                .closeTimeout(bridgeProperties.getCloseTimeout())
                .commitRetryStrategy(KafkaClientConfigs.commitRetryStrategy(bridgeProperties.getCommitRetry()))
                .readinessSource(CatchupThenPushReadiness.memoized(applicationContext, model));
        DestinationResolver<KafkaDestination> resolver = resolverProvider.getIfAvailable();
        if (resolver != null) {
            builder.resolver(resolver);
        }
        bridgeProperties.getParkingDestination().toDestination().ifPresent(builder::parkingDestination);
        return builder;
    }
}
