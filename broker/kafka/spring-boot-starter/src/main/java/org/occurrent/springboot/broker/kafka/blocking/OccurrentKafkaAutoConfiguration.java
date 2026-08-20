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
import org.occurrent.broker.kafka.blocking.KafkaCloudEventSink;
import org.occurrent.broker.kafka.blocking.KafkaDestination;
import org.occurrent.broker.kafka.blocking.KafkaSharedTopicDestinationResolver;
import org.occurrent.springboot.broker.kafka.blocking.domain.KafkaDomainBrokerConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import java.util.Map;

/**
 * Property-driven construction of the Kafka broker sink and consume bridges, per
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0133-a-broker-is-a-transport-for-the-push-feed-and-never-a-subscription-model.md">ADR 133</a>.
 * Enabled with {@link EnableOccurrentKafkaBroker}, the same {@code @Import}-based activation the MongoDB starter
 * uses, rather than Spring Boot's automatic classpath scanning.
 * <p>
 * Activates once {@code occurrent.broker.kafka.bootstrap-servers} is configured, checked through
 * {@link KafkaBootstrapServersConfiguredCondition} rather than a plain {@code @ConditionalOnProperty}, since a
 * property-presence check only recognizes the comma-separated scalar form and misses the indexed YAML list form.
 * Unlike the RabbitMQ starter, no external connection object is required here: Kafka's own builders take a
 * configuration {@link Map} rather than a client object, so this auto-configuration builds that map itself from
 * {@code bootstrap.servers} and {@code occurrent.broker.kafka.producer|consumer.additional-properties}.
 */
@AutoConfiguration
@ConditionalOnClass(KafkaCloudEventSink.class)
@Conditional(KafkaBootstrapServersConfiguredCondition.class)
@EnableConfigurationProperties(KafkaBrokerProperties.class)
@Import(KafkaDomainBrokerConfiguration.class)
public class OccurrentKafkaAutoConfiguration {

    /**
     * The zero-config {@link DestinationResolver} a sink or a bridge factory falls back to when the application
     * declares none, active once {@code occurrent.broker.kafka.topic} is set. {@code @Fallback} rather than
     * {@code @ConditionalOnMissingBean}: this configuration is activated by {@link EnableOccurrentKafkaBroker}'s
     * plain {@code @Import}, so a {@code @ConditionalOnMissingBean} condition can be evaluated before the
     * application's own resolver bean is registered and let both through, the same import-ordering gap ADR 72
     * documents for the MongoDB starter's own {@code Default*Provider} beans. A {@code @Fallback} bean is excluded at
     * dependency-resolution time instead, which registration order cannot affect.
     */
    @Bean
    @Fallback
    @ConditionalOnProperty(prefix = "occurrent.broker.kafka", name = "topic")
    KafkaSharedTopicDestinationResolver occurrentKafkaDestinationResolver(KafkaBrokerProperties properties) {
        return new KafkaSharedTopicDestinationResolver(properties.getTopic());
    }

    /**
     * The zero-config {@code CloudEventSink}. {@code @Lazy} so a consume-only deployment, one that never publishes
     * and so never needs {@code occurrent.broker.kafka.topic} configured, is never forced to satisfy this bean's
     * {@link DestinationResolver} requirement just because the starter is on the classpath. Instantiated, and so
     * validated, only the first time something actually asks for a {@code CloudEventSink}.
     */
    @Bean
    @Lazy
    @Fallback
    KafkaCloudEventSink occurrentKafkaCloudEventSink(DestinationResolver<KafkaDestination> resolver, KafkaBrokerProperties properties) {
        KafkaBrokerProperties.Retry retry = properties.getSink().getRetry();
        return KafkaCloudEventSink.builder(KafkaClientConfigs.producerConfig(properties), resolver)
                .acknowledgementTimeout(properties.getSink().getAcknowledgementTimeout())
                .retryStrategy(KafkaClientConfigs.publishRetryStrategy(retry))
                .build();
    }

    /**
     * Pre-seeds {@link KafkaCloudEventBridgeFactory} with {@code bootstrap.servers}, the resolver bean if one
     * exists (a caller supplying its own bindings needs none), and every {@code occurrent.broker.kafka.bridge.*}
     * default. Not conditioned on a resolver, since building this factory does nothing but capture configuration.
     * Nothing about it opens a consumer until a caller actually calls
     * {@link KafkaCloudEventBridgeFactory#forGroup}.
     */
    @Bean
    KafkaCloudEventBridgeFactory occurrentKafkaCloudEventBridgeFactory(KafkaBrokerProperties properties, ObjectProvider<DestinationResolver<KafkaDestination>> resolver) {
        return new DefaultKafkaCloudEventBridgeFactory(properties, resolver);
    }
}
