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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.RetriableException;
import org.occurrent.broker.kafka.blocking.KafkaPublishException;
import org.occurrent.broker.kafka.blocking.KafkaPublishTimeoutException;
import org.occurrent.retry.RetryStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the base {@code Map<String, Object>} {@code KafkaCloudEventSink.builder(...)} and the bridge builders
 * take, from {@link KafkaBrokerProperties}. {@code additional-properties} fills in anything else a caller wants
 * and can override a seeded default such as {@link #consumerConfig}'s {@code enable.auto.commit}, since a caller
 * deliberately setting that back should still reach the underlying builder's own refusal. It can never override
 * {@code bootstrap.servers} or, for {@link #consumerConfig}, {@code group.id}, which are set last and always win.
 * ADR 90 requires one consumer group per consumer, so letting a stray {@code additional-properties} entry collapse
 * two different {@link #consumerConfig} calls onto the same group would be a correctness bug, not a convenience.
 * <p>
 * Public rather than package-private because the domain-level bridge factory in the sibling {@code .domain}
 * package needs it too. Java visibility has no notion of "package family", and duplicating this map-building logic
 * there instead would risk the two drifting apart.
 */
public final class KafkaClientConfigs {

    private KafkaClientConfigs() {
    }

    public static Map<String, Object> producerConfig(KafkaBrokerProperties properties) {
        Map<String, Object> config = new HashMap<>(properties.getProducer().getAdditionalProperties());
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        return config;
    }

    /**
     * {@code enable.auto.commit} is seeded {@code false} here, matching what every bridge requires anyway, so an
     * application configuring nothing beyond {@code bootstrap-servers} and a group id gets a working consumer
     * rather than a build-time refusal. {@code additional-properties} is applied after that seed, so a caller that
     * deliberately sets {@code enable.auto.commit} back to {@code true} still gets the same refusal the underlying
     * builder already makes for a caller constructing one directly. {@code bootstrap.servers} and {@code group.id}
     * are set after {@code additional-properties} instead, so neither can be overridden that way.
     */
    public static Map<String, Object> consumerConfig(KafkaBrokerProperties properties, String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.putAll(properties.getConsumer().getAdditionalProperties());
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return config;
    }

    /**
     * The same {@code RetriableException}-only predicate {@code KafkaCloudEventBridge.Builder}'s and
     * {@code KafkaDomainEventBridge.Builder}'s own default commit retry strategy uses. Calling
     * {@code commitRetryStrategy(...)} replaces that default entirely, so applying the property-driven timing
     * without this predicate would retry a permanent commit failure forever instead of surfacing it.
     */
    public static RetryStrategy.Retry commitRetryStrategy(KafkaBrokerProperties.Retry retry) {
        return RetryStrategy.exponentialBackoff(retry.getInitial(), retry.getMax(), retry.getMultiplier())
                .retryIf(throwable -> throwable instanceof RetriableException);
    }

    /**
     * The same predicate {@code KafkaCloudEventSink.Builder}'s own default retry strategy uses, a
     * {@link KafkaPublishException} that is not a {@link KafkaPublishTimeoutException} and whose cause Kafka
     * itself marks {@link RetriableException}. Calling {@code retryStrategy(...)} replaces that default entirely,
     * so applying the property-driven timing without this predicate would retry a permanent publish failure
     * forever instead of surfacing it.
     */
    public static RetryStrategy.Retry publishRetryStrategy(KafkaBrokerProperties.Retry retry) {
        return RetryStrategy.exponentialBackoff(retry.getInitial(), retry.getMax(), retry.getMultiplier())
                .retryIf(throwable -> throwable instanceof KafkaPublishException publishException
                        && !(publishException instanceof KafkaPublishTimeoutException)
                        && publishException.getCause() instanceof RetriableException);
    }
}
