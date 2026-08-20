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

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the base {@code Map<String, Object>} {@code KafkaCloudEventSink.builder(...)} and the bridge builders
 * take, from {@link KafkaBrokerProperties}. Every entry here is a default a caller can still override by putting
 * the same key in {@code additional-properties}, since {@link Map#putAll} below applies the property-supplied
 * additions last. The underlying builders make their own final, unoverridable checks on {@code acks} and
 * {@code enable.auto.commit} regardless of what this produces.
 * <p>
 * Public rather than package-private because the domain-level bridge factory in the sibling {@code .domain}
 * package needs it too. Java visibility has no notion of "package family", and duplicating this map-building logic
 * there instead would risk the two drifting apart.
 */
public final class KafkaClientConfigs {

    private KafkaClientConfigs() {
    }

    public static Map<String, Object> producerConfig(KafkaBrokerProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        config.putAll(properties.getProducer().getAdditionalProperties());
        return config;
    }

    /**
     * {@code enable.auto.commit} is seeded {@code false} here, matching what every bridge requires anyway, so an
     * application configuring nothing beyond {@code bootstrap-servers} and a group id gets a working consumer
     * rather than a build-time refusal. {@code additional-properties} is still applied after, so a caller that
     * deliberately sets it back to {@code true} gets the same refusal the underlying builder already makes for a
     * caller constructing one directly, not a silently stripped override.
     */
    public static Map<String, Object> consumerConfig(KafkaBrokerProperties properties, String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.putAll(properties.getConsumer().getAdditionalProperties());
        return config;
    }
}
