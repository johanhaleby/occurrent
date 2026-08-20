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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stray {@code group.id} in {@code additional-properties} used to override the per-call {@code groupId}
 * argument, letting two different {@link KafkaClientConfigs#consumerConfig} calls collapse onto the same consumer
 * group. ADR 90 requires one group per consumer, so that collapse is a correctness bug, not a convenience, and
 * these tests pin the fix.
 */
class KafkaClientConfigsTest {

    @Test
    void the_groupId_argument_always_wins_over_additional_properties() {
        KafkaBrokerProperties properties = new KafkaBrokerProperties();
        properties.setBootstrapServers(java.util.List.of("localhost:9092"));
        properties.getConsumer().getAdditionalProperties().put(ConsumerConfig.GROUP_ID_CONFIG, "wrong-group");

        Map<String, Object> config = KafkaClientConfigs.consumerConfig(properties, "the-right-group");

        assertThat(config.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("the-right-group");
    }

    @Test
    void bootstrap_servers_always_comes_from_the_dedicated_property_for_both_producer_and_consumer() {
        KafkaBrokerProperties properties = new KafkaBrokerProperties();
        properties.setBootstrapServers(java.util.List.of("configured:9092"));
        properties.getProducer().getAdditionalProperties().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "wrong:9092");
        properties.getConsumer().getAdditionalProperties().put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "wrong:9092");

        assertThat(KafkaClientConfigs.producerConfig(properties).get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("configured:9092");
        assertThat(KafkaClientConfigs.consumerConfig(properties, "g").get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("configured:9092");
    }

    @Test
    void enable_auto_commit_can_still_be_deliberately_overridden_so_the_underlying_builder_can_refuse_it() {
        KafkaBrokerProperties properties = new KafkaBrokerProperties();
        properties.setBootstrapServers(java.util.List.of("localhost:9092"));
        properties.getConsumer().getAdditionalProperties().put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Map<String, Object> config = KafkaClientConfigs.consumerConfig(properties, "g");

        assertThat(config.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo("true");
    }

    @Test
    void enable_auto_commit_defaults_to_false_when_not_overridden() {
        KafkaBrokerProperties properties = new KafkaBrokerProperties();
        properties.setBootstrapServers(java.util.List.of("localhost:9092"));

        Map<String, Object> config = KafkaClientConfigs.consumerConfig(properties, "g");

        assertThat(config.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo("false");
    }
}
