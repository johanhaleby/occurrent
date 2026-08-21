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
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.kafka.blocking.KafkaPublishException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A stray {@code group.id} in {@code additional-properties} used to override the per-call {@code groupId}
 * argument, letting two different {@link KafkaClientConfigs#consumerConfig} calls collapse onto the same consumer
 * group. ADR 90 requires one group per consumer, so that collapse is a correctness bug, not a convenience, and
 * these tests assert the fix stays in place.
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

    /**
     * Applying the property-driven timing with {@code exponentialBackoff(...)} alone, with no predicate, retries
     * every exception forever. Both retry strategies below must keep the predicate the underlying builder's own
     * default uses, so a permanent failure is attempted once and propagates instead of retrying indefinitely.
     */
    @Test
    void publish_retry_strategy_does_not_retry_a_permanent_publish_failure() {
        KafkaBrokerProperties.Retry retry = fastRetry();
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> KafkaClientConfigs.publishRetryStrategy(retry).execute(() -> {
            attempts.incrementAndGet();
            throw new KafkaPublishException("permanent", new IllegalStateException("not retriable"));
        })).isInstanceOf(KafkaPublishException.class);

        assertThat(attempts).hasValue(1);
    }

    /**
     * A retriable failure has no attempt cap, only {@link #execute} eventually returning ends the loop, so this
     * throws a retriable {@link TimeoutException}-caused failure twice and then a non-retriable one to terminate
     * it, rather than an always-retriable failure that would retry forever.
     */
    @Test
    void publish_retry_strategy_retries_a_retriable_publish_failure() {
        KafkaBrokerProperties.Retry retry = fastRetry();
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> KafkaClientConfigs.publishRetryStrategy(retry).execute(() -> {
            int attempt = attempts.incrementAndGet();
            if (attempt <= 2) {
                throw new KafkaPublishException("transient", new TimeoutException("broker busy"));
            }
            throw new KafkaPublishException("permanent", new IllegalStateException("not retriable"));
        })).isInstanceOf(KafkaPublishException.class);

        assertThat(attempts).hasValue(3);
    }

    @Test
    void commit_retry_strategy_does_not_retry_a_non_retriable_exception() {
        KafkaBrokerProperties.Retry retry = fastRetry();
        AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> KafkaClientConfigs.commitRetryStrategy(retry).execute(() -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("not retriable");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(attempts).hasValue(1);
    }

    private static KafkaBrokerProperties.Retry fastRetry() {
        KafkaBrokerProperties.Retry retry = new KafkaBrokerProperties.Retry();
        retry.setInitial(Duration.ofMillis(1));
        retry.setMax(Duration.ofMillis(2));
        retry.setMultiplier(1.0);
        return retry;
    }
}
