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

package org.occurrent.broker.kafka.blocking;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure unit coverage for {@link KafkaOrderingPrerequisites#brokenOrderingGuarantee(Map)}, one config map at a
 * time, no broker needed. {@code KafkaCloudEventSinkTest.OrderingPrerequisiteWarning} covers the wiring, that
 * {@link KafkaCloudEventSink.Builder#build()} actually calls this predicate and logs, so it is not repeated here.
 */
class KafkaOrderingPrerequisitesTest {

    @Test
    void an_empty_producerConfig_keeps_both_legs() {
        Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of());

        assertThat(result).isEmpty();
    }

    /**
     * The partitioning leg, whether the producer still partitions by record key.
     */
    @Nested
    class PartitioningLeg {

        @Test
        void partitioner_ignore_keys_true_breaks_it() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, "true"));

            assertThat(result).isPresent();
            assertThat(result.get()).contains(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG + "=true");
        }

        @Test
        void partitioner_ignore_keys_false_keeps_it() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, "false"));

            assertThat(result).isEmpty();
        }

        @Test
        void a_custom_partitioner_class_breaks_it_even_when_it_never_touches_ignore_keys() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class));

            assertThat(result).isPresent();
            assertThat(result.get()).contains(ProducerConfig.PARTITIONER_CLASS_CONFIG);
            assertThat(result.get()).contains(RoundRobinPartitioner.class.toString());
        }
    }

    /**
     * The retry-ordering leg, whether a retried send can ever land after a later one that already succeeded on the
     * same partition.
     */
    @Nested
    class RetryOrderingLeg {

        @Test
        void enable_idempotence_explicitly_false_breaks_it() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"));

            assertThat(result).isPresent();
            assertThat(result.get()).contains(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG + "=false");
        }

        @Test
        void enable_idempotence_explicitly_false_is_safe_once_max_in_flight_requests_is_pinned_to_one() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false",
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"));

            assertThat(result).isEmpty();
        }

        @Test
        void retries_zero_breaks_it_even_though_idempotence_is_never_mentioned() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.RETRIES_CONFIG, "0"));

            assertThat(result).isPresent();
            assertThat(result.get()).contains(ProducerConfig.RETRIES_CONFIG + "=0");
        }

        @Test
        void retries_zero_is_safe_once_max_in_flight_requests_is_pinned_to_one() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.RETRIES_CONFIG, "0",
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"));

            assertThat(result).isEmpty();
        }

        @Test
        void retries_left_unset_keeps_idempotence_effectively_enabled() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "irrelevant:9092"));

            assertThat(result).isEmpty();
        }

        @Test
        void max_in_flight_requests_pinned_to_a_value_other_than_one_does_not_cover_for_a_broken_leg() {
            Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false",
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "2"));

            assertThat(result).isPresent();
            assertThat(result.get()).contains(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG + "=false");
        }
    }

    @Test
    void both_legs_broken_at_once_names_a_cause_from_each() {
        Optional<String> result = KafkaOrderingPrerequisites.brokenOrderingGuarantee(Map.of(
                ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, "true",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"));

        assertThat(result).isPresent();
        assertThat(result.get()).contains(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG + "=true");
        assertThat(result.get()).contains(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG + "=false");
    }
}
