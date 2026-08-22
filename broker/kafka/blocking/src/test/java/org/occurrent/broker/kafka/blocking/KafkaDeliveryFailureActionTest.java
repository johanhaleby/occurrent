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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaDeliveryFailureAction#create(Map, DeliveryFailurePolicy, KafkaDestination, org.slf4j.Logger)}
 * against a real, but never network-connecting, {@code KafkaProducer} construction, for behaviour a real broker
 * has no way to force on demand and Kafka's client itself already validates synchronously at construction. One
 * exception below points the parking producer at an address nothing answers, deliberately forcing the one publish
 * path a real broker cannot force on demand either, a failed park.
 */
class KafkaDeliveryFailureActionTest {

    /**
     * The parking producer has to be seeded from the bridge's whole {@code consumerConfig}, not only
     * {@code bootstrap.servers}, or a cluster secured with {@code security.protocol}, SASL or SSL settings leaves
     * the consumer able to consume but the parking producer unable to connect at all. Proven here without a real
     * secured broker. A producer-only setting ({@code partitioner.class}, a consumer never reads) is planted in
     * {@code consumerConfig} with a class name that does not exist. {@code KafkaProducer}'s own constructor
     * instantiates a configured partitioner eagerly, so this only throws if {@code create(...)} actually carried
     * that setting through to the producer it built, rather than starting from an empty config and adding only
     * {@code bootstrap.servers} back in.
     */
    @Test
    void create_seeds_the_parking_producer_from_the_whole_consumerConfig_not_only_bootstrap_servers() {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.NoSuchPartitionerClassAtAll");
        KafkaDestination parkingDestination = KafkaDestination.of("parking-topic");

        assertThatThrownBy(() -> KafkaDeliveryFailureAction.create(consumerConfig, DeliveryFailurePolicy.PARK,
                parkingDestination, LoggerFactory.getLogger(getClass())))
                .hasMessageContaining("NoSuchPartitionerClassAtAll");
    }

    /**
     * The inverse of the test above. With nothing left over to break, {@code create(...)} builds cleanly under
     * {@code PARK} with a literal {@code parkingDestination}.
     */
    @Test
    void create_builds_cleanly_under_PARK_with_a_literal_parkingDestination_and_no_leftover_producer_settings() {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        KafkaDestination parkingDestination = KafkaDestination.of("parking-topic");

        assertThatCode(() -> KafkaDeliveryFailureAction.create(consumerConfig, DeliveryFailurePolicy.PARK,
                parkingDestination, LoggerFactory.getLogger(getClass())).close())
                .doesNotThrowAnyException();
    }

    /**
     * A {@code transactional.id} carried over from {@code consumerConfig} puts the parking producer in
     * transactional mode, which requires calling {@code initTransactions()} and the rest of that lifecycle before
     * any {@code send()}, and this class never does, so every park would be rejected or withheld indefinitely.
     * Refused at {@code create(...)} instead, the same shape {@link KafkaCloudEventSink.Builder#build()} already
     * refuses a transactional id in its own producer config.
     */
    @Test
    void create_refuses_a_transactional_id_carried_over_from_consumerConfig() {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "some-transactional-id");
        KafkaDestination parkingDestination = KafkaDestination.of("parking-topic");

        assertThatThrownBy(() -> KafkaDeliveryFailureAction.create(consumerConfig, DeliveryFailurePolicy.PARK,
                parkingDestination, LoggerFactory.getLogger(getClass())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    /**
     * {@code interceptor.classes} names the same key in both {@code ConsumerConfig} and {@code ProducerConfig},
     * with a different expected interface on each side. A {@code ConsumerInterceptor} a caller configured for the
     * bridge's own {@code Consumer} would otherwise carry straight into the parking producer's config too, where
     * {@code ProducerConfig} eagerly instantiates it expecting a {@code ProducerInterceptor}, failing construction
     * for a perfectly valid consumer-side setting that has nothing to do with parking.
     */
    @Test
    void create_strips_a_colliding_interceptor_classes_key_rather_than_failing_construction() {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, NoOpConsumerInterceptor.class.getName());
        KafkaDestination parkingDestination = KafkaDestination.of("parking-topic");

        assertThatCode(() -> KafkaDeliveryFailureAction.create(consumerConfig, DeliveryFailurePolicy.PARK,
                parkingDestination, LoggerFactory.getLogger(getClass())).close())
                .doesNotThrowAnyException();
    }

    public static final class NoOpConsumerInterceptor implements ConsumerInterceptor<String, byte[]> {
        @Override
        public ConsumerRecords<String, byte[]> onConsume(ConsumerRecords<String, byte[]> records) {
            return records;
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    /**
     * The no-loss guarantee the class javadoc states, a failed park redelivers rather than losing the record.
     * {@code bootstrap.servers} points at an address nothing answers, so the parking publish can never complete
     * within {@code max.block.ms}/{@code delivery.timeout.ms}, both bounded by the same
     * {@code PARK_ACKNOWLEDGEMENT_TIMEOUT} {@code create(...)} configures. Waits out that bound (five seconds) to
     * prove the catch branch itself runs, not merely that it exists.
     * <p>
     * Also proves the total wall-clock bound {@code park(ProducerRecord)} now applies, {@code send()} plus its
     * confirm together, comfortably below what the pre-fix doubling would have allowed (roughly ten seconds,
     * {@code max.block.ms} then the full {@code PARK_ACKNOWLEDGEMENT_TIMEOUT} again for the confirm wait). This
     * specific unreachable-broker scenario has {@code send()} itself throw the {@code TimeoutException} directly,
     * so {@code park(ProducerRecord)} never reaches its {@code future.get(...)} call at all here, which means
     * this timing assertion cannot mutation-fail by reverting the elapsed-time deduction alone. That deduction's
     * own bound holds by construction instead, {@code remainingForAcknowledgement} is
     * {@code PARK_ACKNOWLEDGEMENT_TIMEOUT} minus whatever {@code send()} already spent, floored at zero, both
     * clocked from the same instant, so the total can never exceed {@code PARK_ACKNOWLEDGEMENT_TIMEOUT} plus
     * negligible method-call overhead regardless of how that budget splits between the two calls. Forcing a real
     * broker to make {@code send()} itself consume a meaningful, non-zero share of that budget without also
     * throwing would need network-level fault injection this module does not otherwise use.
     */
    @Test
    void a_park_publish_that_never_completes_redelivers_instead_of_losing_the_record() {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        KafkaDestination parkingDestination = KafkaDestination.of("parking-topic");
        KafkaDeliveryFailureAction action = KafkaDeliveryFailureAction.create(consumerConfig, DeliveryFailurePolicy.PARK,
                parkingDestination, LoggerFactory.getLogger(getClass()));
        try {
            ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("source-topic", 0, 0L, "key",
                    "value".getBytes());

            long startedAtNanos = System.nanoTime();
            KafkaDeliveryFailureAction.Outcome outcome = action.apply(record);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAtNanos);

            assertThat(outcome).isEqualTo(KafkaDeliveryFailureAction.Outcome.REDELIVER);
            assertThat(elapsed).isLessThan(Duration.ofSeconds(7));
        } finally {
            action.close();
        }
    }
}
