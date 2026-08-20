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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Whether a {@code producerConfig} still keeps the two things stream-id keying needs to actually order records,
 * checked as exactly two legs rather than as a growing list of individual Kafka settings known to break one of
 * them. The partitioner has to respect record keys, and a retried send has to never be appended after a later one
 * that already succeeded. Either leg failing reorders records that share a key on the same partition regardless
 * of how correctly {@link KafkaSharedTopicDestinationResolver} or {@link KafkaTopicPerTypeDestinationResolver}
 * derived that key, so a new Kafka setting discovered to break one of them is a new way to fail the same leg, not
 * a reason to add a third check.
 * <p>
 * The order either leg protects is the order one producer is actually asked to send records in, not some order
 * imposed on top of the caller. A caller whose own publishes are sequential, the next {@link KafkaCloudEventSink#publish(io.cloudevents.CloudEvent)}
 * only starting once the previous one already returned, hands the producer that same order, and both legs holding
 * is what keeps the producer from disturbing it on the way out. A caller that instead calls
 * {@code publish} for the same sink from more than one thread at once never gave those calls a relative order to
 * begin with, so no producer setting restores one afterward. Whichever thread's record actually reaches the
 * producer's accumulator first wins, and that race is Kafka's own accumulator, not something either leg closes or
 * claims to.
 */
final class KafkaOrderingPrerequisites {

    private KafkaOrderingPrerequisites() {
    }

    /**
     * The {@code producerConfig} settings responsible for breaking one or both legs, joined into one string, or
     * {@link Optional#empty()} when both legs hold.
     * <p>
     * <b>The partitioner must respect record keys.</b> It does unless {@code producerConfig} sets
     * {@link ProducerConfig#PARTITIONER_CLASS_CONFIG} to a custom partitioner, {@code RoundRobinPartitioner} being
     * the built-in example, Kafka's own documentation for it says plainly that every record goes to a different
     * partition regardless of whether a key is present, or sets {@link ProducerConfig#PARTITIONER_IGNORE_KEYS_CONFIG}
     * to {@code true}, which makes the default partitioner ignore a key it would otherwise have hashed.
     * <p>
     * <b>A retried send must never be appended after a later one that already succeeded.</b> Kafka's own
     * documentation for {@link ProducerConfig#ENABLE_IDEMPOTENCE_CONFIG} states the interplay directly, idempotence
     * is enabled by default, requires {@code retries} greater than zero, {@code acks=all}, and
     * {@code max.in.flight.requests.per.connection} no greater than five, and a conflicting setting disables
     * idempotence silently when idempotence was never explicitly enabled, or is refused with the producer's own
     * {@code ConfigException} at construction when it was. That refusal is why this checks only whether idempotence
     * ends up effectively enabled, not explicitly {@code false} and {@code retries} not explicitly {@code 0},
     * rather than also refusing an explicit {@code enable.idempotence=true} paired with a conflicting setting.
     * {@link KafkaCloudEventSink.Builder#build()} never returns for that combination, Kafka's own construction
     * already refuses it. Effectively disabled idempotence still keeps ordering when
     * {@code max.in.flight.requests.per.connection} is pinned to {@code 1}, since only one request is ever
     * outstanding on this producer's one connection for a retry to overtake. That pin says nothing about two
     * different threads calling {@code publish} on this sink at once, each is its own caller with its own record
     * on its own path into the accumulator, and the pin only ever orders requests already queued behind one
     * another, never decides which of two concurrent callers gets there first. A retriable failure only backs off
     * and retries once the failed send's outcome is already fully known, {@code KafkaCloudEventSink.publishOnce}
     * only throws after {@code Future#get} for the previous attempt has itself returned, so one caller's own
     * retries never overlap that same caller's later attempt on this producer regardless of this leg. What the pin
     * and idempotence protect against instead is a second, genuinely concurrent {@code publish} call from another
     * thread landing on the accumulator while the first caller's retry is still backing off, since nothing about
     * that second call was ever ordered after the first to begin with.
     */
    static Optional<String> brokenOrderingGuarantee(Map<String, Object> config) {
        List<String> causes = new ArrayList<>();

        Object configuredPartitionerClass = config.get(ProducerConfig.PARTITIONER_CLASS_CONFIG);
        if (configuredPartitionerClass != null) {
            causes.add(ProducerConfig.PARTITIONER_CLASS_CONFIG + "=\"" + configuredPartitionerClass + "\"");
        }
        if (Boolean.parseBoolean(String.valueOf(config.get(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG)))) {
            causes.add(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG + "=true");
        }

        Object configuredIdempotence = config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        boolean idempotenceExplicitlyFalse = configuredIdempotence != null && !Boolean.parseBoolean(String.valueOf(configuredIdempotence));
        Object configuredRetries = config.get(ProducerConfig.RETRIES_CONFIG);
        boolean retriesExplicitlyZero = configuredRetries != null && "0".equals(String.valueOf(configuredRetries).trim());
        boolean idempotenceEffectivelyEnabled = !idempotenceExplicitlyFalse && !retriesExplicitlyZero;

        Object configuredMaxInFlight = config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        boolean maxInFlightPinnedToOne = configuredMaxInFlight != null && "1".equals(String.valueOf(configuredMaxInFlight).trim());

        if (!idempotenceEffectivelyEnabled && !maxInFlightPinnedToOne) {
            if (idempotenceExplicitlyFalse) {
                causes.add(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG + "=false");
            }
            if (retriesExplicitlyZero) {
                causes.add(ProducerConfig.RETRIES_CONFIG + "=0");
            }
        }

        return causes.isEmpty() ? Optional.empty() : Optional.of(String.join(", ", causes));
    }
}
