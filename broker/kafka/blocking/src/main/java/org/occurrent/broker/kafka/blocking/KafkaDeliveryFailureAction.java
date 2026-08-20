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

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.KafkaMessageFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * What a consume-side bridge does with a record it will not commit, per ADR 133 decision 7, mirroring
 * {@code RabbitMqDeliveryFailureAction}'s shape but returning a decision instead of acting on a channel directly,
 * since Kafka has no per-record acknowledgement to call. {@link DeliveryFailurePolicy#REDELIVER} always reports
 * {@link #REDELIVER}, telling the bridge's poll loop to {@code seek} back to this record's offset and stop that
 * partition's remaining records for this poll. {@link DeliveryFailurePolicy#PARK} republishes the record to a
 * parking destination, waits for that publish's own broker acknowledgement, and reports {@link #RESOLVED} only once
 * it has one, so the bridge's poll loop stages this record's offset for the next commit exactly as it would for a
 * successful delivery. A failed park (the parking topic unreachable, the acknowledgement wait expiring, ...) reports
 * {@link #REDELIVER} instead, the same as a plain {@code REDELIVER} policy, so a park that goes nowhere never loses
 * the original. Nothing commits past this record until it is somewhere it can be read from again.
 * <p>
 * Unlike {@code RabbitMqDeliveryFailureAction}, this holds no dedicated confirm-publisher wrapper. A RabbitMQ
 * publisher confirm is correlated across the whole channel, which is why {@code RabbitMqConfirmPublisher} exists to
 * track that per publish. A Kafka {@code Producer.send(...)} already returns a {@code Future} correlated to that one
 * record on its own, so this class talks to a small internally-owned {@link Producer} directly.
 */
public final class KafkaDeliveryFailureAction implements AutoCloseable {

    /**
     * How long a park publish waits for its confirm, and, since {@link #create(Map, DeliveryFailurePolicy, KafkaDestination, Logger)}
     * also configures the parking producer's {@code delivery.timeout.ms} and {@code request.timeout.ms} to this same
     * bound, how long the publish itself is allowed to still be in flight for. Fixed rather than configurable,
     * matching {@link KafkaCloudEventSink}'s own default, since a failure policy is a coarse operational choice that
     * does not need its own tunable separate from the sink's.
     */
    private static final Duration PARK_ACKNOWLEDGEMENT_TIMEOUT = Duration.ofSeconds(5);

    public enum Outcome {
        /** Safe to stage this record's offset for the next commit. */
        RESOLVED,
        /** Seek back to this record's offset and stop this partition's remaining records for this poll. */
        REDELIVER
    }

    private final DeliveryFailurePolicy policy;
    private final @Nullable Producer<String, byte[]> parkingProducer;
    private final @Nullable KafkaDestination parkingDestination;
    private final Logger log;

    private KafkaDeliveryFailureAction(DeliveryFailurePolicy policy, @Nullable Producer<String, byte[]> parkingProducer,
                                        @Nullable KafkaDestination parkingDestination, Logger log) {
        this.policy = policy;
        this.parkingProducer = parkingProducer;
        this.parkingDestination = parkingDestination;
        this.log = log;
    }

    /**
     * Builds the action for {@code policy}, and the parking {@link Producer} it publishes through when
     * {@code policy} is {@link DeliveryFailurePolicy#PARK} and {@code parkingDestination} is given. Refuses when
     * {@code policy} is {@code PARK} and {@code parkingDestination} is {@code null}, and when it is given but
     * {@link KafkaDestination#topicIsPattern()}, since a pattern is meant for {@code Consumer.subscribe(Pattern)}, never
     * for publishing, and using its regex text as a literal producer topic either fails on every park (most
     * patterns are not legal topic names) or silently publishes to a topic nobody meant to name. No parking
     * resource is opened at all when {@code policy} is not {@code PARK}, even if {@code parkingDestination} happens
     * to be given anyway.
     *
     * @param consumerConfig The bridge's own consumer configuration, copied as the base for the parking producer's
     *                       configuration so a cluster secured with {@code security.protocol}, SASL or SSL settings
     *                       reaches the parking topic too, not only the topic this bridge consumes from. A handful
     *                       of producer-specific settings are then forced on top, {@code acks=all} and
     *                       {@code max.block.ms}, {@code delivery.timeout.ms} and {@code request.timeout.ms} all
     *                       bounded by {@link #PARK_ACKNOWLEDGEMENT_TIMEOUT}, so a park publish is a genuine single
     *                       attempt within that bound rather than one that can still be quietly retrying in the
     *                       background after this class has already given up and redelivered instead, which risks
     *                       parking the same record twice. Consumer-only keys left over from the copy (
     *                       {@code group.id}, {@code enable.auto.commit}, the deserializer classes, ...) are simply
     *                       unused by a producer, not rejected. {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} is
     *                       the one key refused rather than silently carried over, the same refusal
     *                       {@link KafkaCloudEventSink.Builder#build()} already applies to its own producer config,
     *                       since a transactional id would put the parking producer in transactional mode and this
     *                       class never calls {@code initTransactions()} or any other transaction lifecycle method.
     */
    public static KafkaDeliveryFailureAction create(Map<String, Object> consumerConfig, DeliveryFailurePolicy policy,
                                                      @Nullable KafkaDestination parkingDestination, Logger log) {
        if (policy == DeliveryFailurePolicy.PARK) {
            if (parkingDestination == null) {
                throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
            }
            if (parkingDestination.topicIsPattern()) {
                throw new IllegalStateException("parkingDestination \"" + parkingDestination.topic() + "\" is " +
                        "pattern-typed (topicIsPattern() is true), meant for subscribing, never for publishing. " +
                        "PARK needs a literal topic name to park a failed delivery to.");
            }
        }
        if (policy != DeliveryFailurePolicy.PARK) {
            return new KafkaDeliveryFailureAction(policy, null, null, log);
        }
        Map<String, Object> producerConfig = new HashMap<>(consumerConfig);
        Object configuredTransactionalId = producerConfig.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        if (configuredTransactionalId != null) {
            throw new IllegalStateException("consumerConfig sets \"" + ProducerConfig.TRANSACTIONAL_ID_CONFIG +
                    "\" to \"" + configuredTransactionalId + "\", carried over into the parking producer's own " +
                    "config. " + KafkaDeliveryFailureAction.class.getSimpleName() + " never calls initTransactions() " +
                    "or any other transaction lifecycle method, so every park would be rejected or withheld " +
                    "indefinitely by a producer configured this way.");
        }
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PARK_ACKNOWLEDGEMENT_TIMEOUT.toMillis());
        producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) PARK_ACKNOWLEDGEMENT_TIMEOUT.toMillis());
        producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) PARK_ACKNOWLEDGEMENT_TIMEOUT.toMillis() - 1000);
        Producer<String, byte[]> parkingProducer = new KafkaProducer<>(producerConfig, new StringSerializer(), new ByteArraySerializer());
        return new KafkaDeliveryFailureAction(policy, parkingProducer, parkingDestination, log);
    }

    /**
     * Applies this failure action to {@code record}, rebuilt as {@code cloudEvent}. Never itself commits or seeks.
     * Only reports which the caller should do.
     */
    public Outcome apply(ConsumerRecord<String, byte[]> record, CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        if (policy == DeliveryFailurePolicy.REDELIVER) {
            return Outcome.REDELIVER;
        }
        KafkaDestination destination = requireNonNull(parkingDestination);
        ProducerRecord<String, byte[]> parkRecord = KafkaMessageFactory
                .<String>createWriter(destination.topic(), null, null, destination.key())
                .writeBinary(cloudEvent);
        for (Map.Entry<String, String> header : destination.headers().entrySet()) {
            parkRecord.headers().add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
        }
        return park(parkRecord);
    }

    /**
     * Applies this failure action to {@code record}, for one {@link KafkaCloudEventMapper#toCloudEvent(ConsumerRecord)}
     * could not turn into a {@link CloudEvent} at all. {@link DeliveryFailurePolicy#PARK} republishes
     * {@code record}'s own key, headers and value unchanged rather than a rebuilt {@link CloudEvent}, since none
     * exists to rebuild.
     */
    public Outcome applyToUndecodable(ConsumerRecord<String, byte[]> record) {
        requireNonNull(record, "record cannot be null");
        if (policy == DeliveryFailurePolicy.REDELIVER) {
            return Outcome.REDELIVER;
        }
        KafkaDestination destination = requireNonNull(parkingDestination);
        ProducerRecord<String, byte[]> parkRecord = new ProducerRecord<>(destination.topic(), null, record.key(), record.value());
        for (Header header : record.headers()) {
            parkRecord.headers().add(header);
        }
        for (Map.Entry<String, String> header : destination.headers().entrySet()) {
            parkRecord.headers().add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
        }
        return park(parkRecord);
    }

    private Outcome park(ProducerRecord<String, byte[]> parkRecord) {
        try {
            requireNonNull(parkingProducer).send(parkRecord)
                    .get(PARK_ACKNOWLEDGEMENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            log.warn("Failed to park a record nothing consumed. Redelivering it instead of losing it.", e);
            return Outcome.REDELIVER;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while parking a record nothing consumed. Redelivering it instead of losing it.", e);
            return Outcome.REDELIVER;
        }
        return Outcome.RESOLVED;
    }

    /**
     * Closes the parking producer this action built, if {@link DeliveryFailurePolicy#PARK} was configured. Best
     * effort during teardown, a failure closing it is logged rather than thrown.
     */
    @Override
    public void close() {
        if (parkingProducer != null) {
            try {
                parkingProducer.close(Duration.ofSeconds(30));
            } catch (RuntimeException e) {
                log.warn("Failed to close the parking producer cleanly during shutdown.", e);
            }
        }
    }
}
