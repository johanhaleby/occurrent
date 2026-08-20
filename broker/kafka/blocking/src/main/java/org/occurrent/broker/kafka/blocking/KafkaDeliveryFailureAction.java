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
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * since Kafka has no per-record acknowledgement to call: {@link DeliveryFailurePolicy#REDELIVER} always reports
 * {@link #REDELIVER}, telling the bridge's poll loop to {@code seek} back to this record's offset and stop that
 * partition's remaining records for this poll. {@link DeliveryFailurePolicy#PARK} republishes the record to a
 * parking destination, waits for that publish's own broker acknowledgement, and reports {@link #RESOLVED} only once
 * it has one, so the bridge's poll loop stages this record's offset for the next commit exactly as it would for a
 * successful delivery. A failed park (the parking topic unreachable, the acknowledgement wait expiring, ...) reports
 * {@link #REDELIVER} instead, the same as a plain {@code REDELIVER} policy, so a park that goes nowhere never loses
 * the original: nothing commits past this record until it is somewhere it can be read from again.
 * <p>
 * Unlike {@code RabbitMqDeliveryFailureAction}, this holds no dedicated confirm-publisher wrapper. A RabbitMQ
 * publisher confirm is correlated across the whole channel, which is why {@code RabbitMqConfirmPublisher} exists to
 * track that per publish; a Kafka {@code Producer.send(...)} already returns a {@code Future} correlated to that one
 * record on its own, so this class talks to a small internally-owned {@link Producer} directly.
 */
public final class KafkaDeliveryFailureAction implements AutoCloseable {

    /**
     * How long a park publish waits for its confirm. Fixed rather than configurable, matching
     * {@link KafkaCloudEventSink}'s own default, since a failure policy is a coarse operational choice that does not
     * need its own tunable separate from the sink's.
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
     * {@code policy} is {@code PARK} and {@code parkingDestination} is {@code null}. No parking resource is opened
     * at all when {@code policy} is not {@code PARK}, even if {@code parkingDestination} happens to be given anyway.
     *
     * @param consumerConfig Read only for {@link ConsumerConfig#BOOTSTRAP_SERVERS_CONFIG}, reused to build the
     *                       parking producer's own minimal config (forced {@code acks=all}, no retry, a single
     *                       attempt bounded by {@link #PARK_ACKNOWLEDGEMENT_TIMEOUT}).
     */
    public static KafkaDeliveryFailureAction create(Map<String, Object> consumerConfig, DeliveryFailurePolicy policy,
                                                      @Nullable KafkaDestination parkingDestination, Logger log) {
        if (policy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
            throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
        }
        if (policy != DeliveryFailurePolicy.PARK) {
            return new KafkaDeliveryFailureAction(policy, null, null, log);
        }
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PARK_ACKNOWLEDGEMENT_TIMEOUT.toMillis());
        Producer<String, byte[]> parkingProducer = new KafkaProducer<>(producerConfig, new StringSerializer(), new ByteArraySerializer());
        return new KafkaDeliveryFailureAction(policy, parkingProducer, parkingDestination, log);
    }

    /**
     * Applies this failure action to {@code record}, rebuilt as {@code cloudEvent}. Never itself commits or seeks;
     * only reports which the caller should do.
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
