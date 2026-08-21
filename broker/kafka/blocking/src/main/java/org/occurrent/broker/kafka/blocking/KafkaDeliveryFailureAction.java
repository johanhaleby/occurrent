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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
     * The total wall-clock bound on one park attempt, {@code send()} and its confirm together, not the confirm
     * wait alone. {@code send()} can itself consume up to {@code max.block.ms} waiting for a usable view of the
     * cluster before it ever returns a {@code Future}, so {@link #park(ProducerRecord)} measures that elapsed
     * time and waits only what remains of this bound for the confirm, rather than granting the full duration
     * again and letting one attempt stall this bridge's only consume loop for roughly twice as long as this
     * documents. This bounds how long the consume loop itself waits, not how long the underlying send can still
     * be in flight for. {@link #create(Map, DeliveryFailurePolicy, KafkaDestination, Logger)} also configures the
     * parking producer's {@code delivery.timeout.ms} and {@code request.timeout.ms} to this same bound, but
     * {@code delivery.timeout.ms} clocks from when {@code send()} returns, not from when it was called, so a
     * {@code send()} that itself consumed most of {@code max.block.ms} before returning can still complete in the
     * background after this bound has already elapsed and {@link #park(ProducerRecord)} has already chosen
     * {@link Outcome#REDELIVER}. A duplicate park is possible in that case, not eliminated by this bound, and is
     * accepted rather than guarded against, the same way at-least-once delivery already requires every handler
     * downstream of this bridge to tolerate a repeat. Fixed rather than configurable, matching
     * {@link KafkaCloudEventSink}'s own default, since a failure policy is a coarse operational choice that does
     * not need its own tunable separate from the sink's.
     */
    private static final Duration PARK_ACKNOWLEDGEMENT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Keys {@code ConsumerConfig} and {@link ProducerConfig} both define with a different meaning, stripped from
     * the copied producer config rather than carried over silently. {@link ProducerConfig#INTERCEPTOR_CLASSES_CONFIG}
     * is the one known so far, since a class a caller configured as a {@code ConsumerInterceptor} for the bridge's
     * own {@code Consumer} fails {@link KafkaProducer}'s construction outright, {@code ProducerConfig} eagerly
     * instantiates every listed class expecting a {@code ProducerInterceptor} instead.
     */
    private static final Set<String> STRIPPED_COLLIDING_CONFIG_KEYS = Set.of(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);

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
     *                       bounded by {@link #PARK_ACKNOWLEDGEMENT_TIMEOUT}, so this class itself never waits
     *                       past that bound for a park to confirm, see {@link #PARK_ACKNOWLEDGEMENT_TIMEOUT}'s own
     *                       javadoc for why an accepted send can still complete in the background after that wait
     *                       gives up, a duplicate park this bounding does not eliminate. Consumer-only keys left
     *                       over from the copy (
     *                       {@code group.id}, {@code enable.auto.commit}, the deserializer classes, ...) are simply
     *                       unused by a producer, not rejected. Two keys get different treatment.
     *                       {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} is refused outright, the same refusal
     *                       {@link KafkaCloudEventSink.Builder#build()} already applies to its own producer config,
     *                       since a transactional id would put the parking producer in transactional mode and this
     *                       class never calls {@code initTransactions()} or any other transaction lifecycle method.
     *                       {@link ProducerConfig#INTERCEPTOR_CLASSES_CONFIG} is stripped silently instead, since a
     *                       {@code ConsumerInterceptor} a caller configured for the bridge's own {@code Consumer}
     *                       under that same key name fails {@link KafkaProducer}'s construction outright rather
     *                       than being merely unused, {@code ProducerConfig} expects every listed class to be a
     *                       {@code ProducerInterceptor} instead.
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
        STRIPPED_COLLIDING_CONFIG_KEYS.forEach(producerConfig::remove);
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
     * Applies this failure action to {@code record}. {@link DeliveryFailurePolicy#PARK} republishes {@code record}'s
     * own headers and value unchanged, whether or not the bridge managed to rebuild a {@link CloudEvent} from them,
     * rather than a value rebuilt from a decoded {@link CloudEvent} that would carry only the attributes
     * {@link KafkaCloudEventMapper} maps and lose every other original Kafka header or key. {@code destination}'s
     * own configured headers, if any, are added on top rather than replacing the record's own. The key follows ADR
     * 133's rule that publishing uses every configured destination component, {@code destination.key()} when one
     * is configured, {@code record}'s own key otherwise, the same fallback {@code RabbitMqDeliveryFailureAction}
     * has no equivalent of since {@code RabbitMqDestination} carries no per-message key of its own to begin with.
     * Never itself commits or seeks. Only reports which the caller should do.
     */
    public Outcome apply(ConsumerRecord<String, byte[]> record) {
        requireNonNull(record, "record cannot be null");
        if (policy == DeliveryFailurePolicy.REDELIVER) {
            // The one warn line this failure needs: a bridge routing a genuine failure here must not also log the
            // same event itself, the same way a caller relying on park's own log line below already does not.
            log.warn("Redelivering a record from topic \"{}\" partition {} offset {}; nothing consumed it.",
                    record.topic(), record.partition(), record.offset());
            return Outcome.REDELIVER;
        }
        KafkaDestination destination = requireNonNull(parkingDestination);
        String key = destination.key() != null ? destination.key() : record.key();
        ProducerRecord<String, byte[]> parkRecord = new ProducerRecord<>(destination.topic(), null, key, record.value());
        for (Header header : record.headers()) {
            parkRecord.headers().add(header);
        }
        for (Map.Entry<String, String> header : destination.headers().entrySet()) {
            parkRecord.headers().add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
        }
        return park(parkRecord);
    }

    private Outcome park(ProducerRecord<String, byte[]> parkRecord) {
        long sendStartedAtNanos = System.nanoTime();
        try {
            Future<?> future = requireNonNull(parkingProducer).send(parkRecord);
            // send() above can itself consume up to max.block.ms waiting for a usable view of the cluster, so
            // this wait only gets what remains of PARK_ACKNOWLEDGEMENT_TIMEOUT, not the full duration again, or
            // a metadata outage could stall this bridge's only consume loop roughly twice the documented bound.
            // A budget already spent by send() waits zero here, since Future#get(0, ...) checks once and fails
            // immediately rather than blocking, the same accounting KafkaCloudEventSink.publishOnce(CloudEvent)
            // already applies to its own acknowledgementTimeout.
            Duration elapsedSending = Duration.ofNanos(System.nanoTime() - sendStartedAtNanos);
            Duration remainingForAcknowledgement = PARK_ACKNOWLEDGEMENT_TIMEOUT.minus(elapsedSending);
            if (remainingForAcknowledgement.isNegative()) {
                remainingForAcknowledgement = Duration.ZERO;
            }
            future.get(remainingForAcknowledgement.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            log.warn("Failed to park a record nothing consumed. Redelivering it instead of losing it.", e);
            return Outcome.REDELIVER;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while parking a record nothing consumed. Redelivering it instead of losing it.", e);
            return Outcome.REDELIVER;
        }
        log.warn("Parked a record nothing consumed to topic \"{}\" and staged the original's offset for commit.",
                parkRecord.topic());
        return Outcome.RESOLVED;
    }

    /**
     * The {@link DeliveryFailurePolicy} this action applies.
     */
    public DeliveryFailurePolicy policy() {
        return policy;
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
