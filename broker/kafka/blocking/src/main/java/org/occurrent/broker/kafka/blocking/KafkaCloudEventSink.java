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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.retry.RetryStrategy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Publishes a {@link CloudEvent} to Kafka, in the CloudEvents binary content mode
 * {@code io.cloudevents.kafka.KafkaMessageFactory} writes. Bring-your-own-sink is the primary way to use the broker
 * modules at all, so use this one when you have not already written your own {@link CloudEventSink}. It is a
 * complete, acknowledged, at-least-once publisher rather than a starting point to copy from.
 * <p>
 * {@link #publish(CloudEvent)} does not return until the broker has acknowledged the send. {@link #builder(Map, DestinationResolver)}
 * builds and owns its own {@link Producer}, from the {@code producerConfig} given to it, so it can inspect and
 * correct the one setting that decides whether that acknowledgement means anything: {@link ProducerConfig#ACKS_CONFIG}.
 * A config with no {@code acks} entry has {@code "all"} set on its behalf, and a config that sets {@code acks} to
 * anything other than {@code "all"} or {@code "-1"} is refused at {@link Builder#build()} rather than silently
 * honoured, since under a weaker setting a send future can complete, and this sink would then wait, succeed, and let
 * {@link CloudEventForwarder} advance its checkpoint past an event no broker ever durably stored. That mistake is
 * invisible afterwards, so startup is the only place it can be caught. The two serializers are forced the same way,
 * to {@link StringSerializer} and {@link ByteArraySerializer}, regardless of what {@code producerConfig} sets for
 * {@code key.serializer} and {@code value.serializer}, since this sink's wire format is fixed by
 * {@code KafkaMessageFactory}'s binary writer rather than left to the caller's configuration.
 * <p>
 * An acknowledgement timeout, five seconds by default, bounds the whole of {@link #publish(CloudEvent)}, not only
 * the wait on the send's {@link Future}. {@link Builder#build()} forces Kafka's own {@code max.block.ms} down to
 * this same timeout, so a {@code send} that needs a fresh view of the cluster, most often because the broker was
 * reachable when this producer last used it and has since gone away, cannot block on its own for up to Kafka's
 * default of a minute before the acknowledgement wait even begins. Either failure, {@code send} itself giving up on
 * the cluster or the broker never acknowledging a record it already accepted, fails the same way, with
 * {@link KafkaPublishTimeoutException} rather than blocking forever on a broker that never answers, and
 * {@link Builder#acknowledgementTimeout(Duration)} is not offered as something that can be turned off, for the same
 * reason {@link CloudEventSink}'s own javadoc gives.
 * <p>
 * Unlike a RabbitMQ channel, sending on this sink's {@link Producer} needs no external serialization and an
 * abandoned wait needs no client-side cleanup. {@code KafkaProducer.send} and the {@link Future} it returns are
 * documented thread-safe for concurrent callers, and each record's outcome is tracked independently by the Kafka
 * client's own accumulator and sender thread, so a publish this sink gave up waiting on does not hold up or get
 * blamed for a later, unrelated publish the way an outstanding RabbitMQ publisher confirm does. Kafka's client is
 * also its own proof of routing, unlike RabbitMQ's publisher confirms, which say only that the broker took the
 * message and need a separate {@code basic.return} check to know it was routed anywhere. A Kafka acknowledgement
 * under {@code acks=all} already means the record was written to the partition leader and replicated, so there is
 * no equivalent of {@code RabbitMqUnroutableEventException} here.
 * <p>
 * A transient failure is retried under {@link Builder#retryStrategy(RetryStrategy)} before a caller sees it,
 * exponential backoff from 100 ms up to 2 seconds by default, only when Kafka itself marked the failure retriable.
 * A permanent failure, an unknown topic or an authorization error for example, is never retried by the default,
 * since ADR 133 asks this retry to guard against a transient failure and retrying a permanent one without limit
 * would keep {@link #publish(CloudEvent)} from ever returning. The retry is also not a substitute for the
 * acknowledgement wait, since a publish that was never acknowledged is not known to have failed, only unresolved.
 * Per ADR 133, an expired {@link KafkaPublishTimeoutException} is for the caller to decide on rather than
 * something this retry absorbs, so the default excludes it too.
 * <p>
 * Call {@link #close()} once the sink is no longer needed. It closes the {@link Producer} this sink created and
 * owns.
 */
public final class KafkaCloudEventSink implements CloudEventSink, AutoCloseable {

    private static final long CLOSE_TIMEOUT_SECONDS = 30;

    private final Producer<String, byte[]> producer;
    private final DestinationResolver<KafkaDestination> resolver;
    private final Duration acknowledgementTimeout;
    private final RetryStrategy retryStrategy;

    private KafkaCloudEventSink(Producer<String, byte[]> producer, DestinationResolver<KafkaDestination> resolver, Duration acknowledgementTimeout, RetryStrategy retryStrategy) {
        this.producer = producer;
        this.resolver = resolver;
        this.acknowledgementTimeout = acknowledgementTimeout;
        this.retryStrategy = retryStrategy;
    }

    /**
     * @param producerConfig Kafka producer configuration, {@code bootstrap.servers} at minimum. Read once, at
     *                        {@link Builder#build()}, to construct and own this sink's own {@link Producer}. This
     *                        sink sets {@code acks} to {@code "all"} when {@code producerConfig} does not, and
     *                        refuses to build when {@code producerConfig} sets it to anything else, and it always
     *                        supplies its own key and value serializers regardless of what {@code producerConfig}
     *                        says about them.
     * @param resolver        Derives where each published event goes.
     */
    public static Builder builder(Map<String, Object> producerConfig, DestinationResolver<KafkaDestination> resolver) {
        return new Builder(producerConfig, resolver);
    }

    @Override
    public void publish(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        retryStrategy.execute(() -> publishOnce(cloudEvent));
    }

    private void publishOnce(CloudEvent cloudEvent) {
        KafkaDestination destination = resolver.destinationFor(cloudEvent);
        ProducerRecord<String, byte[]> record = KafkaMessageFactory
                .<String>createWriter(destination.topic(), null, null, destination.key())
                .writeBinary(cloudEvent);
        for (Map.Entry<String, String> header : destination.headers().entrySet()) {
            record.headers().add(header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8));
        }

        Future<RecordMetadata> future;
        try {
            future = producer.send(record);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            // send() itself blocks up to max.block.ms waiting for a usable view of the cluster, most often
            // because a broker that was reachable when this producer last used it has since gone away.
            // Builder.build() forces max.block.ms down to acknowledgementTimeout so this can never silently
            // outlast it, and it is reported the same way an expired acknowledgement wait is.
            throw new KafkaPublishTimeoutException(acknowledgementTimeout, e);
        } catch (InterruptException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while sending to topic \"" + destination.topic() + "\"", e);
        } catch (KafkaException e) {
            throw new KafkaPublishException("Failed to send to topic \"" + destination.topic() + "\"", e);
        } catch (RuntimeException e) {
            // A producer this client has already closed throws IllegalStateException here rather than a
            // KafkaException, and the default retry strategy excludes it since retrying against a closed
            // producer can never succeed.
            throw new KafkaPublishException("Failed to send to topic \"" + destination.topic() + "\"", e);
        }

        try {
            future.get(acknowledgementTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.kafka.common.errors.TimeoutException timeoutCause) {
                throw new KafkaPublishTimeoutException(acknowledgementTimeout, timeoutCause);
            }
            throw new KafkaPublishException("Broker failed to acknowledge a send to topic \"" + destination.topic() + "\"", cause == null ? e : cause);
        } catch (TimeoutException e) {
            throw new KafkaPublishTimeoutException(acknowledgementTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while waiting for a Kafka broker acknowledgement", e);
        }
    }

    /**
     * Closes the {@link Producer} this sink created and owns, waiting up to {@value #CLOSE_TIMEOUT_SECONDS} seconds
     * for any outstanding send to finish. {@code Producer#close()} with no argument waits indefinitely instead, and
     * an outstanding send to a broker that has gone away can otherwise hold this call open far longer than a caller
     * closing this sink down would expect.
     */
    @Override
    public void close() {
        producer.close(Duration.ofSeconds(CLOSE_TIMEOUT_SECONDS));
    }

    public static final class Builder {
        private final Map<String, Object> producerConfig;
        private final DestinationResolver<KafkaDestination> resolver;
        private Duration acknowledgementTimeout = Duration.ofSeconds(5);
        private RetryStrategy retryStrategy = defaultRetryStrategy();

        private Builder(Map<String, Object> producerConfig, DestinationResolver<KafkaDestination> resolver) {
            requireNonNull(producerConfig, "producerConfig cannot be null");
            this.producerConfig = new HashMap<>(producerConfig);
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
        }

        /**
         * How long {@link #publish(CloudEvent)} waits for the broker's acknowledgement before failing with
         * {@link KafkaPublishTimeoutException}. Five seconds by default. This is a timeout, not a switch. There is
         * deliberately no way to publish without waiting for it, for the reason {@link CloudEventSink}'s javadoc
         * gives, so a duration that truncates to zero or fewer milliseconds is refused rather than accepted and
         * read as "wait indefinitely".
         */
        public Builder acknowledgementTimeout(Duration acknowledgementTimeout) {
            requireNonNull(acknowledgementTimeout, "acknowledgementTimeout cannot be null");
            if (acknowledgementTimeout.toMillis() <= 0) {
                throw new IllegalArgumentException("acknowledgementTimeout must be at least 1 millisecond, was " + acknowledgementTimeout);
            }
            this.acknowledgementTimeout = acknowledgementTimeout;
            return this;
        }

        /**
         * How a transient publish failure is retried before {@link #publish(CloudEvent)} throws. Exponential
         * backoff from 100 ms up to 2 seconds by default, {@link CloudEventForwarder}'s own template for an
         * external store, retrying a {@link KafkaPublishException} only when it is not
         * {@link KafkaPublishTimeoutException} (excluded per ADR 133) and its cause is an
         * {@code org.apache.kafka.common.errors.RetriableException}, Kafka's own marker for a failure the client
         * itself considers worth retrying. A permanent failure such as an unknown topic, a record too large, or an
         * authorization error is not retriable by that marker, and is never retried by this default, since ADR 133
         * only asks this retry to guard against a transient failure and an unbounded retry of a permanent one would
         * never let {@link #publish(CloudEvent)} return. Passing a {@link RetryStrategy} here replaces that
         * predicate too, so a caller that wants a wider retry configures its own. It never substitutes for the
         * acknowledgement wait {@link #acknowledgementTimeout(Duration)} configures.
         */
        public Builder retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * Builds and owns a {@link KafkaProducer} from {@code producerConfig}, forcing {@link StringSerializer} and
         * {@link ByteArraySerializer} regardless of what {@code producerConfig} says about {@code key.serializer}
         * and {@code value.serializer}. {@code acks} is set to {@code "all"} when {@code producerConfig} does not
         * set it, and this method throws {@link IllegalArgumentException} when {@code producerConfig} sets it to
         * anything other than {@code "all"} or {@code "-1"}, per ADR 133 decision 7: waiting for an acknowledgement
         * is only worth anything if the broker is configured to give one that means the write is durable, and a
         * weaker setting has to be caught here, since afterwards it looks exactly like success. {@code max.block.ms}
         * is always forced down to {@link #acknowledgementTimeout(Duration)}, regardless of what {@code producerConfig}
         * says, so a {@code send} that needs a fresh view of the cluster is bounded by the same timeout as the
         * acknowledgement wait rather than by Kafka's own unrelated default of a minute.
         */
        public KafkaCloudEventSink build() {
            Map<String, Object> config = new HashMap<>(producerConfig);
            Object configuredAcks = config.get(ProducerConfig.ACKS_CONFIG);
            if (configuredAcks == null) {
                config.put(ProducerConfig.ACKS_CONFIG, "all");
            } else if (!isAll(configuredAcks.toString())) {
                throw new IllegalArgumentException("producerConfig sets \"" + ProducerConfig.ACKS_CONFIG + "\" to \"" +
                        configuredAcks + "\", but " + KafkaCloudEventSink.class.getSimpleName() + " requires \"all\" " +
                        "(or the equivalent \"-1\"), since an acknowledgement wait under a weaker setting can " +
                        "succeed for a send the broker never durably stored");
            }
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, acknowledgementTimeout.toMillis());
            Producer<String, byte[]> producer = new KafkaProducer<>(config, new StringSerializer(), new ByteArraySerializer());
            return new KafkaCloudEventSink(producer, resolver, acknowledgementTimeout, retryStrategy);
        }

        private static boolean isAll(String acks) {
            return "all".equals(acks) || "-1".equals(acks);
        }

        private static RetryStrategy defaultRetryStrategy() {
            return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f)
                    .retryIf(throwable -> throwable instanceof KafkaPublishException publishException
                            && !(publishException instanceof KafkaPublishTimeoutException)
                            && publishException.getCause() instanceof org.apache.kafka.common.errors.RetriableException);
        }
    }
}
