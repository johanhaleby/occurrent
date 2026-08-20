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

import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for the Kafka broker auto-configuration, every default copied from the builder default it
 * configures rather than invented here. {@code group.id} is deliberately absent: ADR 90 requires one consumer
 * group per consumer, so it is supplied per call to the bridge factory, never as a blanket property, the same
 * reason {@code queue} is not a top-level RabbitMQ property either.
 */
@ConfigurationProperties(prefix = "occurrent.broker.kafka")
public class KafkaBrokerProperties {

    /**
     * Kafka's {@code bootstrap.servers}. Required for the sink and bridge factory beans to activate. Accepts
     * either a comma-separated string ({@code host1:9092,host2:9092}) or a YAML list, both bind to this the same
     * way, see {@link KafkaBootstrapServersConfiguredCondition}, which is what this auto-configuration's own
     * activation check is written against instead of a plain property-presence check.
     */
    private List<String> bootstrapServers = new ArrayList<>();

    /**
     * The topic the auto-configured {@code KafkaSharedTopicDestinationResolver} publishes to, binds against and
     * subscribes to. Required for that resolver bean to activate. A deployment supplying its own
     * {@code DestinationResolver<KafkaDestination>}, such as {@code KafkaTopicPerTypeDestinationResolver}, does not
     * need this set.
     */
    private @Nullable String topic;

    private final Producer producer = new Producer();
    private final Consumer consumer = new Consumer();
    private final Sink sink = new Sink();
    private final Bridge bridge = new Bridge();

    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public @Nullable String getTopic() {
        return topic;
    }

    public void setTopic(@Nullable String topic) {
        this.topic = topic;
    }

    public Producer getProducer() {
        return producer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public Sink getSink() {
        return sink;
    }

    public Bridge getBridge() {
        return bridge;
    }

    public static class Producer {

        /**
         * Passthrough Kafka producer configuration beyond {@code bootstrap.servers}, for anything this starter
         * does not otherwise expose, TLS and SASL settings included. {@code acks} and the serializers are always
         * forced by {@code KafkaCloudEventSink.Builder#build()} regardless of what is set here, the same refusal
         * that builder already makes for a caller constructing it directly.
         */
        private Map<String, String> additionalProperties = new LinkedHashMap<>();

        public Map<String, String> getAdditionalProperties() {
            return additionalProperties;
        }

        public void setAdditionalProperties(Map<String, String> additionalProperties) {
            this.additionalProperties = additionalProperties;
        }
    }

    public static class Consumer {

        /**
         * Passthrough Kafka consumer configuration beyond {@code bootstrap.servers} and {@code group.id}, for
         * anything this starter does not otherwise expose. {@code enable.auto.commit} is always forced to
         * {@code false} regardless of what is set here, the same refusal the underlying bridge builders already
         * make for a caller constructing one directly.
         */
        private Map<String, String> additionalProperties = new LinkedHashMap<>();

        public Map<String, String> getAdditionalProperties() {
            return additionalProperties;
        }

        public void setAdditionalProperties(Map<String, String> additionalProperties) {
            this.additionalProperties = additionalProperties;
        }
    }

    public static class Sink {

        /**
         * {@code KafkaCloudEventSink.Builder#acknowledgementTimeout(Duration)}. Five seconds by default, matching
         * that builder's own default.
         */
        private Duration acknowledgementTimeout = Duration.ofSeconds(5);

        private final Retry retry = new Retry();

        public Duration getAcknowledgementTimeout() {
            return acknowledgementTimeout;
        }

        public void setAcknowledgementTimeout(Duration acknowledgementTimeout) {
            this.acknowledgementTimeout = acknowledgementTimeout;
        }

        public Retry getRetry() {
            return retry;
        }
    }

    public static class Bridge {

        /**
         * {@code KafkaCloudEventBridge.Builder#pollTimeout(Duration)} and
         * {@code KafkaDomainEventBridge.Builder#pollTimeout(Duration)}. One second by default, matching those
         * builders' own default.
         */
        private Duration pollTimeout = Duration.ofSeconds(1);

        /**
         * {@code KafkaCloudEventBridge.Builder#closeTimeout(Duration)} and
         * {@code KafkaDomainEventBridge.Builder#closeTimeout(Duration)}. Thirty seconds by default, matching those
         * builders' own default.
         */
        private Duration closeTimeout = Duration.ofSeconds(30);

        /**
         * {@code KafkaCloudEventBridge.Builder#onDeliveryFailure(DeliveryFailurePolicy)} and
         * {@code KafkaDomainEventBridge.Builder#onDeliveryFailure(DeliveryFailurePolicy)}.
         * {@link DeliveryFailurePolicy#REDELIVER} by default, matching those builders' own default.
         */
        private DeliveryFailurePolicy onDeliveryFailure = DeliveryFailurePolicy.REDELIVER;

        private final ParkingDestination parkingDestination = new ParkingDestination();
        private final Retry commitRetry = new Retry();

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public Duration getCloseTimeout() {
            return closeTimeout;
        }

        public void setCloseTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
        }

        public DeliveryFailurePolicy getOnDeliveryFailure() {
            return onDeliveryFailure;
        }

        public void setOnDeliveryFailure(DeliveryFailurePolicy onDeliveryFailure) {
            this.onDeliveryFailure = onDeliveryFailure;
        }

        public ParkingDestination getParkingDestination() {
            return parkingDestination;
        }

        public Retry getCommitRetry() {
            return commitRetry;
        }
    }

    /**
     * The destination {@code DeliveryFailurePolicy#PARK} publishes a failed delivery to. Required only when
     * {@link Bridge#getOnDeliveryFailure()} is {@code PARK}. Read the same "accepted but unused outside PARK" way
     * the underlying builder documents for a value given without {@code PARK} when this is blank.
     */
    public static class ParkingDestination {

        private @Nullable String topic;

        public @Nullable String getTopic() {
            return topic;
        }

        public void setTopic(@Nullable String topic) {
            this.topic = topic;
        }
    }

    /**
     * The {@code initial}/{@code max}/{@code multiplier} shape {@code OccurrentProperties.ProjectionProperties.AppliedAppendProperties.WaitBackoffProperties}
     * already established for exposing a {@code RetryStrategy.exponentialBackoff(...)} as configuration.
     */
    public static class Retry {

        /**
         * The first retry delay. 100 milliseconds by default, matching the underlying builder's own default
         * backoff.
         */
        private Duration initial = Duration.ofMillis(100);

        /**
         * The longest the retry delay grows to. Two seconds by default, matching the underlying builder's own
         * default backoff.
         */
        private Duration max = Duration.ofSeconds(2);

        /**
         * What the delay is multiplied by after each retried attempt. {@code 2.0} by default, matching the
         * underlying builder's own default backoff.
         */
        private double multiplier = 2.0;

        public Duration getInitial() {
            return initial;
        }

        public void setInitial(Duration initial) {
            this.initial = initial;
        }

        public Duration getMax() {
            return max;
        }

        public void setMax(Duration max) {
            this.max = max;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }
    }
}
