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

package org.occurrent.springboot.broker.rabbitmq.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Optional;

/**
 * Configuration for the RabbitMQ broker auto-configuration, every default copied from the builder default it
 * configures rather than invented here. {@code exchange} has no default, the same way
 * {@code RabbitMqTopicExchangeDestinationResolver} takes one as a required constructor argument rather than
 * guessing a name, since a wrong guess is a topology mistake an operator has to notice rather than a startup
 * error that catches it.
 */
@ConfigurationProperties(prefix = "occurrent.broker.rabbitmq")
public class RabbitMqBrokerProperties {

    /**
     * The exchange the auto-configured {@code RabbitMqTopicExchangeDestinationResolver} publishes to and binds
     * against. Required for that resolver bean to activate. A deployment supplying its own
     * {@code DestinationResolver<RabbitMqDestination>} bean does not need this set.
     */
    private @Nullable String exchange;

    private final Sink sink = new Sink();
    private final Bridge bridge = new Bridge();

    public @Nullable String getExchange() {
        return exchange;
    }

    public void setExchange(@Nullable String exchange) {
        this.exchange = exchange;
    }

    public Sink getSink() {
        return sink;
    }

    public Bridge getBridge() {
        return bridge;
    }

    public static class Sink {

        /**
         * {@code RabbitMqCloudEventSink.Builder#acknowledgementTimeout(Duration)}. Five seconds by default,
         * matching that builder's own default.
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
         * {@code RabbitMqCloudEventBridge.Builder#pollInterval(Duration)} and
         * {@code RabbitMqDomainEventBridge.Builder#pollInterval(Duration)}. One second by default, matching those
         * builders' own default.
         */
        private Duration pollInterval = Duration.ofSeconds(1);

        /**
         * {@code RabbitMqCloudEventBridge.Builder#prefetchCount(int)} and
         * {@code RabbitMqDomainEventBridge.Builder#prefetchCount(int)}. One by default, matching those builders'
         * own default.
         */
        private int prefetchCount = 1;

        /**
         * {@code RabbitMqCloudEventBridge.Builder#declareTopology(boolean)} and
         * {@code RabbitMqDomainEventBridge.Builder#declareTopology(boolean)}. {@code true} by default, matching
         * those builders' own default. Set to {@code false} for a deployment whose platform team owns the queue
         * and its bindings itself.
         */
        private boolean declareTopology = true;

        /**
         * {@code RabbitMqCloudEventBridge.Builder#onDeliveryFailure(DeliveryFailurePolicy)} and
         * {@code RabbitMqDomainEventBridge.Builder#onDeliveryFailure(DeliveryFailurePolicy)}.
         * {@link DeliveryFailurePolicy#REDELIVER} by default, matching those builders' own default.
         */
        private DeliveryFailurePolicy onDeliveryFailure = DeliveryFailurePolicy.REDELIVER;

        private final ParkingDestination parkingDestination = new ParkingDestination();

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public int getPrefetchCount() {
            return prefetchCount;
        }

        public void setPrefetchCount(int prefetchCount) {
            this.prefetchCount = prefetchCount;
        }

        public boolean isDeclareTopology() {
            return declareTopology;
        }

        public void setDeclareTopology(boolean declareTopology) {
            this.declareTopology = declareTopology;
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
    }

    /**
     * The destination {@code DeliveryFailurePolicy#PARK} publishes a failed delivery to. Required only when
     * {@link Bridge#getOnDeliveryFailure()} is {@code PARK}. When either field is blank, a bridge factory does not
     * call {@code parkingDestination(...)} at all, rather than calling it with a half-configured value, the same
     * "accepted but unused outside PARK" choice the underlying builder documents for a value given without
     * {@code PARK}.
     */
    public static class ParkingDestination {

        private @Nullable String exchange;
        private @Nullable String routingKey;

        public @Nullable String getExchange() {
            return exchange;
        }

        public void setExchange(@Nullable String exchange) {
            this.exchange = exchange;
        }

        public @Nullable String getRoutingKey() {
            return routingKey;
        }

        public void setRoutingKey(@Nullable String routingKey) {
            this.routingKey = routingKey;
        }

        /**
         * The configured destination, or empty when either field is null or blank. A bridge factory calls this
         * rather than checking the fields itself, so a blank value from an unset placeholder in a property file
         * is treated the same as an absent one instead of reaching {@code parkingDestination(...)} as a
         * half-configured value.
         */
        public Optional<RabbitMqDestination> toDestination() {
            if (exchange == null || exchange.isBlank() || routingKey == null || routingKey.isBlank()) {
                return Optional.empty();
            }
            return Optional.of(RabbitMqDestination.of(exchange, routingKey));
        }
    }

    /**
     * The {@code initial}/{@code max}/{@code multiplier} shape {@code OccurrentProperties.ProjectionProperties.AppliedAppendProperties.WaitBackoffProperties}
     * already established for exposing a {@code RetryStrategy.exponentialBackoff(...)} as configuration.
     */
    public static class Retry {

        /**
         * The first retry delay. 100 milliseconds by default, matching the sink builder's own default backoff.
         */
        private Duration initial = Duration.ofMillis(100);

        /**
         * The longest the retry delay grows to. Two seconds by default, matching the sink builder's own default
         * backoff.
         */
        private Duration max = Duration.ofSeconds(2);

        /**
         * What the delay is multiplied by after each retried attempt. {@code 2.0} by default, matching the sink
         * builder's own default backoff.
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
