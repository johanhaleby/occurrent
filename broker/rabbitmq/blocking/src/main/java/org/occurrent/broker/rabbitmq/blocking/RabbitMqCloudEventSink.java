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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.retry.RetryStrategy;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Publishes a {@link CloudEvent} to RabbitMQ, in the CloudEvents binary content mode
 * {@link RabbitMqCloudEventMapper} defines. Bring-your-own-sink is the primary way to use the broker modules at all,
 * so use this one when you have not already written your own {@link CloudEventSink}. It is a complete,
 * confirmed, at-least-once publisher rather than a starting point to copy from.
 * <p>
 * {@link #publish(CloudEvent)} does not return until the broker has both confirmed the message and routed it.
 * Publisher confirms are required, enabled once when {@link #builder(Connection, DestinationResolver)} builds the
 * sink's own {@link Channel}, and a confirm alone is not enough, since RabbitMQ confirms a publish to an exchange
 * with no matching binding and then discards it. Every publish also sets {@code mandatory}, and a
 * {@code basic.return} for it fails the publish with {@link RabbitMqUnroutableEventException} even though the
 * confirm that follows it would otherwise look like success. An acknowledgement timeout, five seconds by default,
 * bounds the wait and fails it with {@link RabbitMqPublishTimeoutException} rather than blocking forever on a
 * broker that never answers, and {@link Builder#acknowledgementTimeout(Duration)} is not offered as something that
 * can be turned off, for the same reason {@link CloudEventSink}'s own javadoc gives. A publish that ends this way,
 * or one abandoned to an interrupted wait, is left outstanding on the broker's side of that channel, so this sink
 * retires the channel and opens a fresh one underneath it, and a later publish is never kept waiting on an
 * abandoned one or blamed for its eventual nack.
 * <p>
 * Each publish carries its own random {@code correlationId}, so a {@code basic.return} is matched to the publish it
 * belongs to and never to a different one, including a retry of the same event after an earlier attempt's
 * acknowledgement timed out. Publishes on one sink are still serialized on its channel, the simplest way to avoid
 * depending on exactly how this client version handles concurrent publishing on one {@link Channel}. An application
 * that needs more publish throughput than one serialized channel gives builds more than one sink, each with its own
 * {@link Channel}.
 * <p>
 * A transient failure is retried under {@link Builder#retryStrategy(RetryStrategy)} before a caller sees it,
 * exponential backoff from 100 ms up to 2 seconds by default. The retry is not a substitute for the acknowledgement
 * wait, since a publish that was never acknowledged is not known to have failed, only unresolved. Per ADR 133, an
 * expired {@link RabbitMqPublishTimeoutException} is for the caller to decide on rather than something this retry
 * absorbs, so the default excludes it, along with {@link RabbitMqUnroutableEventException}, a channel this client
 * has already closed, an interrupted wait, and an unrecognised cloud event type from the resolver, none of which a
 * retry can turn into success.
 * <p>
 * Call {@link #close()} once the sink is no longer needed. It closes the {@link Channel} this sink created, not the
 * {@link Connection} it was given, since the connection may be shared with other channels the caller still owns.
 */
public final class RabbitMqCloudEventSink implements CloudEventSink, AutoCloseable {

    private final DestinationResolver<RabbitMqDestination> resolver;
    private final RetryStrategy retryStrategy;
    private final RabbitMqConfirmPublisher publisher;

    private RabbitMqCloudEventSink(DestinationResolver<RabbitMqDestination> resolver, RetryStrategy retryStrategy, RabbitMqConfirmPublisher publisher) {
        this.resolver = resolver;
        this.retryStrategy = retryStrategy;
        this.publisher = publisher;
    }

    /**
     * @param connection The connection this sink creates its own confirm-mode {@link Channel} on. Not closed by
     *                    {@link #close()}, since other channels on it may still be in use.
     * @param resolver    Derives where each published event goes.
     */
    public static Builder builder(Connection connection, DestinationResolver<RabbitMqDestination> resolver) {
        return new Builder(connection, resolver);
    }

    @Override
    public void publish(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        retryStrategy.execute(() -> publishOnce(cloudEvent));
    }

    private void publishOnce(CloudEvent cloudEvent) {
        RabbitMqDestination destination = resolver.destinationFor(cloudEvent);
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, destination.headers());
        byte[] body = RabbitMqCloudEventMapper.toBody(cloudEvent);
        publisher.publish(destination.exchange(), destination.routingKey(), properties, body);
    }

    /**
     * Closes the {@link Channel} this sink created. Does not close the {@link Connection} it was built from.
     */
    @Override
    public void close() throws IOException, TimeoutException {
        publisher.close();
    }

    public static final class Builder {
        private final Connection connection;
        private final DestinationResolver<RabbitMqDestination> resolver;
        private Duration acknowledgementTimeout = Duration.ofSeconds(5);
        private RetryStrategy retryStrategy = defaultRetryStrategy();

        private Builder(Connection connection, DestinationResolver<RabbitMqDestination> resolver) {
            this.connection = requireNonNull(connection, "connection cannot be null");
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
        }

        /**
         * How long {@link #publish(CloudEvent)} waits for the broker's publisher confirm before failing with
         * {@link RabbitMqPublishTimeoutException}. Five seconds by default. This is a timeout, not a switch.
         * There is deliberately no way to publish without waiting for it, for the reason {@link CloudEventSink}'s
         * javadoc gives, so a duration that truncates to zero or fewer milliseconds is refused rather than accepted
         * and read by the RabbitMQ client as "wait indefinitely".
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
         * external store, retrying a {@link RabbitMqPublishException} only when it is not
         * {@link RabbitMqUnroutableEventException}, not {@link RabbitMqPublishTimeoutException} (excluded per ADR
         * 133), and not caused by a channel this client has already closed. Passing a {@link RetryStrategy} here
         * replaces that predicate too, so a caller that wants a wider retry configures its own. It never
         * substitutes for the acknowledgement wait {@link #acknowledgementTimeout(Duration)} configures.
         */
        public Builder retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        public RabbitMqCloudEventSink build() {
            RabbitMqConfirmPublisher publisher = new RabbitMqConfirmPublisher(connection, acknowledgementTimeout);
            return new RabbitMqCloudEventSink(resolver, retryStrategy, publisher);
        }

        /**
         * Retries a {@link RabbitMqPublishException} that is neither {@link RabbitMqUnroutableEventException}
         * (a configuration bug, not a transient failure) nor caused by a {@link ShutdownSignalException} (this
         * sink's channel is closed by then, per the RabbitMQ client, so retrying against it can never succeed).
         * {@link RabbitMqPublishTimeoutException} is excluded too, per ADR 133, since an expired acknowledgement
         * timeout is for the caller to decide on, not for this retry to absorb. A wait interrupted by
         * {@link InterruptedException} is excluded as well, since the thread asked to stop is not asking to try
         * again. Whatever the resolver's {@code CloudEventTypeMapper} throws for a type it does not recognise is not
         * a {@link RabbitMqPublishException} at all, so it is never retried either.
         */
        private static RetryStrategy defaultRetryStrategy() {
            return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f)
                    .retryIf(throwable -> throwable instanceof RabbitMqPublishException publishException
                            && !(publishException instanceof RabbitMqUnroutableEventException)
                            && !(publishException instanceof RabbitMqPublishTimeoutException)
                            && !(publishException.getCause() instanceof ShutdownSignalException)
                            && !(publishException.getCause() instanceof InterruptedException));
        }
    }
}
