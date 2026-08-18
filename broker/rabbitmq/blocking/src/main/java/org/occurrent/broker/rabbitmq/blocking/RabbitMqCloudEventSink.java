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
import io.cloudevents.CloudEvent;
import org.occurrent.broker.api.blocking.CloudEventForwarder;
import org.occurrent.broker.api.blocking.CloudEventSink;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.retry.RetryStrategy;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
 * can be turned off, for the same reason {@link CloudEventSink}'s own javadoc gives.
 * <p>
 * Each publish carries its own random {@code correlationId}, so a {@code basic.return} is matched to the publish it
 * belongs to and never to a different one, including a retry of the same event after an earlier attempt's
 * acknowledgement timed out. Publishes on one sink are still serialized on its channel, the simplest way to avoid
 * depending on exactly how this client version handles concurrent publishing on one {@link Channel}. An application
 * that needs more publish throughput than one serialized channel gives builds more than one sink, each with its own
 * {@link Channel}.
 * <p>
 * A transient failure, a dropped connection mid-publish for example, is retried under {@link Builder#retryStrategy(RetryStrategy)}
 * before a caller sees it, exponential backoff from 100 ms up to 2 seconds by default. The retry is not a substitute
 * for the acknowledgement wait, since a publish that was never acknowledged is not known to have failed, only unresolved.
 * The default only retries a failure whose outcome was never established, {@link RabbitMqPublishTimeoutException}
 * included. {@link RabbitMqUnroutableEventException} and an unrecognised cloud event type from the resolver are
 * configuration bugs, not transient failures, and reach the caller on the first attempt instead.
 * <p>
 * Call {@link #close()} once the sink is no longer needed. It closes the {@link Channel} this sink created, not the
 * {@link Connection} it was given, since the connection may be shared with other channels the caller still owns.
 */
public final class RabbitMqCloudEventSink implements CloudEventSink, AutoCloseable {

    private final Channel channel;
    private final DestinationResolver<RabbitMqDestination> resolver;
    private final Duration acknowledgementTimeout;
    private final RetryStrategy retryStrategy;
    private final Lock publishLock = new ReentrantLock();
    private final Set<String> returnedCorrelationIds = ConcurrentHashMap.newKeySet();

    private RabbitMqCloudEventSink(Channel channel, DestinationResolver<RabbitMqDestination> resolver, Duration acknowledgementTimeout, RetryStrategy retryStrategy) {
        this.channel = channel;
        this.resolver = resolver;
        this.acknowledgementTimeout = acknowledgementTimeout;
        this.retryStrategy = retryStrategy;
        channel.addReturnListener(returned -> {
            String correlationId = returned.getProperties().getCorrelationId();
            if (correlationId != null) {
                returnedCorrelationIds.add(correlationId);
            }
        });
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
        // basic.return carries no delivery tag, so this internal correlationId is the only way to tell which
        // publish a return belongs to. It is not part of the CloudEvent mapping and carries no other meaning.
        String correlationId = UUID.randomUUID().toString();
        BasicProperties properties = RabbitMqCloudEventMapper.toBasicProperties(cloudEvent, destination.headers())
                .builder().correlationId(correlationId).build();
        byte[] body = RabbitMqCloudEventMapper.toBody(cloudEvent);

        publishLock.lock();
        try {
            channel.basicPublish(destination.exchange(), destination.routingKey(), true, properties, body);
            channel.waitForConfirmsOrDie(acknowledgementTimeout.toMillis());
            if (returnedCorrelationIds.remove(correlationId)) {
                throw new RabbitMqUnroutableEventException(destination.exchange(), destination.routingKey());
            }
        } catch (IOException e) {
            throw new RabbitMqPublishException("Failed to publish to exchange \"" + destination.exchange() +
                    "\" with routing key \"" + destination.routingKey() + "\"", e);
        } catch (TimeoutException e) {
            throw new RabbitMqPublishTimeoutException(acknowledgementTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RabbitMqPublishException("Interrupted while waiting for a RabbitMQ publisher confirm", e);
        } finally {
            publishLock.unlock();
        }
    }

    /**
     * Closes the {@link Channel} this sink created. Does not close the {@link Connection} it was built from.
     */
    @Override
    public void close() throws IOException, TimeoutException {
        channel.close();
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
         * javadoc gives.
         */
        public Builder acknowledgementTimeout(Duration acknowledgementTimeout) {
            this.acknowledgementTimeout = requireNonNull(acknowledgementTimeout, "acknowledgementTimeout cannot be null");
            return this;
        }

        /**
         * How a transient publish failure is retried before {@link #publish(CloudEvent)} throws. Exponential
         * backoff from 100 ms up to 2 seconds by default, {@link CloudEventForwarder}'s own template for an
         * external store, retrying only {@link RabbitMqPublishTimeoutException} and the general
         * {@link RabbitMqPublishException} rather than {@link RabbitMqUnroutableEventException}. Passing a
         * {@link RetryStrategy} here replaces that predicate too, so a caller that wants unroutable events retried
         * as well configures its own. It never substitutes for the acknowledgement wait
         * {@link #acknowledgementTimeout(Duration)} configures.
         */
        public Builder retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        public RabbitMqCloudEventSink build() {
            try {
                Channel channel = connection.createChannel();
                channel.confirmSelect();
                return new RabbitMqCloudEventSink(channel, resolver, acknowledgementTimeout, retryStrategy);
            } catch (IOException e) {
                throw new RabbitMqPublishException("Failed to create a confirm-mode RabbitMQ channel", e);
            }
        }

        /**
         * Retries {@link RabbitMqPublishTimeoutException} and every other {@link RabbitMqPublishException}, since
         * both mean the publish's outcome was never established. {@link RabbitMqUnroutableEventException} and
         * whatever the resolver's {@code CloudEventTypeMapper} throws for a type it does not recognise are
         * configuration bugs, not transient failures, so they are excluded and reach the caller on the first
         * attempt instead of retrying forever.
         */
        private static RetryStrategy defaultRetryStrategy() {
            return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f)
                    .retryIf(throwable -> throwable instanceof RabbitMqPublishException && !(throwable instanceof RabbitMqUnroutableEventException));
        }
    }
}
