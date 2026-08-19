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
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * What a consume-side bridge does with a delivery it will not acknowledge, per ADR 133 decision 7. Shared between
 * {@link RabbitMqCloudEventBridge} and {@code RabbitMqDomainEventBridge} rather than written twice, since both apply
 * exactly the same sequence: {@link DeliveryFailurePolicy#REDELIVER} negatively acknowledges the original with
 * requeue, and {@link DeliveryFailurePolicy#PARK} republishes the original to a parking destination, waits for that
 * publish's own confirm, and only then acknowledges the original. Neither branch ever acknowledges the original
 * directly on the failure path; that is the one thing this class exists to make impossible to get wrong twice.
 * <p>
 * A failed parking publish (the parking exchange unavailable, its own confirm timing out, a {@code basic.return}
 * because nothing is bound to the parking routing key, ...) negatively acknowledges the original with requeue
 * instead, the same as {@code REDELIVER}, so the original is redelivered rather than lost. Logged at {@code warn}
 * either way, since a park that silently falls back to redelivery is a configuration problem an operator needs to
 * see, and RabbitMQ's own dead-lettering is never used for this, per ADR 133 decision 7: it can discard the message
 * outright with no matching dead-letter exchange, and even with one it republishes without publisher confirms.
 * <p>
 * {@link #applyToUndecodable(long, BasicProperties, byte[])} covers a message {@link RabbitMqCloudEventMapper#toCloudEvent(BasicProperties, byte[])}
 * could not turn into a {@link CloudEvent} at all, so there is nothing to hand {@link #apply(long, CloudEvent)}. It
 * still honours {@link DeliveryFailurePolicy#PARK} rather than always redelivering, publishing the delivery's own
 * raw {@code properties} and {@code body} to the parking destination unchanged, since no {@link CloudEvent} exists
 * to republish through the ordinary {@link RabbitMqCloudEventSink}-backed path.
 * <p>
 * {@link AutoCloseable} so a bridge can own exactly one of these and close it, rather than separately tracking and
 * closing the parking sink (and, under {@code PARK}, the raw parking channel) it may have built.
 */
public final class RabbitMqDeliveryFailureAction implements AutoCloseable {

    /**
     * How long {@link #applyToUndecodable(long, BasicProperties, byte[])} waits for the raw parking publish's
     * confirm. Fixed rather than configurable, since this path exists for a message so malformed it could not be
     * decoded at all, a rare case that does not need its own tunable.
     */
    private static final Duration RAW_PARK_TIMEOUT = Duration.ofSeconds(5);

    private final Channel consumeChannel;
    private final DeliveryFailurePolicy policy;
    private final @Nullable RabbitMqCloudEventSink parkingSink;
    private final @Nullable Channel rawParkChannel;
    private final @Nullable RabbitMqDestination parkingDestination;
    private final Logger log;

    // Guards a raw park publish and the wait that follows it, since both share rawParkMessageReturned and this
    // class makes no promise that its caller will not raise a bridge's prefetch above one, letting two undecodable
    // deliveries reach applyToUndecodable at once.
    private final Object rawParkLock = new Object();
    private volatile boolean rawParkMessageReturned;

    /**
     * @param consumeChannel    The channel the failing delivery arrived on. Acknowledgement and negative
     *                          acknowledgement must happen on this exact channel.
     * @param policy            What to do with a delivery nothing consumed.
     * @param parkingSink       Publishes a rebuilt {@link CloudEvent} to the parking destination. Required when
     *                          {@code policy} is {@link DeliveryFailurePolicy#PARK}, otherwise ignored.
     * @param rawParkChannel    A confirm-mode channel this action publishes an undecodable delivery's raw bytes
     *                          through. Required together with {@code parkingDestination} when {@code policy} is
     *                          {@link DeliveryFailurePolicy#PARK}, otherwise ignored.
     * @param parkingDestination The exchange and routing key {@code rawParkChannel} publishes to.
     */
    public RabbitMqDeliveryFailureAction(Channel consumeChannel, DeliveryFailurePolicy policy, @Nullable RabbitMqCloudEventSink parkingSink,
                                          @Nullable Channel rawParkChannel, @Nullable RabbitMqDestination parkingDestination, Logger log) {
        this.consumeChannel = requireNonNull(consumeChannel, "consumeChannel cannot be null");
        this.policy = requireNonNull(policy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
        this.parkingSink = parkingSink;
        this.rawParkChannel = rawParkChannel;
        this.parkingDestination = parkingDestination;
        this.log = requireNonNull(log, "log cannot be null");
        if (policy == DeliveryFailurePolicy.PARK && (parkingSink == null || rawParkChannel == null || parkingDestination == null)) {
            throw new IllegalArgumentException("A parking sink, a raw park channel and a parkingDestination are all required when the policy is PARK");
        }
        if (rawParkChannel != null) {
            rawParkChannel.addReturnListener(returned -> rawParkMessageReturned = true);
        }
    }

    /**
     * Builds the action for {@code policy}, and the {@link RabbitMqCloudEventSink} plus raw parking {@link Channel}
     * it parks through when {@code policy} is {@link DeliveryFailurePolicy#PARK} and {@code parkingDestination} is
     * given. Refuses when {@code policy} is {@code PARK} and {@code parkingDestination} is {@code null}, the same
     * requirement {@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK}'s own javadoc states. Both
     * parking paths publish to {@code parkingDestination} alone, regardless of the event's own type, since parking
     * is not a routing decision.
     */
    public static RabbitMqDeliveryFailureAction create(Connection connection, Channel consumeChannel, DeliveryFailurePolicy policy,
                                                         @Nullable RabbitMqDestination parkingDestination, Logger log) {
        if (policy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
            throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
        }
        if (parkingDestination == null) {
            return new RabbitMqDeliveryFailureAction(consumeChannel, policy, null, null, null, log);
        }
        // Unwound in the catch below on any later failure, so a parking sink already built here (which owns its
        // own channel) is never left open behind a create() that ends up throwing.
        RabbitMqCloudEventSink parkingSink = RabbitMqCloudEventSink.builder(connection, new SingleDestinationResolver(parkingDestination)).build();
        Channel rawParkChannel = null;
        try {
            rawParkChannel = openRawParkChannel(connection);
            return new RabbitMqDeliveryFailureAction(consumeChannel, policy, parkingSink, rawParkChannel, parkingDestination, log);
        } catch (RuntimeException e) {
            closeQuietly(parkingSink, log);
            if (rawParkChannel != null) {
                closeQuietly(rawParkChannel, log);
            }
            throw e;
        }
    }

    private static Channel openRawParkChannel(Connection connection) {
        try {
            Channel channel = connection.openChannel()
                    .orElseThrow(() -> new RabbitMqBridgeException("No RabbitMQ channel number was available to create the raw parking channel on"));
            channel.confirmSelect();
            return channel;
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to create the raw parking confirm-mode channel", e);
        }
    }

    // Best effort, unwinding a partially built create(): a failure closing one already-acquired resource is logged
    // rather than allowed to replace the failure the caller is already unwinding for.
    private static void closeQuietly(RabbitMqCloudEventSink parkingSink, Logger log) {
        try {
            parkingSink.close();
        } catch (IOException | TimeoutException e) {
            log.warn("Failed to close the parking sink cleanly while unwinding a failed create().", e);
        }
    }

    private static void closeQuietly(Channel channel, Logger log) {
        try {
            channel.close();
        } catch (IOException | ShutdownSignalException | TimeoutException e) {
            log.warn("Failed to close the raw parking channel cleanly while unwinding a failed create().", e);
        }
    }

    /**
     * Applies this failure action to the delivery {@code deliveryTag} identifies, rebuilt as {@code cloudEvent}.
     * Never acknowledges {@code deliveryTag} directly; {@link DeliveryFailurePolicy#PARK} acknowledges it only once
     * the parking publish has been confirmed.
     */
    public void apply(long deliveryTag, CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "cloudEvent cannot be null");
        if (policy == DeliveryFailurePolicy.REDELIVER) {
            redeliver(deliveryTag);
            return;
        }
        try {
            requireNonNull(parkingSink).publish(cloudEvent);
        } catch (RuntimeException e) {
            log.warn("Failed to park a delivery nothing consumed. Redelivering it instead of losing it.", e);
            redeliver(deliveryTag);
            return;
        }
        ack(deliveryTag);
    }

    /**
     * Applies this failure action to the delivery {@code deliveryTag} identifies, for a message
     * {@link RabbitMqCloudEventMapper#toCloudEvent(BasicProperties, byte[])} could not rebuild as a {@link CloudEvent}
     * at all. {@link DeliveryFailurePolicy#REDELIVER} behaves exactly as {@link #apply(long, CloudEvent)}.
     * {@link DeliveryFailurePolicy#PARK} publishes {@code properties} and {@code body} to the parking destination
     * unchanged, with confirms and {@code mandatory} routing exactly as {@link RabbitMqCloudEventSink} publishes,
     * and acknowledges the original only once that publish is confirmed and not returned as unroutable. Never
     * acknowledges {@code deliveryTag} directly.
     */
    public void applyToUndecodable(long deliveryTag, BasicProperties properties, byte[] body) {
        requireNonNull(properties, "properties cannot be null");
        requireNonNull(body, "body cannot be null");
        if (policy == DeliveryFailurePolicy.REDELIVER) {
            redeliver(deliveryTag);
            return;
        }
        if (parkRaw(properties, body)) {
            ack(deliveryTag);
        } else {
            redeliver(deliveryTag);
        }
    }

    private boolean parkRaw(BasicProperties properties, byte[] body) {
        Channel channel = requireNonNull(rawParkChannel);
        RabbitMqDestination destination = requireNonNull(parkingDestination);
        synchronized (rawParkLock) {
            rawParkMessageReturned = false;
            try {
                channel.basicPublish(destination.exchange(), destination.routingKey(), true, properties, body);
                boolean confirmed = channel.waitForConfirms(RAW_PARK_TIMEOUT.toMillis());
                if (!confirmed) {
                    log.warn("Raw parking publish to exchange \"{}\" with routing key \"{}\" was not confirmed. Redelivering the original instead of losing it.",
                            destination.exchange(), destination.routingKey());
                    return false;
                }
                if (rawParkMessageReturned) {
                    log.warn("Raw parking publish to exchange \"{}\" with routing key \"{}\" was returned as unroutable. Redelivering the original instead of losing it.",
                            destination.exchange(), destination.routingKey());
                    return false;
                }
                return true;
            } catch (IOException | ShutdownSignalException e) {
                // ShutdownSignalException is unchecked and covers exactly the failure a missing or misconfigured
                // parking exchange produces, the broker closing this channel with a protocol error rather than
                // basicPublish throwing a checked IOException, so it is caught here on the same footing as one.
                log.warn("Failed to publish an undecodable delivery to the parking destination. Redelivering it instead of losing it.", e);
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for the raw parking publish's confirm. Redelivering the original instead of losing it.", e);
                return false;
            } catch (TimeoutException e) {
                log.warn("Timed out waiting for the raw parking publish's confirm. Redelivering the original instead of losing it.", e);
                return false;
            }
        }
    }

    /**
     * Acknowledges {@code deliveryTag} on the delivery's own channel. Used for a delivery that succeeded, and
     * internally once a park publish has been confirmed.
     */
    public void ack(long deliveryTag) {
        try {
            consumeChannel.basicAck(deliveryTag, false);
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to acknowledge delivery tag " + deliveryTag, e);
        }
    }

    private void redeliver(long deliveryTag) {
        try {
            consumeChannel.basicNack(deliveryTag, false, true);
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + deliveryTag, e);
        }
    }

    /**
     * Closes the parking sink and the raw parking channel this action built, if {@link DeliveryFailurePolicy#PARK}
     * was configured. Does not touch {@code consumeChannel}, which the bridge that owns it closes itself. Best
     * effort: a failure closing either is logged rather than thrown, since it happens during teardown with nothing
     * left to hand it to.
     */
    @Override
    public void close() {
        if (parkingSink != null) {
            closeQuietly(parkingSink, log);
        }
        if (rawParkChannel != null) {
            closeQuietly(rawParkChannel, log);
        }
    }
}
