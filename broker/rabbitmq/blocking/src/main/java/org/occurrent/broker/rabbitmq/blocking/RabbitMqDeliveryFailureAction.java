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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.Logger;

import java.io.IOException;
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
 * A failed parking publish (the parking exchange unavailable, its own confirm timing out, ...) negatively
 * acknowledges the original with requeue instead, the same as {@code REDELIVER}, so the original is redelivered
 * rather than lost. Logged at {@code warn} either way, since a park that silently falls back to redelivery is a
 * configuration problem an operator needs to see, and RabbitMQ's own dead-lettering is never used for this, per ADR
 * 133 decision 7: it can discard the message outright with no matching dead-letter exchange, and even with one it
 * republishes without publisher confirms.
 * <p>
 * {@link AutoCloseable} so a bridge can own exactly one of these and close it, rather than separately tracking and
 * closing the parking sink it may have built for {@link DeliveryFailurePolicy#PARK}.
 */
public final class RabbitMqDeliveryFailureAction implements AutoCloseable {

    private final Channel consumeChannel;
    private final DeliveryFailurePolicy policy;
    private final @Nullable RabbitMqCloudEventSink parkingSink;
    private final Logger log;

    /**
     * @param consumeChannel The channel the failing delivery arrived on. Acknowledgement and negative acknowledgement
     *                       must happen on this exact channel.
     * @param policy         What to do with a delivery nothing consumed.
     * @param parkingSink    Publishes to the parking destination. Required when {@code policy} is {@link DeliveryFailurePolicy#PARK},
     *                       otherwise ignored.
     */
    public RabbitMqDeliveryFailureAction(Channel consumeChannel, DeliveryFailurePolicy policy, @Nullable RabbitMqCloudEventSink parkingSink, Logger log) {
        this.consumeChannel = requireNonNull(consumeChannel, "consumeChannel cannot be null");
        this.policy = requireNonNull(policy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
        this.parkingSink = parkingSink;
        this.log = requireNonNull(log, "log cannot be null");
        if (policy == DeliveryFailurePolicy.PARK && parkingSink == null) {
            throw new IllegalArgumentException("A parking sink is required when the policy is PARK");
        }
    }

    /**
     * Builds the action for {@code policy}, and the {@link RabbitMqCloudEventSink} it parks through when
     * {@code policy} is {@link DeliveryFailurePolicy#PARK} and {@code parkingDestination} is given. Refuses when
     * {@code policy} is {@code PARK} and {@code parkingDestination} is {@code null}, the same requirement
     * {@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK}'s own javadoc states. The parking sink
     * publishes to {@code parkingDestination} alone, regardless of the event's own type, since parking is not a
     * routing decision.
     */
    public static RabbitMqDeliveryFailureAction create(Connection connection, Channel consumeChannel, DeliveryFailurePolicy policy,
                                                         @Nullable RabbitMqDestination parkingDestination, Logger log) {
        if (policy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
            throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
        }
        RabbitMqCloudEventSink parkingSink = parkingDestination == null ? null :
                RabbitMqCloudEventSink.builder(connection, new SingleDestinationResolver(parkingDestination)).build();
        return new RabbitMqDeliveryFailureAction(consumeChannel, policy, parkingSink, log);
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

    /**
     * Negatively acknowledges {@code deliveryTag} with requeue, on the delivery's own channel, regardless of the
     * configured {@link DeliveryFailurePolicy}. Exposed for a bridge that cannot rebuild a {@link CloudEvent} at all
     * (an undecodable message) and so has nothing to hand {@link #apply(long, CloudEvent)}, whatever policy is
     * configured, since there is nothing to park.
     */
    public void redeliverRegardlessOfPolicy(long deliveryTag) {
        redeliver(deliveryTag);
    }

    private void redeliver(long deliveryTag) {
        try {
            consumeChannel.basicNack(deliveryTag, false, true);
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + deliveryTag, e);
        }
    }

    /**
     * Closes the parking sink this action built, if {@link DeliveryFailurePolicy#PARK} was configured. Does not
     * touch {@code consumeChannel}, which the bridge that owns it closes itself. Best effort: a failure closing the
     * parking sink is logged rather than thrown, since it happens during teardown with nothing left to hand it to.
     */
    @Override
    public void close() {
        if (parkingSink != null) {
            try {
                parkingSink.close();
            } catch (IOException | TimeoutException e) {
                log.warn("Failed to close the parking sink cleanly during shutdown.", e);
            }
        }
    }
}
