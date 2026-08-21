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
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.subscription.RoutingOutcome;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * What a consume-side bridge does with a delivery it will not acknowledge, per ADR 133 decision 7. Shared between
 * {@link RabbitMqCloudEventBridge} and {@code RabbitMqDomainEventBridge} rather than written twice, since both apply
 * exactly the same sequence: {@link DeliveryFailurePolicy#REDELIVER} negatively acknowledges the original with
 * requeue, and {@link DeliveryFailurePolicy#PARK} republishes the original to a parking destination through a
 * {@link RabbitMqConfirmPublisher}, waits for that publish's own confirm, and only then acknowledges the original.
 * Neither branch ever acknowledges the original directly on the failure path. That is the one thing this class
 * exists to make impossible to get wrong twice.
 * <p>
 * {@link #apply(long, BasicProperties, byte[])} always parks or redelivers the delivery's own raw {@code properties}
 * and {@code body}, unchanged apart from {@link RabbitMqDestination#headers() parkingDestination}'s own configured
 * headers layered on top, whether or not the bridge managed to rebuild a CloudEvent from them. A parked message
 * therefore keeps every AMQP field outside the CloudEvents mapping, a caller-supplied {@code correlationId},
 * {@code appId} or {@code replyTo} among them, rather than only the attributes
 * {@link RabbitMqCloudEventMapper#toBasicProperties(io.cloudevents.CloudEvent, java.util.Map)} would have rebuilt
 * from a decoded event.
 * <p>
 * A failed parking publish (the parking exchange unavailable, its own confirm timing out, a {@code basic.return}
 * because nothing is bound to the parking routing key, ...) negatively acknowledges the original with requeue
 * instead, the same as {@code REDELIVER}, so the original is redelivered rather than lost. Logged at {@code warn}
 * either way, since a park that silently falls back to redelivery is a configuration problem an operator needs to
 * see, and RabbitMQ's own dead-lettering is never used for this, per ADR 133 decision 7: it can discard the message
 * outright with no matching dead-letter exchange, and even with one it republishes without publisher confirms.
 * <p>
 * {@link AutoCloseable} so a bridge can own exactly one of these and close it, rather than separately tracking and
 * closing the parking publisher it may have built.
 */
public final class RabbitMqDeliveryFailureAction implements AutoCloseable {

    /**
     * How long a park publish waits for its confirm. Fixed rather than configurable, matching
     * {@link RabbitMqCloudEventSink}'s own default, since a failure policy is a coarse operational choice that does
     * not need its own tunable separate from the sink's.
     */
    private static final Duration PARK_ACKNOWLEDGEMENT_TIMEOUT = Duration.ofSeconds(5);

    private final Channel consumeChannel;
    private final DeliveryFailurePolicy policy;
    private final @Nullable RabbitMqConfirmPublisher parkingPublisher;
    private final @Nullable RabbitMqDestination parkingDestination;
    private final Logger log;

    /**
     * @param consumeChannel   The channel the failing delivery arrived on. Acknowledgement and negative
     *                         acknowledgement must happen on this exact channel.
     * @param policy           What to do with a delivery nothing consumed.
     * @param parkingPublisher Publishes to the parking destination. Required when {@code policy} is
     *                         {@link DeliveryFailurePolicy#PARK}, otherwise ignored.
     * @param parkingDestination The exchange and routing key {@code parkingPublisher} publishes to. Required
     *                         together with {@code parkingPublisher} when {@code policy} is {@code PARK}.
     */
    public RabbitMqDeliveryFailureAction(Channel consumeChannel, DeliveryFailurePolicy policy, @Nullable RabbitMqConfirmPublisher parkingPublisher,
                                          @Nullable RabbitMqDestination parkingDestination, Logger log) {
        this.consumeChannel = requireNonNull(consumeChannel, "consumeChannel cannot be null");
        this.policy = requireNonNull(policy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
        this.parkingPublisher = parkingPublisher;
        this.parkingDestination = parkingDestination;
        this.log = requireNonNull(log, "log cannot be null");
        if (policy == DeliveryFailurePolicy.PARK && (parkingPublisher == null || parkingDestination == null)) {
            throw new IllegalArgumentException("A parkingPublisher and a parkingDestination are both required when the policy is PARK");
        }
    }

    /**
     * Builds the action for {@code policy}, and the {@link RabbitMqConfirmPublisher} it parks through when
     * {@code policy} is {@link DeliveryFailurePolicy#PARK} and {@code parkingDestination} is given. Refuses when
     * {@code policy} is {@code PARK} and {@code parkingDestination} is {@code null}, the same requirement
     * {@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK}'s own javadoc states. No parking resource
     * is opened at all when {@code policy} is not {@code PARK}, even if {@code parkingDestination} happens to be
     * given anyway, since nothing would ever publish through it.
     */
    public static RabbitMqDeliveryFailureAction create(Connection connection, Channel consumeChannel, DeliveryFailurePolicy policy,
                                                         @Nullable RabbitMqDestination parkingDestination, Logger log) {
        if (policy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
            throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
        }
        if (policy != DeliveryFailurePolicy.PARK) {
            return new RabbitMqDeliveryFailureAction(consumeChannel, policy, null, null, log);
        }
        RabbitMqConfirmPublisher parkingPublisher = new RabbitMqConfirmPublisher(connection, PARK_ACKNOWLEDGEMENT_TIMEOUT);
        return new RabbitMqDeliveryFailureAction(consumeChannel, policy, parkingPublisher, parkingDestination, log);
    }

    /**
     * Applies this failure action to the delivery {@code deliveryTag} identifies, republishing its own raw
     * {@code properties} and {@code body}, plus the parking destination's own configured headers, when
     * {@link DeliveryFailurePolicy#PARK} is configured, whether or not the bridge managed to rebuild a CloudEvent
     * from them. {@link DeliveryFailurePolicy#PARK} acknowledges {@code deliveryTag} only once that publish has
     * been confirmed and not returned as unroutable. Never acknowledges {@code deliveryTag} directly.
     */
    public void apply(long deliveryTag, BasicProperties properties, byte[] body) {
        requireNonNull(properties, "properties cannot be null");
        requireNonNull(body, "body cannot be null");
        if (policy == DeliveryFailurePolicy.REDELIVER) {
            redeliverFailure(deliveryTag);
            return;
        }
        park(deliveryTag, properties, body);
    }

    private void park(long deliveryTag, BasicProperties properties, byte[] body) {
        RabbitMqDestination destination = requireNonNull(parkingDestination);
        BasicProperties parkedProperties = withDestinationHeaders(properties, destination);
        try {
            requireNonNull(parkingPublisher).publish(destination.exchange(), destination.routingKey(), parkedProperties, body);
        } catch (RuntimeException e) {
            log.warn("Failed to park a delivery nothing consumed. Redelivering it instead of losing it.", e);
            redeliver(deliveryTag);
            return;
        }
        log.warn("Parked delivery tag {} to exchange \"{}\" routing key \"{}\" and acknowledged the original; " +
                "nothing consumed it.", deliveryTag, destination.exchange(), destination.routingKey());
        ack(deliveryTag);
    }

    /**
     * The {@link DeliveryFailurePolicy} this action applies. Exposed so a bridge can pace a
     * {@link DeliveryFailurePolicy#REDELIVER} failure itself, held and released once per poll the same way it
     * already paces {@link RoutingOutcome#DEFERRED}, rather than nacking it immediately on every attempt, without
     * duplicating the policy this action was already built with.
     */
    public DeliveryFailurePolicy policy() {
        return policy;
    }

    // A copy of properties with destination's own configured headers added on top of the original ones, so a
    // parking marker (a tenant or reason header, say) reaches the parked message alongside the delivery's own
    // AMQP fields, all of which properties.builder() already carries over unchanged. destination's headers win
    // on a key collision, since RabbitMqDestination's own constructor already reserves the cloudEvents_ prefix
    // against ever colliding with the mapping, so the only collision possible here is a deliberate one.
    private static BasicProperties withDestinationHeaders(BasicProperties properties, RabbitMqDestination destination) {
        if (destination.headers().isEmpty()) {
            return properties;
        }
        Map<String, Object> headers = properties.getHeaders() == null ? new HashMap<>() : new HashMap<>(properties.getHeaders());
        headers.putAll(destination.headers());
        return properties.builder().headers(headers).build();
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
     * Negatively acknowledges {@code deliveryTag} with requeue, on the delivery's own channel, unconditionally,
     * bypassing {@link DeliveryFailurePolicy} entirely, and logging nothing itself. Public, unlike
     * {@link #apply(long, BasicProperties, byte[])}, for {@link RoutingOutcome#DEFERRED} and a lifecycle
     * {@link RoutingOutcome#NOT_DELIVERABLE}: a message a catch-up-then-live engine cannot accept yet, or a
     * subscription paused or not running, is never a candidate for {@link DeliveryFailurePolicy#PARK}, whatever
     * this bridge is configured with, since nothing here is broken or wrong, only not ready yet, and pacing a
     * bridge's own consumer this way happens far too often, by design, for a log line per occurrence to be useful.
     * See {@link #redeliverFailure(long)} for the equivalent used on an actual failure, which does log.
     */
    public void redeliver(long deliveryTag) {
        try {
            consumeChannel.basicNack(deliveryTag, false, true);
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to negatively acknowledge delivery tag " + deliveryTag, e);
        }
    }

    /**
     * {@link #redeliver(long)}, plus a single {@code warn} log line, for a genuine {@link DeliveryFailurePolicy#REDELIVER}
     * failure rather than pacing: a handler or filter that failed, say. The one line an operator needs for that
     * event. A caller reaching this must not also log the same failure itself, the same way a caller of
     * {@link #apply(long, BasicProperties, byte[])}'s {@code PARK} branch already relies on {@link #park} alone to
     * log it.
     */
    public void redeliverFailure(long deliveryTag) {
        log.warn("Redelivered delivery tag {} with requeue; nothing consumed it.", deliveryTag);
        redeliver(deliveryTag);
    }

    /**
     * Closes the parking publisher this action built, if {@link DeliveryFailurePolicy#PARK} was configured. Does
     * not touch {@code consumeChannel}, which the bridge that owns it closes itself. Best effort, a failure closing
     * it is logged rather than thrown, since it happens during teardown with nothing left to hand it to.
     */
    @Override
    public void close() {
        if (parkingPublisher != null) {
            try {
                parkingPublisher.close();
            } catch (IOException | ShutdownSignalException | TimeoutException e) {
                log.warn("Failed to close the parking publisher cleanly during shutdown.", e);
            }
        }
    }
}
