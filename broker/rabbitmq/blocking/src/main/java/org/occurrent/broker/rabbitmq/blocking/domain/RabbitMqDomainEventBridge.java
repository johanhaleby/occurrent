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

package org.occurrent.broker.rabbitmq.blocking.domain;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBridgeException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventBridge;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopology;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnreadableLiveFilterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * Bridges a RabbitMQ queue into a {@link DomainEventFeed}, the domain-level consume side ADR 133 decision 5
 * describes. Rebuilds each message as a {@link CloudEvent} through {@link RabbitMqCloudEventMapper} and calls
 * {@link DomainEventFeed#acceptCloudEvent(CloudEvent)}, which is where the matching, the decoding and the delivery
 * all happen; this bridge does no filtering of its own, since the feed is the only thing that can decide per ADR 133
 * decision 5.
 * <p>
 * <strong>Acknowledgement</strong> follows the {@link RoutingOutcome} {@code acceptCloudEvent(...)} returns, exactly
 * as {@link RabbitMqCloudEventBridge} follows the one its own model reports:
 * {@link RoutingOutcome#DELIVERED} or {@link RoutingOutcome#FILTERED} acknowledges, {@link RoutingOutcome#NOT_DELIVERABLE}
 * and a thrown exception both apply this bridge's configured {@link DeliveryFailurePolicy} instead, never
 * acknowledging directly.
 * <p>
 * <strong>{@link UnreadableLiveFilterException} is different, and permanent.</strong> It means the projection this
 * feed carries was registered with a {@code data} payload filter this feed has no {@link org.occurrent.filtermatching.DataFieldReader}
 * for, a configuration error that cannot change without a new registration, and the same exception instance is
 * thrown again on every later call. On catching it, this bridge logs the failure, cancels its own consumer and stops
 * its coarse poll for good, and <strong>leaves the triggering delivery neither acknowledged nor negatively
 * acknowledged</strong>, exactly as {@link UnreadableLiveFilterException}'s own javadoc requires. It is not requeued
 * (which would only redeliver it into the same permanent failure, the retry loop this exists to avoid) and it is not
 * parked (parking would still acknowledge the original once the park is confirmed, and this must never acknowledge
 * it at all). The delivery stays outstanding on this bridge's channel until an operator fixes the registration and
 * restarts, or the application calls {@link #close()}, at which point RabbitMQ requeues it for whoever consumes next,
 * so the event survives rather than being lost.
 * <p>
 * <strong>Coarse lifecycle.</strong> A background poll, {@link Builder#pollInterval(Duration)} apart (one second by
 * default), reads {@link DomainEventFeed#hasProjection()} and {@link DomainEventFeed#isReadyForLiveDelivery()} and
 * starts or cancels this bridge's own AMQP consumer to match, consuming only once a projection is registered and its
 * catch-up-then-live transition has actually reached live, not merely started. This exists for the same reason
 * {@link RabbitMqCloudEventBridge} polls {@code isRunning(...)}. Without the registration half, a message arriving
 * before the application registers its projection would hit {@code acceptCloudEvent(...)}'s
 * {@link IllegalStateException} refusal on every delivery, and under {@link DeliveryFailurePolicy#REDELIVER} that is
 * an instant requeue-and-redeliver loop, not a wait. Without the readiness half, a message arriving before the
 * application calls {@code catchUpAll()}/{@code catchUp(...)} or {@code goLive(...)}, or while a
 * {@code catchUpAll()}/{@code catchUp(...)} replay is still actively running, would only ever buffer with nothing
 * behind it, which {@code acceptCloudEvent(...)} answers with {@link RoutingOutcome#NOT_DELIVERABLE} rather than
 * {@link RoutingOutcome#DELIVERED} for exactly that reason (see its own javadoc), and under
 * {@link DeliveryFailurePolicy#REDELIVER} that is the same instant requeue-and-redeliver loop, this time against a
 * buffer that never drains until live is actually reached. Feeding {@code acceptCloudEvent(...)} can still throw
 * {@link IllegalStateException} despite the poll, for the narrow race where the check ran just before the one
 * registration this feed ever accepts. That case, unlike {@link UnreadableLiveFilterException}, applies the
 * configured {@link DeliveryFailurePolicy} like any other failure, since it is transient rather than permanent.
 * <p>
 * <strong>This bridge consumes nothing for the entire duration of a replay</strong>, which for a large event store
 * can be minutes, not the instant {@code catchUpAll()}/{@code catchUp(...)} is called. A message published during
 * that window queues up on the broker rather than being pulled off and held here, since nothing is consuming it
 * yet. That is harmless on the durable, unbounded queue this bridge declares by default when
 * {@link Builder#declareTopology(boolean) declareTopology(false)} is not set. Nothing there expires and nothing
 * is dropped for space, so every message is still on it once the replay reaches live. It stops being harmless the
 * moment the queue itself carries a bound, an {@code x-max-length} with a drop-oldest overflow policy, or an
 * {@code x-message-ttl}. Either can discard a message queued during a long replay before this bridge ever gets to
 * consume it, a consequence of that queue policy, not of anything this bridge does. Size a bound or a TTL against
 * how long a replay can run, or leave the queue unbounded, before pointing this bridge at it.
 */
public final class RabbitMqDomainEventBridge<E> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqDomainEventBridge.class);

    private final DomainEventFeed<E> feed;
    private final Channel consumeChannel;
    private final String queue;
    private final int prefetchCount;
    private final Duration pollInterval;
    private final RabbitMqDeliveryFailureAction failureAction;
    private final ScheduledExecutorService scheduler;

    private final Lock consumeLock = new ReentrantLock();
    private @Nullable String consumerTag;
    private volatile boolean permanentlyStopped;

    private RabbitMqDomainEventBridge(DomainEventFeed<E> feed, Channel consumeChannel, String queue, int prefetchCount,
                                       Duration pollInterval, RabbitMqDeliveryFailureAction failureAction) {
        this.feed = feed;
        this.consumeChannel = consumeChannel;
        this.queue = queue;
        this.prefetchCount = prefetchCount;
        this.pollInterval = pollInterval;
        this.failureAction = failureAction;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rabbitmq-domainevent-bridge-" + queue);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * @param connection The connection this bridge creates its own {@link Channel} on for consuming, declaring
     *                   topology, and acknowledging.
     * @param feed       The feed this bridge calls {@link DomainEventFeed#acceptCloudEvent(CloudEvent)} on.
     * @param queue      The queue this bridge consumes from, and declares unless {@link Builder#declareTopology(boolean)}
     *                   is set to {@code false}.
     */
    public static <E> Builder<E> builder(Connection connection, DomainEventFeed<E> feed, String queue) {
        return new Builder<>(connection, feed, queue);
    }

    private void start(Builder<E> builder) {
        try {
            if (builder.declareTopology) {
                consumeChannel.queueDeclare(queue, true, false, false, null);
                Set<RabbitMqDestination> destinations = RabbitMqTopology.destinationsToBind(builder.resolver, builder.bindingFilter, builder.bindings);
                for (RabbitMqDestination destination : destinations) {
                    consumeChannel.queueBind(queue, destination.exchange(), destination.routingKey());
                }
            }
            consumeChannel.basicQos(prefetchCount);
        } catch (IOException e) {
            throw new RabbitMqBridgeException("Failed to declare topology for queue \"" + queue + "\"", e);
        }
        scheduler.scheduleWithFixedDelay(this::reconcileConsumption, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    // Coarse lifecycle control, per the class javadoc. Consumes only once a projection is registered and ready for
    // live delivery, checked on a fixed poll rather than on every message, and never resumes once permanentlyStopped
    // is set. permanentlyStopped is read here under consumeLock, the same lock stopPermanently() sets it under,
    // rather than checked once before acquiring the lock. A poll that read the flag as false, then blocked on the
    // lock while stopPermanently() ran and cancelled the consumer, would otherwise restart the very consumer that
    // permanent stop just cancelled once the lock finally freed, feeding a live event straight back into the same
    // permanently refused filter this exists to stop redelivering into.
    private void reconcileConsumption() {
        try {
            consumeLock.lock();
            try {
                if (permanentlyStopped) {
                    return;
                }
                boolean shouldConsume = feed.hasProjection() && feed.isReadyForLiveDelivery();
                if (shouldConsume && consumerTag == null) {
                    consumerTag = consumeChannel.basicConsume(queue, false, this::handleDelivery, this::handleCancel);
                } else if (!shouldConsume && consumerTag != null) {
                    consumeChannel.basicCancel(consumerTag);
                    consumerTag = null;
                }
            } finally {
                consumeLock.unlock();
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Failed to reconcile consumption for queue \"{}\" against the feed's registration state. " +
                    "Retrying on the next poll.", queue, e);
        }
    }

    private void handleCancel(String cancelledConsumerTag) {
        consumeLock.lock();
        try {
            if (cancelledConsumerTag.equals(consumerTag)) {
                consumerTag = null;
            }
        } finally {
            consumeLock.unlock();
        }
    }

    private void handleDelivery(String deliveryConsumerTag, Delivery delivery) {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        CloudEvent cloudEvent;
        try {
            cloudEvent = RabbitMqCloudEventMapper.toCloudEvent(delivery.getProperties(), delivery.getBody());
        } catch (RuntimeException e) {
            log.warn("Failed to rebuild a CloudEvent from a message on queue \"{}\".", queue, e);
            failureAction.apply(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }

        RoutingOutcome outcome;
        try {
            outcome = feed.acceptCloudEvent(cloudEvent);
        } catch (UnreadableLiveFilterException e) {
            log.error("The registration on queue \"{}\"'s feed has a data payload filter this feed cannot answer " +
                    "live. This is a permanent configuration error; stopping this bridge rather than redelivering " +
                    "into the same failure. Delivery tag {} is left unacknowledged so it survives for the next " +
                    "consumer once the registration is fixed. Register a new DomainEventFeed with a Filter that " +
                    "does not reference the field, or with a DataFieldReader that can read it.", queue, deliveryTag, e);
            stopPermanently();
            return;
        } catch (RuntimeException | AssertionError e) {
            // Either the projection handler itself threw, or the narrow registeredProjection() race the class
            // javadoc describes (an IllegalStateException that is not an UnreadableLiveFilterException). Both are
            // ordinary failure-policy cases, unlike the permanent one caught above. AssertionError is caught here
            // too, since the converter, the live matcher or the projection can throw one, and leaving it uncaught
            // would strand the delivery unacked at prefetch one. Any other Error still propagates.
            failureAction.apply(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }
        if (outcome == RoutingOutcome.DELIVERED || outcome == RoutingOutcome.FILTERED) {
            failureAction.ack(deliveryTag);
        } else {
            failureAction.apply(deliveryTag, delivery.getProperties(), delivery.getBody());
        }
    }

    // Cancels this bridge's own consumer and stops the coarse poll for good, without closing the channel: the
    // triggering delivery must stay on it, unacknowledged, until close() or an operator-driven restart, per
    // UnreadableLiveFilterException's own javadoc ("must never acknowledge and redeliver the event that triggered
    // it expecting a different answer").
    private void stopPermanently() {
        consumeLock.lock();
        try {
            // Set under the same lock reconcileConsumption() now reads it under, so the two can never interleave:
            // whichever of this method and a concurrent poll acquires the lock first fully decides the consumer's
            // fate before the other even reads the flag.
            permanentlyStopped = true;
            if (consumerTag != null) {
                try {
                    consumeChannel.basicCancel(consumerTag);
                } catch (IOException e) {
                    log.warn("Failed to cancel the consumer on queue \"{}\" while stopping permanently.", queue, e);
                }
                consumerTag = null;
            }
        } finally {
            consumeLock.unlock();
        }
        scheduler.shutdown();
    }

    /**
     * Stops the background poll, cancels this bridge's consumer if it has one, and closes the {@link Channel} (and,
     * with {@link DeliveryFailurePolicy#PARK}, the parking sink) this bridge created. Does not close the
     * {@link Connection} it was built from.
     */
    @Override
    public void close() {
        scheduler.shutdownNow();
        consumeLock.lock();
        try {
            if (consumerTag != null) {
                try {
                    consumeChannel.basicCancel(consumerTag);
                } catch (IOException ignored) {
                    // Best effort: the channel is about to be closed either way.
                }
                consumerTag = null;
            }
        } finally {
            consumeLock.unlock();
        }
        try {
            consumeChannel.close();
        } catch (IOException | ShutdownSignalException | TimeoutException ignored) {
            // Best effort, mirroring RabbitMqCloudEventSink#close's own channel teardown.
        }
        failureAction.close();
    }

    public static final class Builder<E> {
        private final Connection connection;
        private final DomainEventFeed<E> feed;
        private final String queue;
        private @Nullable DestinationResolver<RabbitMqDestination> resolver;
        private @Nullable SubscriptionFilter bindingFilter;
        private @Nullable Set<RabbitMqDestination> bindings;
        private boolean declareTopology = true;
        private DeliveryFailurePolicy deliveryFailurePolicy = DeliveryFailurePolicy.REDELIVER;
        private @Nullable RabbitMqDestination parkingDestination;
        private Duration pollInterval = Duration.ofSeconds(1);
        private int prefetchCount = 1;

        private Builder(Connection connection, DomainEventFeed<E> feed, String queue) {
            this.connection = requireNonNull(connection, "connection cannot be null");
            this.feed = requireNonNull(feed, DomainEventFeed.class.getSimpleName() + " cannot be null");
            this.queue = requireNonNull(queue, "queue cannot be null");
        }

        /**
         * Derives the queue's bindings from {@link #bindingFilter(SubscriptionFilter)} or, absent one, from
         * {@link DestinationResolver#catchAllDestination()}. Required unless {@link #bindings(Set)} is given or
         * {@link #declareTopology(boolean)} is set to {@code false}.
         */
        public Builder<E> resolver(DestinationResolver<RabbitMqDestination> resolver) {
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * Narrows the declared bindings to {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for this
         * filter, falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot derive
         * one. Requires {@link #resolver(DestinationResolver)}. Per ADR 133 decision 5, this filter narrows what
         * arrives; it must be at least as inclusive as the registered projection's own replay filter, or events the
         * projection would have accepted never arrive at all.
         */
        public Builder<E> bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Declares exactly these bindings instead of deriving any from a resolver, the explicit escape hatch for a
         * binding scheme a resolver cannot express.
         */
        public Builder<E> bindings(Set<RabbitMqDestination> bindings) {
            this.bindings = Set.copyOf(requireNonNull(bindings, "bindings cannot be null"));
            return this;
        }

        /**
         * Whether this bridge declares its queue and bindings at all. {@code true} by default. Set to {@code false}
         * for a deployment whose platform team owns the queue and its bindings itself, per #415; this bridge then
         * only consumes from {@code queue} and never calls {@code queueDeclare} or {@code queueBind}.
         */
        public Builder<E> declareTopology(boolean declareTopology) {
            this.declareTopology = declareTopology;
            return this;
        }

        /**
         * What this bridge does with a delivery it will not acknowledge. {@link DeliveryFailurePolicy#REDELIVER} by
         * default. Never consulted for {@link UnreadableLiveFilterException}, see the class javadoc.
         */
        public Builder<E> onDeliveryFailure(DeliveryFailurePolicy deliveryFailurePolicy) {
            this.deliveryFailurePolicy = requireNonNull(deliveryFailurePolicy, DeliveryFailurePolicy.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * The destination {@link DeliveryFailurePolicy#PARK} publishes a failed delivery to. Required when
         * {@link #onDeliveryFailure(DeliveryFailurePolicy)} is {@code PARK} ({@link #build()} refuses otherwise).
         * Given without {@code PARK}, this is accepted but unused, not refused, the same choice
         * {@link RabbitMqDeliveryFailureAction#create(Connection, Channel, DeliveryFailurePolicy, RabbitMqDestination, Logger)}
         * makes and documents, so switching {@link #onDeliveryFailure} back to {@code REDELIVER} in application
         * config never has to strip this call out along with it.
         */
        public Builder<E> parkingDestination(RabbitMqDestination parkingDestination) {
            this.parkingDestination = requireNonNull(parkingDestination, "parkingDestination cannot be null");
            return this;
        }

        /**
         * How often the coarse lifecycle poll checks {@link DomainEventFeed#hasProjection()} and
         * {@link DomainEventFeed#isReadyForLiveDelivery()}. One second by default.
         */
        public Builder<E> pollInterval(Duration pollInterval) {
            requireNonNull(pollInterval, "pollInterval cannot be null");
            if (pollInterval.toMillis() <= 0) {
                throw new IllegalArgumentException("pollInterval must be at least 1 millisecond, was " + pollInterval);
            }
            this.pollInterval = pollInterval;
            return this;
        }

        /**
         * How many unacknowledged deliveries this bridge allows itself at once. One by default, which is what keeps
         * deliveries processed one at a time.
         */
        public Builder<E> prefetchCount(int prefetchCount) {
            if (prefetchCount < 1) {
                throw new IllegalArgumentException("prefetchCount must be at least 1, was " + prefetchCount);
            }
            this.prefetchCount = prefetchCount;
            return this;
        }

        public RabbitMqDomainEventBridge<E> build() {
            if (declareTopology && bindings == null && resolver == null) {
                throw new IllegalStateException("A resolver(...), or explicit bindings(...), is required unless declareTopology(false) is set");
            }
            if (deliveryFailurePolicy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
                throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
            }
            // Validated above, before opening anything: a failure past this point has a channel (and, under PARK, a
            // parking sink) already open, so every later failure path in this method closes what it opened rather
            // than leaking it.
            Channel channel = openChannel(connection);
            RabbitMqDeliveryFailureAction failureAction = null;
            RabbitMqDomainEventBridge<E> bridge = null;
            try {
                failureAction = RabbitMqDeliveryFailureAction.create(connection, channel, deliveryFailurePolicy, parkingDestination, log);
                bridge = new RabbitMqDomainEventBridge<>(feed, channel, queue, prefetchCount, pollInterval, failureAction);
                bridge.start(this);
                return bridge;
            } catch (RuntimeException e) {
                closeQuietly(channel);
                if (failureAction != null) {
                    failureAction.close();
                }
                if (bridge != null) {
                    // start() can fail after the bridge, and its scheduler, already exist. The scheduler has no
                    // thread yet here, since scheduleWithFixedDelay(...) is the last thing start() does, but this
                    // stays defensive against that ordering changing later.
                    bridge.scheduler.shutdownNow();
                }
                throw e;
            }
        }

        private static Channel openChannel(Connection connection) {
            try {
                return connection.openChannel()
                        .orElseThrow(() -> new RabbitMqBridgeException("No RabbitMQ channel number was available to create the bridge's channel on"));
            } catch (IOException e) {
                throw new RabbitMqBridgeException("Failed to create the bridge's RabbitMQ channel", e);
            }
        }

        // Best effort: build() is already failing for another reason by the time this runs, so a failure here is
        // logged rather than allowed to replace the failure the caller actually asked about.
        private static void closeQuietly(Channel channel) {
            try {
                channel.close();
            } catch (IOException | ShutdownSignalException | TimeoutException e) {
                log.warn("Failed to close the bridge's channel while unwinding a failed build().", e);
            }
        }
    }
}
