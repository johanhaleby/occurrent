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
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.push.blocking.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
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
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Bridges a RabbitMQ queue into a {@link PushSubscriptionModel}, the CloudEvent-level consume side ADR 133 decision 1
 * describes. Rebuilds each message as a {@link CloudEvent} through {@link RabbitMqCloudEventMapper}, hands it to
 * {@link PushSubscriptionModel#accept(CloudEvent)}, and acknowledges only once the {@link RoutingOutcome} that
 * {@code accept(...)} reported through a shared {@link RoutingOutcomeChannel} says the event was actually consumed.
 * <p>
 * <strong>Holds a {@link PushSubscriptionModel}, never a {@link CatchupThenPushSubscriptionModel}.</strong> ADR 133
 * decision 1 is explicit that a bridge feeds the live model, not the catch-up wrapper in front of it, since a
 * {@code CatchupThenPushSubscriptionModel} is not itself a push target: it composes a {@link PushSubscriptionModel}
 * as a constructor argument and replays history through it before handing over. An application that wants catch-up
 * builds one from the same {@link PushSubscriptionModel} this bridge is given, in front of it, not instead of it.
 * <p>
 * <strong>Acknowledgement.</strong> {@code accept(...)} throwing (a handler exception, or a subscription filter
 * that failed to evaluate) never acknowledges. A normal return with {@link RoutingOutcome#DELIVERED} or
 * {@link RoutingOutcome#FILTERED} acknowledges. A normal return with {@link RoutingOutcome#NOT_DELIVERABLE} never
 * acknowledges. In every case that does not acknowledge, this bridge's configured {@link DeliveryFailurePolicy}
 * applies: {@link DeliveryFailurePolicy#REDELIVER} (the default) negatively acknowledges with requeue,
 * {@link DeliveryFailurePolicy#PARK} republishes to a parking destination and only then acknowledges the original.
 * <p>
 * <strong>Topology.</strong> By default this bridge declares its own queue (durable, not exclusive, not
 * auto-delete) and binds it to {@link Builder#bindings(Set)} if given, or else to
 * {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for {@link Builder#bindingFilter(SubscriptionFilter)}
 * if given (falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot narrow it),
 * or else to {@link DestinationResolver#catchAllDestination()} outright. {@link Builder#declareTopology(boolean)
 * declareTopology(false)} skips all of this for a deployment whose platform team owns the queue and its bindings
 * itself, per #415. A binding only narrows what arrives; a {@link SubscriptionFilter} on anything other than the
 * event type is invisible to it, and {@code accept(...)} still applies the subscription's own filter regardless of
 * what was bound.
 * <p>
 * <strong>Coarse lifecycle.</strong> A background poll, {@link Builder#pollInterval(Duration)} apart (one second by
 * default), reads {@link PushSubscriptionModel#subscriptionIds()} and {@link PushSubscriptionModel#isRunning(String)}
 * and starts or cancels this bridge's own AMQP consumer to match: consuming while the model has a running
 * subscription, not consuming otherwise. This is deliberately coarse, a small delay either way is harmless, and it
 * exists so this bridge never feeds a stopped or paused model, which per ADR 85 and ADR 104 drops the event rather
 * than holding it. Never used to decide a single message; that decision comes from the {@link RoutingOutcome} above.
 * The same poll also reads {@link Builder#readinessSource(Predicate)} for the subscription id, {@code true} by
 * default, so this bridge never pulls a message off the queue while a {@link CatchupThenPushSubscriptionModel}
 * wrapping {@code model} is still replaying or draining into it. The message waits on the broker instead of being
 * buffered here, since acknowledging on {@link RoutingOutcome#DELIVERED} a message the wrapper has only buffered,
 * not yet applied, is exactly the loss a consume bridge exists to prevent. See
 * {@link Builder#readinessSource(Predicate)} for how to wire it.
 */
public final class RabbitMqCloudEventBridge implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqCloudEventBridge.class);

    private final PushSubscriptionModel model;
    private final RoutingOutcomeChannel outcomeChannel;
    private final Channel consumeChannel;
    private final String queue;
    private final int prefetchCount;
    private final Duration pollInterval;
    private final RabbitMqDeliveryFailureAction failureAction;
    private final Predicate<String> readinessSource;
    private final ScheduledExecutorService scheduler;

    private final Lock consumeLock = new ReentrantLock();
    private @Nullable String consumerTag;

    private RabbitMqCloudEventBridge(PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, Channel consumeChannel,
                                      String queue, int prefetchCount, Duration pollInterval, RabbitMqDeliveryFailureAction failureAction,
                                      Predicate<String> readinessSource) {
        this.model = model;
        this.outcomeChannel = outcomeChannel;
        this.consumeChannel = consumeChannel;
        this.queue = queue;
        this.prefetchCount = prefetchCount;
        this.pollInterval = pollInterval;
        this.failureAction = failureAction;
        this.readinessSource = readinessSource;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rabbitmq-cloudevent-bridge-" + queue);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * @param connection     The connection this bridge creates its own {@link Channel} on for consuming, declaring
     *                       topology, and acknowledging.
     * @param model          The live model this bridge feeds. Never a {@link CatchupThenPushSubscriptionModel}, see
     *                       the class javadoc.
     * @param outcomeChannel Shared with {@code model}'s own constructor, see {@link RoutingOutcomeChannel}.
     * @param queue          The queue this bridge consumes from, and declares unless {@link Builder#declareTopology(boolean)}
     *                       is set to {@code false}.
     */
    public static Builder builder(Connection connection, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, String queue) {
        return new Builder(connection, model, outcomeChannel, queue);
    }

    private void start(Builder builder) {
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

    // Coarse lifecycle control, per the class javadoc: consumes while the model has a running subscription and
    // readinessSource agrees, not otherwise, checked on a fixed poll rather than on every message. subscriptionIds()
    // has at most one element by ADR 90 (PushSubscriptionModel declares Consumers.ONE), so there is never an
    // ambiguity about which id to ask isRunning(...) or readinessSource about.
    private void reconcileConsumption() {
        try {
            Set<String> subscriptionIds = model.subscriptionIds();
            String subscriptionId = subscriptionIds.isEmpty() ? null : subscriptionIds.iterator().next();
            boolean shouldConsume = subscriptionId != null && model.isRunning(subscriptionId) && readinessSource.test(subscriptionId);
            consumeLock.lock();
            try {
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
            log.warn("Failed to reconcile consumption for queue \"{}\" against the subscription model's running state. " +
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
        try {
            model.accept(cloudEvent);
        } catch (RuntimeException | AssertionError e) {
            // Catches AssertionError too, since a filter or the handler can throw one, and an uncaught Error here
            // would leave the delivery unacked and stall the consumer at prefetch one. Any other Error still propagates.
            outcomeChannel.takeLastOutcome();
            failureAction.apply(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }
        RoutingOutcome outcome = outcomeChannel.takeLastOutcome();
        if (outcome == RoutingOutcome.DELIVERED || outcome == RoutingOutcome.FILTERED) {
            failureAction.ack(deliveryTag);
        } else {
            failureAction.apply(deliveryTag, delivery.getProperties(), delivery.getBody());
        }
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

    public static final class Builder {
        private final Connection connection;
        private final PushSubscriptionModel model;
        private final RoutingOutcomeChannel outcomeChannel;
        private final String queue;
        private @Nullable DestinationResolver<RabbitMqDestination> resolver;
        private @Nullable SubscriptionFilter bindingFilter;
        private @Nullable Set<RabbitMqDestination> bindings;
        private boolean declareTopology = true;
        private DeliveryFailurePolicy deliveryFailurePolicy = DeliveryFailurePolicy.REDELIVER;
        private @Nullable RabbitMqDestination parkingDestination;
        private Duration pollInterval = Duration.ofSeconds(1);
        private int prefetchCount = 1;
        private Predicate<String> readinessSource = subscriptionId -> true;

        private Builder(Connection connection, PushSubscriptionModel model, RoutingOutcomeChannel outcomeChannel, String queue) {
            this.connection = requireNonNull(connection, "connection cannot be null");
            this.model = requireNonNull(model, PushSubscriptionModel.class.getSimpleName() + " cannot be null");
            this.outcomeChannel = requireNonNull(outcomeChannel, RoutingOutcomeChannel.class.getSimpleName() + " cannot be null");
            this.queue = requireNonNull(queue, "queue cannot be null");
        }

        /**
         * Derives the queue's bindings from {@link #bindingFilter(SubscriptionFilter)} or, absent one, from
         * {@link DestinationResolver#catchAllDestination()}. Required unless {@link #bindings(Set)} is given or
         * {@link #declareTopology(boolean)} is set to {@code false}.
         */
        public Builder resolver(DestinationResolver<RabbitMqDestination> resolver) {
            this.resolver = requireNonNull(resolver, DestinationResolver.class.getSimpleName() + " cannot be null");
            return this;
        }

        /**
         * Narrows the declared bindings to {@link DestinationResolver#destinationsFor(SubscriptionFilter)} for this
         * filter, falling back to {@link DestinationResolver#catchAllDestination()} when the resolver cannot derive
         * one. Requires {@link #resolver(DestinationResolver)}. Per ADR 133 decision 5, this filter narrows what
         * arrives; it must be at least as inclusive as the subscription's own filter, or events the subscription
         * would have accepted never arrive at all.
         */
        public Builder bindingFilter(SubscriptionFilter bindingFilter) {
            this.bindingFilter = requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return this;
        }

        /**
         * Declares exactly these bindings instead of deriving any from a resolver, the explicit escape hatch for a
         * binding scheme a resolver cannot express.
         */
        public Builder bindings(Set<RabbitMqDestination> bindings) {
            this.bindings = Set.copyOf(requireNonNull(bindings, "bindings cannot be null"));
            return this;
        }

        /**
         * Whether this bridge declares its queue and bindings at all. {@code true} by default. Set to {@code false}
         * for a deployment whose platform team owns the queue and its bindings itself, per #415; this bridge then
         * only consumes from {@code queue} and never calls {@code queueDeclare} or {@code queueBind}.
         */
        public Builder declareTopology(boolean declareTopology) {
            this.declareTopology = declareTopology;
            return this;
        }

        /**
         * What this bridge does with a delivery it will not acknowledge. {@link DeliveryFailurePolicy#REDELIVER} by
         * default.
         */
        public Builder onDeliveryFailure(DeliveryFailurePolicy deliveryFailurePolicy) {
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
        public Builder parkingDestination(RabbitMqDestination parkingDestination) {
            this.parkingDestination = requireNonNull(parkingDestination, "parkingDestination cannot be null");
            return this;
        }

        /**
         * How often the coarse lifecycle poll checks the model's running state. One second by default. A small
         * delay is harmless, see the class javadoc, so this rarely needs changing.
         */
        public Builder pollInterval(Duration pollInterval) {
            requireNonNull(pollInterval, "pollInterval cannot be null");
            if (pollInterval.toMillis() <= 0) {
                throw new IllegalArgumentException("pollInterval must be at least 1 millisecond, was " + pollInterval);
            }
            this.pollInterval = pollInterval;
            return this;
        }

        /**
         * How many unacknowledged deliveries this bridge allows itself at once. One by default, which is what keeps
         * deliveries processed one at a time, matching {@code PushSubscriptionModel}'s single-evaluation contract
         * and this bridge's one-delivery-tag-outstanding acknowledgement model. Raising it is only safe if the
         * application's own handler is safe to run concurrently with itself.
         */
        public Builder prefetchCount(int prefetchCount) {
            if (prefetchCount < 1) {
                throw new IllegalArgumentException("prefetchCount must be at least 1, was " + prefetchCount);
            }
            this.prefetchCount = prefetchCount;
            return this;
        }

        /**
         * Asked, alongside {@code model}'s own running state, on every coarse lifecycle poll (see the class
         * javadoc). {@code true} for every subscription id by default, which is exactly right for {@code model} fed
         * directly with no catch-up in front of it, or for {@code catchup = NONE}, since no wrapper exists there to
         * buffer anything for this to protect against.
         * <p>
         * Wrap {@code model} in a {@link CatchupThenPushSubscriptionModel} and pass
         * {@code catchupThenPush::isReadyForLiveDelivery} here, so this bridge stops pulling from the queue for as
         * long as that wrapper's replay is still running or draining, and resumes once it reaches live. Without
         * this, a message this bridge acknowledges on {@link RoutingOutcome#DELIVERED} while the wrapper is only
         * buffering it is durable nowhere but this process's memory until the drain completes. A crash before that
         * loses it for good, which the local event store cannot restore for an event a consume bridge exists
         * precisely to receive from another service. Built by an {@code @Projection(source = PUSH)} or
         * {@code @Saga(source = PUSH)} bean instead of by hand, the wrapper is published as a Spring bean named
         * {@code "catchupThenPushSubscriptionModel-" + id}, so {@code applicationContext.getBean(name,
         * CatchupThenPushSubscriptionModel.class)::isReadyForLiveDelivery} reaches the same object.
         */
        public Builder readinessSource(Predicate<String> readinessSource) {
            this.readinessSource = requireNonNull(readinessSource, "readinessSource cannot be null");
            return this;
        }

        public RabbitMqCloudEventBridge build() {
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
            RabbitMqCloudEventBridge bridge = null;
            try {
                failureAction = RabbitMqDeliveryFailureAction.create(connection, channel, deliveryFailurePolicy, parkingDestination, log);
                bridge = new RabbitMqCloudEventBridge(model, outcomeChannel, channel, queue, prefetchCount,
                        pollInterval, failureAction, readinessSource);
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
                    // stays defensive against that ordering changing later, and symmetric with the domain bridge's
                    // own build() failure path.
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
