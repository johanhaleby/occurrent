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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
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
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Bridges a RabbitMQ queue into a {@link DomainEventFeed}, the domain-level consume side ADR 133 decision 5
 * describes. Rebuilds each message as a {@link CloudEvent} through {@link RabbitMqCloudEventMapper} and calls
 * {@link DomainEventFeed#acceptCloudEvent(CloudEvent)}, which is where the matching, the decoding and the delivery
 * all happen. This bridge does no filtering of its own, since the feed is the only thing that can decide per ADR 133
 * decision 5.
 * <p>
 * <strong>Acknowledgement</strong> follows the {@link RoutingOutcome} {@code acceptCloudEvent(...)} returns, exactly
 * as {@link RabbitMqCloudEventBridge} follows the one its own model reports:
 * {@link RoutingOutcome#DELIVERED} or {@link RoutingOutcome#FILTERED} acknowledges, {@link RoutingOutcome#NOT_DELIVERABLE}
 * and a thrown exception both apply this bridge's configured {@link DeliveryFailurePolicy} instead, never
 * acknowledging directly. {@link RoutingOutcome#DEFERRED} also never acknowledges, but always negatively
 * acknowledges with requeue, bypassing {@link DeliveryFailurePolicy} entirely: nothing here is broken, only not
 * ready yet, and {@code PARK} exists for failures, not for pacing.
 * <p>
 * <strong>{@link UnreadableLiveFilterException} is different, and permanent.</strong> It means the projection this
 * feed carries was registered with a {@code data} payload filter this feed has no {@link org.occurrent.filtermatching.DataFieldReader}
 * for, a configuration error that cannot change without a new registration, and the same exception instance is
 * thrown again on every later call. On catching it, this bridge logs the failure and calls {@link #stopPermanently()},
 * which cancels this bridge's own consumer, releases every other tag this bridge is still holding, and closes the
 * consume channel, all under one lock, in that order, see {@link #stopPermanently()}'s own javadoc. <strong>This
 * bridge never acknowledges or negatively acknowledges the triggering delivery itself</strong>, exactly as
 * {@link UnreadableLiveFilterException}'s own javadoc requires: it is not nacked (which would only redeliver it into
 * the same permanent failure, the retry loop this exists to avoid) and it is not parked (parking would still
 * acknowledge the original once the park is confirmed, and this must never acknowledge it at all). Closing the
 * channel does requeue it, RabbitMQ's own guarantee for a closed channel with an unacked delivery. That requeue is
 * not this bridge redelivering into its own refusal (its consumer is already cancelled, so nothing of this bridge's
 * can ever see it again), only what makes the event survive, visible for whoever consumes next once the
 * registration is fixed and restarted.
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
 * behind it, which {@code acceptCloudEvent(...)} answers with {@link RoutingOutcome#DEFERRED} rather than
 * {@link RoutingOutcome#DELIVERED} for exactly that reason (see its own javadoc): refused outright rather than
 * buffered, bypassing {@link DeliveryFailurePolicy} regardless of what this bridge is configured with, so
 * {@code PARK} can never fire for a message that only needs the replay to catch up. A {@code DEFERRED} delivery is
 * also held unacked rather than nacked immediately, the same mechanism {@code RabbitMqCloudEventBridge} applies, so
 * with {@link Builder#prefetchCount(int)} left at its default of one, the broker sends nothing further on this
 * consumer until the next poll releases it, bounding a replay to at most one redelivery per
 * {@link Builder#pollInterval(Duration)} rather than saturating the channel for the whole replay. Feeding
 * {@code acceptCloudEvent(...)} can still throw {@link IllegalStateException} despite the poll, for the narrow race where the check ran just before the one
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
 * <p>
 * <strong>A permanently failed catch-up stops this bridge, the same as {@link UnreadableLiveFilterException}.</strong>
 * {@link DomainEventFeed#acceptCloudEvent(CloudEvent)} throws {@code BlockingHandover.PreDispatchRefusalException}
 * unwrapped, rather than reporting {@link RoutingOutcome#NOT_DELIVERABLE}, once the projection's catch-up-then-live
 * handover has permanently failed, since that failure never clears. This bridge catches that exception by type ahead
 * of the generic failure branch, logs at error once, and calls {@link #stopPermanently()}, which cancels the
 * consumer, releases every tag this bridge is still holding (generation-safely, negatively acknowledged with
 * requeue) and closes the consume channel, all under one lock, in that order. Closing the channel also requeues the
 * triggering delivery itself, along with anything else still unacknowledged on it, so this bridge never has to
 * acknowledge that delivery tag by hand once a permanent stop has decided to close the channel out from under it.
 * Bypasses {@link DeliveryFailurePolicy} entirely the same way {@link RoutingOutcome#DEFERRED} already does, so
 * every message this permanent stop touches stays visibly on the source queue rather than being parked or committed
 * into the same permanent refusal. {@code BlockingHandover} is an internal type. This bridge imports it anyway,
 * narrowly, for this one {@code catch}, since matching on the exception's message, or treating every
 * {@code NOT_DELIVERABLE}-shaped failure as potentially permanent, is both more fragile and slower to notice than
 * catching the type the engine itself already throws for exactly this.
 * <p>
 * <strong>A REDELIVER-policy failure is paced, the same as a {@code DEFERRED} delivery.</strong> Held and released
 * once per {@link Builder#pollInterval(Duration)}, through a second deque, rather than negatively acknowledged on
 * the spot, so a message that fails on every attempt (a handler that always throws, an event type this feed's
 * converter cannot decode) is bounded to one redelivery per {@code pollInterval} instead of nacking as fast as the
 * broker round-trips it. {@link DeliveryFailurePolicy#PARK} is unaffected: parking exists to move a failed delivery
 * out of the retry loop, not to pace it, so it still applies immediately.
 * <p>
 * <strong>A delivery tag is invalidated across an automatic connection recovery.</strong> {@code connection}'s
 * delivery tags restart at 1 on a fresh channel, so a tag captured before a recovery can silently identify a
 * completely different message afterward. This bridge tracks a generation counter, bumped under {@code consumeLock}
 * by a {@code RecoveryListener} on {@code connection} and by this bridge's own consumer shutdown callback, so the
 * bump can never interleave with a concurrent immediate acknowledgement, negative acknowledgement or park, each of
 * which re-checks the generation under that same lock immediately before acting. Both held-tag deques carry their
 * own tag's generation alongside it and are revalidated tag by tag at release time rather than cleared outright on
 * a bump, since a tag can be appended to either deque without {@code consumeLock}. A stale tag, wherever it is
 * found, is never acknowledged, negatively acknowledged or parked, logging at warn instead: the message is
 * redelivered by the broker regardless, once the dead channel's own requeue runs.
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

    // A held delivery tag together with the channelGeneration it was captured under. See RabbitMqCloudEventBridge's
    // identical record and channelGeneration's own javadoc below. Package-private, not private, so
    // releaseHeldDeferredDelivery(Deque, LongConsumer, long) stays directly testable, matching that method's own
    // visibility.
    record HeldDelivery(long deliveryTag, long generation) {
    }

    // Appended to (never under consumeLock) by handleDelivery the instant a delivery reports DEFERRED, in place of
    // nacking it there and then, the same mechanism RabbitMqCloudEventBridge uses. See that class's own javadoc on
    // its equivalent field for the full reasoning. In short: with prefetchCount == 1 (the default) leaving a
    // DEFERRED delivery unacked is what stops the broker sending anything further on this consumer, so the churn
    // stops at that instant with no cancel involved at all, and reconcileConsumption releases a snapshot of what
    // is held, under consumeLock, at most once per pollInterval. A deque rather than a single held tag, so a
    // bridge configured with prefetchCount above 1 never drops an earlier held tag under a later one, and a
    // failed release can push a tag back to the front rather than only ever appending to the back.
    private final Deque<HeldDelivery> heldDeferredDeliveryTags = new ConcurrentLinkedDeque<>();
    // A REDELIVER-policy failure, held and released the same way heldDeferredDeliveryTags is (a snapshot per
    // pollInterval under consumeLock, via the same releaseHeldDeferredDelivery(Deque, LongConsumer, long) helper),
    // instead of nacked on the spot. Without this, REDELIVER (the default policy) nacks and gets redelivered as
    // fast as the broker round-trips it for a message that fails on every attempt, unlike DEFERRED, which is
    // already paced this way, and unlike Kafka's own failure path, which is paced through its seek-back-and-throttle
    // mechanism. PARK is never held here: parking exists to move a failed delivery out of the retry loop, not to
    // pace it, so it still applies immediately, through failureAction, from the point of failure.
    private final Deque<HeldDelivery> heldFailedDeliveryTags = new ConcurrentLinkedDeque<>();
    private volatile boolean permanentlyStopped;
    // Tracks whether this bridge has ever seen feed.isReadyForLiveDelivery() answer true, so reconcileConsumption
    // can tell a feed that has never gone live (still replaying, or nothing registered yet, both ordinary startup
    // states) apart from one that reached live and then stopped, which DomainEventFeed's own contract says only
    // ever happens for a permanently failed catch-up (see its isReadyForLiveDelivery() javadoc: false forever once
    // the handover has thrown, with no path back to true). Read and written only under consumeLock, inside
    // reconcileConsumption, so the ready-to-not-ready transition is never observed twice.
    private boolean everReadyForLiveDelivery;
    private boolean readinessFailureLogged;
    // Bumped under consumeLock on every automatic connection recovery and every consumer shutdown on this bridge's
    // own channel, so a delivery tag captured before either event is never mistaken for a tag on the channel that
    // replaced it, and so the bump can never interleave with a concurrent immediate acknowledgement, negative
    // acknowledgement or park. See RabbitMqCloudEventBridge's identical field for the full reasoning.
    private final AtomicLong channelGeneration = new AtomicLong(0);

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
        if (builder.connection instanceof Recoverable recoverableConnection) {
            // Only a Recoverable connection (automatic recovery enabled, the client's default) ever needs this, see
            // RabbitMqCloudEventBridge's own identical registration for the full reasoning.
            recoverableConnection.addRecoveryListener(new RecoveryListener() {
                @Override
                public void handleRecovery(Recoverable recoverable) {
                    invalidateChannelGeneration();
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                }
            });
        }
        scheduler.scheduleWithFixedDelay(this::reconcileConsumption, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    // See channelGeneration's own javadoc, and RabbitMqCloudEventBridge's identical pair of methods. The bump
    // happens under consumeLock so it can never land between an immediate action's own generation check and the
    // AMQP call that check guards.
    private void invalidateChannelGeneration() {
        consumeLock.lock();
        try {
            channelGeneration.incrementAndGet();
        } finally {
            consumeLock.unlock();
        }
    }

    private void handleConsumerShutdown(String shutdownConsumerTag, ShutdownSignalException signal) {
        invalidateChannelGeneration();
    }

    private boolean isStaleGeneration(long deliveryGeneration) {
        return deliveryGeneration != channelGeneration.get();
    }

    private void logStaleGeneration(long deliveryTag) {
        log.warn("Delivery tag {} on queue \"{}\" belongs to a channel generation this bridge has since moved " +
                "past (an automatic connection recovery, or a consumer shutdown, happened while this delivery was " +
                "still in flight). Not acknowledging, negatively acknowledging or parking it; the broker already " +
                "redelivers it once the dead channel's own unacked deliveries are requeued.", deliveryTag, queue);
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
                boolean readyForLiveDelivery = feed.isReadyForLiveDelivery();
                if (readyForLiveDelivery) {
                    everReadyForLiveDelivery = true;
                } else if (everReadyForLiveDelivery && !readinessFailureLogged) {
                    // DomainEventFeed's own contract has no path back from ready to not-ready other than a
                    // permanently failed catch-up (see its isReadyForLiveDelivery() javadoc), so having been ready
                    // before and not being ready now can only mean that. Logged once, not on every poll.
                    readinessFailureLogged = true;
                    log.error("The projection registered on queue \"{}\"'s feed was ready for live delivery and " +
                            "is not anymore. Its catch-up-then-live handover has no way back to ready once it " +
                            "leaves it, so this is a permanently failed catch-up, not a pause. This bridge has " +
                            "stopped consuming for good.", queue);
                }
                boolean shouldConsume = feed.hasProjection() && readyForLiveDelivery;
                if (shouldConsume && consumerTag == null) {
                    consumerTag = consumeChannel.basicConsume(queue, false, this::handleDelivery, this::handleCancel, this::handleConsumerShutdown);
                } else if (!shouldConsume && consumerTag != null) {
                    consumeChannel.basicCancel(consumerTag);
                    consumerTag = null;
                }
                releaseHeldDeferredDelivery();
                releaseHeldFailedDelivery();
            } finally {
                consumeLock.unlock();
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Failed to reconcile consumption for queue \"{}\" against the feed's registration state. " +
                    "Retrying on the next poll.", queue, e);
        }
    }

    // Nacks (with requeue) a snapshot of what handleDelivery left held, if anything, bypassing DeliveryFailurePolicy
    // exactly as the DEFERRED branch there always has. Always called under consumeLock, since it issues a
    // blocking Channel call per held tag. With prefetchCount == 1 (the default), the broker sends nothing further
    // on this consumer between a delivery being held and this releasing it, so calling this once per pollInterval
    // is what bounds a DEFERRED delivery to at most one redelivery per poll interval by construction.
    //
    // The snapshot size is read once, up front, rather than draining until empty: a nack this loop issues can
    // cause an immediate redelivery whose DEFERRED handleDelivery call appends a fresh tag to this same deque
    // while this loop is still running, and draining until empty would nack that fresh tag too, in the same
    // pass, collapsing the at-most-one-per-poll-interval bound this exists to keep. A tag appended mid-loop waits
    // for the next poll instead.
    //
    // A failed nack (an IOException surfacing as RabbitMqBridgeException from basicNack) never drops the tag it
    // was for: it goes back to the front, ahead of whatever this pass has not gotten to yet, and this pass stops
    // rather than trying the next slot, since a channel failing to nack once is not going to succeed on a
    // different tag straight after. The next poll retries it. A channel that has actually died requeues
    // everything still unacked on it by itself regardless, so this is only for one that survives the failure.
    private void releaseHeldDeferredDelivery() {
        releaseHeldDeferredDelivery(heldDeferredDeliveryTags, failureAction::redeliver, channelGeneration.get());
    }

    // Releases a REDELIVER-policy failure paced behind heldFailedDeliveryTags, the same mechanism and the same
    // once-per-poll bound as releaseHeldDeferredDelivery() above, see that field's own javadoc.
    // failureAction.redeliverFailure, not the plain redeliver() heldDeferredDeliveryTags releases through: this
    // deque holds a genuine failure, and redeliverFailure logs the one warn line that failure needs, where
    // heldDeferredDeliveryTags' own pacing release (DEFERRED) is not a failure and stays silent.
    private void releaseHeldFailedDelivery() {
        releaseHeldDeferredDelivery(heldFailedDeliveryTags, failureAction::redeliverFailure, channelGeneration.get());
    }

    // Package-private and static, parameterized on the deque, the release call and the caller's current
    // channelGeneration, so this bridge's redelivery bookkeeping is directly testable with a stub that throws on
    // demand, no real Channel or broker required. A held tag whose own generation no longer matches
    // currentGeneration is dropped rather than redelivered: the channel it was captured on is already dead, and a
    // dead channel requeues everything it was holding unacked by itself. Always called under consumeLock, the same
    // lock a generation bump takes, so the generation this reads can never change mid-pass.
    static void releaseHeldDeferredDelivery(Deque<HeldDelivery> heldDeferredDeliveryTags, LongConsumer redeliver, long currentGeneration) {
        int snapshotCount = heldDeferredDeliveryTags.size();
        for (int i = 0; i < snapshotCount; i++) {
            HeldDelivery held = heldDeferredDeliveryTags.pollFirst();
            if (held == null) {
                return;
            }
            if (held.generation() != currentGeneration) {
                continue;
            }
            try {
                redeliver.accept(held.deliveryTag());
            } catch (RuntimeException e) {
                heldDeferredDeliveryTags.offerFirst(held);
                throw e;
            }
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
        // Captured once, at the start, and checked again right before every point below that would act on
        // deliveryTag. See channelGeneration's own javadoc and RabbitMqCloudEventBridge's identical field.
        long deliveryGeneration = channelGeneration.get();
        CloudEvent cloudEvent;
        try {
            cloudEvent = RabbitMqCloudEventMapper.toCloudEvent(delivery.getProperties(), delivery.getBody());
        } catch (RuntimeException e) {
            log.debug("Failed to rebuild a CloudEvent from a message on queue \"{}\", delivery tag {}.", queue, deliveryTag, e);
            routeFailure(deliveryTag, deliveryGeneration, delivery.getProperties(), delivery.getBody());
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
            if (feed.refusesPermanently()) {
                // The registered projection's own catch-up has permanently failed, so this refusal is never going
                // to clear (see the class javadoc). Permanent, exactly like UnreadableLiveFilterException above,
                // so stop rather than park or redeliver into the same refusal forever. Asked of the feed rather
                // than read off the exception type, because a handler that reached into some other permanently
                // failed engine throws the same type without this feed being broken at all.
                log.error("The projection registered on queue \"{}\"'s feed has a permanently failed catch-up. "
                        + "Stopping this bridge rather than parking or committing into the same refusal. Delivery "
                        + "tag {}, and every other tag this bridge is still holding, is requeued by the channel "
                        + "this permanent stop closes, so it stays visible on the queue until the registration's "
                        + "catch-up is fixed and restarted.", queue, deliveryTag, e);
                stopPermanently();
                return;
            }

            // Either the projection handler itself threw, or the narrow registeredProjection() race the class
            // javadoc describes (an IllegalStateException that is not an UnreadableLiveFilterException). Both are
            // ordinary failure-policy cases, unlike the permanent ones caught above. AssertionError is caught here
            // too, since the converter, the live matcher or the projection can throw one, and leaving it uncaught
            // would strand the delivery unacked at prefetch one. Any other Error still propagates.
            log.debug("The projection registered on queue \"{}\"'s feed failed for delivery tag {}.", queue, deliveryTag, e);
            routeFailure(deliveryTag, deliveryGeneration, delivery.getProperties(), delivery.getBody());
            return;
        }
        if (isStaleGeneration(deliveryGeneration)) {
            logStaleGeneration(deliveryTag);
            return;
        }
        if (outcome == RoutingOutcome.DELIVERED || outcome == RoutingOutcome.FILTERED) {
            ackNow(deliveryTag, deliveryGeneration);
        } else if (outcome == RoutingOutcome.DEFERRED) {
            // Held unacked rather than nacked here, deliberately. Never takes consumeLock here, see
            // heldDeferredDeliveryTags's own javadoc for the full reasoning.
            heldDeferredDeliveryTags.add(new HeldDelivery(deliveryTag, deliveryGeneration));
        } else {
            // Unreachable today: DomainEventFeed#acceptCloudEvent never returns NOT_DELIVERABLE (see its own
            // javadoc), it only ever returns FILTERED, DELIVERED or DEFERRED, or throws one of the two exceptions
            // caught above. Kept as a defensive fallback rather than an assertion, so a future outcome this bridge
            // does not yet know about still fails safe through the configured policy instead of silently landing
            // nowhere.
            log.debug("A message on queue \"{}\", delivery tag {}, reported an outcome this bridge does not " +
                    "recognize. Routing it as a failure.", queue, deliveryTag);
            routeFailure(deliveryTag, deliveryGeneration, delivery.getProperties(), delivery.getBody());
        }
    }

    // Acknowledges deliveryTag, but only once consumeLock confirms deliveryGeneration is still current: taking the
    // same lock a generation bump takes makes the check-then-act atomic. See channelGeneration's own javadoc.
    private void ackNow(long deliveryTag, long deliveryGeneration) {
        consumeLock.lock();
        try {
            if (isStaleGeneration(deliveryGeneration)) {
                logStaleGeneration(deliveryTag);
                return;
            }
            failureAction.ack(deliveryTag);
        } finally {
            consumeLock.unlock();
        }
    }

    // Routes a genuine failure (UnreadableLiveFilterException and a permanent catch-up refusal are both handled
    // separately above, never reaching here) to this bridge's configured DeliveryFailurePolicy. PARK applies
    // immediately, through failureAction, since parking exists to move a failed delivery out of the retry loop, not
    // to pace it. REDELIVER is paced instead, held and released once per poll exactly like a DEFERRED delivery, via
    // a second deque, so a message that fails on every attempt is bounded to one redelivery per pollInterval rather
    // than nacking as fast as the broker round-trips it. See heldFailedDeliveryTags's own javadoc.
    private void routeFailure(long deliveryTag, long deliveryGeneration, BasicProperties properties, byte[] body) {
        if (failureAction.policy() == DeliveryFailurePolicy.REDELIVER) {
            heldFailedDeliveryTags.add(new HeldDelivery(deliveryTag, deliveryGeneration));
        } else {
            consumeLock.lock();
            try {
                if (isStaleGeneration(deliveryGeneration)) {
                    logStaleGeneration(deliveryTag);
                    return;
                }
                failureAction.apply(deliveryTag, properties, body);
            } finally {
                consumeLock.unlock();
            }
        }
    }

    /**
     * Cancels this bridge's own consumer, releases every tag this bridge is still holding, and closes the channel,
     * in that order, all under the same lock, then stops the coarse poll for good. Closing the channel here, rather
     * than leaving that to {@link #close()} as an earlier revision did, is what keeps a permanent stop from leaving
     * an already-held tag stuck: the poll this method also shuts down was the only thing that would otherwise ever
     * release it. Closing an already-closing or already-closed channel is tolerated the same as {@link #close()}
     * already tolerates it, since an application may still call {@link #close()} afterward. The triggering delivery
     * for {@link UnreadableLiveFilterException} or {@code BlockingHandover.PreDispatchRefusalException} is never
     * acknowledged or negatively acknowledged by hand. Closing the channel here requeues it anyway, RabbitMQ's own
     * guarantee for a closed channel with an unacked delivery on it, which is what {@link UnreadableLiveFilterException}'s
     * own javadoc already expects once this bridge's channel eventually closes.
     */
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
                } catch (IOException | RuntimeException e) {
                    // RuntimeException too, not only IOException: an already-closed channel throws
                    // com.rabbitmq.client.AlreadyClosedException, unchecked, and letting it escape here would skip
                    // scheduler.shutdown() below, leaking the poll thread.
                    log.warn("Failed to cancel the consumer on queue \"{}\" while stopping permanently.", queue, e);
                }
                consumerTag = null;
            }
            try {
                releaseHeldDeferredDelivery();
            } catch (RuntimeException ignored) {
                // Best effort: the channel closes right after regardless, which requeues whatever this could not.
            }
            try {
                releaseHeldFailedDelivery();
            } catch (RuntimeException ignored) {
                // Best effort, same reasoning as releaseHeldDeferredDelivery() above.
            }
            try {
                consumeChannel.close();
            } catch (IOException | TimeoutException | RuntimeException ignored) {
                // Best effort, mirroring close()'s own channel teardown: the channel is going away either way.
                // RuntimeException also catches ShutdownSignalException, which extends it.
            }
        } finally {
            consumeLock.unlock();
        }
        scheduler.shutdown();
    }

    /**
     * Stops the background poll, cancels this bridge's consumer if it has one, releases any {@code DEFERRED}
     * delivery still held unacked, and closes the {@link Channel} (and, with {@link DeliveryFailurePolicy#PARK},
     * the parking sink) this bridge created. Does not close the {@link Connection} it was built from. Never
     * releases the one delivery {@code stopPermanently()} is holding for {@link UnreadableLiveFilterException},
     * since that tag never enters the held-{@code DEFERRED} queue this releases in the first place.
     * <p>
     * Releasing a held {@code DEFERRED} delivery here is belt and braces rather than load bearing: closing a
     * channel with an unacked delivery on it already requeues that delivery at the broker on its own, so skipping
     * this line would still leave nothing stuck. Doing it explicitly means a delivery this bridge is holding is
     * redelivered the instant this method runs rather than whenever the broker notices the channel is gone.
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
            try {
                releaseHeldDeferredDelivery();
            } catch (RuntimeException ignored) {
                // Best effort, matching basicCancel above: the channel is about to be closed either way, and
                // closing it requeues whatever is left held regardless.
            }
            try {
                releaseHeldFailedDelivery();
            } catch (RuntimeException ignored) {
                // Best effort, same reasoning as releaseHeldDeferredDelivery() above.
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
         * arrives. It must be at least as inclusive as the registered projection's own replay filter, or events the
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
         * for a deployment whose platform team owns the queue and its bindings itself, per #415. This bridge then
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
