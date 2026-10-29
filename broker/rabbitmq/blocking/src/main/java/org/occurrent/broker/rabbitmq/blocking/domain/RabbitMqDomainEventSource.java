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
import com.rabbitmq.client.ShutdownSignalException;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.broker.api.blocking.DestinationResolver;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqSourceException;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqBuildFailureClassifier;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventSource;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqCloudEventMapper;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqConsumerRecovery;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryFailureAction;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDeliveryWorker;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqDestination;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTeardown;
import org.occurrent.broker.rabbitmq.blocking.RabbitMqTopology;
import org.occurrent.dsl.projection.blocking.DomainEventFeed;
import org.occurrent.filter.Filter;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Feeds a RabbitMQ queue into a {@link DomainEventFeed}, the domain-level consume side ADR 133 decision 5
 * describes. Rebuilds each message as a {@link CloudEvent} through {@link RabbitMqCloudEventMapper} and calls
 * {@link DomainEventFeed#acceptOrDeferCloudEvent(CloudEvent)}, which is where the matching, the decoding and the delivery
 * all happen. This source does no filtering of its own, since the feed is the only thing that can decide per ADR 133
 * decision 5.
 * <p>
 * <strong>Acknowledgement</strong> follows the {@link RoutingOutcome} {@code acceptOrDeferCloudEvent(...)} returns, exactly
 * as {@link RabbitMqCloudEventSource} follows the one its own model reports, and by reading the same
 * {@link RoutingOutcome#disposition()} that source reads. {@link RoutingOutcome.Disposition#ACKNOWLEDGE}
 * acknowledges. {@link RoutingOutcome.Disposition#FAIL} and a thrown exception both apply this source's configured
 * {@link DeliveryFailurePolicy} instead, without acknowledging directly.
 * {@link RoutingOutcome.Disposition#HOLD} does not acknowledge either. That delivery is held unacknowledged for
 * this source's own poll to negatively acknowledge with requeue, at most once per
 * {@link Builder#pollInterval(Duration)}, rather than nacked on the spot, bypassing {@link DeliveryFailurePolicy}
 * entirely, since nothing here is broken, only not ready yet, and {@code PARK} exists for failures rather than for
 * pacing. {@link RoutingOutcome.Disposition#STOP} stops this
 * source for good, the same way {@link UnreadableLiveFilterException} below does.
 * <p>
 * <strong>{@link UnreadableLiveFilterException} is different, and permanent.</strong> It means the projection this
 * feed carries was registered with a {@code data} payload filter this feed has no {@link org.occurrent.filtermatching.DataFieldReader}
 * for, a configuration error that cannot change without a new registration, and the same exception instance is
 * thrown again on every later call. On catching it, this source logs the failure and calls {@link #stopPermanently()},
 * which cancels this source's own consumer, releases every other tag this source is still holding, and closes the
 * consume channel, all under one lock, in that order, see {@link #stopPermanently()}'s own javadoc. <strong>This
 * source never acknowledges or negatively acknowledges the triggering delivery itself</strong>, exactly as
 * {@link UnreadableLiveFilterException}'s own javadoc requires: it is not nacked (which would only redeliver it into
 * the same permanent failure, the retry loop this exists to avoid) and it is not parked (parking would still
 * acknowledge the original once the park is confirmed, and this must never acknowledge it at all). Closing the
 * channel does requeue it, RabbitMQ's own guarantee for a closed channel with an unacked delivery. That requeue is
 * not this source redelivering into its own refusal (its consumer is already cancelled, so nothing of this source's
 * can ever see it again), only what makes the event survive, visible for whoever consumes next once the
 * registration is fixed and restarted.
 * <p>
 * <strong>{@link Builder#build()} retries a broker briefly unreachable, per #867, on the {@code Connection} it was
 * given.</strong> Opening the channel, declaring the queue and its bindings, and setting QoS all happen inside
 * {@link Builder#retryStrategy(RetryStrategy)}, exponential backoff from 100 ms up to 2 seconds by default, ten
 * attempts in total, every one of them against that same supplied {@code Connection}. {@code build()} never
 * creates or reconnects the {@code Connection} itself, so this survives only a broker briefly unreachable while
 * that {@code Connection}'s own automatic recovery is what is reopening it. A {@code Connection} with automatic
 * recovery disabled, or a broker still unreachable when the {@code Connection} was created in the first place,
 * stays dead for every attempt. See that method's own javadoc for exactly what is retried and what is refused
 * immediately.
 * <p>
 * <strong>Each source handles its deliveries on a thread of its own.</strong> The RabbitMQ client calls every consumer
 * on a connection from one shared pool, so this source only hands a delivery over to its own worker thread there and
 * returns. A projection that blocks, waiting on a database that is down say, holds up only the source it belongs to,
 * never another source built on the same {@link Connection}. That one thread handles deliveries one at a time and in
 * the order the broker sent them, and about {@link Builder#prefetchCount(int)} of them wait for it. An {@code Error}
 * a projection throws, {@link AssertionError} aside, a checked exception it throws, which a projection written in
 * Kotlin has nothing to declare and nothing to stop it from doing, or an acknowledgement this source cannot issue
 * for a reason other than a connection recovery, stops this source and closes its channel, which puts the delivery
 * back on the queue.
 * <p>
 * <strong>Coarse lifecycle.</strong> A background poll, {@link Builder#pollInterval(Duration)} apart (one second by
 * default), reads {@link DomainEventFeed#hasProjection()} and {@link DomainEventFeed#isReadyForLiveDelivery()} and
 * starts or cancels this source's own AMQP consumer to match, consuming only once a projection is registered and its
 * catch-up-then-live transition has actually reached live, not merely started. This exists for the same reason
 * {@link RabbitMqCloudEventSource} polls {@code isRunning(...)}. Without the registration half, a message arriving
 * before the application registers its projection would hit {@code acceptOrDeferCloudEvent(...)}'s
 * {@link IllegalStateException} refusal on every delivery, and under {@link DeliveryFailurePolicy#REDELIVER} that is
 * an instant requeue-and-redeliver loop, not a wait. Without the readiness half, a message arriving before the
 * application calls {@code catchUpAll()}/{@code catchUp(...)} or {@code goLive(...)}, or while a
 * {@code catchUpAll()}/{@code catchUp(...)} replay is still actively running, would only ever buffer with nothing
 * behind it, which {@code acceptOrDeferCloudEvent(...)} answers with {@link RoutingOutcome#DEFERRED} rather than
 * {@link RoutingOutcome#DELIVERED} for exactly that reason (see its own javadoc): refused outright rather than
 * buffered, bypassing {@link DeliveryFailurePolicy} regardless of what this source is configured with, so
 * {@code PARK} can never fire for a message that only needs the replay to catch up. A {@code DEFERRED} delivery is
 * also held unacked rather than nacked immediately, the same mechanism {@code RabbitMqCloudEventSource} applies, so
 * with {@link Builder#prefetchCount(int)} left at its default of one, the broker sends nothing further on this
 * consumer until the next poll releases it, bounding a replay to at most one redelivery per
 * {@link Builder#pollInterval(Duration)} rather than saturating the channel for the whole replay. Feeding
 * {@code acceptOrDeferCloudEvent(...)} can still throw {@link IllegalStateException} despite the poll, for the narrow race where the check ran just before the one
 * registration this feed ever accepts. That case, unlike {@link UnreadableLiveFilterException}, applies the
 * configured {@link DeliveryFailurePolicy} like any other failure, since it is transient rather than permanent.
 * <p>
 * <strong>This source consumes nothing for the entire duration of a replay</strong>, which for a large event store
 * can be minutes, not the instant {@code catchUpAll()}/{@code catchUp(...)} is called. A message published during
 * that window queues up on the broker rather than being pulled off and held here, since nothing is consuming it
 * yet. That is harmless on the durable, unbounded queue this source declares by default when
 * {@link Builder#declareTopology(boolean) declareTopology(false)} is not set. Nothing there expires and nothing
 * is dropped for space, so every message is still on it once the replay reaches live. It stops being harmless the
 * moment the queue itself carries a bound, an {@code x-max-length} with a drop-oldest overflow policy, or an
 * {@code x-message-ttl}. Either can discard a message queued during a long replay before this source ever gets to
 * consume it, a consequence of that queue policy, not of anything this source does. Size a bound or a TTL against
 * how long a replay can run, or leave the queue unbounded, before pointing this source at it.
 * <p>
 * A message reaches this source's queue only if the queue and a matching binding exist when the message is
 * published, whether that is during a replay or after {@code goLive}. Build this source before calling
 * {@code catchUp}, {@code catchUpAll} or {@code goLive} on the feed, or with {@code declareTopology(false)} have
 * whoever owns the topology create them before then. The Topology paragraph of {@link RabbitMqCloudEventSource} says
 * what is lost otherwise.
 * <p>
 * <strong>A permanently failed catch-up stops this source, the same as {@link UnreadableLiveFilterException}.</strong>
 * Once the registered projection's own catch-up-then-live handover has failed, that failure never clears, so
 * {@link DomainEventFeed#acceptOrDeferCloudEvent(CloudEvent)} refuses every later event. This source asks the feed rather
 * than reading the exception's type. {@link DomainEventFeed#refusesPermanently()} is false until this feed's own
 * catch-up throws and true for good afterwards, so reading it inside the failure branch answers about this feed
 * and nothing else. A handler that reached into some other projection or subscription model whose catch-up has
 * failed throws the same type without this feed being broken at all, and that goes through
 * {@link DeliveryFailurePolicy} like any other handler failure.
 * <p>
 * On a permanent refusal this source logs at error once and calls {@link #stopPermanently()}, which cancels the
 * consumer, releases every tag this source is still holding (negatively acknowledged with
 * requeue) and closes the consume channel, all under one lock, in that order. Closing the channel also requeues the
 * triggering delivery itself, along with anything else still unacknowledged on it, so this source never has to
 * acknowledge that delivery tag by hand once a permanent stop has decided to close the channel out from under it.
 * Bypasses {@link DeliveryFailurePolicy} entirely the same way {@link RoutingOutcome#DEFERRED} already does, so
 * every message this permanent stop touches stays visibly on the original queue rather than being parked or committed
 * into the same permanent refusal.
 * <p>
 * <strong>A REDELIVER-policy failure is paced, the same as a {@code DEFERRED} delivery.</strong> Held and released
 * once per {@link Builder#pollInterval(Duration)}, through a second deque, rather than negatively acknowledged on
 * the spot, so a message that fails on every attempt (a handler that always throws, an event type this feed's
 * converter cannot decode) is bounded to one redelivery per {@code pollInterval} instead of nacking as fast as the
 * broker round-trips it. {@link DeliveryFailurePolicy#PARK} is unaffected: parking exists to move a failed delivery
 * out of the retry loop, not to pace it, so it still applies immediately.
 * <p>
 * <strong>A delivery tag from before an automatic connection recovery is acted on like any other.</strong> The
 * RabbitMQ client already refuses to act on one for you. Every channel a {@code Recoverable} connection hands out
 * offsets its own delivery tags past everything the channel it replaced ever issued, so {@code basicAck} and
 * {@code basicNack} send nothing at all for a tag from the dead channel. A connection that never recovers
 * automatically cannot produce such a tag either, since its channel stays dead and no further delivery arrives on
 * it.
 * <p>
 * <strong>A delivery finishing while the connection is down is left for RabbitMQ to deliver again.</strong> Its
 * acknowledgement cannot be sent, since the channel it would go on is closed, and RabbitMQ has put the delivery back on
 * the queue for the dropped connection. On a connection with automatic recovery this source logs that at {@code warn}
 * and keeps its channel, and the recovered channel delivers the message again once the recovery has registered the
 * consumer on it. With topology recovery turned on, which a {@code ConnectionFactory} has by default, the client
 * registers the consumer. With it turned off this source registers a new consumer on its first poll after the
 * recovery has finished, once the worker has no delivery left from the old one. Stopping instead would mean closing the
 * channel, which takes it out of the connection's recovery, so this source would never consume again while every other
 * source on the connection did. A connection without automatic recovery never comes back, so on one of those this
 * source stops for good, as it does for an {@link Error} a projection throws, an {@link AssertionError} aside, which
 * goes through {@link DeliveryFailurePolicy} like any other projection failure.
 * <p>
 * This source also stops for good when the broker closes its channel, after a delivery outlived the broker's
 * {@code consumer_timeout} say, since nothing reopens that channel.
 * <p>
 * Under {@link DeliveryFailurePolicy#PARK} that costs one duplicate. A delivery that fails while the connection is
 * recovering is published to the parking destination, and the acknowledgement that normally follows the park does
 * nothing, so RabbitMQ requeues the message as well. You end up with a parked copy and a copy still on the original
 * queue, which is the same at-least-once delivery this source gives you everywhere else. A delivery still waiting
 * for the worker thread when the connection drops is not handled at all. It is dropped as soon as the recovery
 * starts, since RabbitMQ delivers it again on the recovered channel.
 */
public final class RabbitMqDomainEventSource<E> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqDomainEventSource.class);

    private final DomainEventFeed<E> feed;
    private final Channel consumeChannel;
    private final String queue;
    private final int prefetchCount;
    private final Duration pollInterval;
    private final RabbitMqDeliveryFailureAction failureAction;
    private final ScheduledExecutorService scheduler;
    private final RabbitMqDeliveryWorker worker;
    private final Duration closeTimeout;

    private final Lock consumeLock = new ReentrantLock();
    private final AtomicReference<@Nullable String> consumerTag = new AtomicReference<>();
    private final RabbitMqConsumerRecovery consumerRecovery;

    // Appended to (never under consumeLock) by handleDelivery the instant a delivery reports DEFERRED, in place of
    // nacking it there and then, the same mechanism RabbitMqCloudEventSource uses. See that class's own javadoc on
    // its equivalent field for the full reasoning. In short: with prefetchCount == 1 (the default) leaving a
    // DEFERRED delivery unacked is what stops the broker sending anything further on this consumer, so the churn
    // stops at that instant with no cancel involved at all, and reconcileConsumption releases a snapshot of what
    // is held, under consumeLock, at most once per pollInterval. A deque rather than a single held tag, so a
    // source configured with prefetchCount above 1 never drops an earlier held tag under a later one, and a
    // failed release can push a tag back to the front rather than only ever appending to the back.
    private final Deque<Long> heldDeferredDeliveryTags = new ConcurrentLinkedDeque<>();
    // A REDELIVER-policy failure, held and released the same way heldDeferredDeliveryTags is (a snapshot per
    // pollInterval under consumeLock, via the same releaseHeldDeferredDelivery(Deque, LongConsumer) helper),
    // instead of nacked on the spot. Without this, REDELIVER (the default policy) nacks and gets redelivered as
    // fast as the broker round-trips it for a message that fails on every attempt, unlike DEFERRED, which is
    // already paced this way, and unlike Kafka's own failure path, which is paced through its seek-back-and-throttle
    // mechanism. PARK is never held here: parking exists to move a failed delivery out of the retry loop, not to
    // pace it, so it still applies immediately, through failureAction, from the point of failure.
    private final Deque<Long> heldFailedDeliveryTags = new ConcurrentLinkedDeque<>();
    private volatile boolean permanentlyStopped;
    // Raised by onChannelShutdown for every close of the channel that a connection recovery reopens. The client adds the
    // listener again to every channel a recovery opens, so this counts each drop, and the end of a recovery counts
    // nothing.
    private final AtomicLong connectionDrops = new AtomicLong();
    // A close of the channel that no recovery reopens. The next poll stops this source.
    private volatile @Nullable ShutdownSignalException closedForGood;
    // An IOException from a nack or an ack on a recovering connection, which a failed socket write throws and so does a
    // TrafficListener or MetricsCollector written in Kotlin. Judged by stopUnlessConnectionDropped. The time is the
    // oldest unresolved failure's, and the drop count is the one read before the newest unresolved failure's call. A
    // drop after that read requeued every delivery the failures were for. Under consumeLock.
    private boolean unexplainedFailure;
    private long unexplainedFailureNanos;
    private long dropsBeforeUnexplainedFailure;
    // When close() has to give up on a delivery, which is at this moment. Published before close() starts waiting and
    // read under consumeLock, so a projection that returns around the deadline either acknowledges before it or is
    // fenced by it, rather than racing a flag close() would set afterwards. A park or acknowledgement that already
    // holds the lock finishes, since the projection returned before it started. Long.MAX_VALUE until close() runs.
    private final AtomicLong closeDeadlineNanos = new AtomicLong(Long.MAX_VALUE);
    // Tracks whether this source has ever seen feed.isReadyForLiveDelivery() answer true, so reconcileConsumption
    // can tell a feed that has never gone live (still replaying, or nothing registered yet, both ordinary startup
    // states) apart from one that reached live and then stopped, which DomainEventFeed's own contract says only
    // ever happens for a permanently failed catch-up (see its isReadyForLiveDelivery() javadoc: false forever once
    // the handover has thrown, with no path back to true). Read and written only under consumeLock, inside
    // reconcileConsumption, so the ready-to-not-ready transition is never observed twice.
    private boolean everReadyForLiveDelivery;
    private boolean readinessFailureLogged;

    // Package-private rather than private so RabbitMqDomainEventSourceOutcomeRoutingTest can build one over a
    // mocked Channel. Nothing here talks to a broker, the builder's own start(..) does that.
    RabbitMqDomainEventSource(DomainEventFeed<E> feed, Channel consumeChannel, String queue, int prefetchCount,
                                       Duration pollInterval, RabbitMqDeliveryFailureAction failureAction, Duration closeTimeout) {
        this.feed = feed;
        this.consumeChannel = consumeChannel;
        this.queue = queue;
        this.prefetchCount = prefetchCount;
        this.pollInterval = pollInterval;
        this.failureAction = failureAction;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rabbitmq-domainevent-source-" + queue);
            thread.setDaemon(true);
            return thread;
        });
        this.worker = new RabbitMqDeliveryWorker("rabbitmq-domainevent-source-worker-" + queue, queue, log);
        this.consumerRecovery = new RabbitMqConsumerRecovery(consumerTag, queue, log);
        this.closeTimeout = closeTimeout;
        consumeChannel.addShutdownListener(this::onChannelShutdown);
    }

    // Runs on a thread of the client, so it only records the close for the poll to act on.
    private void onChannelShutdown(ShutdownSignalException cause) {
        if (failureAction.isLostToConnectionRecovery(cause)) {
            connectionDrops.incrementAndGet();
        } else {
            closedForGood = cause;
        }
    }

    /**
     * @param connection The connection this source creates its own {@link Channel} on for consuming, declaring
     *                   topology, and acknowledging.
     * @param feed       The feed this source calls {@link DomainEventFeed#acceptOrDeferCloudEvent(CloudEvent)} on.
     * @param queue      The queue this source consumes from, and declares unless {@link Builder#declareTopology(boolean)}
     *                   is set to {@code false}.
     */
    public static <E> Builder<E> builder(Connection connection, DomainEventFeed<E> feed, String queue) {
        return new Builder<>(connection, feed, queue);
    }

    private void start(Builder<E> builder, Set<RabbitMqDestination> destinations) {
        try {
            if (builder.declareTopology) {
                consumeChannel.queueDeclare(queue, true, false, false, null);
                for (RabbitMqDestination destination : destinations) {
                    consumeChannel.queueBind(queue, destination.exchange(), destination.routingKey());
                }
            }
            consumeChannel.basicQos(prefetchCount);
        } catch (IOException e) {
            throw new RabbitMqSourceException("Failed to declare topology for queue \"" + queue + "\"", e);
        }
        worker.discardOnRecovery(consumeChannel);
        consumerRecovery.attachTo(builder.connection, consumeChannel);
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
                if (permanentlyStopped || stopIfClosedForGood() || stopUnlessConnectionDropped()) {
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
                            "leaves it, so this is a permanently failed catch-up, not a pause. This source has " +
                            "stopped consuming for good.", queue);
                }
                boolean shouldConsume = feed.hasProjection() && readyForLiveDelivery;
                String tag = consumerTag.get();
                if (!shouldConsume && tag != null) {
                    consumerRecovery.retire(tag);
                }
                try {
                    // Waits for the worker to finish what an earlier consumer sent it, see RabbitMqDeliveryWorker#isIdle.
                    if (consumerRecovery.cancelRetired(consumeChannel) && shouldConsume && consumerTag.get() == null && worker.isIdle()) {
                        consumerRecovery.consume(() -> consumeChannel.basicConsume(queue, false, this::submitDelivery, this::handleCancel));
                    }
                } catch (Throwable e) {
                    // A cancel that keeps failing would otherwise leave a held delivery unanswered on every poll
                    new RabbitMqTeardown(e).run(this::releaseHeldDeliveriesOrStop);
                    throw e;
                }
                releaseHeldDeliveriesOrStop();
            } finally {
                consumeLock.unlock();
            }
        } catch (Throwable e) {
            // Throwable rather than the IOException and RuntimeException this used to name, because anything escaping
            // this method cancels the scheduled poll for good. The source would never start or cancel a consumer
            // again, and a held delivery would sit unacknowledged with nothing left to release it, all without a word.
            // What reaches that here is an Error out of anything called below, since the feed is Occurrent's own and
            // throws no checked exception the way the CloudEvent source's readinessSource can. An OutOfMemoryError is
            // swallowed along with the rest, the same as the saga timer poll's own wrapper, since keeping the poll
            // alive is worth more than passing a failure on to a scheduled task that does nothing with it.
            log.warn("Failed to reconcile consumption for queue \"{}\" against the feed's registration state. " +
                    "Retrying on the next poll.", queue, e);
        }
    }

    // Under consumeLock. A release that may have sent its nack can't be retried, since a second nack for a tag makes the
    // broker close the channel, and can't be left either, since at the default prefetchCount of one the broker sends
    // nothing further. Closing the channel requeues it.
    private void releaseHeldDeliveriesOrStop() {
        long dropsBeforeRelease = connectionDrops.get();
        try {
            releaseHeldDeferredDelivery();
            releaseHeldFailedDelivery();
        } catch (Throwable e) {
            boolean lostToConnectionRecovery = failureAction.isLostToConnectionRecovery(e);
            if (nackNotSent(e) && lostToConnectionRecovery) {
                throw e;
            }
            if (lostToConnectionRecovery) {
                log.warn("Releasing a held delivery on queue \"{}\" failed with an IOException, which a dropped connection "
                        + "throws. Unless the connection drops within a poll interval, this source then stops and closes the "
                        + "channel, which puts that delivery back on the queue.", queue, e);
                awaitConnectionDrop(dropsBeforeRelease);
                return;
            }
            if (nackNotSent(e)) {
                log.error("Releasing a held delivery on queue \"{}\" failed because its channel is closed, and the "
                        + "connection is not going to recover it. Stopping this source. Closing the channel put that "
                        + "delivery back on the queue.", queue, e);
            } else {
                log.error("Releasing a held delivery on queue \"{}\" failed, and the negative acknowledgement may or may "
                        + "not have been sent. Stopping this source and closing its channel, which puts that delivery back "
                        + "on the queue.", queue, e);
            }
            stopPermanently();
        }
    }

    // Under consumeLock. dropsBeforeCall is read after the delivery arrived and before the call that failed, so a drop
    // counted since then came after the delivery, and the broker requeued it. A later failure keeps the time of the
    // first, so a stream of failures can't put the judgement off.
    private void awaitConnectionDrop(long dropsBeforeCall) {
        if (!unexplainedFailure) {
            unexplainedFailure = true;
            unexplainedFailureNanos = System.nanoTime();
            dropsBeforeUnexplainedFailure = dropsBeforeCall;
        }
        dropsBeforeUnexplainedFailure = Math.max(dropsBeforeUnexplainedFailure, dropsBeforeCall);
    }

    // Under consumeLock. A failed socket write on a recovering connection shuts the connection down within
    // milliseconds, and the broker requeues every unacknowledged delivery on it. No drop a full poll interval after the
    // oldest failure, counted since the newest failure's call, means an IOException came from somewhere else, and a
    // delivery may sit unanswered. Stopping closes the channel, which requeues it. Returns whether this source stopped.
    private boolean stopUnlessConnectionDropped() {
        if (!unexplainedFailure || System.nanoTime() - unexplainedFailureNanos < pollInterval.toNanos()) {
            return false;
        }
        unexplainedFailure = false;
        if (connectionDrops.get() != dropsBeforeUnexplainedFailure) {
            return false;
        }
        log.error("A negative acknowledgement or an acknowledgement on queue \"{}\" failed with an IOException, and the "
                + "connection under its channel did not drop, so the frame may or may not have been sent. Stopping this "
                + "source and closing its channel, which puts that delivery back on the queue.", queue);
        stopPermanently();
        return true;
    }

    // Under consumeLock. The broker closes a channel after a channel error, such as 406 PRECONDITION_FAILED when a
    // delivery outlives consumer_timeout, and a connection without automatic recovery closes its channels for good.
    // With nothing held, no release or acknowledgement would notice, and this source would stay silent and consume
    // nothing. Returns whether this source stopped.
    private boolean stopIfClosedForGood() {
        ShutdownSignalException cause = closedForGood;
        if (cause == null) {
            return false;
        }
        log.error("The channel of queue \"{}\" was closed, and the connection is not going to reopen it. Stopping this "
                + "source. Closing the channel put every delivery this source had not acknowledged back on the queue.",
                queue, cause);
        stopPermanently();
        return true;
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
    // A nack that failed on a closed channel never drops the tag it was for. It goes back to the front, ahead of
    // whatever this pass has not gotten to yet, and this pass stops rather than trying the next slot. The next poll
    // retries it while the connection recovers, and stops this source otherwise. Any other failure may come before the
    // frame is written, from an application's TrafficListener, or after it, from its MetricsCollector, an IOException
    // that basicNack declares included. The tag is not put back, see releaseHeldDeliveriesOrStop.
    private void releaseHeldDeferredDelivery() {
        releaseHeldDeferredDelivery(heldDeferredDeliveryTags, failureAction::redeliver);
    }

    // Releases a REDELIVER-policy failure paced behind heldFailedDeliveryTags, the same mechanism and the same
    // once-per-poll bound as releaseHeldDeferredDelivery() above, see that field's own javadoc.
    // failureAction.redeliverFailure, not the plain redeliver() heldDeferredDeliveryTags releases through: this
    // deque holds a genuine failure, and redeliverFailure logs the one warn line that failure needs, where
    // heldDeferredDeliveryTags' own pacing release (DEFERRED) is not a failure and stays silent.
    private void releaseHeldFailedDelivery() {
        releaseHeldDeferredDelivery(heldFailedDeliveryTags, failureAction::redeliverFailure);
    }

    // Package-private and static, parameterized on the deque and the release call, so this source's redelivery
    // bookkeeping is directly testable with a stub that throws on demand, no real Channel or broker required.
    // Always called under consumeLock.
    static void releaseHeldDeferredDelivery(Deque<Long> heldDeferredDeliveryTags, LongConsumer redeliver) {
        int snapshotCount = heldDeferredDeliveryTags.size();
        for (int i = 0; i < snapshotCount; i++) {
            Long heldDeliveryTag = heldDeferredDeliveryTags.pollFirst();
            if (heldDeliveryTag == null) {
                return;
            }
            try {
                redeliver.accept(heldDeliveryTag);
            } catch (RuntimeException e) {
                if (nackNotSent(e)) {
                    // Without it nothing would nack the tag, and at the default prefetchCount of one the broker sends
                    // the consumer nothing further.
                    heldDeferredDeliveryTags.offerFirst(heldDeliveryTag);
                }
                // Anything else may have been thrown after the nack was sent, and a second nack for the same tag
                // makes the broker close the channel, so that tag is not put back.
                throw e;
            }
        }
    }

    // How a nack that was not sent fails. AMQChannel.transmit throws it from ensureIsOpen before it writes anything.
    // An IOException, which failureAction wraps in RabbitMqSourceException, doesn't show that. ChannelN.basicNack
    // calls the application's MetricsCollector after it writes the frame, and one written in Kotlin can throw a
    // checked IOException there.
    static boolean nackNotSent(Throwable failure) {
        return failure instanceof ShutdownSignalException;
    }

    private void handleCancel(String cancelledConsumerTag) {
        consumeLock.lock();
        try {
            // The client keeps its record of a consumer the broker cancelled, and a recovery with topology recovery on
            // would register it again, so it is retired for a cancel that removes the record
            consumerRecovery.retire(cancelledConsumerTag);
        } finally {
            consumeLock.unlock();
        }
    }

    // Runs on the client's shared consumer thread, so it only hands the delivery to this source's own worker.
    private void submitDelivery(String deliveryConsumerTag, Delivery delivery) {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        worker.submit(deliveryTag, () -> handleDeliveryOrStop(delivery, deliveryTag));
    }

    // handleDelivery routes a projection's own RuntimeException and AssertionError through the delivery failure policy,
    // so anything escaping it is an Error the projection threw, a checked exception it threw, or an acknowledgement
    // this source could not issue. Throwable rather than the exception types javac allows here, because a projection
    // written in Kotlin has no checked exception to declare and throws one straight through the Java interface this
    // source calls it behind. An acknowledgement that failed because the connection dropped and is being recovered does
    // not stop this source, since RabbitMQ puts the delivery back on the queue when the connection drops, and closing
    // this channel would take it out of the connection's recovery so the source never consumed again. Anything else
    // stops this source and closes the channel, which is what puts the delivery back on the queue. Without that the
    // delivery would sit unacknowledged on a consumer the broker sends nothing further to.
    // A failure after this source has already stopped does not stop it again. It is logged at debug when the
    // projection was interrupted or close() has stopped waiting for it, and at warn otherwise, since nothing else
    // reports it then. An interrupt is passed on to the thread. Not rethrown, since rethrowing would only end the
    // worker thread.
    private void handleDeliveryOrStop(Delivery delivery, long deliveryTag) {
        // Before the acknowledgement, and after the delivery arrived, see awaitConnectionDrop
        long dropsBeforeAcknowledging = connectionDrops.get();
        try {
            handleDelivery(delivery);
        } catch (Throwable e) {
            if (permanentlyStopped) {
                boolean interrupted = e instanceof InterruptedException || Thread.currentThread().isInterrupted();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (interrupted || closeDeadlinePassed()) {
                    log.debug("Handling delivery tag {} on queue \"{}\" failed outside this source's delivery failure "
                            + "policy after close() stopped waiting for it.", deliveryTag, queue, e);
                } else {
                    log.warn("Handling delivery tag {} on queue \"{}\" failed outside this source's delivery failure "
                            + "policy after this source had stopped. Closing its channel puts that delivery back on the "
                            + "queue.", deliveryTag, queue, e);
                }
                return;
            }
            if (failureAction.isLostToConnectionRecovery(e) && e instanceof ShutdownSignalException) {
                log.warn("The connection under queue \"{}\" dropped before delivery tag {} could be acknowledged or "
                        + "rejected. RabbitMQ puts it back on the queue, and this source receives it again once a "
                        + "consumer is registered on the recovered channel.",
                        queue, deliveryTag, e);
                return;
            }
            if (failureAction.isLostToConnectionRecovery(e)) {
                log.warn("Acknowledging or rejecting delivery tag {} on queue \"{}\" failed with an IOException, which a "
                        + "dropped connection throws. Unless the connection drops within a poll interval, this source then "
                        + "stops and closes the channel, which puts that delivery back on the queue.",
                        deliveryTag, queue, e);
                consumeLock.lock();
                try {
                    awaitConnectionDrop(dropsBeforeAcknowledging);
                } finally {
                    consumeLock.unlock();
                }
                return;
            }
            log.error("Handling delivery tag {} on queue \"{}\" failed outside this source's delivery failure policy. "
                    + "Stopping this source and closing its channel, which puts that delivery back on the queue.",
                    deliveryTag, queue, e);
            stopPermanently();
        }
    }

    private void handleDelivery(Delivery delivery) {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        CloudEvent cloudEvent;
        try {
            cloudEvent = RabbitMqCloudEventMapper.toCloudEvent(delivery.getProperties(), delivery.getBody());
        } catch (RuntimeException e) {
            log.debug("Failed to rebuild a CloudEvent from a message on queue \"{}\", delivery tag {}.", queue, deliveryTag, e);
            routeFailure(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }

        RoutingOutcome outcome;
        try {
            outcome = feed.acceptOrDeferCloudEvent(cloudEvent);
        } catch (UnreadableLiveFilterException e) {
            log.error("The registration on queue \"{}\"'s feed has a data payload filter this feed cannot answer " +
                    "live. This is a permanent configuration error; stopping this source rather than redelivering " +
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
                        + "Stopping this source rather than parking or committing into the same refusal. Delivery "
                        + "tag {}, and every other tag this source is still holding, is requeued by the channel "
                        + "this permanent stop closes, so it stays visible on the queue until the registration's "
                        + "catch-up is fixed and restarted.", queue, deliveryTag, e);
                stopPermanently();
                return;
            }

            // Either the projection handler itself threw, or the narrow registeredProjection() race the class
            // javadoc describes (an IllegalStateException that is not an UnreadableLiveFilterException). Both are
            // ordinary failure-policy cases, unlike the permanent ones caught above. AssertionError is caught here
            // too, since the converter, the live matcher or the projection can throw one, and it belongs in the
            // failure policy like any other projection failure. Any other Error stops this source instead, see
            // handleDeliveryOrStop.
            log.debug("The projection registered on queue \"{}\"'s feed failed for delivery tag {}.", queue, deliveryTag, e);
            routeFailure(deliveryTag, delivery.getProperties(), delivery.getBody());
            return;
        }
        route(outcome, deliveryTag, delivery.getProperties(), delivery.getBody());
    }

    // Acts on one outcome and reports which of the four dispositions it acted on. Package-private, and returning
    // that disposition rather than nothing, so RabbitMqDomainEventSourceOutcomeRoutingTest can drive all six
    // outcomes through it with no broker behind it. A switch expression rather than a switch statement because
    // javac only checks an expression for exhaustiveness, so a statement here would compile clean with a
    // disposition missing.
    RoutingOutcome.Disposition route(RoutingOutcome outcome, long deliveryTag, BasicProperties properties, byte[] body) {
        return switch (outcome.disposition()) {
            case ACKNOWLEDGE -> {
                ackNow(deliveryTag);
                yield RoutingOutcome.Disposition.ACKNOWLEDGE;
            }
            case HOLD -> {
                // Held unacked rather than nacked here, deliberately. Never takes consumeLock here, see
                // heldDeferredDeliveryTags's own javadoc for the full reasoning. DomainEventFeed#acceptOrDeferCloudEvent
                // reports DEFERRED for this today, not UNAVAILABLE, per its own javadoc, and both share this
                // disposition either way.
                heldDeferredDeliveryTags.add(deliveryTag);
                yield RoutingOutcome.Disposition.HOLD;
            }
            case FAIL -> {
                // DomainEventFeed#acceptOrDeferCloudEvent does not report NOT_DELIVERABLE today, per its own javadoc, so
                // this branch is what a later one would reach rather than a path a shipped feed takes.
                log.debug("A message on queue \"{}\", delivery tag {}, could not be delivered. Routing it " +
                        "through the configured delivery failure policy.", queue, deliveryTag);
                routeFailure(deliveryTag, properties, body);
                yield RoutingOutcome.Disposition.FAIL;
            }
            case STOP -> {
                // Same permanent stop UnreadableLiveFilterException and a permanently failed catch-up get, for a
                // refusal reported as an outcome rather than thrown. Offering the message again gets the same
                // refusal, so the failure policy is the wrong place for it.
                log.error("A message on queue \"{}\", delivery tag {}, was permanently refused. Stopping this "
                        + "source rather than parking or committing into the same refusal. That tag, and every "
                        + "other tag this source is still holding, is requeued by the channel this permanent stop "
                        + "closes, so it stays visible on the queue until the refusing registration is fixed and "
                        + "restarted.", queue, deliveryTag);
                stopPermanently();
                yield RoutingOutcome.Disposition.STOP;
            }
        };
    }

    // Under consumeLock, the lock reconcileConsumption also holds across its own blocking Channel calls, so two
    // threads never talk to consumeChannel at once.
    private void ackNow(long deliveryTag) {
        consumeLock.lock();
        try {
            if (closeDeadlinePassed()) {
                return;
            }
            failureAction.ack(deliveryTag);
        } finally {
            consumeLock.unlock();
        }
    }

    // Routes a genuine failure (UnreadableLiveFilterException and a permanent catch-up refusal are both handled
    // separately above, never reaching here) to this source's configured DeliveryFailurePolicy. PARK applies
    // immediately, through failureAction, since parking exists to move a failed delivery out of the retry loop, not
    // to pace it. REDELIVER is paced instead, held and released once per poll exactly like a DEFERRED delivery, via
    // a second deque, so a message that fails on every attempt is bounded to one redelivery per pollInterval rather
    // than nacking as fast as the broker round-trips it. See heldFailedDeliveryTags's own javadoc.
    private void routeFailure(long deliveryTag, BasicProperties properties, byte[] body) {
        if (failureAction.policy() == DeliveryFailurePolicy.REDELIVER) {
            heldFailedDeliveryTags.add(deliveryTag);
        } else {
            consumeLock.lock();
            try {
                if (closeDeadlinePassed()) {
                    return;
                }
                failureAction.apply(deliveryTag, properties, body);
            } finally {
                consumeLock.unlock();
            }
        }
    }

    /**
     * Stops the worker from starting another delivery, cancels this source's own consumer, releases every tag this
     * source is still holding, and closes the channel, in that order, all under the same lock, then stops the coarse
     * poll for good. Closing the channel here, rather than leaving that to {@link #close()} as an earlier revision did,
     * is what keeps a permanent stop from leaving an already-held tag stuck. The poll this method also shuts down was
     * the only thing that would otherwise ever release it. Closing an already-closing or already-closed channel is
     * tolerated the same as {@link #close()} already tolerates it, since an application may still call {@link #close()}
     * afterward. The triggering delivery for {@link UnreadableLiveFilterException} or a permanently refusing feed is
     * never acknowledged or negatively acknowledged by hand. Closing the channel here requeues it anyway, RabbitMQ's
     * own guarantee for a closed channel with an unacked delivery on it, which is what
     * {@link UnreadableLiveFilterException}'s own javadoc already expects once this source's channel eventually closes.
     */
    private void stopPermanently() {
        consumeLock.lock();
        // Every step runs, also after one throws, since the channel close is what requeues a held delivery and the
        // poll and the worker still need stopping. The first failure is thrown with the later ones suppressed on it.
        new RabbitMqTeardown(null)
                // Set under the same lock reconcileConsumption() reads it under, so whichever of the two takes the
                // lock first decides the consumer's fate before the other reads the flag.
                .run(() -> permanentlyStopped = true)
                // Before the channel close requeues a delivery the client may still dispatch, like close() does
                .run(worker::stopAcceptingWork)
                .run(consumerRecovery::detach)
                .run(this::cancelConsumerWhileStopping)
                .run(this::releaseHeldIgnoringFailure)
                .run(this::closeChannelIgnoringFailure)
                .run(consumeLock::unlock)
                .run(scheduler::shutdown)
                .finish();
    }

    private void cancelConsumerWhileStopping() {
        String tag = consumerTag.getAndSet(null);
        if (tag != null) {
            try {
                consumeChannel.basicCancel(tag);
            } catch (IOException | RuntimeException e) {
                // RuntimeException too, since an already closed channel throws AlreadyClosedException
                log.warn("Failed to cancel the consumer on queue \"{}\" while stopping permanently.", queue, e);
            }
        }
    }

    // Best effort, the channel close that follows requeues whatever this could not release.
    private void releaseHeldIgnoringFailure() {
        try {
            releaseHeldDeferredDelivery();
        } catch (RuntimeException ignored) {
            // See above
        }
        try {
            releaseHeldFailedDelivery();
        } catch (RuntimeException ignored) {
            // See above
        }
    }

    // Best effort, mirroring close()'s own channel teardown. The channel is going away either way, and an
    // already-failed or already-closed channel has nothing further this source can do about it. RuntimeException
    // also catches ShutdownSignalException, which extends it.
    private void closeChannelIgnoringFailure() {
        try {
            consumeChannel.close();
        } catch (IOException | TimeoutException | RuntimeException ignored) {
            // See above.
        }
    }

    /**
     * Stops the background poll, cancels this source's consumer if it has one, waits up to
     * {@link Builder#closeTimeout(Duration)} for a delivery already being handled to finish, releases any
     * {@code DEFERRED} delivery still held unacked, and closes the {@link Channel} (and, with
     * {@link DeliveryFailurePolicy#PARK}, the parking sink) this source created. Does not close the
     * {@link Connection} it was built from. Never releases the one delivery {@code stopPermanently()} is holding for
     * {@link UnreadableLiveFilterException}, since that tag never enters the held-{@code DEFERRED} queue this
     * releases in the first place.
     * <p>
     * A delivery that arrived but was not yet being handled is not started, and a projection still running after
     * {@code closeTimeout} is interrupted and logged at {@code warn}. Neither was acknowledged, so closing the channel
     * puts both back on the queue and they are delivered again. Nothing acknowledges or parks a delivery once that
     * deadline has passed, so a projection finishing just after it has its delivery redelivered rather than committed.
     * The same holds from the moment this method stops waiting early, which happens when the calling thread is
     * interrupted or when a projection is closing its own source.
     * Called from inside a projection, this method does not wait for that projection, since it is the one calling.
     * <p>
     * A projection that ignores its interrupt keeps running after this returns, so whatever it writes is written
     * whenever it finishes, which for a source built again on the same queue in the same process can be after events
     * that later source has already handled. {@code closeTimeout} bounds how long this waits for a projection and for the lock,
     * not how long the method takes, since closing the channel and the parking sink happens after that deadline.
     * <p>
     * Releasing a held {@code DEFERRED} delivery here is belt and braces rather than load bearing: closing a
     * channel with an unacked delivery on it already requeues that delivery at the broker on its own, so skipping
     * this line would still leave nothing stuck. Doing it explicitly means a delivery this source is holding is
     * redelivered the instant this method runs rather than whenever the broker notices the channel is gone.
     */
    @Override
    public void close() {
        long deadline = System.nanoTime() + RabbitMqTeardown.timeoutNanos(closeTimeout);
        // Stays false when the wait throws, so a projection that may still be running is fenced and interrupted
        boolean[] workerFinished = {false};
        // Every step runs, also after one throws, like stopPermanently()'s. The first failure is thrown with the later
        // ones suppressed on it.
        new RabbitMqTeardown(null)
                .run(() -> bringCloseDeadlineForwardTo(deadline))
                .run(scheduler::shutdownNow)
                // Stops a poll that is already running from starting a new consumer while this waits for the worker.
                .run(() -> permanentlyStopped = true)
                // Before the cancel, since a delivery the client has already dispatched could otherwise reach an idle
                // worker and start a projection while this is closing.
                .run(worker::stopAcceptingWork)
                .run(consumerRecovery::detach)
                // Each step under consumeLock is skipped once closeTimeout has run out, since the worker can hold that
                // lock while a park waits for its confirm. Closing the channel cancels the consumer and requeues the rest.
                .run(() -> underLockBefore(deadline, this::cancelConsumerOnClose))
                .run(() -> workerFinished[0] = worker.stop(remainingUntil(deadline)))
                .run(() -> {
                    if (!workerFinished[0] || worker.isWorkerThread()) {
                        // The teardown goes ahead with a delivery still running, either because the wait ended early
                        // or because a projection is closing its own source, so fence the acknowledgement from here
                        // rather than at a deadline that may still be in the future.
                        bringCloseDeadlineForwardTo(System.nanoTime());
                    }
                })
                .run(() -> {
                    if (!workerFinished[0] && !worker.isWorkerThread()) {
                        worker.interruptRunningWork(closeTimeout);
                    }
                })
                .run(() -> underLockBefore(deadline, this::releaseHeldIgnoringFailure))
                .run(this::closeChannelIgnoringIoFailure)
                .run(failureAction::close)
                .finish();
    }

    private void cancelConsumerOnClose() {
        String tag = consumerTag.getAndSet(null);
        if (tag != null) {
            try {
                consumeChannel.basicCancel(tag);
            } catch (IOException ignored) {
                // Best effort, the channel is about to be closed either way
            }
        }
    }

    private void underLockBefore(long deadlineNanos, Runnable step) {
        if (lockBefore(deadlineNanos)) {
            try {
                step.run();
            } finally {
                consumeLock.unlock();
            }
        }
    }

    private void closeChannelIgnoringIoFailure() {
        try {
            consumeChannel.close();
        } catch (IOException | ShutdownSignalException | TimeoutException ignored) {
            // Best effort, mirroring RabbitMqCloudEventSink#close's own channel teardown.
        }
    }

    // Only ever moved earlier, so a second close() cannot push a deadline the first one has already passed back into
    // the future and let a projection that ignored its interrupt acknowledge after all.
    private void bringCloseDeadlineForwardTo(long deadlineNanos) {
        closeDeadlineNanos.accumulateAndGet(deadlineNanos,
                (current, candidate) -> current == Long.MAX_VALUE || candidate - current < 0 ? candidate : current);
    }

    private boolean closeDeadlinePassed() {
        long deadline = closeDeadlineNanos.get();
        return deadline != Long.MAX_VALUE && System.nanoTime() - deadline >= 0;
    }

    private boolean lockBefore(long deadlineNanos) {
        try {
            return consumeLock.tryLock(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Duration remainingUntil(long deadlineNanos) {
        return Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime()));
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
        private Duration closeTimeout = Duration.ofSeconds(30);
        private RetryStrategy retryStrategy;

        private Builder(Connection connection, DomainEventFeed<E> feed, String queue) {
            this.connection = requireNonNull(connection, "connection cannot be null");
            this.feed = requireNonNull(feed, DomainEventFeed.class.getSimpleName() + " cannot be null");
            this.queue = requireNonNull(queue, "queue cannot be null");
            this.retryStrategy = defaultRetryStrategy(queue);
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
         * The same narrowing for a plain {@link Filter}, wrapped in an {@link AgnosticSubscriptionFilter} so it
         * applies whichever event store the events came from. Pass a {@link SubscriptionFilter} instead when the
         * narrowing has to be stream-specific or DCB-specific.
         */
        public Builder<E> bindingFilter(Filter bindingFilter) {
            requireNonNull(bindingFilter, "bindingFilter cannot be null");
            return bindingFilter(AgnosticSubscriptionFilter.filter(bindingFilter));
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
         * Whether this source declares its queue and bindings at all. {@code true} by default. Set to {@code false}
         * for a deployment whose platform team owns the queue and its bindings itself, per #415. This source then
         * only consumes from {@code queue} and never calls {@code queueDeclare} or {@code queueBind}. With
         * {@code false}, building this source creates nothing, so whoever owns the topology must create the queue and
         * every binding before {@code catchUp}, {@code catchUpAll} or {@code goLive} is called on the feed. The
         * Topology paragraph of {@link RabbitMqCloudEventSource} says what is lost otherwise.
         */
        public Builder<E> declareTopology(boolean declareTopology) {
            this.declareTopology = declareTopology;
            return this;
        }

        /**
         * What this source does with a delivery it will not acknowledge. {@link DeliveryFailurePolicy#REDELIVER} by
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
         * How many unacknowledged deliveries this source allows itself at once. One by default. This source handles
         * its deliveries one at a time and in order on its own thread whatever this is set to, so raising it only
         * lets more of them wait in memory for that thread, which is also how many a connection recovery or a
         * {@link RabbitMqDomainEventSource#close()} can hand back to the queue to be delivered again.
         */
        public Builder<E> prefetchCount(int prefetchCount) {
            if (prefetchCount < 1) {
                throw new IllegalArgumentException("prefetchCount must be at least 1, was " + prefetchCount);
            }
            this.prefetchCount = prefetchCount;
            return this;
        }

        /**
         * How long {@link RabbitMqDomainEventSource#close()} waits for a delivery already being handled to finish.
         * Thirty seconds by default, the same as the Kafka sources. A projection still running after that is
         * interrupted, and its delivery goes back on the queue when the channel closes, so it is handled again. A
         * timeout longer than about 146 years counts as that long.
         */
        public Builder<E> closeTimeout(Duration closeTimeout) {
            requireNonNull(closeTimeout, "closeTimeout cannot be null");
            if (closeTimeout.isNegative()) {
                throw new IllegalArgumentException("closeTimeout cannot be negative, was " + closeTimeout);
            }
            this.closeTimeout = closeTimeout;
            return this;
        }

        /**
         * How a broker briefly unreachable while {@link #build()} runs is retried before it throws. This retries
         * only opening a channel, declaring topology, and setting QoS on the {@code Connection} this builder was
         * given. It neither creates nor reconnects that {@code Connection}, so surviving anything past the very
         * first attempt needs that {@code Connection}'s own automatic recovery already enabled. Exponential backoff from
         * 100 ms up to 2 seconds by default, ten attempts in total, matching the shape
         * {@code AGENTS.md} sets for every component that talks to an external store, capped at that count because a
         * {@link #build()} that never gives up turns a broker that is permanently misconfigured, a rejected
         * credential or a nonexistent vhost, into an application that hangs at startup with no diagnosis, which is
         * worse than the failure this retry exists to absorb. By default {@link #build()} logs each retried
         * attempt at {@code WARN} so a retrying startup is never mistaken for a hung one. A caller-supplied
         * {@link RetryStrategy} replaces that logging along with everything else this default configures. See
         * {@link RabbitMqBuildFailureClassifier} for exactly what is
         * retried and what is refused immediately, including under
         * {@link org.occurrent.broker.api.blocking.DeliveryFailurePolicy#PARK}, where the parking publisher's own
         * channel can fail too. Never retries the {@link IllegalStateException} a missing {@code resolver} or
         * {@code parkingDestination} throws above, since that failure is identical on every attempt regardless of
         * the broker's state, and never any other {@link RuntimeException}, since that is a bug this retry cannot
         * fix by trying again. Passing a {@link RetryStrategy} here replaces that classification too, so a caller
         * that wants a different bound or a wider retry configures its own.
         */
        public Builder<E> retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = requireNonNull(retryStrategy, RetryStrategy.class.getSimpleName() + " cannot be null");
            return this;
        }

        public RabbitMqDomainEventSource<E> build() {
            if (declareTopology && bindings == null && resolver == null) {
                throw new IllegalStateException("A resolver(...), or explicit bindings(...), is required unless declareTopology(false) is set");
            }
            if (deliveryFailurePolicy == DeliveryFailurePolicy.PARK && parkingDestination == null) {
                throw new IllegalStateException("A parkingDestination is required when onDeliveryFailure(PARK) is set");
            }
            Set<RabbitMqDestination> destinations = declareTopology
                    ? RabbitMqTopology.destinationsToBind(resolver, bindingFilter, bindings)
                    : Set.of();
            // retryStrategy wraps only this call, not the validation above: a failed validation throws the same
            // way on every attempt regardless of the broker's state, so retrying it would spend the whole backoff
            // window on a failure a retry can never fix. A failed attempt closes the local resources it opened,
            // the channel, the failure action, the scheduler, before rethrowing, but not a durable queue
            // declaration or binding it already completed on the broker. Redeclaring those on the next attempt
            // is a safe no-op, since queueDeclare and queueBind succeed again, unchanged, against a queue or
            // binding that already exists exactly as declared.
            return retryStrategy.execute(() -> buildOnce(destinations));
        }

        // Validated above, before opening anything: a failure past this point has a channel (and, under PARK, a
        // parking sink) already open, so every later failure path in this method closes what it opened rather
        // than leaking it.
        private RabbitMqDomainEventSource<E> buildOnce(Set<RabbitMqDestination> destinations) {
            Channel channel = openChannel(connection);
            RabbitMqDeliveryFailureAction failureAction = null;
            RabbitMqDomainEventSource<E> source = null;
            try {
                failureAction = RabbitMqDeliveryFailureAction.create(connection, channel, deliveryFailurePolicy, parkingDestination, log);
                source = new RabbitMqDomainEventSource<>(feed, channel, queue, prefetchCount, pollInterval, failureAction, closeTimeout);
                source.start(this, destinations);
                return source;
            } catch (RuntimeException e) {
                unwind(e, channel, failureAction, source);
                throw e;
            }
        }

        /**
         * See {@link RabbitMqBuildFailureClassifier} for the classification
         * and {@link #retryStrategy(RetryStrategy)} for the rest of this default. Takes {@code queue} explicitly
         * rather than reading the field: this runs from the constructor, before the field assignment it would
         * otherwise read completes.
         */
        private static RetryStrategy defaultRetryStrategy(String queue) {
            return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0)
                    .maxAttempts(10)
                    .retryIf(RabbitMqBuildFailureClassifier::isTransient)
                    .onRetryableError((info, throwable) -> log.warn(
                            "Attempt {} of {} to build the RabbitMQ domain event source for queue \"{}\" failed. Retrying in {}.",
                            info.getAttemptNumber(), info.getMaxAttempts(), queue, info.getBackoffBeforeNextRetryAttempt(), throwable));
        }

        private static Channel openChannel(Connection connection) {
            try {
                return connection.openChannel()
                        .orElseThrow(() -> new RabbitMqSourceException("No RabbitMQ channel number was available to create the source's channel on"));
            } catch (IOException e) {
                throw new RabbitMqSourceException("Failed to create the source's RabbitMQ channel", e);
            }
        }

        // A step that fails is added to the failure build() is already throwing as suppressed instead of replacing it.
        // The scheduler has no thread yet when start() fails, since scheduleWithFixedDelay(...) is the last thing it
        // does, but shutting it down stays defensive against that ordering changing.
        private void unwind(RuntimeException failure, Channel channel, @Nullable RabbitMqDeliveryFailureAction failureAction,
                            @Nullable RabbitMqDomainEventSource<E> source) {
            RabbitMqTeardown teardown = new RabbitMqTeardown(failure).run(() -> closeQuietly(channel));
            if (failureAction != null) {
                teardown.run(failureAction::close);
            }
            if (source != null) {
                teardown.run(source.scheduler::shutdownNow);
                teardown.run(source.worker::stopAcceptingWork);
                teardown.run(source.consumerRecovery::detach);
            }
        }

        // Best effort: build() is already failing for another reason by the time this runs, so a failure here is
        // logged rather than allowed to replace the failure the caller actually asked about.
        private static void closeQuietly(Channel channel) {
            try {
                channel.close();
            } catch (IOException | ShutdownSignalException | TimeoutException e) {
                log.warn("Failed to close the source's channel while unwinding a failed build().", e);
            }
        }
    }
}
