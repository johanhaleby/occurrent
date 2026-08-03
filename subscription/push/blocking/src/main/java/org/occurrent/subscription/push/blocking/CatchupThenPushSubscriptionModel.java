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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.ReplayFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A one-time <strong>catch-up</strong> in front of a {@link PushSubscriptionModel}: on first subscribe it
 * replays a projection's history from the event store, then hands over to the live push feed, so a brand-new or rebuilt
 * projection is backfilled before it starts consuming the broker. It exists because a broker is not a log, so the push
 * feed alone cannot backfill a projection that started after events were already written.
 * <p>
 * Contract (the "broker owns live-resume" model, see ADR 62):
 * <ul>
 *   <li><strong>Catch-up</strong> is Occurrent's job and runs once per subscription id. On subscribe this model
 *       registers on the live feed first and buffers, replays the store {@code position}-ordered up to the head at read
 *       time via {@link PositionOrderedReader}, then drains the buffer and goes live. An event that commits during the
 *       replay is delivered either by the replay or by the buffered feed, and the overlap is de-duplicated by event id
 *       (not by a position watermark: Occurrent positions can commit late and have permanent gaps, so a watermark would
 *       drop a late-committing low-position event, see ADR 62). Because buffering starts before the head is read, no
 *       reconcile pass is needed.</li>
 *   <li><strong>Live resume</strong> is the broker's job, not Occurrent's. After catch-up, the listener consumes the
 *       broker and acknowledges each message only once {@code accept(...)} returns, so an unprocessed event is
 *       redelivered by the broker. This model persists no live position watermark. Delivery is therefore at-least-once,
 *       so the projection fold must be idempotent, the same contract as the change-stream path. The "acknowledge after
 *       processing" guarantee holds for the live phase. During the catch-up window {@code accept(...)} buffers the event
 *       and returns before it is folded (the calling thread is not blocked for the whole replay), so a message may be
 *       acknowledged before it is applied. That is safe because the catch-up-complete marker is written only after the
 *       drain, so a crash mid-catch-up re-replays the whole history from the store, which is the backstop for any
 *       event acknowledged but not yet folded.</li>
 *   <li>A one-shot <strong>catch-up-complete marker</strong> (an optional {@link CheckpointStorage}) records that the
 *       replay finished, so a restart skips it and lets the broker resume. The stored value marks completion, it is not
 *       a live resume position. Correctness across a restart then depends on the broker retaining the backlog for an
 *       offline consumer (a durable queue with a preserved offset). If the marker is lost or absent, the projection is
 *       caught up again.</li>
 * </ul>
 * Only stream and capability-agnostic subscription filters can be replayed (their plain {@link Filter} drives the
 * position-ordered read). A DCB subscription filter is rejected, since a DCB boundary needs a different replay read.
 * <p>
 * The catch-up-then-live coordination itself (the buffer, the de-dup cache, and the drain-then-mark ordering) is
 * delegated per-subscription to {@link BlockingHandover}, shared with {@code CatchupProjectionFeed}.
 */
@NullMarked
public class CatchupThenPushSubscriptionModel implements SubscriptionModel {

    private static final Logger log = LoggerFactory.getLogger(CatchupThenPushSubscriptionModel.class);

    // Long enough that a replay noticing the shutdown at its next event always makes it, short enough that a parked
    // fold cannot hold a closing context open. Matches how SagaSubscription bounds its own poller shutdown.
    private static final Duration SHUTDOWN_REPLAY_TIMEOUT = Duration.ofSeconds(5);

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CatchupThenLiveOptions options;

    // Set by stop(), cleared by start(...). Read by the replay so stopping the model interrupts a replay in flight, not
    // just the live feed the replay has not handed over to yet.
    private volatile boolean stopped = false;
    private volatile boolean shuttingDown = false;
    // Subscriptions whose replay is running. The live feed cannot answer for them: it knows the id (this model
    // registers there first) but it is buffering rather than delivering, so it would report a subscription that is
    // not yet folding anything as running.
    private final ConcurrentMap<String, Future<Boolean>> replayingSubscriptions = new ConcurrentHashMap<>();
    // A pause asked for while a replay is in flight. The replay itself keeps running, since resuming it would mean
    // persisting the exact replay cursor, which this model does not do. Applied at the handover instead.
    private final ConcurrentMap<String, Boolean> pauseRequestedDuringReplay = new ConcurrentHashMap<>();

    /**
     * @param reader          Reads the projection's history in position order for the catch-up replay.
     * @param liveFeed        The live push feed the listener drives with {@code accept(...)}.
     * @param catchupMarker Records that the one-time catch-up finished so a restart skips it, or {@code null} to
     *                        catch up on every subscribe.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker) {
        this(reader, liveFeed, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    /**
     * @param options De-dup cache size and live-buffer cap for the catch-up-to-live handover.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!startAt.isDefault()) {
            throw new IllegalArgumentException(HandoverMessages.NON_DEFAULT_START_AT_NOT_SUPPORTED);
        }

        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        BlockingHandover<CloudEvent> handover = BlockingHandover.create(action, CloudEvent::getId, options, "subscription");
        // Register on the live feed first, so any event that commits during the replay is captured (buffered) and not
        // lost in the gap between the replay head and going live.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), handover::accept);
        // The task needs to name itself to forget(), so the entry it removes is its own rather than whatever holds
        // the id by then. Without that, a cancel followed by a re-subscribe of the same id lets this replay keep
        // going against the new subscription's entry and then delete it, silently killing the new subscription.
        AtomicReference<Future<Boolean>> self = new AtomicReference<>();
        BlockingHandover.Source<CloudEvent> source = new BlockingHandover.Source<>() {
            @Override
            public boolean isAlreadyCaughtUp() {
                return CatchupThenPushSubscriptionModel.this.isAlreadyCaughtUp(subscriptionId);
            }

            @Override
            public Stream<CloudEvent> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning());
            }

            @Override
            public boolean keepReplaying() {
                return shouldKeepReplaying(subscriptionId, self.get());
            }

            @Override
            public void markCaughtUp() {
                CatchupThenPushSubscriptionModel.this.markCaughtUp(subscriptionId);
            }
        };

        FutureTask<Boolean> replay = new FutureTask<>(() -> {
            final boolean caughtUp;
            try {
                caughtUp = handover.catchUp(source);
            } catch (RuntimeException | Error e) {
                // The handover was registered before the replay, so a failure here would otherwise leave a handler
                // that rethrows it for every later event while holding the id. Released here rather than in
                // waitUntilStarted so a caller that never waits still gets it, and one that does finds the id free.
                // Logged because under startupMode = BACKGROUND nobody waits, and the failure would otherwise reach
                // no one. Error is caught alongside RuntimeException because the handover records only the latter, so
                // something like a NoClassDefFoundError from the fold would leave the registration in place.
                log.error("Catch-up failed for subscription {}, releasing its registration on the live feed. It received "
                        + "no replay and will receive no live events until it is subscribed again.", subscriptionId, e);
                forget(subscriptionId, self.get());
                liveFeed.cancelSubscription(subscriptionId);
                throw e;
            }
            forget(subscriptionId, self.get());
            if (!caughtUp) {
                // Stopped rather than failed, so the handover is intact and nothing is marked. Release the
                // registration anyway: nothing will revive this replay, and leaving it registered would leave a
                // subscription that silently drops every live event. A fresh subscribe is the recovery, the same as
                // for the event-store catch-up model.
                liveFeed.cancelSubscription(subscriptionId);
                return false;
            }
            applyPendingPauseIfAny(subscriptionId);
            return true;
        });
        self.set(replay);
        // Registered before the thread starts, so isRunning(id) answers for it the moment subscribe returns rather than
        // whenever the replay thread happens to get scheduled.
        replayingSubscriptions.put(subscriptionId, replay);
        Thread.ofVirtual().name("occurrent-push-catchup-" + subscriptionId).start(replay);
        return new CatchingUpSubscription(subscriptionId, replay);
    }

    // Removes this replay's own entry, never one a later subscribe put there under the same id.
    private void forget(String subscriptionId, @Nullable Future<Boolean> replay) {
        if (replay != null) {
            replayingSubscriptions.remove(subscriptionId, replay);
        }
    }

    private boolean isAlreadyCaughtUp(String subscriptionId) {
        return catchupMarker != null && catchupMarker.exists(subscriptionId);
    }

    private void markCaughtUp(String subscriptionId) {
        if (catchupMarker != null) {
            // The stored position marks that the catch-up replay completed at this head, not a live resume watermark.
            catchupMarker.save(subscriptionId, GlobalCheckpoint.of(reader.currentPosition()));
        }
    }

    /**
     * Whether the replay for {@code subscriptionId} should keep going: the model is neither shutting down nor stopped,
     * and the subscription has not been cancelled out from under it.
     */
    private boolean shouldKeepReplaying(String subscriptionId, @Nullable Future<Boolean> replay) {
        return !shuttingDown && !stopped && replay != null && replayingSubscriptions.get(subscriptionId) == replay;
    }

    // Checked against the live feed rather than applied blindly. A stop landing between the last replayed event and
    // here already paused everything, and pausing again throws, which would report a catch-up that actually finished
    // as a failure.
    private void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null && liveFeed.isRunning(subscriptionId)) {
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    // --- Life cycle. The live feed owns delivery, so most of this is a fan-out; what this model adds is an answer for
    // the window where a replay is in flight, which the live feed cannot give because it is buffering rather than
    // delivering. ---

    @Override
    public void stop() {
        stopped = true;
        liveFeed.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        liveFeed.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return !replayingSubscriptions.isEmpty() || liveFeed.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return replayingSubscriptions.containsKey(subscriptionId) || liveFeed.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringReplay.containsKey(subscriptionId) || liveFeed.isPaused(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (replayingSubscriptions.containsKey(subscriptionId)) {
            // The live feed would accept the pause, but the replay does not go through it, so pausing there now would
            // report the subscription paused while its history keeps folding. Record it and apply it at the handover.
            pauseRequestedDuringReplay.put(subscriptionId, true);
        } else {
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null) {
            // Paused and resumed while its replay was still running, so the live feed was never told and has nothing to
            // resume. Dropping the request is the whole of it, but hand back a handle that still tracks the replay
            // rather than one that claims to be started.
            Future<Boolean> replay = replayingSubscriptions.get(subscriptionId);
            if (replay != null) {
                return new CatchingUpSubscription(subscriptionId, replay);
            }
            // The replay finished between dropping the request and looking it up, so whether the handover managed to
            // apply the pause first is a race. Resume only if it actually landed, since the live feed refuses to
            // resume a subscription it never paused.
            return liveFeed.isPaused(subscriptionId)
                    ? liveFeed.resumeSubscription(subscriptionId)
                    : new CatchingUpSubscription(subscriptionId, CompletableFuture.completedFuture(true));
        }
        return liveFeed.resumeSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        // Removing it here is what stops a replay in flight: shouldKeepReplaying reads this map.
        replayingSubscriptions.remove(subscriptionId);
        pauseRequestedDuringReplay.remove(subscriptionId);
        liveFeed.cancelSubscription(subscriptionId);
    }

    /**
     * Stops every replay still in flight and waits for them to unwind before shutting the live feed down.
     * <p>
     * The waiting is the point. A replay runs on its own thread, so without it a context that is closing would leave
     * one folding into a store that is closing with it, surfacing as an error from a thread nobody owns. A replay
     * notices the shutdown at its next event, so the wait is normally brief. It gives up after five seconds anyway,
     * because the fold is application code and may never return.
     */
    @Override
    public void shutdown() {
        shuttingDown = true;
        awaitReplays(SHUTDOWN_REPLAY_TIMEOUT);
        replayingSubscriptions.clear();
        pauseRequestedDuringReplay.clear();
        liveFeed.shutdown();
    }

    private void awaitReplays(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Future<Boolean> replay : replayingSubscriptions.values()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                replay.get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (TimeoutException e) {
                return;
            } catch (ExecutionException e) {
                // Already reported to whoever waited on this subscription, and a shutdown has nowhere useful to put it.
            }
        }
    }

    /**
     * A subscription whose catch-up is running on its own thread. {@code waitUntilStarted} is the only thing that joins
     * it, which is what lets a caller choose to keep the replay off the startup path.
     */
    private record CatchingUpSubscription(String id, Future<Boolean> replay) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
            try {
                // false when the replay was stopped rather than finished: not started, but not a failure either.
                return replay.get(safeTimeout.timeout(), safeTimeout.timeUnit());
            } catch (TimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                // Rethrown rather than reported as false, unlike the event-store catch-up's handle. A projection's
                // runner discards this return value, so swallowing a replay failure would start an application whose
                // read model is silently empty.
                switch (e.getCause()) {
                    case RuntimeException cause -> throw cause;
                    case Error cause -> throw cause;
                    case null, default -> throw new IllegalStateException("The catch-up for subscription '" + id + "' failed", e.getCause());
                }
            }
        }
    }
}
