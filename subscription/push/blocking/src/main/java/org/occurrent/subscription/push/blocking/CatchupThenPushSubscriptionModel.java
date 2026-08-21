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
import org.occurrent.subscription.*;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.ReplayFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
public class CatchupThenPushSubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, ReplayAwareSubscriptions {

    private static final Logger log = LoggerFactory.getLogger(CatchupThenPushSubscriptionModel.class);

    // Long enough that a replay noticing the shutdown at its next event always makes it, short enough that a parked
    // fold cannot hold a closing context open. Matches how SagaSubscription bounds its own poller shutdown.
    private static final Duration SHUTDOWN_REPLAY_TIMEOUT = Duration.ofSeconds(5);

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CatchupThenLiveOptions options;
    private final @Nullable CheckpointWriteVersionSource writeVersionSource;

    // Set by stop(), cleared by start(...). Read by the replay so stopping the model interrupts a replay in flight, not
    // just the live feed the replay has not handed over to yet.
    private volatile boolean stopped = false;
    private volatile boolean shuttingDown = false;
    // Subscriptions whose replay is running. The live feed cannot answer for them: it knows the id (this model
    // registers there first) but it is buffering rather than delivering, so it would report a subscription that is
    // not yet folding anything as running.
    private final ConcurrentMap<String, Future<Boolean>> replayingSubscriptions = new ConcurrentHashMap<>();
    // Ids whose history read is done and whose buffered live events are being delivered. Kept beside
    // replayingSubscriptions rather than replacing its value, because isCatchingUp and isRunning both read that map
    // and neither changes here. Entries are removed by forget, alongside the replay entry itself.
    private final Set<String> reconcilingSubscriptions = ConcurrentHashMap.newKeySet();
    // A pause asked for while a replay is in flight. The replay itself keeps running, since resuming it would mean
    // persisting the exact replay cursor, which this model does not do. Applied at the handover instead.
    private final ConcurrentMap<String, Boolean> pauseRequestedDuringReplay = new ConcurrentHashMap<>();
    // How to launch a subscription's replay again, kept only while there is a replay worth launching. Removed once one
    // finishes (nothing left to replay), once one fails (it is refusing, not stopped, and restarting it would turn a
    // loud refusal into a restart loop), and on cancel or shutdown. What is left is exactly the replays a stop
    // interrupted, which start(true) and resumeSubscription bring back. Without this a stop during a replay was
    // permanent, because the replay is the only thing that reaches the handover (ADR 104).
    private final ConcurrentMap<String, Supplier<Future<Boolean>>> interruptibleReplays = new ConcurrentHashMap<>();

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
        this(reader, liveFeed, catchupMarker, options, null);
    }

    /**
     * @param catchupMarker      Records that the one-time catch-up finished so a restart skips it, or {@code null} to
     *                           catch up on every subscribe.
     * @param writeVersionSource Asked for a version before the one-shot marker write and every checkpoint write this
     *                           model makes. A version stamps the write {@code notOlderThan} it, an empty answer or
     *                           no source at all stamps it {@code any()}.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker,
                                            @Nullable CheckpointWriteVersionSource writeVersionSource) {
        this(reader, liveFeed, catchupMarker, CatchupThenLiveOptions.defaults(), writeVersionSource);
    }

    /**
     * @param options            De-dup cache size and live-buffer cap for the catch-up-to-live handover.
     * @param writeVersionSource Asked for a version before the one-shot marker write and every checkpoint write this
     *                           model makes. A version stamps the write {@code notOlderThan} it, an empty answer or
     *                           no source at all stamps it {@code any()}.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker,
                                            CatchupThenLiveOptions options, @Nullable CheckpointWriteVersionSource writeVersionSource) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
        this.writeVersionSource = writeVersionSource;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!startAt.isDefault()) {
            throw new UnsupportedStartAtException(startAt, HandoverMessages.NON_DEFAULT_START_AT_NOT_SUPPORTED);
        }

        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        BlockingHandover<CloudEvent> handover = BlockingHandover.create(action, CloudEvent::getId, options, "subscription");
        // Register on the live feed first, so any event that commits during the replay is captured (buffered) and not
        // lost in the gap between the replay head and going live.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), handover::accept);

        // Kept rather than launched once, so a replay a stop interrupts can be launched again over the same handover.
        // The handover has to be the same one: it holds the live buffer and the de-dup cache, so a second one would
        // replay into a projection that had already seen part of the history.
        Supplier<Future<Boolean>> launch = () -> launchReplay(subscriptionId, handover, replayFilter);
        interruptibleReplays.put(subscriptionId, launch);
        return new CatchingUpSubscription(subscriptionId, launch.get());
    }

    // Starts one replay for subscriptionId and returns its handle. Called by subscribe, and again by start(true) or
    // resumeSubscription for a replay that a stop interrupted.
    private Future<Boolean> launchReplay(String subscriptionId, BlockingHandover<CloudEvent> handover, Filter replayFilter) {
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

            @Override
            public void historyDone() {
                reconcilingSubscriptions.add(subscriptionId);
            }
        };

        FutureTask<Boolean> replay = new FutureTask<>(() -> {
            final boolean caughtUp;
            try {
                caughtUp = handover.catchUp(source);
            } catch (RuntimeException | Error e) {
                // The registration stays. The handover was registered before the replay and recorded this failure, so
                // every later live event is refused rather than acknowledged, and the broker keeps holding them
                // (ADR 104). Releasing it here used to be the point, and it was the wrong trade: it freed the id at
                // the cost of acknowledging every later event into a subscription that handled nothing. Recovery is
                // cancelSubscription(id), which frees both the id and the slot, followed by a fresh subscribe.
                // Only the replay entry is forgotten, so isCatchingUp(id) stops answering true for a replay that
                // ended, while isRunning(id) answers true for the registration that is now refusing.
                // Logged because under startupMode = BACKGROUND nobody waits, and the failure would otherwise reach
                // no one.
                log.error("Catch-up failed for subscription {}. Its registration on the live feed is kept and now "
                        + "refuses every event, so the source redelivers rather than losing them. Cancel the "
                        + "subscription and subscribe again once the cause is fixed.", subscriptionId, e);
                // Dropped before the replay entry, so a start(true) racing this never sees a launcher with no replay
                // running and relaunches a catch-up that failed.
                interruptibleReplays.remove(subscriptionId);
                forget(subscriptionId, self.get());
                throw e;
            }
            if (!caughtUp) {
                // Stopped rather than failed, so the handover is intact, nothing is marked, and the registration is
                // kept. The launcher is kept too, so start(true) replays the whole history again, which is the answer
                // CatchupProjectionFeed.stopCatchUp() already records (ADR 104). Live events in the meantime are
                // dropped rather than refused, per ADR 85: the operator stopped this, and the window closes at
                // start(). Forgetting the replay entry last is what makes "launcher present, nothing replaying" mean
                // stopped.
                forget(subscriptionId, self.get());
                return false;
            }
            interruptibleReplays.remove(subscriptionId);
            forget(subscriptionId, self.get());
            applyPendingPauseIfAny(subscriptionId);
            return true;
        });
        self.set(replay);
        // Registered before the thread starts, so isRunning(id) answers for it the moment subscribe returns rather than
        // whenever the replay thread happens to get scheduled.
        replayingSubscriptions.put(subscriptionId, replay);
        Thread.ofVirtual().name("occurrent-push-catchup-" + subscriptionId).start(replay);
        return replay;
    }

    // Relaunches the replay for subscriptionId if a stop interrupted it, and returns its handle, or null if there was
    // nothing to relaunch. A replay is restartable when its launcher survived and nothing is replaying under that id,
    // which is exactly the state a stop leaves behind.
    //
    // Synchronized because the check and the launch have to be one step. Two callers reaching this together (start and
    // resumeSubscription, or two starts) would otherwise both see nothing replaying and put two replays on one
    // handover, and the replay path folds every event without consulting the de-dup cache, so the history would be
    // applied twice. Lifecycle calls are rare enough that the lock costs nothing.
    private synchronized @Nullable Future<Boolean> relaunchInterruptedReplay(String subscriptionId) {
        Supplier<Future<Boolean>> launch = interruptibleReplays.get(subscriptionId);
        if (launch == null || replayingSubscriptions.containsKey(subscriptionId)) {
            return null;
        }
        // Unpaused here rather than by the caller, so a caller that loses the race above does not leave the
        // subscription unpaused and then try to resume it a second time, which the live feed refuses. A no-op under
        // start(true), which cleared every pause before calling this.
        if (liveFeed.isPaused(subscriptionId)) {
            liveFeed.resumeSubscription(subscriptionId);
        }
        return launch.get();
    }

    // Removes this replay's own entry, never one a later subscribe put there under the same id.
    private void forget(String subscriptionId, @Nullable Future<Boolean> replay) {
        if (replay != null) {
            replayingSubscriptions.remove(subscriptionId, replay);
            reconcilingSubscriptions.remove(subscriptionId);
        }
    }

    private boolean isAlreadyCaughtUp(String subscriptionId) {
        return catchupMarker != null && catchupMarker.exists(subscriptionId);
    }

    private void markCaughtUp(String subscriptionId) {
        if (catchupMarker != null) {
            // The stored position marks that the catch-up replay completed at this head, not a live resume watermark.
            catchupMarker.save(subscriptionId, GlobalCheckpoint.of(reader.currentPosition()), writeConditionFor(subscriptionId));
        }
    }

    // A version from writeVersionSource stamps notOlderThan. An empty answer or no source stamps any(). Always the
    // 3-arg save, never a choice between two.
    private CheckpointWriteCondition writeConditionFor(String subscriptionId) {
        if (writeVersionSource == null) {
            return CheckpointWriteCondition.any();
        }
        OptionalLong version = writeVersionSource.writeVersion(subscriptionId);
        return version.isPresent() ? CheckpointWriteCondition.notOlderThan(version.getAsLong()) : CheckpointWriteCondition.any();
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

    /**
     * Stops the live feed and any catch-up replay still in flight. Reversible: a stopped replay keeps its registration
     * on the live feed and is replayed from the beginning by {@link #start(boolean)}, because a stop is not a failure
     * and nothing was marked. That is the decision {@code CatchupProjectionFeed.stopCatchUp()} already records, ported
     * here rather than re-derived (ADR 104).
     * <p>
     * Live events fed while stopped are dropped rather than refused, the dropped-not-deferred contract every stopped
     * subscription model has (ADR 85). That is bounded here only because the stop is reversible: the window closes at
     * {@code start(..)}.
     */
    @Override
    public void stop() {
        stopped = true;
        liveFeed.stop();
    }

    /**
     * Starts the live feed and, when {@code resumeSubscriptionsAutomatically}, replays the history again for every
     * subscription whose catch-up {@link #stop()} interrupted.
     * <p>
     * A stop is not a failure, so nothing was marked and the replay starts from the beginning rather than from a
     * cursor this model does not keep. Under {@code start(false)} the interrupted replays are left for
     * {@link #resumeSubscription(String)} to pick up one at a time, which is what "do not resume subscriptions
     * automatically" has to mean for a subscription whose catch-up is the thing that was stopped.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        // Before the replays, so the registrations they hand over to are unpaused by the time one finishes.
        liveFeed.start(resumeSubscriptionsAutomatically);
        if (resumeSubscriptionsAutomatically) {
            // Skips an id already replaying, so a start() while a replay is in flight does not put a second replay on
            // the same handover.
            interruptibleReplays.keySet().forEach(this::relaunchInterruptedReplay);
        }
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
    public Set<String> subscriptionIds() {
        // A subscription is registered on the live feed before its replay is recorded, and the replay is only forgotten
        // when the live feed either keeps the registration or loses it too, so the live feed knows every id this model
        // knows. That is why this does not also read replayingSubscriptions, unlike isRunning.
        return liveFeed.subscriptionIds();
    }

    /**
     * Whether {@code subscriptionId} is still replaying history and has not yet handed over to the live feed. Here
     * {@link #isRunning(String)} is {@code true} throughout the replay, matching what an event-store catch-up model
     * reports, which is why the handover needs an answer of its own.
     */
    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return replayingSubscriptions.containsKey(subscriptionId);
    }

    /**
     * Whether the history read for {@code subscriptionId} is still running, rather than the live events buffered
     * while it ran being delivered. Those are handed over exactly once, since the feed that supplied them was already
     * told they were handled, so a recording projection has to treat them as live rather than as part of a replay.
     */
    @Override
    public boolean isReplayingHistory(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return replayingSubscriptions.containsKey(subscriptionId) && !reconcilingSubscriptions.contains(subscriptionId);
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
        Future<Boolean> relaunched = relaunchInterruptedReplay(subscriptionId);
        if (relaunched != null) {
            // Its catch-up was interrupted by a stop, so resuming it means replaying the history again, since this
            // model keeps no replay cursor to resume from.
            pauseRequestedDuringReplay.remove(subscriptionId);
            return new CatchingUpSubscription(subscriptionId, relaunched);
        }
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
        reconcilingSubscriptions.remove(subscriptionId);
        pauseRequestedDuringReplay.remove(subscriptionId);
        // A cancel is not a stop, so nothing is kept to launch again. This is also the recovery from a failed
        // catch-up: it frees the id and releases the registration that was refusing (ADR 104).
        interruptibleReplays.remove(subscriptionId);
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
        reconcilingSubscriptions.clear();
        pauseRequestedDuringReplay.clear();
        // Unlike stop(), a shutdown keeps nothing to launch again: it drops the registrations too.
        interruptibleReplays.clear();
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
     * <p>
     * It tracks the one replay it was created for. A replay that {@link #stop()} interrupted answers {@code false}
     * here and keeps answering {@code false} after {@link #start(boolean)} launches a fresh one, since this handle
     * cannot see it. Ask {@link #isCatchingUp(String)} or {@link #isRunning(String)} about a restarted replay, or take
     * the handle {@link #resumeSubscription(String)} hands back.
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
