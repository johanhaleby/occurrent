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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final ConcurrentMap<String, Boolean> replayingSubscriptions = new ConcurrentHashMap<>();
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
        // Marked before the replay begins, so isRunning(id) answers for it from the moment it is registered.
        replayingSubscriptions.put(subscriptionId, true);

        try {
            handover.catchUp(new BlockingHandover.Source<>() {
                @Override
                public boolean isAlreadyCaughtUp() {
                    return CatchupThenPushSubscriptionModel.this.isAlreadyCaughtUp(subscriptionId);
                }

                @Override
                public Stream<CloudEvent> replay() {
                    return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning());
                }

                @Override
                public void markCaughtUp() {
                    CatchupThenPushSubscriptionModel.this.markCaughtUp(subscriptionId);
                }
            });
        } catch (RuntimeException | Error e) {
            // The handover was registered before the replay, so a failure here would otherwise leave a handler that
            // rethrows the failure for every later event, taking the id with it and starving the handlers behind it.
            // Error is caught alongside RuntimeException and rethrown unchanged, because the handover only records a
            // RuntimeException as its stored failure. So something like a NoClassDefFoundError from a lazily loaded
            // class inside the fold would otherwise leave the registration in place AND leave the handover buffering
            // every live event until it overflows, surfacing far from the cause.
            replayingSubscriptions.remove(subscriptionId);
            pauseRequestedDuringReplay.remove(subscriptionId);
            liveFeed.cancelSubscription(subscriptionId);
            throw e;
        }
        replayingSubscriptions.remove(subscriptionId);
        applyPendingPauseIfAny(subscriptionId);
        return new AlreadyStartedSubscription(subscriptionId);
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
    private boolean shouldKeepReplaying(String subscriptionId) {
        return !shuttingDown && !stopped && replayingSubscriptions.containsKey(subscriptionId);
    }

    private void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null) {
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
            // Paused and resumed while its replay was still running, so the live feed was never told and has nothing
            // to resume. Dropping the request is the whole of it.
            return new AlreadyStartedSubscription(subscriptionId);
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

    @Override
    public void shutdown() {
        shuttingDown = true;
        replayingSubscriptions.clear();
        pauseRequestedDuringReplay.clear();
        liveFeed.shutdown();
    }

    private record AlreadyStartedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            // The catch-up replay completes synchronously in subscribe before this handle is returned.
            return true;
        }
    }
}
