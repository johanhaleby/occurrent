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
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.internal.BlockingHandover;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.HandoverOptions;
import org.occurrent.subscription.internal.ReplayFilters;

import java.time.Duration;
import java.util.Objects;
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
public class CatchupThenPushSubscriptionModel implements Subscribable {

    /**
     * The default number of recently delivered event ids retained to de-duplicate the replay-to-live overlap. Beyond
     * this window the at-least-once contract applies (the idempotent fold absorbs a duplicate).
     */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = HandoverOptions.DEFAULT_DEDUP_CACHE_SIZE;

    /**
     * The default cap on events buffered from the live feed during a catch-up replay before the model fails loud.
     */
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = HandoverOptions.DEFAULT_MAX_BUFFERED_EVENTS;

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final HandoverOptions options;

    /**
     * @param reader          Reads the projection's history in position order for the catch-up replay.
     * @param liveFeed        The live push feed the listener drives with {@code accept(...)}.
     * @param catchupMarker Records that the one-time catch-up finished so a restart skips it, or {@code null} to
     *                        catch up on every subscribe.
     */
    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker) {
        this(reader, liveFeed, catchupMarker, DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = new HandoverOptions(dedupCacheSize, maxBufferedEvents);
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");

        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        BlockingHandover<CloudEvent, CloudEvent> handover = BlockingHandover.create(
                action, CloudEvent::getId, action, CloudEvent::getId, options, "subscription");
        // Register on the live feed first, so any event that commits during the replay is captured (buffered) and not
        // lost in the gap between the replay head and going live.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), handover::accept);

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

    private record AlreadyStartedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            // The catch-up replay completes synchronously in subscribe before this handle is returned.
            return true;
        }
    }
}
