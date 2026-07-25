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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.internal.ReactiveHandover;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.HandoverOptions;
import org.occurrent.subscription.internal.ReplayFilters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/**
 * The reactive counterpart of the blocking {@code CatchupThenPushSubscriptionModel}: a one-time <strong>catch-up</strong>
 * in front of a reactive {@link PushSubscriptionModel}. On first subscribe it replays a projection's
 * history from the event store, then hands over to the live push feed, so a brand-new or rebuilt projection is
 * backfilled before it consumes the broker.
 * <p>
 * The replay, a catch-up-complete marker step, and the live feed are composed into one ordered pipeline with
 * {@link Flux#concat}: the replay is consumed first, then the marker is recorded, then the live feed. Live events that
 * arrive during the replay are buffered in a unicast sink until the pipeline reaches them, so nothing is lost across the
 * seam, and the overlap is de-duplicated by event id. Because the whole pipeline is serialized by {@code concatMap}, the
 * de-dup cache needs no locking.
 * <p>
 * Contract (see ADR 62 and the blocking model): catch-up is Occurrent's job and runs once per subscription id, guarded
 * by an optional {@link CheckpointStorage} marker so a restart skips it. Live-resume is the broker's job, so no live
 * position watermark is persisted and delivery is at-least-once over idempotent folds. A live event's {@code accept}
 * {@link Mono} completes only once its handler has run (including events buffered during the replay), so the listener
 * can acknowledge after processing. Only stream and capability-agnostic subscription filters can be replayed.
 * <p>
 * The catch-up-then-live coordination itself (the bounded live sink, the de-dup cache, and the
 * replay-then-marker-then-live pipeline shape) is delegated per-subscription to {@link ReactiveHandover}, shared with
 * {@code CatchupProjectionFeed}.
 */
@NullMarked
public class CatchupThenPushSubscriptionModel implements Subscribable {

    /** @see org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = HandoverOptions.DEFAULT_DEDUP_CACHE_SIZE;
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = HandoverOptions.DEFAULT_MAX_BUFFERED_EVENTS;

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final HandoverOptions options;

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
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        ReactiveHandover<CloudEvent, CloudEvent> handover = ReactiveHandover.create(
                action, CloudEvent::getId, action, CloudEvent::getId, options);

        // Register on the live feed first, so events committing during the replay are buffered in the sink, not lost.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), handover::accept);

        Mono<Void> catchupDone = handover.catchUp(new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return CatchupThenPushSubscriptionModel.this.alreadyCaughtUp(subscriptionId);
            }

            @Override
            public Flux<CloudEvent> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning());
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return CatchupThenPushSubscriptionModel.this.markCaughtUp(subscriptionId);
            }
        });

        return new AlreadyStartedSubscription(subscriptionId, catchupDone);
    }

    private Mono<Boolean> alreadyCaughtUp(String subscriptionId) {
        return catchupMarker == null ? Mono.just(false) : catchupMarker.read(subscriptionId).hasElement();
    }

    private Mono<Void> markCaughtUp(String subscriptionId) {
        if (catchupMarker == null) {
            return Mono.empty();
        }
        // The stored position marks that the catch-up replay completed at this head, not a live resume watermark.
        return reader.currentPosition()
                .flatMap(head -> catchupMarker.save(subscriptionId, GlobalCheckpoint.of(head)))
                .then();
    }

    private record AlreadyStartedSubscription(String id, Mono<Void> started) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
