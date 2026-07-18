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
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.BoundedIdCache;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A one-time <strong>bootstrap catch-up</strong> in front of a {@link PushSubscriptionModel}: on first subscribe it
 * replays a projection's history from the event store, then hands over to the live push feed, so a brand-new or rebuilt
 * projection is backfilled before it starts consuming the broker. It exists because a broker is not a log, so the push
 * feed alone cannot backfill a projection that started after events were already written.
 * <p>
 * Contract (the "broker owns live-resume" model, see ADR 62):
 * <ul>
 *   <li><strong>Bootstrap</strong> is Occurrent's job and runs once per subscription id. On subscribe this model
 *       registers on the live feed first and buffers, replays the store {@code position}-ordered up to the head at read
 *       time via {@link PositionOrderedReader}, then drains the buffer and goes live. An event that commits during the
 *       replay is delivered either by the replay or by the buffered feed, and the overlap is de-duplicated by event id
 *       (not by a position watermark: Occurrent positions can commit late and have permanent gaps, so a watermark would
 *       drop a late-committing low-position event, see ADR 62). Because buffering starts before the head is read, no
 *       reconcile pass is needed.</li>
 *   <li><strong>Live resume</strong> is the broker's job, not Occurrent's. After bootstrap, the listener consumes the
 *       broker and acknowledges each message only once {@code accept(...)} returns, so an unprocessed event is
 *       redelivered by the broker. This model persists no live position watermark. Delivery is therefore at-least-once,
 *       so the projection fold must be idempotent, the same contract as the change-stream path. The "acknowledge after
 *       processing" guarantee holds for the live phase. During the bootstrap window {@code accept(...)} buffers the event
 *       and returns before it is folded (the calling thread is not blocked for the whole replay), so a message may be
 *       acknowledged before it is applied. That is safe because the bootstrap-complete marker is written only after the
 *       drain, so a crash mid-bootstrap re-replays the whole history from the store, which is the backstop for any
 *       event acknowledged but not yet folded.</li>
 *   <li>A one-shot <strong>bootstrap-complete marker</strong> (an optional {@link CheckpointStorage}) records that the
 *       replay finished, so a restart skips it and lets the broker resume. The stored value marks completion, it is not
 *       a live resume position. Correctness across a restart then depends on the broker retaining the backlog for an
 *       offline consumer (a durable queue with a preserved offset). If the marker is lost or absent, the projection is
 *       bootstrapped again.</li>
 * </ul>
 * Only stream and capability-agnostic subscription filters can be replayed (their plain {@link Filter} drives the
 * position-ordered read). A DCB subscription filter is rejected, since a DCB boundary needs a different replay read.
 */
@NullMarked
public class ReplayThenPushSubscriptionModel implements Subscribable {

    /**
     * The default number of recently delivered event ids retained to de-duplicate the replay-to-live overlap. Beyond
     * this window the at-least-once contract applies (the idempotent fold absorbs a duplicate).
     */
    public static final int DEFAULT_DEDUP_CACHE_SIZE = 10_000;

    /**
     * The default cap on events buffered from the live feed during a bootstrap replay before the model fails loud.
     */
    public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100_000;

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage bootstrapMarker;
    private final int dedupCacheSize;
    private final int maxBufferedEvents;

    /**
     * @param reader          Reads the projection's history in position order for the bootstrap replay.
     * @param liveFeed        The live push feed the listener drives with {@code accept(...)}.
     * @param bootstrapMarker Records that the one-time bootstrap finished so a restart skips it, or {@code null} to
     *                        bootstrap on every subscribe.
     */
    public ReplayThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage bootstrapMarker) {
        this(reader, liveFeed, bootstrapMarker, DEFAULT_DEDUP_CACHE_SIZE, DEFAULT_MAX_BUFFERED_EVENTS);
    }

    public ReplayThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage bootstrapMarker, int dedupCacheSize, int maxBufferedEvents) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.bootstrapMarker = bootstrapMarker;
        if (dedupCacheSize <= 0) {
            throw new IllegalArgumentException("dedupCacheSize must be greater than zero");
        }
        if (maxBufferedEvents <= 0) {
            throw new IllegalArgumentException("maxBufferedEvents must be greater than zero");
        }
        this.dedupCacheSize = dedupCacheSize;
        this.maxBufferedEvents = maxBufferedEvents;
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");

        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = replayFilterFor(filter);

        Handover handover = new Handover(action, dedupCacheSize, maxBufferedEvents);
        // Register on the live feed first, so any event that commits during the replay is captured (buffered) and not
        // lost in the gap between the replay head and going live.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), handover::onLiveEvent);

        if (isAlreadyBootstrapped(subscriptionId)) {
            // The broker owns live-resume from here, so skip the replay and just start delivering the live feed.
            handover.drainBufferAndGoLive();
        } else {
            try (Stream<CloudEvent> history = reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning())) {
                history.forEach(handover::deliverReplayed);
            }
            handover.drainBufferAndGoLive();
            markBootstrapped(subscriptionId);
        }
        return new AlreadyStartedSubscription(subscriptionId);
    }

    private boolean isAlreadyBootstrapped(String subscriptionId) {
        return bootstrapMarker != null && bootstrapMarker.exists(subscriptionId);
    }

    private void markBootstrapped(String subscriptionId) {
        if (bootstrapMarker != null) {
            // The stored position marks that the bootstrap replay completed at this head, not a live resume watermark.
            bootstrapMarker.save(subscriptionId, GlobalCheckpoint.of(reader.currentPosition()));
        }
    }

    private static Filter replayFilterFor(@Nullable SubscriptionFilter filter) {
        return switch (filter) {
            case null -> Filter.all();
            case StreamSubscriptionFilter streamSubscriptionFilter -> streamSubscriptionFilter.filter();
            case AgnosticSubscriptionFilter agnosticSubscriptionFilter -> agnosticSubscriptionFilter.filter();
            default ->
                    throw new IllegalArgumentException("Cannot bootstrap-replay a " + filter.getClass().getSimpleName()
                            + ". Only a stream or capability-agnostic subscription filter can be replayed in position order.");
        };
    }

    /**
     * Per-subscription state machine coordinating the bootstrap replay (on the subscribe thread) with the live feed
     * (on the listener thread). While buffering, live events are queued; the drain delivers the queued events not
     * already seen in the replay and then flips to live delivery, all under one lock so no event is lost or reordered
     * across the seam.
     */
    private static final class Handover {
        private final Consumer<CloudEvent> action;
        private final int maxBufferedEvents;
        private final Object lock = new Object();
        private final Queue<CloudEvent> buffer = new ArrayDeque<>();
        private final BoundedIdCache deliveredIds;
        private boolean live = false;

        private Handover(Consumer<CloudEvent> action, int dedupCacheSize, int maxBufferedEvents) {
            this.action = action;
            this.maxBufferedEvents = maxBufferedEvents;
            this.deliveredIds = new BoundedIdCache(dedupCacheSize);
        }

        // Called on the subscribe thread during the replay. The fold runs outside the lock (the live feed only buffers
        // meanwhile), then the id is recorded so the drain can de-duplicate a live copy of the same event.
        void deliverReplayed(CloudEvent cloudEvent) {
            action.accept(cloudEvent);
            synchronized (lock) {
                deliveredIds.add(cloudEvent.getId());
            }
        }

        // Called on the listener thread for every live event. Buffered until the handover completes, then delivered.
        void onLiveEvent(CloudEvent cloudEvent) {
            synchronized (lock) {
                if (live) {
                    deliverLive(cloudEvent);
                    return;
                }
                if (buffer.size() >= maxBufferedEvents) {
                    throw new IllegalStateException("Live event buffer overflowed during bootstrap replay (cap "
                            + maxBufferedEvents + "). The history is too large to buffer the live feed across a full replay. "
                            + "Rebuild offline from the event store instead of bootstrapping over a live feed.");
                }
                buffer.add(cloudEvent);
            }
        }

        void drainBufferAndGoLive() {
            synchronized (lock) {
                for (CloudEvent buffered : buffer) {
                    deliverLive(buffered);
                }
                buffer.clear();
                live = true;
            }
        }

        // Must be called holding lock. Delivers unless the event was already delivered by the replay or an earlier
        // live copy (id de-dup over the overlap window).
        private void deliverLive(CloudEvent cloudEvent) {
            String id = cloudEvent.getId();
            if (deliveredIds.contains(id)) {
                return;
            }
            action.accept(cloudEvent);
            deliveredIds.add(id);
        }
    }

    private record AlreadyStartedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            // The bootstrap replay completes synchronously in subscribe before this handle is returned.
            return true;
        }
    }
}
