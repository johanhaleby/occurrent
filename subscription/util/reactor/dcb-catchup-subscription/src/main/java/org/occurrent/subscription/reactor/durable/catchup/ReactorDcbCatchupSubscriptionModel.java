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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.GlobalSubscriptionPosition;
import org.occurrent.subscription.PositionAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

/**
 * Reactive DCB catch-up: replays the DCB history matching a {@link DcbQuery} by {@code position}, then hands over to
 * a live subscription, as a single {@link Flux}. It lets a reactive read model rebuild from the beginning of the DCB
 * sequence and then keep up with new events.
 * <p>
 * The handover preserves the central invariant of the blocking catch-up. The live change-stream resume token is captured
 * before the bulk replay, not after, so a DCB event that commits during the replay is still delivered by the live
 * subscription. The replay pages the sequence in {@code position} windows (no count and no time sort, because
 * {@code position} is monotonic and server-assigned), then a reconciliation pass keeps paging until the head stops
 * advancing to deliver events written during the replay in order. The handover seam is deduplicated with a bounded id
 * cache so a reconciliation event the live subscription also sees is delivered once.
 * <p>
 * Trade-off: if the replay runs longer than the change stream history (the MongoDB oplog window), the captured token
 * ages out and the live resume fails loudly rather than silently dropping an event. Size the oplog for very large
 * rebuilds. If the model cannot report a resume token at all (for example an empty oplog or a restricted cluster), the
 * subscription fails loudly for the same reason, rather than replaying without a guaranteed handover to live.
 * <p>
 * This is the DCB path only. Stream time-based catch-up is not provided here, and this model does not persist
 * subscription positions, so layer a durable model on top (for example {@code ReactorDurableSubscriptionModel}) if
 * resume across restarts is needed.
 * <p>
 * It implements {@link PositionAwareSubscriptionModel}, so it can sit as a plain (cold) subscription model underneath a
 * durable model or be handed to the reactive DCB subscription DSL. Its generic {@link #subscribe(SubscriptionFilter, StartAt)}
 * only understands a {@link DcbSubscriptionFilter} (or no filter, in which case a default {@link DcbQuery} supplied to the
 * constructor is used), since catch-up is DCB-specific.
 */
@NullMarked
public class ReactorDcbCatchupSubscriptionModel implements PositionAwareSubscriptionModel {

    /**
     * Default number of DCB positions read per replay window.
     */
    public static final long DEFAULT_POSITION_WINDOW_SIZE = 1000;
    /**
     * Default number of event ids kept to deduplicate the replay-to-live handover seam.
     */
    public static final int DEFAULT_HANDOVER_CACHE_SIZE = 1000;

    private final PositionAwareSubscriptionModel subscriptionModel;
    private final DcbEventStore dcbEventStore;
    private final @Nullable DcbQuery defaultQuery;
    private final long windowSize;
    private final int handoverCacheSize;

    public ReactorDcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore) {
        this(subscriptionModel, dcbEventStore, null, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorDcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, long windowSize, int handoverCacheSize) {
        this(subscriptionModel, dcbEventStore, null, windowSize, handoverCacheSize);
    }

    /**
     * Create a catch-up model with a default {@link DcbQuery} used by {@link #subscribe(SubscriptionFilter, StartAt)}
     * when it is called without a filter. This mirrors the blocking {@code CatchupSubscriptionModel} constructor that
     * takes a shared {@code DcbQuery.all()}, so the reactive starter can wire one model that every DCB subscription
     * narrows with its own query in the consumer.
     */
    public ReactorDcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbQuery defaultQuery) {
        this(subscriptionModel, dcbEventStore, defaultQuery, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorDcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, @Nullable DcbQuery defaultQuery, long windowSize, int handoverCacheSize) {
        this.subscriptionModel = requireNonNull(subscriptionModel, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.dcbEventStore = requireNonNull(dcbEventStore, DcbEventStore.class.getSimpleName() + " cannot be null");
        this.defaultQuery = defaultQuery;
        if (windowSize <= 0) {
            throw new IllegalArgumentException("Window size must be greater than zero");
        }
        if (handoverCacheSize <= 0) {
            throw new IllegalArgumentException("Handover cache size must be greater than zero");
        }
        this.windowSize = windowSize;
        this.handoverCacheSize = handoverCacheSize;
    }

    /**
     * The generic (cold) subscription-model entry point. The {@code filter} must be a {@link DcbSubscriptionFilter}, or
     * {@code null} to use the default {@link DcbQuery} supplied to the constructor. A {@code startAt} that resolves to a
     * {@code position} replays history from that position and then goes live, anything else goes straight to live.
     * This is how a durable model wrapping this catch-up model, and the reactive DCB subscription DSL, drive it.
     */
    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        final DcbQuery query;
        if (filter == null) {
            if (defaultQuery == null) {
                return Flux.error(new IllegalArgumentException("A " + DcbSubscriptionFilter.class.getSimpleName() + " is required unless a default " + DcbQuery.class.getSimpleName() + " was supplied to the constructor."));
            }
            query = defaultQuery;
        } else if (filter instanceof DcbSubscriptionFilter dcbSubscriptionFilter) {
            query = dcbSubscriptionFilter.query();
        } else {
            return Flux.error(new IllegalArgumentException(ReactorDcbCatchupSubscriptionModel.class.getSimpleName() + " only supports a " + DcbSubscriptionFilter.class.getSimpleName() + ", but got " + filter.getClass().getName()));
        }
        return subscribe(query, startAt);
    }

    @Override
    public Mono<SubscriptionPosition> globalSubscriptionPosition() {
        return subscriptionModel.globalSubscriptionPosition();
    }

    /**
     * Subscribe to DCB events matching {@code query}. A {@link DcbStartAt} that carries a {@code position} (for
     * example {@link DcbStartAt#beginning()} or {@link DcbStartAt#afterPosition(long)}) replays history from that
     * position and then goes live. Any other start (now or the subscription model default) goes straight to live.
     */
    public Flux<CloudEvent> subscribe(DcbQuery query, DcbStartAt startAt) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");
        return subscribe(query, startAt.toStartAt());
    }

    private Flux<CloudEvent> subscribe(DcbQuery query, StartAt startAt) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        StartAt resolved = startAt.get(new SubscriptionModelContext(ReactorDcbCatchupSubscriptionModel.class));
        if (!(resolved instanceof StartAt.StartAtSubscriptionPosition position) || !GlobalSubscriptionPosition.isGlobalSubscriptionPosition(position.subscriptionPosition)) {
            // Not a DCB catch-up position, so go straight to live. Apply the same in-process DCB floor the replay-to-live
            // path and the DcbSubscriptionModel adapter apply, so a backend that does not honor the filter server-side
            // still only delivers events matching the query.
            return subscriptionModel.subscribe(DcbSubscriptionFilter.filter(query), resolved == null ? startAt : resolved)
                    .filter(cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent) && DcbCloudEvents.matches(cloudEvent, query));
        }

        long startPosition = GlobalSubscriptionPosition.positionOf(position.subscriptionPosition);
        // Capture the live resume token before the bulk replay so an event committing during the replay is still
        // delivered by the live subscription. If the model reports no token (for example an empty oplog or a restricted
        // cluster) a no-loss handover to live cannot be guaranteed, so fail loudly instead of silently dropping the
        // events committed between the end of the replay and going live.
        return subscriptionModel.globalSubscriptionPosition()
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Cannot run a DCB catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.")))
                .flatMapMany(liveToken ->
                readHead(query, startPosition).flatMapMany(bulkHead -> {
                    HandoverCache cache = new HandoverCache(handoverCacheSize);
                    // Cache the replayed ids, including the bulk tail, because the reactive global subscription position
                    // resumes inclusively, so the live change stream re-delivers boundary events the replay already
                    // emitted. Dedup is by id, not by position, so an in-flight event below the replay head that was
                    // never seen during the replay is still delivered once by the live change stream.
                    Flux<CloudEvent> bulk = windows(query, startPosition, bulkHead, cache);
                    Flux<CloudEvent> reconcile = reconcile(query, bulkHead, cache);
                    Flux<CloudEvent> live = subscriptionModel.subscribe(DcbSubscriptionFilter.filter(query), StartAt.subscriptionPosition(liveToken))
                            .filter(cloudEvent -> DcbCloudEvents.isDcbEvent(cloudEvent)
                                    && DcbCloudEvents.matches(cloudEvent, query)
                                    && !cache.contains(cloudEvent.getId()));
                    return Flux.concat(bulk, reconcile, live);
                }));
    }

    // Reads the store's current global DCB head as a single read. lastSequencePosition is the global head at read
    // time regardless of whether the query matched anything.
    private Mono<Long> readHead(DcbQuery query, long cursor) {
        return dcbEventStore.read(query, DcbReadOptions.between(cursor, cursor)).map(stream -> stream.lastSequencePosition());
    }

    // Emits events in (fromExclusive, toInclusive] paging in position windows. Records every emitted id in the cache so
    // the inclusive live resume can skip the replayed events at the handover seam. Used by both the bulk and the
    // reconciliation phases.
    private Flux<CloudEvent> windows(DcbQuery query, long fromExclusive, long toInclusive, HandoverCache cache) {
        if (fromExclusive >= toInclusive) {
            return Flux.empty();
        }
        long upTo = Math.min(fromExclusive + windowSize, toInclusive);
        return dcbEventStore.read(query, DcbReadOptions.between(fromExclusive, upTo))
                .flatMapMany(stream -> {
                    var events = stream.events();
                    events.forEach(event -> cache.add(event.getId()));
                    // Attach the position as the subscription position so a durable model layered on top can persist
                    // the replay progress (a raw event read from the store carries no change-stream position). Mirrors
                    // the blocking CatchupSubscriptionModel, which wraps each replayed DCB event with
                    // GlobalSubscriptionPosition.of(its position).
                    return Flux.fromIterable(events)
                            .map(event -> (CloudEvent) new PositionAwareCloudEvent(event, GlobalSubscriptionPosition.of(OccurrentCloudEventExtension.getPosition(event))));
                })
                .concatWith(Flux.defer(() -> windows(query, upTo, toInclusive, cache)));
    }

    // Pages from the bulk head onward, re-reading the head each round, until it stops advancing. This delivers events
    // written during the bulk replay in position order.
    private Flux<CloudEvent> reconcile(DcbQuery query, long cursor, HandoverCache cache) {
        return readHead(query, cursor).flatMapMany(head -> head > cursor
                ? windows(query, cursor, head, cache).concatWith(Flux.defer(() -> reconcile(query, head, cache)))
                : Flux.empty());
    }

    // A bounded, insertion-ordered set of event ids covering the recently replayed tail. The live change stream resumes
    // inclusively, so it re-delivers events near the captured token that the replay already emitted, and this cache
    // skips those. It does not need the whole history, only the tail the live resume can overlap.
    private static final class HandoverCache {
        private final int maxSize;
        private final Set<String> ids;

        private HandoverCache(int maxSize) {
            this.maxSize = maxSize;
            this.ids = Collections.synchronizedSet(new LinkedHashSet<>());
        }

        private void add(String id) {
            synchronized (ids) {
                if (ids.add(id) && ids.size() > maxSize) {
                    Iterator<String> iterator = ids.iterator();
                    iterator.next();
                    iterator.remove();
                }
            }
        }

        private boolean contains(String id) {
            return ids.contains(id);
        }
    }
}
