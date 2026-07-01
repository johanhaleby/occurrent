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
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.DcbSubscriptionPosition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Reactive DCB catch-up: replays the DCB history matching a {@link DcbQuery} by {@code dcbposition}, then hands over to
 * a live subscription, as a single {@link Flux}. It lets a reactive read model rebuild from the beginning of the DCB
 * sequence and then keep up with new events.
 * <p>
 * The handover preserves the central invariant of the blocking catch-up. The live change-stream resume token is captured
 * before the bulk replay, not after, so a DCB event that commits during the replay is still delivered by the live
 * subscription. The replay pages the sequence in {@code dcbposition} windows (no count and no time sort, because
 * {@code dcbposition} is monotonic and server-assigned), then a reconciliation pass keeps paging until the head stops
 * advancing to deliver events written during the replay in order. The handover seam is deduplicated with a bounded id
 * cache so a reconciliation event the live subscription also sees is delivered once.
 * <p>
 * Trade-off: if the replay runs longer than the change stream history (the MongoDB oplog window), the captured token
 * ages out and the live resume fails loudly rather than silently dropping an event. Size the oplog for very large
 * rebuilds. If the model cannot report a resume token at all (for example an empty oplog or a restricted cluster), the
 * subscription fails loudly for the same reason, rather than replaying without a guaranteed handover to live.
 * <p>
 * This is the DCB path only. Stream time-based catch-up is not provided here, and this model does not persist
 * subscription positions, so layer a durable model on top if resume across restarts is needed.
 */
@NullMarked
public class ReactorDcbCatchupSubscriptionModel {

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
    private final long windowSize;
    private final int handoverCacheSize;

    public ReactorDcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore) {
        this(subscriptionModel, dcbEventStore, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorDcbCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, DcbEventStore dcbEventStore, long windowSize, int handoverCacheSize) {
        this.subscriptionModel = requireNonNull(subscriptionModel, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.dcbEventStore = requireNonNull(dcbEventStore, DcbEventStore.class.getSimpleName() + " cannot be null");
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
     * Subscribe to DCB events matching {@code query}. A {@link DcbStartAt} that carries a {@code dcbposition} (for
     * example {@link DcbStartAt#beginning()} or {@link DcbStartAt#afterPosition(long)}) replays history from that
     * position and then goes live. Any other start (now or the subscription model default) goes straight to live.
     */
    public Flux<CloudEvent> subscribe(DcbQuery query, DcbStartAt startAt) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(startAt, DcbStartAt.class.getSimpleName() + " cannot be null");

        StartAt resolved = startAt.toStartAt().get(new SubscriptionModelContext(ReactorDcbCatchupSubscriptionModel.class));
        if (!(resolved instanceof StartAt.StartAtSubscriptionPosition position) || !DcbSubscriptionPosition.isDcbSubscriptionPosition(position.subscriptionPosition)) {
            // Not a DCB catch-up position, so go straight to live through the shared facade.
            return DcbSubscriptionModel.from(subscriptionModel).subscribe(query, startAt);
        }

        long startPosition = DcbSubscriptionPosition.dcbPositionOf(position.subscriptionPosition);
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
                            .filter(cloudEvent -> DcbCloudEvents.getPosition(cloudEvent) > 0
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
                    stream.events().forEach(event -> cache.add(event.getId()));
                    return Flux.fromIterable(stream.events());
                })
                .concatWith(Flux.defer(() -> windows(query, upTo, toInclusive, cache)));
    }

    // Pages from the bulk head onward, re-reading the head each round, until it stops advancing. This delivers events
    // written during the bulk replay in dcbposition order.
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
