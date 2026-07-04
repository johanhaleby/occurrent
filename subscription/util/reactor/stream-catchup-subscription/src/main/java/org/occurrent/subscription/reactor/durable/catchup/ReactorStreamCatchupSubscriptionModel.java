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
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.inmemory.filtermatching.FilterMatcher;
import org.occurrent.subscription.GlobalSubscriptionPosition;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
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

/**
 * Reactive stream catch-up: replays stream history matching a {@link Filter} in global {@code position} order via
 * {@link PositionOrderedReader}, then hands over to a live subscription, all as a single {@link Flux}. It lets a
 * reactive read model rebuild from the start of the stream sequence and then keep up with new events.
 * <p>
 * This is the stream counterpart of {@link ReactorDcbCatchupSubscriptionModel}. It replays through
 * {@link PositionOrderedReader#readInPositionOrder(Filter, PositionRange)} and matches a stream {@link Filter}
 * in-process, where the DCB model uses a {@code DcbQuery}. Otherwise the two are the same, because both read the
 * same global {@code position} sequence.
 * <p>
 * Only meaningful for a store that writes a {@code position} on stream events. This model cannot check that itself
 * (it depends only on {@link PositionOrderedReader}), so do not wire it up against a store that does not write
 * position. If you do, {@link PositionOrderedReader#readInPositionOrder(Filter, PositionRange)} throws
 * {@link UnsupportedOperationException}.
 * <p>
 * The live resume token is captured before the bulk replay, not after, so an event that commits during the replay is
 * still delivered by the live subscription. The replay pages the sequence in {@code position} windows, then a
 * reconciliation pass keeps paging until the head stops advancing so events written during the replay are delivered
 * in order. A bounded id cache dedupes events that both the replay and the live subscription see.
 * <p>
 * If the replay runs longer than the change stream history (the MongoDB oplog window), the captured token ages out
 * and the live resume fails loudly rather than silently dropping an event. Size the oplog for very large rebuilds.
 * If the model reports no resume token at all (for example an empty oplog or a restricted cluster), the subscription
 * fails loudly for the same reason.
 * <p>
 * This model does not persist subscription positions, so layer a durable model on top (for example
 * {@code ReactorDurableSubscriptionModel}) if resume across restarts is needed.
 * <p>
 * It implements {@link PositionAwareSubscriptionModel}, so it can sit as a plain (cold) subscription model underneath
 * a durable model. Its generic {@link #subscribe(SubscriptionFilter, StartAt)} only accepts an
 * {@link OccurrentSubscriptionFilter}, or no filter, in which case the default {@link Filter} passed to the
 * constructor is used.
 */
@NullMarked
public class ReactorStreamCatchupSubscriptionModel implements PositionAwareSubscriptionModel {

    /**
     * Default number of positions read per replay window.
     */
    public static final long DEFAULT_POSITION_WINDOW_SIZE = 1000;
    /**
     * Default number of event ids kept to deduplicate the replay-to-live handover seam.
     */
    public static final int DEFAULT_HANDOVER_CACHE_SIZE = 1000;

    private final PositionAwareSubscriptionModel subscriptionModel;
    private final PositionOrderedReader positionOrderedReader;
    private final @Nullable Filter defaultFilter;
    private final long windowSize;
    private final int handoverCacheSize;

    public ReactorStreamCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader) {
        this(subscriptionModel, positionOrderedReader, null, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorStreamCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, long windowSize, int handoverCacheSize) {
        this(subscriptionModel, positionOrderedReader, null, windowSize, handoverCacheSize);
    }

    /**
     * Create a catch-up model with a default {@link Filter} used by {@link #subscribe(SubscriptionFilter, StartAt)}
     * when it is called without a filter. Lets one model serve every stream subscription, each narrowing with its own
     * filter.
     */
    public ReactorStreamCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter) {
        this(subscriptionModel, positionOrderedReader, defaultFilter, DEFAULT_POSITION_WINDOW_SIZE, DEFAULT_HANDOVER_CACHE_SIZE);
    }

    public ReactorStreamCatchupSubscriptionModel(PositionAwareSubscriptionModel subscriptionModel, PositionOrderedReader positionOrderedReader, @Nullable Filter defaultFilter, long windowSize, int handoverCacheSize) {
        this.subscriptionModel = requireNonNull(subscriptionModel, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.positionOrderedReader = requireNonNull(positionOrderedReader, PositionOrderedReader.class.getSimpleName() + " cannot be null");
        this.defaultFilter = defaultFilter;
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
     * The generic (cold) subscription-model entry point. The {@code filter} must be an
     * {@link OccurrentSubscriptionFilter}, or {@code null} to use the default {@link Filter} supplied to the
     * constructor. A {@code startAt} that resolves to a {@code position} replays history from that position and then
     * goes live, anything else goes straight to live.
     */
    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");
        final Filter resolvedFilter;
        if (filter == null) {
            if (defaultFilter == null) {
                return Flux.error(new IllegalArgumentException("A " + OccurrentSubscriptionFilter.class.getSimpleName() + " is required unless a default " + Filter.class.getSimpleName() + " was supplied to the constructor."));
            }
            resolvedFilter = defaultFilter;
        } else if (filter instanceof OccurrentSubscriptionFilter occurrentSubscriptionFilter) {
            resolvedFilter = occurrentSubscriptionFilter.filter();
        } else {
            return Flux.error(new IllegalArgumentException(ReactorStreamCatchupSubscriptionModel.class.getSimpleName() + " only supports an " + OccurrentSubscriptionFilter.class.getSimpleName() + ", but got " + filter.getClass().getName()));
        }
        return subscribe(resolvedFilter, startAt);
    }

    @Override
    public Mono<SubscriptionPosition> globalSubscriptionPosition() {
        return subscriptionModel.globalSubscriptionPosition();
    }

    /**
     * Subscribe to stream events matching {@code filter}, starting from a {@code position}-based
     * {@link StartAt#subscriptionPosition(SubscriptionPosition)} built from {@link GlobalSubscriptionPosition} (for
     * example {@code GlobalSubscriptionPosition.of(0)} to replay from the beginning) to replay history then go live.
     * Any other start (now or the subscription model default) goes straight to live.
     */
    public Flux<CloudEvent> subscribe(Filter filter, StartAt startAt) {
        requireNonNull(filter, "Filter cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        StartAt resolved = startAt.get(new SubscriptionModelContext(ReactorStreamCatchupSubscriptionModel.class));
        if (!(resolved instanceof StartAt.StartAtSubscriptionPosition position) || !GlobalSubscriptionPosition.isGlobalSubscriptionPosition(position.subscriptionPosition)) {
            // Not a catch-up position, so go straight to live. Filter in-process too, so a backend that does not
            // honor the filter server-side still only delivers matching events, and skip events without a position.
            return subscriptionModel.subscribe(OccurrentSubscriptionFilter.filter(filter), resolved == null ? startAt : resolved)
                    .filter(cloudEvent -> OccurrentCloudEventExtension.getPosition(cloudEvent) > 0 && FilterMatcher.matchesFilter(cloudEvent, filter));
        }

        long startPosition = GlobalSubscriptionPosition.positionOf(position.subscriptionPosition);
        // Capture the live resume token before the bulk replay so an event committing during the replay is still
        // delivered by the live subscription. If the model reports no token (for example an empty oplog or a
        // restricted cluster) events could be dropped between the end of the replay and going live, so fail loudly.
        return subscriptionModel.globalSubscriptionPosition()
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Cannot run a stream catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.")))
                .flatMapMany(liveToken ->
                positionOrderedReader.currentPosition().flatMapMany(bulkHead -> {
                    HandoverCache cache = new HandoverCache(handoverCacheSize);
                    // Cache the replayed ids so the live change stream can skip them. It resumes inclusively and so
                    // re-delivers boundary events the replay already emitted. Dedup by id, not by position, so an
                    // event below the replay head that the replay never saw is still delivered once by the live stream.
                    Flux<CloudEvent> bulk = windows(filter, startPosition, bulkHead, cache);
                    Flux<CloudEvent> reconcile = reconcile(filter, bulkHead, cache);
                    Flux<CloudEvent> live = subscriptionModel.subscribe(OccurrentSubscriptionFilter.filter(filter), StartAt.subscriptionPosition(liveToken))
                            .filter(cloudEvent -> OccurrentCloudEventExtension.getPosition(cloudEvent) > 0
                                    && FilterMatcher.matchesFilter(cloudEvent, filter)
                                    && !cache.contains(cloudEvent.getId()));
                    return Flux.concat(bulk, reconcile, live);
                }));
    }

    // Emits events in (fromExclusive, toInclusive], paging in position windows. Records every emitted id in the cache
    // so the inclusive live resume can skip the replayed events. Used by both the bulk and the reconciliation phases.
    private Flux<CloudEvent> windows(Filter filter, long fromExclusive, long toInclusive, HandoverCache cache) {
        if (fromExclusive >= toInclusive) {
            return Flux.empty();
        }
        long upTo = Math.min(fromExclusive + windowSize, toInclusive);
        return positionOrderedReader.readInPositionOrder(filter, PositionRange.between(fromExclusive, upTo))
                .doOnNext(event -> cache.add(event.getId()))
                // Attach the position as the subscription position so a durable model layered on top can persist
                // replay progress. A raw event read from the store carries no change-stream position.
                .map(event -> (CloudEvent) new PositionAwareCloudEvent(event, GlobalSubscriptionPosition.of(OccurrentCloudEventExtension.getPosition(event))))
                .concatWith(Flux.defer(() -> windows(filter, upTo, toInclusive, cache)));
    }

    // Pages from the bulk head onward, re-reading the head each round, until it stops advancing. This delivers events
    // written during the bulk replay in position order.
    private Flux<CloudEvent> reconcile(Filter filter, long cursor, HandoverCache cache) {
        return positionOrderedReader.currentPosition().flatMapMany(head -> head > cursor
                ? windows(filter, cursor, head, cache).concatWith(Flux.defer(() -> reconcile(filter, head, cache)))
                : Flux.empty());
    }

    // A bounded, insertion-ordered set of recently replayed event ids. The live change stream resumes inclusively and
    // re-delivers events near the captured token that the replay already emitted, and this cache skips those. It only
    // needs the tail the live resume can overlap, not the whole history.
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
