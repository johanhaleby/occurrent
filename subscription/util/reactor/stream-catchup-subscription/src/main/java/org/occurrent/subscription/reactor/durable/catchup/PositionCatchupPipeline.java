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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * The bulk-then-reconcile-then-live handover shared by every position-ordered reactive catch-up model. A
 * {@link CatchupReader} supplies the window and head reads, so this pipeline is free of any specific store or query
 * type, and is reused by both the stream and the DCB catch-up models.
 * <p>
 * The live resume token is captured before the bulk replay, not after, so an event that commits during the replay is
 * still delivered by the live subscription. The replay pages the sequence in {@code position} windows, then a
 * reconciliation pass keeps paging until the head stops advancing so events written during the replay are delivered
 * in order. A bounded id cache dedupes events that both the replay and the live subscription see.
 * <p>
 * If the replay runs longer than the change stream history (the MongoDB oplog window), the captured token ages out
 * and the live resume fails loudly rather than silently dropping an event. If the model reports no resume token at
 * all (for example an empty oplog or a restricted cluster), the subscription fails loudly for the same reason.
 */
@NullMarked
public final class PositionCatchupPipeline {

    private final CatchupReader reader;
    private final long windowSize;
    private final int handoverCacheSize;

    public PositionCatchupPipeline(CatchupReader reader, long windowSize, int handoverCacheSize) {
        this.reader = requireNonNull(reader, CatchupReader.class.getSimpleName() + " cannot be null");
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
     * Replays from {@code startPosition} and hands over to {@code subscriptionModel}, subscribed with
     * {@code liveSubscriptionFilter} and filtered further by {@code livePredicate}, so only events matching the
     * catch-up's own selection (a stream {@link org.occurrent.filter.Filter} or a DCB query) reach the caller.
     */
    public Flux<CloudEvent> catchup(PositionAwareSubscriptionModel subscriptionModel, SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate, long startPosition) {
        Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        Objects.requireNonNull(liveSubscriptionFilter, "liveSubscriptionFilter cannot be null");
        Objects.requireNonNull(livePredicate, "livePredicate cannot be null");
        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition cannot be negative, was " + startPosition);
        }
        // Capture the live resume token before the bulk replay so an event committing during the replay is still
        // delivered by the live subscription. If the model reports no token (for example an empty oplog or a
        // restricted cluster) a no-loss handover to live cannot be guaranteed, so fail loudly instead of silently
        // dropping the events committed between the end of the replay and going live.
        return subscriptionModel.globalSubscriptionPosition()
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Cannot run a catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.")))
                .flatMapMany(liveToken ->
                        reader.currentHead().flatMapMany(bulkHead -> {
                            HandoverCache cache = new HandoverCache(handoverCacheSize);
                            // Cache the replayed ids, including the bulk tail, because the reactive global
                            // subscription position resumes inclusively, so the live change stream re-delivers
                            // boundary events the replay already emitted. Dedup is by id, not by position, so an
                            // in-flight event below the replay head that was never seen during the replay is still
                            // delivered once by the live change stream.
                            Flux<CloudEvent> bulk = windows(startPosition, bulkHead, cache);
                            Flux<CloudEvent> reconcile = reconcile(bulkHead, cache);
                            Flux<CloudEvent> live = subscriptionModel.subscribe(liveSubscriptionFilter, StartAt.subscriptionPosition(liveToken))
                                    .filter(cloudEvent -> livePredicate.test(cloudEvent) && !cache.contains(cloudEvent.getId()));
                            return Flux.concat(bulk, reconcile, live);
                        }));
    }

    // Emits events in (fromExclusive, toInclusive], paging in position windows. Records every emitted id in the cache
    // so the inclusive live resume can skip the replayed events. Used by both the bulk and the reconciliation phases.
    private Flux<CloudEvent> windows(long fromExclusive, long toInclusive, HandoverCache cache) {
        if (fromExclusive >= toInclusive) {
            return Flux.empty();
        }
        long upTo = Math.min(fromExclusive + windowSize, toInclusive);
        return reader.readWindow(fromExclusive, upTo)
                .doOnNext(event -> cache.add(event.getId()))
                .concatWith(Flux.defer(() -> windows(upTo, toInclusive, cache)));
    }

    // Pages from the bulk head onward, re-reading the head each round, until it stops advancing. This delivers events
    // written during the bulk replay in position order.
    private Flux<CloudEvent> reconcile(long cursor, HandoverCache cache) {
        return reader.currentHead().flatMapMany(head -> head > cursor
                ? windows(cursor, head, cache).concatWith(Flux.defer(() -> reconcile(head, cache)))
                : Flux.empty());
    }
}
