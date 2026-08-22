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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.internal.BoundedIdCache;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Bulk-then-reconcile-then-live handover shared by every position-ordered reactive catch-up model. A
 * {@link CatchupReader} supplies the window and head reads so this pipeline is store-agnostic, reused by both the
 * stream and DCB catch-up models.
 * <p>
 * The live resume token is captured before the bulk replay so an event committing during the replay is still
 * delivered live. The replay pages in {@code position} windows, then reconciles once, draining up to a head
 * snapshotted at reconcile start so writes during replay are delivered in order. It does not chase a moving head,
 * which would never terminate under sustained writes; anything after the snapshot is left to the live subscription
 * (resuming from the pre-bulk token), deduped by the id cache.
 * <p>
 * If the replay runs longer than the change stream history (e.g. the MongoDB oplog window), the captured token ages
 * out and the handover fails loudly rather than silently dropping events. Same if the model reports no resume token
 * at all (e.g. an empty oplog or a restricted cluster).
 */
@NullMarked
final class PositionCatchupPipeline {

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
    public Flux<CloudEvent> catchup(CheckpointAwareSubscriptionModel subscriptionModel, SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate, long startPosition) {
        Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        Objects.requireNonNull(liveSubscriptionFilter, "liveSubscriptionFilter cannot be null");
        Objects.requireNonNull(livePredicate, "livePredicate cannot be null");
        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition cannot be negative, was " + startPosition);
        }
        BoundedIdCache cache = new BoundedIdCache(handoverCacheSize);
        return captureLiveToken(subscriptionModel)
                .flatMapMany(liveToken -> {
                    Flux<CloudEvent> live = subscriptionModel.subscribe(liveSubscriptionFilter, StartAt.checkpoint(liveToken))
                            .filter(cloudEvent -> livePredicate.test(cloudEvent) && !cache.contains(cloudEvent.getId()));
                    return replay(startPosition, cache).concatWith(live);
                });
    }

    /**
     * Captures the live resume token before the bulk replay so an event committing during the replay is still
     * delivered live. If the model reports no token (e.g. an empty oplog or a restricted cluster) a no-loss
     * handover cannot be guaranteed, so it fails loudly instead of silently dropping events. Shared by the cold
     * pipeline above and the named catch-up path in {@code NamedCatchupSupport}.
     */
    Mono<Checkpoint> captureLiveToken(CheckpointAwareSubscriptionModel subscriptionModel) {
        return subscriptionModel.globalCheckpoint()
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Cannot run a catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.")));
    }

    /**
     * The replay half on its own: bulk windows then one reconcile pass, every emitted id recorded in {@code cache}.
     * Cache the replayed ids, including the bulk tail, since the reactive global position resumes inclusively and the
     * live change stream re-delivers boundary events already emitted. Dedup by id, not position, so an in-flight event
     * never seen during the replay is still delivered once, live. Shared by the cold pipeline above and the named
     * catch-up path in {@code NamedCatchupSupport}.
     */
    Flux<CloudEvent> replay(long startPosition, BoundedIdCache cache) {
        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition cannot be negative, was " + startPosition);
        }
        return reader.currentHead().flatMapMany(bulkHead -> {
            Flux<CloudEvent> bulk = windows(startPosition, bulkHead, cache);
            Flux<CloudEvent> reconcile = reconcile(bulkHead, cache);
            return Flux.concat(bulk, reconcile);
        });
    }

    /**
     * The same replay, with {@code action} applied here instead of by the caller, so {@code reconcileStarting} can
     * run between the last history event being handled and the first reconciliation read.
     * <p>
     * A caller applying the action itself cannot get that ordering. {@code concatMap} prefetches, so the history
     * {@code Flux} completes once its events are queued rather than once they are handled, and anything placed
     * between the two halves upstream of the action would run while up to a prefetch worth of history is still
     * waiting to be handled. Those events would then be treated as if they came from the reconciliation.
     * <p>
     * {@code keepReplaying} truncates each half, and the tail is skipped entirely once it answers {@code false}, so
     * a stop that lands after the history has drained costs no head read and no window read.
     */
    Flux<Void> replayApplying(long startPosition, BoundedIdCache cache, BooleanSupplier keepReplaying,
                              Function<CloudEvent, Mono<Void>> action, Runnable reconcileStarting) {
        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition cannot be negative, was " + startPosition);
        }
        return reader.currentHead().flatMapMany(bulkHead -> Flux.concat(
                windows(startPosition, bulkHead, cache).takeWhile(ignored -> keepReplaying.getAsBoolean()).concatMap(action),
                Mono.defer(() -> {
                    if (!keepReplaying.getAsBoolean()) {
                        return Mono.empty();
                    }
                    reconcileStarting.run();
                    return Mono.empty();
                }),
                // Called inside the defer rather than passed into it, since building the reconciliation Flux reads
                // the head.
                Flux.defer(() -> keepReplaying.getAsBoolean()
                        ? reconcile(bulkHead, cache).takeWhile(ignored -> keepReplaying.getAsBoolean()).concatMap(action)
                        : Flux.empty())));
    }

    // Emits events in (fromExclusive, toInclusive], paging in position windows. Records every emitted id in the cache
    // so the inclusive live resume can skip the replayed events. Used by both the bulk and the reconciliation phases.
    private Flux<CloudEvent> windows(long fromExclusive, long toInclusive, BoundedIdCache cache) {
        if (fromExclusive >= toInclusive) {
            return Flux.empty();
        }
        long upTo = Math.min(fromExclusive + windowSize, toInclusive);
        return reader.readWindow(fromExclusive, upTo)
                .doOnNext(event -> cache.add(event.getId()))
                .concatWith(Flux.defer(() -> windows(upTo, toInclusive, cache)));
    }

    // Snapshot the head once and drain events up to it in position order. Re-reading a moving head would advance
    // forever under sustained writes and never hand over to live (livelock). Anything after the snapshot is
    // covered by the live change stream (resumes from the pre-bulk token), deduped by the id cache.
    private Flux<CloudEvent> reconcile(long cursor, BoundedIdCache cache) {
        return reader.currentHead().flatMapMany(snapshotHead -> windows(cursor, snapshotHead, cache));
    }
}
