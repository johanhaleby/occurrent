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
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * which would never terminate under sustained writes, and anything after the snapshot is left to the live
 * subscription (resuming from the pre-bulk token), deduped by the (id, source) cache.
 * <p>
 * Only the reconciliation pass fills that cache. The history windows fill nothing, so the cache never suppresses a
 * live delivery of an event that only the history read had delivered. See
 * <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0135-the-reactive-handover-dedup-is-fed-only-by-the-reconciliation-read.md">ADR 135</a>.
 * <p>
 * Each replayed event that has a {@link GlobalCheckpoint} gets one that also holds the live token, the position the
 * replay started from and the head the replay read, so a durable model that stores it resumes live from that same
 * token after a restart. A token read after the restart would be too late for an event whose position was reserved
 * below the stored position but written only after the earlier replay had read past it. The resumed replay stops at
 * the stored head and does not reconcile, since every event above that head was written after the token and arrives
 * live. When the wrapped model no longer has the history from a
 * stored token, the replay starts over from the stored start position with a token read now, and redelivers the
 * events in between.
 * <p>
 * If the model reports no resume token at all (e.g. an empty oplog or a restricted cluster), the catch-up fails. A
 * token that ages out of the change stream history (e.g. the MongoDB oplog window) during a long replay does not fail
 * here. What the handover does with it is up to the wrapped model, and a Mongo model that restarts on lost history,
 * the Spring Boot starter's default, goes live from the present and skips the events in between.
 */
@NullMarked
final class PositionCatchupPipeline {

    private static final Logger log = LoggerFactory.getLogger(PositionCatchupPipeline.class);

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
     * Replays from {@code start}, a {@link GlobalCheckpoint} or one read back from storage, and hands over to
     * {@code subscriptionModel}, subscribed with {@code liveSubscriptionFilter} and filtered further by
     * {@code livePredicate}, so only events matching the catch-up's own selection (a stream
     * {@link org.occurrent.filter.Filter} or a DCB query) reach the caller.
     */
    public Flux<CloudEvent> catchup(CheckpointAwareSubscriptionModel subscriptionModel, SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate, Checkpoint start) {
        Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        Objects.requireNonNull(liveSubscriptionFilter, "liveSubscriptionFilter cannot be null");
        Objects.requireNonNull(livePredicate, "livePredicate cannot be null");
        GlobalCheckpoint startCheckpoint = GlobalCheckpoint.parse(Objects.requireNonNull(start, "start cannot be null"));
        BoundedIdCache<CatchupEventKey> cache = new BoundedIdCache<>(handoverCacheSize);
        return resolveStart(subscriptionModel, startCheckpoint, null)
                .flatMapMany(replayStart -> {
                    Flux<CloudEvent> live = subscriptionModel.subscribe(liveSubscriptionFilter, StartAt.checkpoint(replayStart.liveFrom()))
                            .filter(cloudEvent -> livePredicate.test(cloudEvent) && !cache.contains(CatchupEventKey.of(cloudEvent)));
                    return replay(replayStart, cache).concatWith(live);
                });
    }

    /**
     * Where a replay starts, the live token it hands over to, the position the first attempt at it started from, and
     * the head an earlier attempt read after it read that token, or null when this attempt reads the head itself.
     */
    record ReplayStart(long replayFrom, Checkpoint liveFrom, long replayOrigin, @Nullable Long replayTo) {
        ReplayStart {
            requireNonNull(liveFrom, "liveFrom cannot be null");
            if (replayFrom < 0) {
                throw new IllegalArgumentException("replayFrom cannot be negative, was " + replayFrom);
            }
        }

        // The head the replay reads up to. A resume from a stored live token replays only up to the head the attempt
        // that read the token read next. Every event above that head was written after the token and arrives live,
        // so replaying it too would deliver it twice.
        Mono<Long> head(CatchupReader reader) {
            return replayTo == null ? reader.currentHead() : Mono.just(replayTo);
        }

        // Only a replay that read its own head reconciles past it
        boolean reconciles() {
            return replayTo == null;
        }

        // Replaces a replayed event's GlobalCheckpoint with one that also holds the live token, the replay origin and
        // the head the replay read. An event without one is passed on as it is.
        CloudEvent stamp(CloudEvent cloudEvent, long head) {
            if (cloudEvent instanceof CheckpointAwareCloudEvent checkpointAware && checkpointAware.getCheckpoint() instanceof GlobalCheckpoint global) {
                return new CheckpointAwareCloudEvent(checkpointAware.getOriginalCloudEvent(), GlobalCheckpoint.of(global.position(), liveFrom, replayOrigin, head));
            }
            return cloudEvent;
        }
    }

    /**
     * Resolves where a replay from {@code start} reads from and goes live from. A {@code start} that has a live
     * token keeps it while {@code subscriptionModel} can still resume from it. Otherwise the replay starts over from
     * the stored start position with a token read now. A {@code start} without a live token gets one read now, before
     * the replay. {@code subscriptionId} only names the subscription in the warning and may be null.
     */
    Mono<ReplayStart> resolveStart(CheckpointAwareSubscriptionModel subscriptionModel, GlobalCheckpoint start, @Nullable String subscriptionId) {
        Checkpoint storedLiveFrom = start.liveFrom().orElse(null);
        if (storedLiveFrom == null) {
            return captureLiveToken(subscriptionModel).map(liveToken -> new ReplayStart(start.position(), liveToken, start.position(), null));
        }
        long replayOrigin = start.replayOrigin().orElse(start.position());
        long storedReplayTo = start.replayTo().orElseThrow();
        return subscriptionModel.canResumeFrom(storedLiveFrom).flatMap(canResume -> {
            if (canResume) {
                return Mono.just(new ReplayStart(start.position(), storedLiveFrom, replayOrigin, storedReplayTo));
            }
            log.warn("The subscription model no longer has the history from the live start stored for catch-up subscription {}, so the catch-up replays again from position {} instead of {} and redelivers the events in between. Live start: {}",
                    subscriptionId == null ? "(unnamed)" : subscriptionId, replayOrigin, start.position(), storedLiveFrom.asString());
            return captureLiveToken(subscriptionModel).map(liveToken -> new ReplayStart(replayOrigin, liveToken, replayOrigin, null));
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
     * The replay half on its own: bulk windows then one reconcile pass, with only the reconcile pass recording its
     * events in {@code cache}. The history windows record nothing, matching the blocking pipeline, because a position
     * is reserved before its write commits, so a write in flight when the head was read can be read by a history
     * window and needs the live delivery that the cache would otherwise suppress. Dedup by an event's (id, source),
     * not position, so an in-flight event never seen during the replay is still delivered once, live. Used by the
     * cold pipeline above. The named catch-up path in {@code NamedCatchupSupport} uses {@link #replayApplying}
     * instead, which applies the same rule.
     */
    Flux<CloudEvent> replay(ReplayStart start, BoundedIdCache<CatchupEventKey> cache) {
        return start.head(reader).flatMapMany(bulkHead -> {
            Flux<CloudEvent> bulk = windows(start.replayFrom(), bulkHead, null);
            Flux<CloudEvent> reconcile = start.reconciles() ? reconcile(bulkHead, cache) : Flux.empty();
            return Flux.concat(bulk, reconcile).map(cloudEvent -> start.stamp(cloudEvent, bulkHead));
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
    Flux<Void> replayApplying(ReplayStart start, BoundedIdCache<CatchupEventKey> cache, BooleanSupplier keepReplaying,
                              Function<CloudEvent, Mono<Void>> action, Runnable reconcileStarting) {
        return start.head(reader).flatMapMany(bulkHead -> {
            Function<CloudEvent, Mono<Void>> stampedAction = cloudEvent -> action.apply(start.stamp(cloudEvent, bulkHead));
            return Flux.concat(
                    windows(start.replayFrom(), bulkHead, null).takeWhile(ignored -> keepReplaying.getAsBoolean()).concatMap(stampedAction),
                    Mono.defer(() -> {
                        if (!keepReplaying.getAsBoolean()) {
                            return Mono.empty();
                        }
                        reconcileStarting.run();
                        return Mono.empty();
                    }),
                    // Called inside the defer rather than passed into it, since building the reconciliation Flux
                    // reads the head.
                    Flux.defer(() -> keepReplaying.getAsBoolean() && start.reconciles()
                            ? reconcile(bulkHead, cache).takeWhile(ignored -> keepReplaying.getAsBoolean()).concatMap(stampedAction)
                            : Flux.empty()));
        });
    }

    // Emits events in (fromExclusive, toInclusive], paging in position windows. A null cache records nothing, which is
    // what the history windows pass, so the live stream can deliver a history event again. Used by both the bulk and
    // the reconciliation phases.
    private Flux<CloudEvent> windows(long fromExclusive, long toInclusive, @Nullable BoundedIdCache<CatchupEventKey> cache) {
        if (fromExclusive >= toInclusive) {
            return Flux.empty();
        }
        long upTo = Math.min(fromExclusive + windowSize, toInclusive);
        Flux<CloudEvent> window = reader.readWindow(fromExclusive, upTo);
        if (cache != null) {
            window = window.doOnNext(event -> cache.add(CatchupEventKey.of(event)));
        }
        return window.concatWith(Flux.defer(() -> windows(upTo, toInclusive, cache)));
    }

    // Snapshot the head once and drain events up to it in position order. Re-reading a moving head would advance
    // forever under sustained writes and never hand over to live (livelock). Anything after the snapshot is
    // covered by the live change stream (resumes from the pre-bulk token), deduped by the (id, source) cache.
    private Flux<CloudEvent> reconcile(long cursor, BoundedIdCache<CatchupEventKey> cache) {
        return reader.currentHead().flatMapMany(snapshotHead -> windows(cursor, snapshotHead, cache));
    }
}
