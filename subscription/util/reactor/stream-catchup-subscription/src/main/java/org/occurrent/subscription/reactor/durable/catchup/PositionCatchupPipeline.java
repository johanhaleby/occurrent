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
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.reactor.HistoryLostException;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.LongestReplay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Bulk-then-reconcile-then-live handover shared by every position-ordered reactive catch-up model. A
 * {@link CatchupScope} supplies the live start and the window and head reads so this pipeline is store-agnostic,
 * reused by both the stream and DCB catch-up models. One scope covers one replay attempt, from resolving where it
 * starts to its last read, and is closed when the attempt ends, however it ends.
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
 * token after a restart. When that position is above zero, the event gets the same in a marked
 * {@link StringBasedCheckpoint} instead, which a version before it refuses rather than replays from with a token read
 * then. A token read after the restart would be too late for an event whose position was reserved
 * below the stored position but written only after the earlier replay had read past it. The resumed replay stops at
 * the stored head and does not reconcile, since every event above that head was written after the token and arrives
 * live. When the wrapped model no longer has the history from a
 * stored token, the replay starts over from the stored start position with a token read now, and redelivers the
 * events in between.
 * <p>
 * If the model reports no resume token at all (e.g. an empty oplog or a restricted cluster), the catch-up fails. A
 * token that leaves the change stream history (e.g. the MongoDB oplog window) during a long replay is not handed
 * over. The replay runs again from the position the first attempt started from, with a token read now, and
 * redelivers the events in between. A catch-up whose replay loses the token 4 times in a row fails instead of
 * replaying a fifth time. A token can still leave the history after that check, before the wrapped model opens its
 * change stream at it. When the wrapped model has {@link HistoryLossReportingSubscriptions}, the live half is read
 * through {@link HistoryLossReportingSubscriptions#subscribeFailingIfStartLost}, and a loss of that token runs the
 * replay again from the position the first attempt started from, with a token read then. That counts as one of the
 * replays run again. Any other wrapped model handles the lost history as it would without this pipeline.
 */
@NullMarked
final class PositionCatchupPipeline {

    private static final Logger log = LoggerFactory.getLogger(PositionCatchupPipeline.class);
    // How many times a replay may run again from its origin after the first replay, when the wrapped model keeps
    // losing the token while the replay runs
    private static final int MAX_REPLAYS_AGAIN = 3;

    private final CatchupScopes scopes;
    private final long windowSize;
    private final int handoverCacheSize;

    public PositionCatchupPipeline(CatchupScopes scopes, long windowSize, int handoverCacheSize) {
        this.scopes = requireNonNull(scopes, CatchupScopes.class.getSimpleName() + " cannot be null");
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
        Objects.requireNonNull(start, "start cannot be null");
        if (CatchupCheckpoints.isMarkedTime(start)) {
            return Flux.error(startsAtTime(null, start));
        }
        CatchupCheckpoints.Position startPosition = positionOf(start);
        BoundedIdCache<CatchupEventKey> cache = new BoundedIdCache<>(handoverCacheSize);
        LongestReplay longestReplay = new LongestReplay();
        return replayThenLive(subscriptionModel, liveSubscriptionFilter, livePredicate, notBefore(startPosition), scope -> resolveStart(subscriptionModel, startPosition, null, scope.liveStart()), cache, 0, longestReplay);
    }

    /**
     * Reads where a replay from {@code start} starts, in any position form a catch-up stores. A caller refuses the
     * marked form of a time replay with {@link #startsAtTime} before this.
     *
     * @throws IllegalArgumentException if {@code start} is not a position a catch-up replays from
     */
    static CatchupCheckpoints.Position positionOf(Checkpoint start) {
        if (!(CatchupCheckpoints.parse(start) instanceof CatchupCheckpoints.Position position)) {
            throw new IllegalArgumentException("Not a position checkpoint: " + start.asString());
        }
        return position;
    }

    /**
     * The refusal of {@code start} when it is the marked form of a time replay, which only a blocking catch-up of a
     * stream subscription stores, since a reactor catch-up replays only by position. {@code subscriptionId} only names
     * the subscription, and may be null.
     */
    static CatchupCannotResumeException startsAtTime(@Nullable String subscriptionId, Checkpoint start) {
        return new CatchupCannotResumeException("Cannot run catch-up subscription " + (subscriptionId == null ? "(unnamed)" : subscriptionId) + " from checkpoint \"" + start.asString()
                + "\", because a catch-up that replays by time stored it, and this catch-up replays only by position."
                + " Run the subscription with the StreamCatchupSubscriptionModel that stored the checkpoint, or delete the checkpoint so that the subscription starts from its StartAt again.");
    }

    // Replays from the start firstStart resolves and goes live from its token, or replays again first when the token left
    // the change stream history during the replay. replaysAgain is how many times the replay already ran again. The
    // start is resolved and the replay read in one scope, which is closed when the replay ends.
    private Flux<CloudEvent> replayThenLive(CheckpointAwareSubscriptionModel subscriptionModel, SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                            @Nullable String notBefore, Function<CatchupScope, Mono<ReplayStart>> firstStart, BoundedIdCache<CatchupEventKey> cache, int replaysAgain,
                                            LongestReplay longestReplay) {
        return Flux.defer(() -> {
            AtomicReference<ReplayStart> replayed = new AtomicReference<>();
            return inScope(notBefore, firstStart, (replayStart, reader) -> replay(reader, replayStart, cache), replayed, longestReplay)
                    .concatWith(Flux.defer(() -> {
                        ReplayStart replayStart = replayed.get();
                        if (replayStart == null) {
                            return Flux.<CloudEvent>empty();
                        }
                        int counted = replayStart.countedFrom(replaysAgain);
                        return liveStartHolds(subscriptionModel, replayStart, null, counted, longestReplay).flatMapMany(holds -> {
                            if (!holds) {
                                return replayThenLive(subscriptionModel, liveSubscriptionFilter, livePredicate, null, fromOrigin(subscriptionModel, replayStart), cache, counted + 1, longestReplay);
                            }
                            Checkpoint liveFrom = replayStart.liveFrom();
                            StartAt liveStartAt = StartAt.checkpoint(liveFrom);
                            Flux<CloudEvent> live = (subscriptionModel instanceof SubscriptionModelCapability capabilities ? HistoryLossReportingSubscriptions.findIn(capabilities) : Optional.<HistoryLossReportingSubscriptions>empty())
                                    .map(reporting -> reporting.subscribeFailingIfStartLost(liveSubscriptionFilter, liveStartAt))
                                    .orElseGet(() -> subscriptionModel.subscribe(liveSubscriptionFilter, liveStartAt));
                            return live
                                    .filter(cloudEvent -> livePredicate.test(cloudEvent) && !cache.contains(CatchupEventKey.of(cloudEvent)))
                                    .onErrorResume(throwable -> throwable instanceof HistoryLostException lost && samePosition(lost.lostFrom(), liveFrom),
                                            __ -> replayThenLive(subscriptionModel, liveSubscriptionFilter, livePredicate, null, replayAgainAfterLiveStartLost(subscriptionModel, replayStart, null, counted, longestReplay),
                                                    cache, counted + 1, longestReplay));
                        });
                    }));
        });
    }

    /**
     * Runs one replay attempt in a scope of its own. The scope is opened, {@code start} resolves where the attempt
     * starts in it, and {@code replay} reads from there through it. The scope is closed when the replay completes,
     * fails or is cancelled, so the start is not resolved after the scope that its live start came from has ended.
     * The start is also stored in {@code resolved} for what follows the replay. It stays empty when {@code start}
     * emits nothing, or fails.
     *
     * @param notBefore The live start an earlier attempt stored, which the scope's reads must see everything up to, or null
     */
    <T> Flux<T> inScope(@Nullable String notBefore, Function<CatchupScope, Mono<ReplayStart>> start, BiFunction<ReplayStart, CatchupReader, Flux<T>> replay, AtomicReference<ReplayStart> resolved,
                        LongestReplay longestReplay) {
        return Flux.usingWhen(scopes.open(notBefore),
                scope -> {
                    longestReplay.liveStartRead();
                    return start.apply(scope).doOnNext(resolved::set).flatMapMany(replayStart -> replay.apply(replayStart, scope));
                },
                CatchupScope::close);
    }

    // A start that stored a live start asks the scope for reads that see everything up to it. By the string
    // form, as samePosition does.
    static @Nullable String notBefore(CatchupCheckpoints.Position start) {
        Checkpoint liveFrom = start.liveFrom();
        return liveFrom == null ? null : liveFrom.asString();
    }

    /**
     * Resolves, in the scope of the next attempt, the start a replay runs again from after the wrapped model lost the
     * history from the token {@code lost} handed over, as it opened its change stream at it. It replays from the
     * position the first attempt started from, with a token read in that scope. Fails with
     * {@link CatchupCannotResumeException} instead once the replay has already run again {@value #MAX_REPLAYS_AGAIN}
     * times, as {@link #liveStartHolds} does, naming the longest replay {@code longestReplay} measured.
     * {@code subscriptionId} only names the subscription in the warning and the failure, and may be null.
     */
    Function<CatchupScope, Mono<ReplayStart>> replayAgainAfterLiveStartLost(CheckpointAwareSubscriptionModel subscriptionModel, ReplayStart lost, @Nullable String subscriptionId, int replaysAgain,
                                                                           LongestReplay longestReplay) {
        return scope -> Mono.defer(() -> {
            if (replaysAgain >= MAX_REPLAYS_AGAIN) {
                return Mono.error(liveStartLostEachTime(subscriptionId, replaysAgain + 1, lost.liveFrom(), longestReplay));
            }
            log.warn("The subscription model lost the history from the live start of catch-up subscription {} as it opened, so the catch-up replays again from position {} and redelivers the events in between. Live start: {}",
                    subscriptionId == null ? "(unnamed)" : subscriptionId, lost.replayOrigin(), lost.liveFrom().asString());
            return fromOrigin(subscriptionModel, lost).apply(scope);
        });
    }

    /**
     * Resolves, in the scope of the next attempt, the start that replays again from the position the first attempt at
     * {@code replayed} started from, with a token read in that scope.
     */
    Function<CatchupScope, Mono<ReplayStart>> fromOrigin(CheckpointAwareSubscriptionModel subscriptionModel, ReplayStart replayed) {
        return scope -> captureLiveToken(subscriptionModel, scope.liveStart()).map(liveToken -> new ReplayStart(replayed.replayOrigin(), liveToken, replayed.replayOrigin(), null));
    }

    // By the string form, since a live start is a typed checkpoint where the wrapped model reads it and a
    // StringBasedCheckpoint where a storage that keeps strings reads it back
    static boolean samePosition(Checkpoint checkpoint, @Nullable Checkpoint other) {
        return other != null && checkpoint.asString().equals(other.asString());
    }

    private static CatchupCannotResumeException liveStartLostEachTime(@Nullable String subscriptionId, int replays, Checkpoint lastLiveStart, LongestReplay longestReplay) {
        return new CatchupCannotResumeException("Cannot hand catch-up subscription " + (subscriptionId == null ? "(unnamed)" : subscriptionId)
                + " over to live delivery, because the subscription model lost the live start during or right after each of its " + replays
                + " replays. The longest replay took " + longestReplay.longest().toMillis() + " ms from reading its live start to checking it, so size the change stream history, such as the MongoDB oplog, to reach back at least that far. Last live start: "
                + lastLiveStart.asString());
    }

    /**
     * Checks, after a replay and before its handover, that {@code subscriptionModel} can still resume from the token
     * {@code replayed} hands over to. Emits {@code true} when it can. Otherwise the token left the change stream
     * history during the replay, and this emits {@code false}, which tells the caller to replay again with
     * {@link #fromOrigin}: from the position the first attempt started from, with a token read in the scope of that
     * attempt. Handing the lost token over instead would leave it to the wrapped model, and a MongoDB model that
     * restarts on lost history goes live from the present and skips every event between the token and the restart. A
     * token read now is asked about after its own replay too, since a long replay can lose it as well.
     * <p>
     * {@code replaysAgain} is how many times the replay has already run again. Once that is
     * {@value #MAX_REPLAYS_AGAIN} and the token is lost once more, this fails with
     * {@link CatchupCannotResumeException} instead, naming the longest time {@code longestReplay} measured from a
     * replay reading its token to this check. So a catch-up goes live only from a token the wrapped model accepted after the last replay, or fails
     * after {@value #MAX_REPLAYS_AGAIN} replays run again. It never hands over a token the wrapped model answered false
     * for, but delivers the events it replays again more than once. The check and the handover are two calls, so a
     * token can still leave the history between them, which {@link #replayAgainAfterLiveStartLost} deals with when
     * the wrapped model reports it. {@code subscriptionId} only names the subscription in the warning and the failure, and may be null.
     */
    Mono<Boolean> liveStartHolds(CheckpointAwareSubscriptionModel subscriptionModel, ReplayStart replayed, @Nullable String subscriptionId, int replaysAgain, LongestReplay longestReplay) {
        return Mono.defer(() -> {
            longestReplay.liveStartChecked();
            return subscriptionModel.canResumeFrom(replayed.liveFrom());
        }).flatMap(canResume -> {
            if (canResume) {
                return Mono.just(true);
            }
            if (replaysAgain >= MAX_REPLAYS_AGAIN) {
                return Mono.error(liveStartLostEachTime(subscriptionId, replaysAgain + 1, replayed.liveFrom(), longestReplay));
            }
            log.warn("The live start of catch-up subscription {} left the subscription model's history during the replay, so the catch-up replays again from position {} and redelivers the events in between. Live start: {}",
                    subscriptionId == null ? "(unnamed)" : subscriptionId, replayed.replayOrigin(), replayed.liveFrom().asString());
            return Mono.just(false);
        });
    }

    /**
     * Where a replay starts, the live token it hands over to, the position the first attempt at it started from, and
     * the head an earlier attempt read after it read that token, or null when this attempt reads the head itself.
     * {@code startsOver} is whether it replays from that first position because the wrapped model lost the token an
     * earlier attempt stored, which counts as a replay run again.
     */
    record ReplayStart(long replayFrom, Checkpoint liveFrom, long replayOrigin, @Nullable Long replayTo, boolean startsOver) {
        ReplayStart {
            requireNonNull(liveFrom, "liveFrom cannot be null");
            if (replayFrom < 0) {
                throw new IllegalArgumentException("replayFrom cannot be negative, was " + replayFrom);
            }
        }

        ReplayStart(long replayFrom, Checkpoint liveFrom, long replayOrigin, @Nullable Long replayTo) {
            this(replayFrom, liveFrom, replayOrigin, replayTo, false);
        }

        // How many times the replay has run again once this start runs, given the count of the replays before it
        int countedFrom(int replaysAgain) {
            return startsOver ? Math.max(replaysAgain, 1) : replaysAgain;
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
        // the head the replay read. In the marked form for an origin above zero, which an earlier version would
        // replay from with a token read then. An event without one is passed on as it is.
        CloudEvent stamp(CloudEvent cloudEvent, long head) {
            if (cloudEvent instanceof CheckpointAwareCloudEvent checkpointAware && checkpointAware.getCheckpoint() instanceof GlobalCheckpoint global) {
                Checkpoint stamped = replayOrigin == 0 ? GlobalCheckpoint.of(global.position(), liveFrom, replayOrigin, head)
                        : new StringBasedCheckpoint(CatchupCheckpoints.format(global.position(), replayOrigin, head, liveFrom));
                return new CheckpointAwareCloudEvent(checkpointAware.getOriginalCloudEvent(), stamped);
            }
            return cloudEvent;
        }
    }

    /**
     * Resolves where a replay from {@code start} reads from and goes live from. A {@code start} that has a live
     * token keeps it while {@code subscriptionModel} can still resume from it. Otherwise the replay starts over from
     * the stored start position with a token read now, which counts as a replay run again. A {@code start} without a
     * live token gets one read now, before the replay. {@code subscriptionId} only names the subscription in the warning and may be null. {@code liveStart}
     * is the one the scope the start is resolved in reports, see {@link #captureLiveToken}.
     */
    Mono<ReplayStart> resolveStart(CheckpointAwareSubscriptionModel subscriptionModel, CatchupCheckpoints.Position start, @Nullable String subscriptionId, @Nullable String liveStart) {
        Checkpoint storedLiveFrom = start.liveFrom();
        if (storedLiveFrom == null) {
            return captureLiveToken(subscriptionModel, liveStart).map(liveToken -> new ReplayStart(start.position(), liveToken, start.position(), null));
        }
        long replayOrigin = start.replayOrigin();
        Long storedReplayTo = start.replayTo();
        return subscriptionModel.canResumeFrom(storedLiveFrom).flatMap(canResume -> {
            if (canResume) {
                return Mono.just(new ReplayStart(start.position(), storedLiveFrom, replayOrigin, storedReplayTo));
            }
            log.warn("The subscription model no longer has the history from the live start stored for catch-up subscription {}, so the catch-up replays again from position {} instead of {} and redelivers the events in between. Live start: {}",
                    subscriptionId == null ? "(unnamed)" : subscriptionId, replayOrigin, start.position(), storedLiveFrom.asString());
            return captureLiveToken(subscriptionModel, liveStart).map(liveToken -> new ReplayStart(replayOrigin, liveToken, replayOrigin, null, true));
        });
    }

    /**
     * Captures the live resume token before the bulk replay so an event committing during the replay is still
     * delivered live. It is the {@code liveStart} the attempt's scope reports, which the event store read in the same
     * session as the replay reads, or the wrapped model's global checkpoint when the store reports none. If there is
     * no token (e.g. an empty oplog or a restricted cluster) a no-loss handover cannot be guaranteed, so it fails
     * loudly instead of silently dropping events. Shared by the cold pipeline above and the named catch-up path in
     * {@code NamedCatchupSupport}.
     */
    Mono<Checkpoint> captureLiveToken(CheckpointAwareSubscriptionModel subscriptionModel, @Nullable String liveStart) {
        if (liveStart != null) {
            return Mono.just(new StringBasedCheckpoint(liveStart));
        }
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
    Flux<CloudEvent> replay(CatchupReader reader, ReplayStart start, BoundedIdCache<CatchupEventKey> cache) {
        return start.head(reader).flatMapMany(bulkHead -> {
            Flux<CloudEvent> bulk = windows(reader, start.replayFrom(), bulkHead, null);
            Flux<CloudEvent> reconcile = start.reconciles() ? reconcile(reader, bulkHead, cache) : Flux.empty();
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
    Flux<Void> replayApplying(CatchupReader reader, ReplayStart start, BoundedIdCache<CatchupEventKey> cache, BooleanSupplier keepReplaying,
                              Function<CloudEvent, Mono<Void>> action, Runnable reconcileStarting) {
        return start.head(reader).flatMapMany(bulkHead -> {
            Function<CloudEvent, Mono<Void>> stampedAction = cloudEvent -> action.apply(start.stamp(cloudEvent, bulkHead));
            return Flux.concat(
                    windows(reader, start.replayFrom(), bulkHead, null).takeWhile(ignored -> keepReplaying.getAsBoolean()).concatMap(stampedAction),
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
                            ? reconcile(reader, bulkHead, cache).takeWhile(ignored -> keepReplaying.getAsBoolean()).concatMap(stampedAction)
                            : Flux.empty()));
        });
    }

    // Emits events in (fromExclusive, toInclusive], paging in position windows. A null cache records nothing, which is
    // what the history windows pass, so the live stream can deliver a history event again. Used by both the bulk and
    // the reconciliation phases.
    private Flux<CloudEvent> windows(CatchupReader reader, long fromExclusive, long toInclusive, @Nullable BoundedIdCache<CatchupEventKey> cache) {
        if (fromExclusive >= toInclusive) {
            return Flux.empty();
        }
        long upTo = Math.min(fromExclusive + windowSize, toInclusive);
        Flux<CloudEvent> window = reader.readWindow(fromExclusive, upTo);
        if (cache != null) {
            window = window.doOnNext(event -> cache.add(CatchupEventKey.of(event)));
        }
        return window.concatWith(Flux.defer(() -> windows(reader, upTo, toInclusive, cache)));
    }

    // Snapshot the head once and drain events up to it in position order. Re-reading a moving head would advance
    // forever under sustained writes and never hand over to live (livelock). Anything after the snapshot is
    // covered by the live change stream (resumes from the pre-bulk token), deduped by the (id, source) cache.
    private Flux<CloudEvent> reconcile(CatchupReader reader, long cursor, BoundedIdCache<CatchupEventKey> cache) {
        return reader.currentHead().flatMapMany(snapshotHead -> windows(reader, cursor, snapshotHead, cache));
    }
}
