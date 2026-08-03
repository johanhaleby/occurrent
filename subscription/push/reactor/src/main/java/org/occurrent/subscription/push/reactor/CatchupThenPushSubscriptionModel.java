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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.subscription.api.reactor.internal.ReactiveHandover;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.internal.ReplayFilters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * {@code CatchupProjectionFeed}. The replay runs on {@code boundedElastic} rather than on the thread that called
 * {@link #subscribe}, so {@code waitUntilStarted()} is the only thing that joins it.
 * <p>
 * It implements {@link SubscriptionModelLifeCycle} rather than the reactor {@code SubscriptionModel}, which is the
 * bare {@code Flux}-returning change-stream primitive this wrapper cannot honour. Most of the life cycle is a fan-out
 * to the live feed. What this model adds is an answer for the window where a replay is in flight, which the live feed
 * cannot give because it is buffering rather than delivering.
 */
@NullMarked
public class CatchupThenPushSubscriptionModel implements Subscribable, SubscriptionModelLifeCycle {

    private static final Logger log = LoggerFactory.getLogger(CatchupThenPushSubscriptionModel.class);

    // Long enough that a replay noticing the shutdown at its next event always makes it, short enough that a parked
    // fold cannot hold a closing context open. Matches the blocking twin.
    private static final Duration SHUTDOWN_REPLAY_TIMEOUT = Duration.ofSeconds(5);

    private final PositionOrderedReader reader;
    private final PushSubscriptionModel liveFeed;
    private final @Nullable CheckpointStorage catchupMarker;
    private final CatchupThenLiveOptions options;

    // Set by stop(), cleared by start(...). Read by the replay so stopping the model interrupts a replay in flight,
    // not just the live feed the replay has not handed over to yet.
    private volatile boolean stopped = false;
    private volatile boolean shuttingDown = false;
    // Subscriptions whose replay is running, each mapped to the signal that fires when it finishes, is stopped, or
    // fails. The live feed cannot answer for them: it knows the id (this model registers there first) but it is
    // buffering rather than delivering, so it would report a subscription that is not yet folding anything as running.
    private final ConcurrentMap<String, Sinks.One<Boolean>> replayingSubscriptions = new ConcurrentHashMap<>();
    // A pause asked for while a replay is in flight. The replay itself keeps running, since resuming it would mean
    // persisting the exact replay cursor, which this model does not do. Applied at the handover instead.
    private final ConcurrentMap<String, Boolean> pauseRequestedDuringReplay = new ConcurrentHashMap<>();

    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker) {
        this(reader, liveFeed, catchupMarker, CatchupThenLiveOptions.defaults());
    }

    public CatchupThenPushSubscriptionModel(PositionOrderedReader reader, PushSubscriptionModel liveFeed, @Nullable CheckpointStorage catchupMarker, CatchupThenLiveOptions options) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
        if (!reader.writesPosition()) {
            throw new IllegalArgumentException(HandoverMessages.POSITIONED_READER_REQUIRED);
        }
        this.liveFeed = Objects.requireNonNull(liveFeed, "liveFeed cannot be null");
        this.catchupMarker = catchupMarker;
        this.options = Objects.requireNonNull(options, "options cannot be null");
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!startAt.isDefault()) {
            throw new IllegalArgumentException(HandoverMessages.NON_DEFAULT_START_AT_NOT_SUPPORTED);
        }
        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        ReactiveHandover<CloudEvent> handover = ReactiveHandover.create(action, CloudEvent::getId, options);

        // Register on the live feed first, so events committing during the replay are buffered in the sink, not lost.
        liveFeed.subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), handover::accept);

        // Registered before the replay is handed to the handover, which subscribes it on boundedElastic straight away.
        // isRunning(id) and keepReplaying() therefore answer for this subscription from the moment subscribe returns,
        // rather than from whenever that pipeline happens to get scheduled.
        Sinks.One<Boolean> replayDone = Sinks.one();
        replayingSubscriptions.put(subscriptionId, replayDone);

        Mono<Boolean> catchupDone = handover.catchUp(new ReactiveHandover.Source<>() {
            @Override
            public Mono<Boolean> isAlreadyCaughtUp() {
                return CatchupThenPushSubscriptionModel.this.alreadyCaughtUp(subscriptionId);
            }

            @Override
            public Flux<CloudEvent> replay() {
                return reader.readInPositionOrder(replayFilter, PositionRange.fromBeginning());
            }

            @Override
            public boolean keepReplaying() {
                return shouldKeepReplaying(subscriptionId);
            }

            @Override
            public Mono<Void> markCaughtUp() {
                return CatchupThenPushSubscriptionModel.this.markCaughtUp(subscriptionId);
            }
        });

        // Subscribed here rather than only handed back, so a caller that never waits still gets the release below. The
        // handover was registered before the replay, so a failure would otherwise leave a handler that fails every
        // later event while holding the id. The release runs before replayDone is emitted, so a caller that does wait
        // finds the id free and can re-subscribe under it. Logged because under startupMode = BACKGROUND nobody waits,
        // and the failure would otherwise reach no one.
        catchupDone.subscribe(
                caughtUp -> {
                    forget(subscriptionId);
                    if (caughtUp) {
                        applyPendingPauseIfAny(subscriptionId);
                    } else {
                        // Stopped rather than failed, so the handover is intact and nothing is marked. Release the
                        // registration anyway: nothing will revive this replay, and leaving it registered would leave
                        // a subscription that silently drops every live event.
                        liveFeed.cancelSubscription(subscriptionId);
                    }
                    replayDone.tryEmitValue(caughtUp);
                },
                error -> {
                    log.error("Catch-up failed for subscription {}, releasing its registration on the live feed. It received no "
                            + "replay and will receive no live events until it is subscribed again.", subscriptionId, error);
                    forget(subscriptionId);
                    liveFeed.cancelSubscription(subscriptionId);
                    replayDone.tryEmitError(error);
                });

        return new CatchingUpSubscription(subscriptionId, replayDone);
    }

    private void forget(String subscriptionId) {
        replayingSubscriptions.remove(subscriptionId);
    }

    /**
     * Whether the replay for {@code subscriptionId} should keep going: the model is neither shutting down nor stopped,
     * and the subscription has not been cancelled out from under it.
     */
    private boolean shouldKeepReplaying(String subscriptionId) {
        return !shuttingDown && !stopped && replayingSubscriptions.containsKey(subscriptionId);
    }

    private void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null) {
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    // --- Life cycle. Mirrors the blocking twin: the live feed owns delivery, so most of this is a fan-out, and what
    // this model adds is an answer for the window where a replay is in flight. ---

    @Override
    public void stop() {
        stopped = true;
        liveFeed.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        liveFeed.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return !replayingSubscriptions.isEmpty() || liveFeed.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return replayingSubscriptions.containsKey(subscriptionId) || liveFeed.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringReplay.containsKey(subscriptionId) || liveFeed.isPaused(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (replayingSubscriptions.containsKey(subscriptionId)) {
            // The live feed would accept the pause, but the replay does not go through it, so pausing there now would
            // report the subscription paused while its history keeps folding. Record it and apply it at the handover.
            pauseRequestedDuringReplay.put(subscriptionId, true);
        } else {
            liveFeed.pauseSubscription(subscriptionId);
        }
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (pauseRequestedDuringReplay.remove(subscriptionId) != null) {
            // Paused and resumed while its replay was still running, so the live feed was never told and has nothing
            // to resume. Dropping the request is the whole of it, but hand back a handle that still tracks the replay
            // rather than one that claims to be started.
            Sinks.One<Boolean> replay = replayingSubscriptions.get(subscriptionId);
            if (replay != null) {
                return new CatchingUpSubscription(subscriptionId, replay);
            }
            // The replay finished between dropping the request and looking it up, so whether the handover managed to
            // apply the pause first is a race. Resume only if it actually landed, since the live feed refuses to
            // resume a subscription it never paused.
            return liveFeed.isPaused(subscriptionId)
                    ? liveFeed.resumeSubscription(subscriptionId)
                    : new CatchingUpSubscription(subscriptionId, finished());
        }
        return liveFeed.resumeSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        // Removing it here is what stops a replay in flight: shouldKeepReplaying reads this map.
        replayingSubscriptions.remove(subscriptionId);
        pauseRequestedDuringReplay.remove(subscriptionId);
        liveFeed.cancelSubscription(subscriptionId);
    }

    /**
     * Stops every replay still in flight and waits for them to unwind before shutting the live feed down.
     * <p>
     * The waiting is the point. A replay runs on a scheduler thread, so without it a context that is closing would
     * leave one folding into a store that is closing with it, surfacing as an error from a thread nobody owns. A
     * replay notices the shutdown at its next event, so the wait is normally brief. It gives up after five seconds
     * anyway, because the fold is application code and may never return.
     */
    @Override
    public void shutdown() {
        shuttingDown = true;
        awaitReplays(SHUTDOWN_REPLAY_TIMEOUT);
        replayingSubscriptions.clear();
        pauseRequestedDuringReplay.clear();
        liveFeed.shutdown();
    }

    private void awaitReplays(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (Sinks.One<Boolean> replay : replayingSubscriptions.values()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                // toFuture().get rather than block(), which Reactor rejects outright on a non-blocking scheduler
                // thread. That rejection is a RuntimeException, so it would be swallowed below and skip the wait
                // entirely, which is the one thing this method exists to do.
                replay.asMono().toFuture().get(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | TimeoutException e) {
                // A failed replay was already reported to whoever waited on this subscription, and a shutdown has
                // nowhere useful to put it, so keep waiting for the rest. A timeout lands here too, and the deadline
                // check above ends the loop on the next iteration rather than this one.
            }
        }
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

    /**
     * A subscription whose catch-up is running on a scheduler thread. {@code waitUntilStarted} is the only thing that
     * joins it, which is what lets a caller choose to keep the replay off the startup path. It completes when the
     * replay finished or was stopped, and errors when it failed: a projection's runner discards the value, so
     * swallowing a replay failure would start an application whose read model is silently empty.
     */
    private record CatchingUpSubscription(String id, Sinks.One<Boolean> replay) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return replay.asMono().then();
        }
    }

    // A signal that is already done, for a handle handed back when there is no longer a replay to track. The blocking
    // twin does the same with CompletableFuture.completedFuture(true) rather than a second Subscription type.
    private static Sinks.One<Boolean> finished() {
        Sinks.One<Boolean> done = Sinks.one();
        done.tryEmitValue(true);
        return done;
    }
}
