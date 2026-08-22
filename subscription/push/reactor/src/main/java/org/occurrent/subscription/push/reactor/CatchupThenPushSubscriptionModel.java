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
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.RegisteringSubscribable;
import org.occurrent.subscription.api.reactor.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.api.reactor.internal.ReactiveHandover;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.internal.HandoverMessages;
import org.occurrent.subscription.internal.ReplayFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;

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
 * It is a {@link SubscriptionModel} but not a {@code FluxSubscriptionModel}: the latter is the bare
 * {@code Flux}-returning primitive, which a model fed by a push source rather than by reading a change stream cannot
 * honour. Most of the life cycle is a fan-out to the live feed. What this model adds is an answer for the window where
 * a replay is in flight, which the live feed cannot give because it is buffering rather than delivering.
 */
@NullMarked
public class CatchupThenPushSubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, ReplayAwareSubscriptions {

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
    // Who to tell about each id's catch-up boundaries, registered before the subscription that produces them. Kept
    // until this model shuts down, since the registration outlives any one catch-up: a stop and start, a resume, or
    // a cancel and re-subscribe all run another catch-up for the same id, and a recorder that stopped being told
    // would record that catch-up's history as though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // A pause asked for while a replay is in flight. The replay itself keeps running, since resuming it would mean
    // persisting the exact replay cursor, which this model does not do. Applied at the handover instead.
    private final ConcurrentMap<String, Boolean> pauseRequestedDuringReplay = new ConcurrentHashMap<>();
    // How to launch a subscription's replay again, kept only while there is a replay worth launching, matching the
    // blocking twin. Removed once one finishes, once one fails (it is refusing, not stopped), and on cancel or
    // shutdown. What is left is exactly the replays a stop interrupted, which start(true) and resumeSubscription
    // bring back (ADR 104).
    private final ConcurrentMap<String, Supplier<Sinks.One<Boolean>>> interruptibleReplays = new ConcurrentHashMap<>();

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
            throw new UnsupportedStartAtException(startAt, HandoverMessages.NON_DEFAULT_START_AT_NOT_SUPPORTED);
        }
        // Fail fast on a filter that cannot be replayed, before registering anything on the live feed.
        Filter replayFilter = ReplayFilters.replayFilterFor(filter);

        ReactiveHandover<CloudEvent> handover = ReactiveHandover.create(action, CloudEvent::getId, options, "subscription");

        // Register on the live feed first, so events committing during the replay are buffered in the sink, not
        // lost. Buffering, the write path, uses acceptReportingDelivery(..), never acceptIfLive(..), which would
        // refuse rather than buffer a payload arriving during the replay. Only the dedicated pre-dispatch exception
        // is wrapped as a Refusal, so routeReportingMatch reports NOT_DELIVERABLE for it and DELIVERED for a
        // handler's own exception, the same one-evaluation fix the blocking stack already has.
        RegisteringSubscribable.RoutingAction routingAction = cloudEvent -> handover.acceptReportingDelivery(cloudEvent)
                .onErrorMap(ReactiveHandover.PreDispatchRefusalException.class, e -> e.thrownBy(handover)
                        ? new RegisteringSubscribable.RoutingAction.Refusal(e, handover.refusesPermanently())
                        // A different handover refused, which this handler reached by calling into it. This
                        // registration ran, so its own outcome is DELIVERED and the error propagates as any other
                        // handler failure would.
                        : e);
        liveFeed.subscribeCatchupThenPush(subscriptionId, filter, StartAt.subscriptionModelDefault(), routingAction);

        // Kept rather than launched once, so a replay a stop interrupts can be launched again over the same handover.
        // The handover has to be the same one: it holds the live sink and the de-dup cache, so a second one would
        // replay into a projection that had already seen part of the history.
        Supplier<Sinks.One<Boolean>> launch = () -> launchReplay(subscriptionId, handover, replayFilter);
        interruptibleReplays.put(subscriptionId, launch);
        return new CatchingUpSubscription(subscriptionId, launch.get());
    }

    // Starts one replay for subscriptionId and returns the signal that fires when it finishes, is stopped, or fails.
    // Called by subscribe, and again by start(true) or resumeSubscription for a replay that a stop interrupted.
    //
    // Relaunching is safe despite the handover's unicast live sink: it is subscribed only after the marker phase, and
    // a stop errors the pipeline before that, so an interrupted replay left it untouched. A replay that finished is
    // never relaunched, which is the case that would fail.
    private Sinks.One<Boolean> launchReplay(String subscriptionId, ReactiveHandover<CloudEvent> handover, Filter replayFilter) {
        // Registered before the replay is handed to the handover, which subscribes it on boundedElastic straight away.
        // isRunning(id) and keepReplaying() therefore answer for this subscription from the moment subscribe returns,
        // rather than from whenever that pipeline happens to get scheduled.
        Sinks.One<Boolean> replayDone = Sinks.one();
        // Cleared here rather than only on the exit paths, so this attempt starts in the history part of its
        // catch-up whatever the previous attempt for the same id left behind.
        replayingSubscriptions.put(subscriptionId, replayDone);
        // Sent inside the map operation that holds this id, which cancelSubscription's own remove also takes, so a
        // cancel and a fresh subscribe cannot slip between taking the id and sending. An attempt that no longer
        // owns the id sends nothing, or its start would arrive after its replacement's and the recorder would adopt
        // a catch-up that is already over. The replay itself is the episode, so a later attempt starts its own.
        replayingSubscriptions.computeIfPresent(subscriptionId, (id, owner) -> {
            if (owner == replayDone) {
                CatchupListener startListener = catchupListeners.get(subscriptionId);
                if (startListener != null) {
                    startListener.catchupStarted(replayDone);
                }
            }
            return owner;
        });

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

            @Override
            public void liveDrained() {
                // Kept registered until here rather than dropped when the catch-up reports done, because the payloads
                // buffered while the history was read are delivered after that and each of them exactly once. A
                // recording projection has to see them as part of this catch-up, not as live delivery.
                forget(subscriptionId, replayDone);
            }

            @Override
            public void historyDone() {
                // The replay itself is the episode, so a listener a later attempt for this id has since started
                // ignores this and no lock is needed to keep this attempt from speaking for that one.
                CatchupListener listener = catchupListeners.get(subscriptionId);
                if (listener != null) {
                    listener.historyRead(replayDone);
                }
            }
        });

        // Subscribed here rather than only handed back, so a caller that never waits still gets the bookkeeping below.
        // Logged because under startupMode = BACKGROUND nobody waits, and a failure would otherwise reach no one.
        catchupDone.subscribe(
                caughtUp -> {
                    if (caughtUp) {
                        interruptibleReplays.remove(subscriptionId);
                        // Not forgotten here: liveDrained does it, once the payloads buffered during the history read
                        // have been delivered.
                        applyPendingPauseIfAny(subscriptionId);
                    } else {
                        // Stopped rather than failed, so the handover is intact, nothing is marked, and both the
                        // registration and the launcher are kept: start(true) replays the whole history again, the
                        // answer CatchupProjectionFeed.stopCatchUp() already records (ADR 104). Forgetting the replay
                        // entry last is what makes "launcher present, nothing replaying" mean stopped.
                        forget(subscriptionId, replayDone);
                    }
                    replayDone.tryEmitValue(caughtUp);
                },
                error -> {
                    // The registration stays, matching the blocking twin. The handover recorded this failure and is
                    // itself the registered action, so every later live event is refused rather than acknowledged and
                    // the broker keeps holding them (ADR 104). Recovery is cancelSubscription(id) plus a fresh
                    // subscribe. Dropped before the replay entry, so a start(true) racing this never sees a launcher
                    // with no replay running and relaunches a catch-up that failed.
                    log.error("Catch-up failed for subscription {}. Its registration on the live feed is kept and now "
                            + "refuses every event, so the source redelivers rather than losing them. Cancel the "
                            + "subscription and subscribe again once the cause is fixed.", subscriptionId, error);
                    interruptibleReplays.remove(subscriptionId);
                    forget(subscriptionId, replayDone);
                    replayDone.tryEmitError(error);
                });

        return replayDone;
    }

    // Relaunches the replay for subscriptionId if a stop interrupted it, and returns its signal, or null if there was
    // nothing to relaunch. A replay is restartable when its launcher survived and nothing is replaying under that id,
    // which is exactly the state a stop leaves behind.
    //
    // Synchronized because the check and the launch have to be one step. Two callers reaching this together (start and
    // resumeSubscription, or two starts) would otherwise both see nothing replaying and put two replays on one
    // handover, and the replay phase folds every event without consulting the de-dup cache, so the history would be
    // applied twice. Lifecycle calls are rare enough that the lock costs nothing.
    private synchronized Sinks.@Nullable One<Boolean> relaunchInterruptedReplay(String subscriptionId) {
        Supplier<Sinks.One<Boolean>> launch = interruptibleReplays.get(subscriptionId);
        if (launch == null || replayingSubscriptions.containsKey(subscriptionId)) {
            return null;
        }
        // Unpaused here rather than by the caller, so a caller that loses the race above does not leave the
        // subscription unpaused and then try to resume it a second time, which the live feed refuses. A no-op under
        // start(true), which cleared every pause before calling this.
        if (liveFeed.isPaused(subscriptionId)) {
            liveFeed.resumeSubscription(subscriptionId);
        }
        return launch.get();
    }

    private void forget(String subscriptionId, Sinks.One<Boolean> replay) {
        replayingSubscriptions.remove(subscriptionId, replay);
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

    /**
     * Stops the live feed and any catch-up replay still in flight. Reversible: a stopped replay keeps its registration
     * on the live feed and is replayed from the beginning by {@link #start(boolean)}, because a stop is not a failure
     * and nothing was marked. That is the decision {@code CatchupProjectionFeed.stopCatchUp()} already records, ported
     * here rather than re-derived (ADR 104).
     * <p>
     * Live events fed while stopped are dropped rather than refused, the dropped-not-deferred contract every stopped
     * subscription model has (ADR 85). That is bounded here only because the stop is reversible: the window closes at
     * {@code start(..)}.
     */
    @Override
    public void stop() {
        stopped = true;
        liveFeed.stop();
    }

    /**
     * Starts the live feed and, when {@code resumeSubscriptionsAutomatically}, replays the history again for every
     * subscription whose catch-up {@link #stop()} interrupted.
     * <p>
     * A stop is not a failure, so nothing was marked and the replay starts from the beginning rather than from a
     * cursor this model does not keep. Under {@code start(false)} the interrupted replays are left for
     * {@link #resumeSubscription(String)} to pick up one at a time.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        // Before the replays, so the registrations they hand over to are unpaused by the time one finishes.
        liveFeed.start(resumeSubscriptionsAutomatically);
        if (resumeSubscriptionsAutomatically) {
            // Skips an id already replaying, so a start() while a replay is in flight does not put a second replay on
            // the same handover.
            interruptibleReplays.keySet().forEach(this::relaunchInterruptedReplay);
        }
    }

    @Override
    public boolean isRunning() {
        return !replayingSubscriptions.isEmpty() || liveFeed.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return replayingSubscriptions.containsKey(subscriptionId) || liveFeed.isRunning(subscriptionId);
    }

    /**
     * Whether {@code subscriptionId} is still replaying history and has not yet handed over to the live feed. Here
     * {@link #isRunning(String)} is {@code true} throughout the replay, matching what an event-store catch-up model
     * reports, which is why the handover needs an answer of its own.
     */
    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        catchupListeners.put(subscriptionId, listener);
        return true;
    }

    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return replayingSubscriptions.containsKey(subscriptionId);
    }

    /**
     * Whether {@code subscriptionId} is paused, counting a pause asked for while its replay was still running and
     * not yet applied to the live feed.
     */
    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringReplay.containsKey(subscriptionId) || liveFeed.isPaused(subscriptionId);
    }

    @Override
    public Set<String> subscriptionIds() {
        // A subscription is registered on the live feed before its replay is recorded, and the replay is only forgotten
        // when the live feed either keeps the registration or loses it too, so the live feed knows every id this model
        // knows. That is why this does not also read replayingSubscriptions, unlike isRunning.
        return liveFeed.subscriptionIds();
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
        Sinks.One<Boolean> relaunched = relaunchInterruptedReplay(subscriptionId);
        if (relaunched != null) {
            // Its catch-up was interrupted by a stop, so resuming it means replaying the history again, since this
            // model keeps no replay cursor to resume from.
            pauseRequestedDuringReplay.remove(subscriptionId);
            return new CatchingUpSubscription(subscriptionId, relaunched);
        }
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
        // A cancel is not a stop, so nothing is kept to launch again. This is also the recovery from a failed
        // catch-up: it frees the id and releases the registration that was refusing (ADR 104).
        interruptibleReplays.remove(subscriptionId);
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
        catchupListeners.clear();
        pauseRequestedDuringReplay.clear();
        // Unlike stop(), a shutdown keeps nothing to launch again: it drops the registrations too.
        interruptibleReplays.clear();
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
     * <p>
     * It tracks the one replay it was created for. A replay that {@link #stop()} interrupted completes here and stays
     * completed after {@link #start(boolean)} launches a fresh one, since this handle cannot see it. Ask
     * {@link #isRunning(String)} about a restarted replay, or take the handle {@link #resumeSubscription(String)}
     * hands back.
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
