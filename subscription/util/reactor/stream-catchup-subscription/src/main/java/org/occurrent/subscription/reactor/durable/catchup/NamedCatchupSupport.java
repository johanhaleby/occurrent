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
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.reactor.RepositionableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LongestReplay;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The named-subscription half of a reactive catch-up model, shared by the stream and DCB models and the routing
 * dispatcher so the replay logic exists once. The cold {@code Flux} path keeps using {@link PositionCatchupPipeline}
 * directly. This class composes the same replay with a <em>delegated</em> live half instead of a cold one. The live
 * subscription is handed to the wrapped model's own named {@code subscribe(..)}, so everything the wrapped model does
 * for a named subscription is inherited, including the retry of a failing action and the synchronous refusal of an
 * unsupported filter. That is the point of the promotion (issues #547 and #550).
 * <p>
 * A failing action during the replay is deliberately not retried. The blocking catch-up models do not retry there
 * either, and the replay failure reaches whoever waits on {@link Subscription#waitUntilStarted()} rather than being
 * logged and swallowed. Live delivery, where retry matters, is the wrapped model's.
 * <p>
 * The wrapped model must itself manage named subscriptions (implement {@link SubscriptionModel}). A catch-up model
 * over a cold-only wrapped model refuses the named {@code subscribe} paths loudly, because the alternative is a
 * second copy of the named-over-cold driver that {@code ReactorDurableSubscriptionModel} already owns. The model-wide
 * life-cycle calls stay safe on such a composition, with no-ops and {@code isRunning()} answering {@code false}. A
 * Spring context close calls {@code shutdown()} and a health check calls {@code isRunning()} regardless of whether
 * the application ever subscribed by name, and a late {@link IllegalStateException} there would fail an application
 * for a capability it never used.
 * <p>
 * Life-cycle semantics for a subscription still replaying. A pause does not abort the replay, the subscription hands
 * over to the wrapped model paused (blocking parity). A replay that a stop interrupts never hands over, and the
 * subscription is paused, as is a subscription created while the model is stopped. Such a subscription receives nothing
 * until {@code start(true)} or {@code resumeSubscription(id)} runs its replay again from its original start position,
 * so replayed events may be delivered again (the composition is at-least-once anyway). {@code start(false)} keeps it
 * paused. Resuming a paused subscription while the model is stopped, whether its replay was interrupted or it had
 * already handed over, runs {@code start(false)} first, so nothing else is resumed. When that start throws, nothing is
 * resumed and the next resume starts the model again. Cancelling or shutting down aborts in-flight replays.
 * Waiting on a subscription that was cancelled, or whose model was shut down, before its handover fails, since that
 * subscription never started and nothing will start it, and the blocking {@code CancelledSubscription} answers
 * {@code false} for the same cases. Model-wide calls forward to the wrapped model, so give each composition its own
 * wrapped model rather than sharing one.
 */
@NullMarked
final class NamedCatchupSupport {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NamedCatchupSupport.class);

    private final CheckpointAwareSubscriptionModel wrapped;
    private final @Nullable SubscriptionModel named;
    private final Class<?> modelClass;
    private final ConcurrentMap<String, CatchupState> catchingUp = new ConcurrentHashMap<>();
    // Set by stop(), cleared by start(..). Gates new replays, truncates in-flight ones (takeWhile), and makes
    // handOver park instead of subscribing the delegate on a stopped model. Same role as the blocking
    // AbstractCatchupSubscriptionModel's stopped flag.
    private volatile boolean stopped = false;
    // Set by shutdown() and never cleared, since a shut-down model cannot be started again. Refuses every named
    // subscribe, so none replays history and then fails at the handover to a wrapped model that is shut down.
    private volatile boolean shutdown = false;
    // Who to tell about each id's catch-up boundaries. Kept until this model shuts down, since the registration
    // outlives any one catch-up and a recorder that stopped being told would record the next one's history as
    // though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // Present when the wrapped model reports lost history and resumes a subscription at a given position, so this
    // model takes over a subscription whose live start the wrapped model loses as it opens
    private final @Nullable HistoryLossReportingSubscriptions historyLossReporting;
    private final @Nullable RepositionableSubscriptions repositionable;
    private final HistoryLossListener historyLossListener = this::takeOverIfHandedOverAt;
    // The live start of each subscription whose replay handed over last, kept until the next handover, a cancel or a
    // shutdown, so a loss of exactly that live start is told apart from a loss after the subscription read on
    private final ConcurrentMap<String, HandedOver> handedOver = new ConcurrentHashMap<>();
    // Each subscription this model made by name, from its subscribe until its cancel, whether it replayed or went
    // straight to live, so every history loss the wrapped model reports for it is this model's to answer
    private final ConcurrentMap<String, Owned> owned = new ConcurrentHashMap<>();
    // One lock per id, held by the subscribes and the cancel of that id, and removed once no thread holds or waits for it
    private final ConcurrentMap<String, IdLock> idLocks = new ConcurrentHashMap<>();
    // Set while a replay is launched on this thread, which can run the caller's action with the state monitor held
    private static final ThreadLocal<Boolean> LAUNCHING_REPLAY = ThreadLocal.withInitial(() -> false);

    NamedCatchupSupport(CheckpointAwareSubscriptionModel wrapped, Class<?> modelClass) {
        this.wrapped = requireNonNull(wrapped);
        this.modelClass = requireNonNull(modelClass);
        this.named = wrapped instanceof SubscriptionModel subscriptionModel ? subscriptionModel : null;
        HistoryLossReportingSubscriptions reporting = wrapped instanceof SubscriptionModelCapability capabilities ? HistoryLossReportingSubscriptions.findIn(capabilities).orElse(null) : null;
        RepositionableSubscriptions resumesAtPosition = wrapped instanceof SubscriptionModelCapability capabilities ? RepositionableSubscriptions.findIn(capabilities).orElse(null) : null;
        boolean takesOver = named != null && reporting != null && resumesAtPosition != null;
        this.historyLossReporting = takesOver ? reporting : null;
        this.repositionable = takesOver ? resumesAtPosition : null;
        if (takesOver) {
            reporting.addHistoryLossListener(historyLossListener);
        }
    }

    boolean managesNamedSubscriptions() {
        return named != null;
    }

    /**
     * Whether a replay for {@code subscriptionId} is in flight here. Only this model can answer for such a
     * subscription, because the wrapped model does not know the id until the handover.
     */
    boolean isCatchingUp(String subscriptionId) {
        return catchingUp.containsKey(subscriptionId);
    }

    // Who to tell about an id's catch-up boundaries, registered before the subscription that produces them.
    boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        catchupListeners.put(subscriptionId, listener);
        return true;
    }

    /**
     * Whether the replay for {@code subscriptionId} is still reading the history that was there when it started,
     * rather than the events written since. False for an id with no replay in flight, so a handed-over subscription
     * and one this model never saw read the same, matching {@link #isCatchingUp(String)}.
     */
    // One map read, so the part and the catch-up it belongs to can never come from two different moments. The two
    // fields inside the state are separate volatiles, but only the launcher writes them and it writes the generation
    // last, so a reader that saw this generation saw at least this launch's history flag.

    private SubscriptionModel requireNamed() {
        if (named == null) {
            throw new IllegalStateException(modelClass.getSimpleName() + " can only manage named subscriptions when the model it wraps manages them itself (implements " + SubscriptionModel.class.getSimpleName() + "). The wrapped " + wrapped.getClass().getName() + " only offers the plain (cold) subscribe(filter, startAt) primitive, so use that primitive directly, or wrap a model that manages named subscriptions.");
        }
        return named;
    }

    private void requireNotShutdown() {
        if (shutdown) {
            throw new SubscriptionModelShutdownException();
        }
    }

    // What every named subscribe checks, run by the models before they evaluate a dynamic StartAt, so a subscribe
    // this model refuses runs none of the caller's code.
    void requireNamedAndNotShutdown() {
        requireNamed();
        requireNotShutdown();
    }

    /**
     * Subscribes with a catch-up phase. It replays from {@code start} through the scope {@code scopes} opens for each
     * replay attempt, applies the
     * caller's {@code action} to each replayed event without retry, then hands the live half to the wrapped model's
     * named {@code subscribe(..)} resuming from a token captured before the replay, or the one {@code start} holds
     * while the wrapped model still has the history from it, deduped against the ids the
     * reconciliation read emitted. The history ids are deliberately not among them, so a write that was still in
     * flight when the head was read is delivered again live and can be recorded there. A token that left the change
     * stream history during the replay is not handed over. The replay runs again from the position the first attempt
     * started from, with a token read now, and the listener is told the catch-up started again. A catch-up whose
     * replay loses the token 4 times in a row fails like any other failed replay.
     */
    Subscription subscribeWithCatchup(String subscriptionId, @Nullable SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                      CatchupScopes scopes, long windowSize, int handoverCacheSize, CatchupCheckpoints.Position start,
                                      Function<CloudEvent, Mono<Void>> action) {
        SubscriptionModel delegate = requireNamed();
        requireNotShutdown();
        BoundedIdCache<CatchupEventKey> cache = new BoundedIdCache<>(handoverCacheSize);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(scopes, windowSize, handoverCacheSize);
        CatchupState state = new CatchupState(null);

        Function<CloudEvent, Mono<Void>> liveAction = cloudEvent ->
                livePredicate.test(cloudEvent) && !cache.contains(CatchupEventKey.of(cloudEvent)) ? action.apply(cloudEvent) : Mono.empty();

        // The replay is relaunchable. stop() aborts and parks it, and start(true) or a resume runs this again from the
        // same start position and resolves its live token again. Re-adding ids to the cache is a no-op, and
        // delivering replayed events again is at-least-once.
        // Made before the launcher, whose error callback removes it
        Owned record = new Owned();
        Replay replay = new Replay(subscriptionId, delegate, pipeline, cache, action, liveSubscriptionFilter, liveAction, new LongestReplay(), record);
        state.launcher = launcher(replay, state, PositionCatchupPipeline.notBefore(start), scope -> pipeline.resolveStart(wrapped, start, subscriptionId, scope.liveStart()), 0, null);

        underIdLock(subscriptionId, () -> {
            // The wrapped model already knowing the id means an earlier catch-up handed it over (or someone subscribed it
            // directly). Refuse synchronously, like every other subscribe path, instead of replaying history a second
            // time and failing asynchronously at the handover.
            if (delegate.isRunning(subscriptionId) || delegate.isPaused(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (catchingUp.putIfAbsent(subscriptionId, state) != null) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (!own(subscriptionId, delegate, record, state)) {
                catchingUp.remove(subscriptionId, state);
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            return null;
        });
        // A shutdown() between the check at the top and the puts above read the ids it cancels before this id was
        // added. It sets the flag before that read, so now that the id is in the map either this check sees the flag
        // or that read cancels the id, and no replay starts on a shut-down model.
        if (shutdown) {
            catchingUp.remove(subscriptionId, state);
            owned.remove(subscriptionId, record);
            throw new SubscriptionModelShutdownException();
        }
        try {
            launchOrPark(state);
        } catch (RuntimeException | Error e) {
            owned.remove(subscriptionId, record);
            throw e;
        }
        return new NamedCatchupSubscription(subscriptionId, state.started.asMono());
    }

    // Runs the replay now, or parks it while the model is stopped
    private void launchOrPark(CatchupState state) {
        synchronized (state) {
            // A shutdown() since the check above may have cancelled the state.
            if (!state.cancelled.get()) {
                if (stopped) {
                    state.parked.set(true);
                } else {
                    launch(state);
                }
            }
        }
    }

    // Called with the state monitor held. A replay whose start, reader and action complete on this thread runs the
    // action here, so the mark refuses a subscribe or cancel from it that would wait for the per-id lock under the monitor.
    // Every id is refused, not only the replaying one. A cancel holds the lock of its id while it waits for that id's
    // state monitor, so waiting for the lock of any id under this monitor can close a cycle with another replay
    private static void launch(CatchupState state) {
        boolean launchingAlready = LAUNCHING_REPLAY.get();
        LAUNCHING_REPLAY.set(true);
        try {
            state.launcher.run();
        } finally {
            if (!launchingAlready) {
                LAUNCHING_REPLAY.remove();
            }
        }
    }

    // What a replay needs to run, and to run again after the wrapped model lost the live start it handed over.
    // longestReplay is the longest any of its runs took from reading the live start to checking it, which the failure
    // once the live start was lost each time names. owned is the record the subscribe that made it put.
    private record Replay(String subscriptionId, SubscriptionModel delegate, PositionCatchupPipeline pipeline, BoundedIdCache<CatchupEventKey> cache,
                          Function<CloudEvent, Mono<Void>> action, @Nullable SubscriptionFilter liveSubscriptionFilter,
                          Function<CloudEvent, Mono<Void>> liveAction, LongestReplay longestReplay, Owned owned) {
    }

    // Runs the replay from the start firstStart resolves in the scope of the attempt, through the caller's action without
    // retry (failure is loud), then hands over to the wrapped model. notBefore is the live start an earlier attempt
    // stored, which the scope's reads must see everything up to, or null. replaysAgain is how many times the replay
    // already ran again. takenOver is the handover whose lost live start this replay takes over, or null for a replay a
    // subscribe started.
    private Runnable launcher(Replay replay, CatchupState state, @Nullable String notBefore, Function<CatchupScope, Mono<PositionCatchupPipeline.ReplayStart>> firstStart, int replaysAgain, @Nullable HandedOver takenOver) {
        String subscriptionId = replay.subscriptionId();
        return () -> {
            // A relaunch reads the history again from the same start position, so it is a different catch-up and
            // announces itself as one. Sent before the subscribe below, so it precedes anything this launch
            // delivers.
            Object launched = new Object();
            state.episode.set(launched);
            announceCatchupStarted(subscriptionId, state, launched);
            Disposable replaying = replayThenHandOver(replay, state, launched, notBefore, firstStart, replaysAgain)
                    .subscribe(unused -> {
                    }, throwable -> {
                        if (takenOver != null) {
                            // So a resume replays again, counting its replays again from 0
                            handedOver.computeIfPresent(subscriptionId, (id, made) -> made == takenOver ? made.withTheReplaysAgainOf(0) : made);
                            takenOver.takingOver().set(false);
                        }
                        if (state.handedOver.get()) {
                            // Only the pause asked for during the replay can fail once the wrapped model has the subscription
                            log.error("Subscription {} went live once its catch-up replay was done, but pausing it as asked for during the replay failed, so it may not be paused.",
                                    subscriptionId, throwable);
                        } else if (takenOver == null) {
                            // A failed replay is a dead subscription, reported to whoever waits AND logged: a caller
                            // that never blocks on waitUntilStarted() would otherwise get a dead subscription with no trace.
                            log.error("The catch-up replay for subscription {} failed; the subscription is dead until it is subscribed again.", subscriptionId, throwable);
                        } else if (requireNonNull(named).isPaused(subscriptionId)) {
                            log.error("The catch-up replay for subscription {} after the subscription model lost the history from its live start failed, so the subscription stays paused, and resuming it replays again.",
                                    subscriptionId, throwable);
                        } else {
                            log.error("The catch-up replay for subscription {} after the subscription model lost the history from its live start failed, and the subscription model no longer holds it paused. The catch-up replays again when the subscription model reports that live start lost again, or when the subscription is resumed once it is paused.",
                                    subscriptionId, throwable);
                        }
                        // Identity-checked: unlike handOver and cancelSubscription, this callback runs outside the
                        // state monitor, so a concurrent cancel-then-resubscribe for the same id can already have
                        // installed a new state by the time a stale failure reaches here. A plain remove(id) would
                        // delete that new attempt's entry instead of this dead one's.
                        catchingUp.remove(subscriptionId, state);
                        // Only a replay that never handed over leaves no subscription behind, so a subscribe of the id replays again
                        if (takenOver == null && !state.handedOver.get()) {
                            owned.remove(subscriptionId, replay.owned());
                        }
                        state.started.tryEmitError(throwable);
                    });
            // A stop() or a start(..) called from inside the subscribe above, by the wrapped model, the reader or the
            // action on this thread, may have parked this launch or run a newer one, which then owns the field.
            if (isCurrent(state, launched)) {
                state.replaying.set(replaying);
            } else {
                replaying.dispose();
            }
            // A synchronous replay failure (or instant hand-over) may already have removed the state.
            if (!catchingUp.containsKey(subscriptionId) && !state.handedOver.get()) {
                replaying.dispose();
            }
        };
    }

    // Replays from the start firstStart resolves through the caller's action, then hands over to its token, or replays
    // again first when the token left the change stream history during the replay. The start is resolved and the replay
    // read in one scope, which is closed when the replay ends. replaysAgain is how many times the replay already ran
    // again.
    private Flux<Void> replayThenHandOver(Replay replay, CatchupState state, Object launched, @Nullable String notBefore,
                                          Function<CatchupScope, Mono<PositionCatchupPipeline.ReplayStart>> firstStart, int replaysAgain) {
        String subscriptionId = replay.subscriptionId();
        PositionCatchupPipeline pipeline = replay.pipeline();
        // A stop between dispose landing and this event truncates here, before the action runs.
        BooleanSupplier keepReplaying = () -> !stopped && isCurrent(state, launched);
        return Flux.defer(() -> {
            AtomicReference<PositionCatchupPipeline.ReplayStart> resolved = new AtomicReference<>();
            return pipeline.<Void>inScope(notBefore, firstStart, (replayStart, reader) -> pipeline.replayApplying(reader, replayStart, replay.cache(), keepReplaying, replay.action(),
                            () -> {
                                CatchupListener boundaryListener = catchupListeners.get(subscriptionId);
                                if (boundaryListener != null) {
                                    // The launch this callback belongs to, not whatever the field holds by the time it
                                    // runs, so a relaunch cannot end its predecessor's history read for it.
                                    boundaryListener.historyRead(launched);
                                }
                            }), resolved, replay.longestReplay())
                    .thenMany(Flux.defer(() -> {
                        PositionCatchupPipeline.ReplayStart replayStart = resolved.get();
                        if (replayStart == null) {
                            return Flux.<Void>empty();
                        }
                        int counted = replayStart.countedFrom(replaysAgain);
                        // A replay a stop cut short goes to handOver without the check, which parks it or leaves it to
                        // the launch that replaced it
                        Mono<Boolean> liveStartHolds = keepReplaying.getAsBoolean()
                                ? pipeline.liveStartHolds(wrapped, replayStart, subscriptionId, counted, replay.longestReplay())
                                : Mono.just(true);
                        return liveStartHolds.flatMapMany(holds -> {
                            if (!holds) {
                                announceCatchupStarted(subscriptionId, state, launched);
                                return replayThenHandOver(replay, state, launched, null, pipeline.fromOrigin(wrapped, replayStart), counted + 1);
                            }
                            handOver(replay, state, launched, replayStart, counted);
                            return Flux.<Void>empty();
                        });
                    }));
        });
    }

    // Announced inside the map operation that holds this id, which cancelSubscription's own remove also takes, so a
    // cancel and a fresh subscribe cannot slip between the ownership check and the send. A launch that no longer owns
    // the id announces nothing, or its start would arrive after its replacement's and the recorder would adopt a
    // catch-up that is already over. The listener only writes its own state here, so running it inside the map
    // operation reenters nothing.
    private void announceCatchupStarted(String subscriptionId, CatchupState state, Object launched) {
        catchingUp.computeIfPresent(subscriptionId, (id, owner) -> {
            if (owner == state && state.episode.get() == launched) {
                CatchupListener startListener = catchupListeners.get(subscriptionId);
                if (startListener != null) {
                    startListener.catchupStarted(launched);
                }
            }
            return owner;
        });
    }

    /**
     * Subscribes with no catch-up phase. It is pure delegation to the wrapped model's named {@code subscribe(..)},
     * filtered in-process by {@code livePredicate} so a backend that does not honor the filter server-side still
     * only delivers matching events.
     */
    Subscription subscribeStraightToLive(String subscriptionId, @Nullable SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                         StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        SubscriptionModel delegate = requireNamed();
        requireNotShutdown();
        return underIdLock(subscriptionId, () -> {
            if (delegate.isRunning(subscriptionId) || delegate.isPaused(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            // Before the wrapped model opens, since it can report a loss before its subscribe returns
            Owned record = new Owned();
            if (!own(subscriptionId, delegate, record, null)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            try {
                return delegate.subscribe(subscriptionId, liveSubscriptionFilter, startAt,
                        cloudEvent -> livePredicate.test(cloudEvent) ? action.apply(cloudEvent) : Mono.empty());
            } catch (RuntimeException | Error e) {
                owned.remove(subscriptionId, record);
                throw e;
            }
        });
    }

    // Called under the per-id lock. Puts record for the id, replacing a record whose subscription is gone, which is one
    // with no replay in flight here, other than ours, that the wrapped model neither runs nor holds paused. False when
    // the id has a live record. catchingUp is read first, since a handover registers the id in the wrapped model
    // before it leaves catchingUp.
    private boolean own(String subscriptionId, SubscriptionModel delegate, Owned record, @Nullable CatchupState ours) {
        Owned existing = owned.putIfAbsent(subscriptionId, record);
        if (existing == null) {
            return true;
        }
        CatchupState inFlight = catchingUp.get(subscriptionId);
        boolean gone = (inFlight == null || inFlight == ours) && !delegate.isRunning(subscriptionId) && !delegate.isPaused(subscriptionId);
        return gone && owned.replace(subscriptionId, existing, record);
    }

    // Runs work holding the lock of subscriptionId, which is never held across anything asynchronous, and always taken
    // before the state monitor
    private <T> T underIdLock(String subscriptionId, Supplier<T> work) {
        if (LAUNCHING_REPLAY.get()) {
            throw new IllegalStateException("Cannot subscribe or cancel subscription " + subscriptionId + " through the catch-up subscription model while a catch-up replay runs on the thread that subscribed it,"
                    + " since a replay that runs on that thread can hold what the call waits for. Subscribe or cancel from another thread.");
        }
        IdLock idLock = idLocks.compute(subscriptionId, (id, current) -> {
            IdLock used = current == null ? new IdLock() : current;
            used.users++;
            return used;
        });
        try {
            idLock.lock.lock();
            try {
                return work.get();
            } finally {
                idLock.lock.unlock();
            }
        } finally {
            idLocks.computeIfPresent(subscriptionId, (id, current) -> --current.users == 0 ? null : current);
        }
    }

    // Registers the delegated live subscription once the replay has drained. Runs inside the replay pipeline, so a
    // cancellation that disposed the replay before its last check never reaches here. A pause that arrived during the
    // replay is applied to the delegated subscription immediately after it is created, mirroring the blocking catch-up
    // models. A stop that truncated the replay parks instead, since handing over to a stopped wrapped model would
    // immediately conflict with its own stop bookkeeping (the pre-fix behavior errored waitUntilStarted with "already
    // paused"). A replay that took over a lost live start resumes the subscription the wrapped model holds paused,
    // at the new live start, instead of subscribing it.
    private void handOver(Replay replay, CatchupState state, Object launched, PositionCatchupPipeline.ReplayStart replayStart, int replaysAgain) {
        String subscriptionId = replay.subscriptionId();
        SubscriptionModel delegate = replay.delegate();
        StartAt liveStartAt = StartAt.checkpoint(replayStart.liveFrom());
        synchronized (state) {
            // A stop() that came after this launch's last check parked the subscription, and a start(..) since then
            // may have cleared stopped or run a newer launch. Either way the subscription is no longer this launch's,
            // and handing it over would hand over a replay the stop cut short.
            if (!isCurrent(state, launched)) {
                return;
            }
            if (stopped) {
                // The takeWhile truncated the replay, so this completion is not a finished replay. start(true) or a
                // resume runs it again from the original start position.
                state.replaying.set(null);
                state.parked.set(true);
                return;
            }
            HandedOver takenOver = state.takesOver;
            // Before the wrapped model opens at the live start, which it can lose as soon as it has it. A takeover's
            // record also answers for the live start the wrapped model lost, which a start(true) can resume the
            // subscription at until the resume below has returned.
            HandedOver made = repositionable == null ? null
                    : new HandedOver(replayStart.liveFrom(), takenOver == null ? null : takenOver.liveStart(), replay, replayStart, replaysAgain, state, new AtomicBoolean(), new AtomicBoolean());
            if (made != null) {
                handedOver.put(subscriptionId, made);
            }
            Subscription delegated;
            try {
                delegated = takenOver == null ? delegate.subscribe(subscriptionId, replay.liveSubscriptionFilter(), liveStartAt, replay.liveAction()) : resumeAt(subscriptionId, delegate, liveStartAt);
            } catch (RuntimeException | Error e) {
                // A takeover puts back the record it took over, so a resume, or a loss of that live start reported
                // again, replays again
                if (made != null) {
                    handedOver.compute(subscriptionId, (id, current) -> current == made ? takenOver : current);
                }
                throw e;
            }
            state.handedOver.set(true);
            if (state.pendingPause.get() && delegate.isRunning(subscriptionId)) {
                delegate.pauseSubscription(subscriptionId);
            }
            // Identity-checked so this removal can never race a later attempt's entry for the same id, even though
            // the putIfAbsent/isRunning guards at subscribeWithCatchup already make that provably unreachable here:
            // the blocking catch-up models had the same shape without those guards (#737), so every removal site in
            // this class stays identity-checked rather than leaning on an invariant proved only by hand.
            catchingUp.remove(subscriptionId, state);
            delegated.waitUntilStarted().subscribe(unused -> {
            }, state.started::tryEmitError, state.started::tryEmitEmpty);
        }
    }

    // Resumes the subscription the wrapped model holds paused at liveStartAt. A start(true) of the wrapped model, or of
    // the catch-up model, can resume it at the lost live start until the resume here returns, and it is paused again
    // each time. Throws when the resume is refused while the wrapped model doesn't run the subscription.
    private Subscription resumeAt(String subscriptionId, SubscriptionModel delegate, StartAt liveStartAt) {
        SubscriptionAlreadyRunningException resumedMeanwhile = null;
        while (true) {
            if (delegate.isRunning(subscriptionId)) {
                try {
                    delegate.pauseSubscription(subscriptionId);
                } catch (SubscriptionNotRunningException pausedOrEndedMeanwhile) {
                    // The resume below says which
                }
            } else if (resumedMeanwhile != null) {
                throw resumedMeanwhile;
            }
            try {
                return requireNonNull(repositionable).resumeSubscription(subscriptionId, liveStartAt);
            } catch (SubscriptionAlreadyRunningException e) {
                resumedMeanwhile = e;
            }
        }
    }

    /**
     * The live start a replay handed over for a subscription, with what that replay needs to run again when the
     * wrapped model loses the history from it as it opens. {@code handingOver} is the state of the replay that handed
     * over, which stays in flight until its handover returns. {@code takeOverAsked} is set once this model has answered
     * that it takes the subscription over, before the wrapped model pauses it, and {@code takingOver} while a takeover
     * replay runs. {@code lostLiveStart} is the live start the wrapped model lost when a takeover replay handed over,
     * and {@code null} otherwise. The wrapped model reports a loss of it only for a subscription a start(true) resumed
     * there before the handover resumed it at {@code liveStart}, and taking that loss over has the wrapped model pause
     * the subscription again.
     */
    private record HandedOver(Checkpoint liveStart, @Nullable Checkpoint lostLiveStart, Replay replay, PositionCatchupPipeline.ReplayStart replayStart, int replaysAgain,
                              CatchupState handingOver, AtomicBoolean takeOverAsked, AtomicBoolean takingOver) {
        HandedOver withTheReplaysAgainOf(int replaysAgain) {
            return new HandedOver(liveStart, lostLiveStart, replay, replayStart, replaysAgain, handingOver, takeOverAsked, new AtomicBoolean());
        }
    }

    // Asked by the wrapped model on a thread that reads subscriptions, without its lock. A subscription this model
    // didn't make is left to the wrapped model. A loss of exactly the live start the last replay handed over replays
    // again. A loss of that live start while a takeover already runs is taken over too, so the wrapped model keeps the
    // subscription paused, and the running takeover goes on. So is a loss of the live start a takeover handing over
    // replaces, which that handover resumes at its own live start. Any other loss pauses the subscription for good.
    private HistoryLossHandling takeOverIfHandedOverAt(String subscriptionId, Checkpoint lostFrom) {
        Owned record = owned.get(subscriptionId);
        if (record == null) {
            return HistoryLossHandling.leaveToSubscriptionModel();
        }
        HandedOver made = handedOver.get(subscriptionId);
        if (made != null && !shutdown) {
            if (PositionCatchupPipeline.samePosition(lostFrom, made.lostLiveStart())) {
                return HistoryLossHandling.takeOver(() -> {
                });
            }
            if (PositionCatchupPipeline.samePosition(lostFrom, made.liveStart())) {
                // Before the wrapped model pauses the subscription, so a resume that finds it paused before the takeover has
                // started replays instead of resuming the wrapped model where it lost the history
                made.takeOverAsked().set(true);
                return HistoryLossHandling.takeOver(() -> {
                    if (made.takingOver().compareAndSet(false, true)) {
                        takeOver(subscriptionId, made);
                    }
                });
            }
        }
        // No replay of this model can take the loss over without skipping events, so the subscription stays paused.
        // Recorded before the wrapped model pauses it, so a resume that finds it paused refuses.
        record.historyLost = lostFrom;
        return HistoryLossHandling.takeOver(() -> {
            String lost = lostFrom.asString();
            log.error(HistoryLossMessages.TERMINAL_FAILURE_LOG, subscriptionId, lost, lost);
        });
    }

    // Replays again from the position the replay that handed over started from, with a live start read now, and
    // registers that replay as the one in flight for the id, so this model answers for the subscription meanwhile,
    // while the wrapped model holds it paused. It replaces the replay that handed over when the loss comes before that
    // handover has returned, and keeps a pause asked for during it. A cancel that came first drops the takeover.
    // Returns the state of the replay it ran, or null when it ran none.
    private @Nullable CatchupState takeOver(String subscriptionId, HandedOver made) {
        Replay replay = made.replay();
        CatchupState state = new CatchupState(made);
        state.launcher = launcher(replay, state, null, replay.pipeline().replayAgainAfterLiveStartLost(wrapped, made.replayStart(), subscriptionId, made.replaysAgain(), replay.longestReplay()),
                made.replaysAgain() + 1, made);
        CatchupState inFlight = catchingUp.compute(subscriptionId, (id, current) -> {
            if (current == made.handingOver()) {
                state.pendingPause.set(current.pendingPause.get());
                return state;
            }
            return current == null ? state : current;
        });
        if (inFlight != state) {
            made.takingOver().set(false);
            return null;
        }
        // Read again after the put, since cancelSubscription and shutdown drop the handover before they read the ids
        // in flight. Either this sees it dropped, or that read finds this state and cancels it.
        if (shutdown || handedOver.get(subscriptionId) != made) {
            catchingUp.remove(subscriptionId, state);
            return null;
        }
        launchOrPark(state);
        return state;
    }

    // --- Life cycle. Every call forwards to the wrapped model; the extra bookkeeping is only for subscriptions whose
    // --- replay has not handed over yet, which the wrapped model does not know about.

    void stop() {
        // Safe no-op on a cold-only composition: nothing named can be running, and a Spring context stop must not
        // throw for a capability the application never used.
        if (!managesNamedSubscriptions()) {
            return;
        }
        stopped = true;
        catchingUp.values().forEach(state -> {
            // The per-state monitor closes the race with handOver: either the handover completed first (the wrapped
            // model's stop() below covers the live subscription), or the replay is disposed before it can hand over.
            synchronized (state) {
                if (!state.handedOver.get() && !state.cancelled.get()) {
                    Disposable replaying = state.replaying.getAndSet(null);
                    if (replaying != null) {
                        replaying.dispose();
                    }
                    state.parked.set(true);
                }
            }
        });
        named.stop();
    }

    void start(boolean resumeSubscriptionsAutomatically) {
        if (!managesNamedSubscriptions()) {
            return;
        }
        if (resumeSubscriptionsAutomatically) {
            // Before the wrapped model starts, since a replay can hand over while it starts and would apply the pause
            catchingUp.values().forEach(state -> {
                synchronized (state) {
                    state.pendingPause.set(false);
                }
            });
        }
        // Cleared only once the wrapped model has started, so a start that throws keeps this model stopped and a
        // parked replay waits for the next start or resume.
        named.start(resumeSubscriptionsAutomatically);
        stopped = false;
        if (!resumeSubscriptionsAutomatically) {
            return;
        }
        catchingUp.values().forEach(state -> {
            synchronized (state) {
                runIfParked(state);
            }
        });
    }

    boolean isStopped() {
        return stopped;
    }

    // Whether launched is the replay that owns the subscription right now. A stop() parks the subscription and a
    // start(true) or resume runs a new launch, so an older launch still running past its last check is not current.
    private static boolean isCurrent(CatchupState state, Object launched) {
        return !state.cancelled.get() && !state.parked.get() && state.episode.get() == launched;
    }

    // Called with the state monitor held. Runs the replay of a subscription that stop() interrupted or that was made
    // while the model was stopped, from its original start position.
    private void runIfParked(CatchupState state) {
        if (!stopped && !state.handedOver.get() && !state.cancelled.get() && state.parked.getAndSet(false)) {
            launch(state);
        }
    }

    // Not running until start(..) has cleared stopped, even when the wrapped model already runs, since a subscribe
    // before that is paused
    boolean isRunning() {
        return managesNamedSubscriptions() && !stopped && named.isRunning();
    }

    boolean isRunning(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return !stopped && !state.parked.get() && !state.pendingPause.get() && !state.cancelled.get();
        }
        return managesNamedSubscriptions() && named.isRunning(subscriptionId);
    }

    boolean isPaused(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return (state.parked.get() || state.pendingPause.get()) && !state.cancelled.get();
        }
        return managesNamedSubscriptions() && named.isPaused(subscriptionId);
    }

    void pauseSubscription(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                if (!state.handedOver.get()) {
                    if (state.parked.get() || state.pendingPause.getAndSet(true)) {
                        throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
                    }
                    return;
                }
            }
        }
        if (!managesNamedSubscriptions()) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        named.pauseSubscription(subscriptionId);
    }

    Subscription resumeSubscription(String subscriptionId) {
        // Before anything starts, so a refused resume changes nothing
        Owned record = owned.get(subscriptionId);
        Checkpoint historyLost = record == null ? null : record.historyLost;
        if (historyLost != null) {
            throw HistoryLossMessages.historyLost(subscriptionId, historyLost);
        }
        // Resuming one subscription starts a stopped model without resuming the others, as the life-cycle contract
        // says, whether its replay is parked or it has handed over. A start that throws resumes nothing.
        if (stopped && isPaused(subscriptionId)) {
            start(false);
        }
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                if (!state.handedOver.get()) {
                    boolean pausedDuringReplay = state.pendingPause.getAndSet(false);
                    if (!state.parked.get() && !pausedDuringReplay) {
                        throw new SubscriptionAlreadyRunningException(subscriptionId);
                    }
                    // A stop() since the start above keeps it parked, as a stop right after this resume would
                    runIfParked(state);
                    return new NamedCatchupSubscription(subscriptionId, state.started.asMono());
                }
            }
        }
        if (!managesNamedSubscriptions()) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        Subscription takenOver = takeOverOnResume(subscriptionId);
        return takenOver == null ? named.resumeSubscription(subscriptionId) : takenOver;
    }

    // The wrapped model lost the live start, holds the subscription paused and no takeover replay runs, because the
    // wrapped model hasn't run the takeover yet or the takeover replay failed. Resuming the wrapped model would open
    // the subscription where it lost the history, so the takeover replay runs now instead.
    private @Nullable Subscription takeOverOnResume(String subscriptionId) {
        HandedOver made = handedOver.get(subscriptionId);
        if (made == null || !made.takeOverAsked().get() || !named.isPaused(subscriptionId) || !made.takingOver().compareAndSet(false, true)) {
            return null;
        }
        CatchupState taking = takeOver(subscriptionId, made);
        return taking == null ? null : new NamedCatchupSubscription(subscriptionId, taking.started.asMono());
    }

    Mono<Void> cancelSubscription(String subscriptionId) {
        return underIdLock(subscriptionId, () -> cancelHoldingIdLock(subscriptionId));
    }

    private Mono<Void> cancelHoldingIdLock(String subscriptionId) {
        // Before the replay in flight is read below, which takeOver depends on
        handedOver.remove(subscriptionId);
        // Forgotten only once the wrapped model has cancelled, so a loss it reports meanwhile still pauses the
        // subscription rather than letting the wrapped model restart it from now
        Owned record = owned.get(subscriptionId);
        CatchupState state = catchingUp.remove(subscriptionId);
        if (state != null) {
            synchronized (state) {
                state.cancelled.set(true);
                Disposable replaying = state.replaying.get();
                if (replaying != null) {
                    replaying.dispose();
                }
                if (!state.handedOver.get()) {
                    // Waiters are failed rather than completed, since completing would claim the subscription started
                    // and doing nothing would hang them. The blocking twin answers false, which a Mono<Void> has no
                    // room for. The wrapped model is still asked to cancel below, since it can hold what an earlier
                    // process stored for this id even though this process never handed the id over.
                    state.started.tryEmitError(new IllegalStateException("Subscription " + subscriptionId + " was cancelled before it started."));
                }
            }
        }
        // A handover that held the state monitor between the removal at the top and the block above put its record then
        handedOver.remove(subscriptionId);
        // A cold-only composition runs nothing by name and stores nothing by id, so there is nothing to stop or delete
        if (!managesNamedSubscriptions()) {
            forget(subscriptionId, record);
            return Mono.empty();
        }
        Mono<Void> cancelled = named.cancelSubscription(subscriptionId).doFinally(__ -> forget(subscriptionId, record)).cache();
        // Subscribed here and cached, so ownership is forgotten even when nothing subscribes to what this returns,
        // and a wrapped cancel that does nothing until subscribed is not run twice
        cancelled.subscribe(unused -> {
        }, throwable -> log.warn("Could not cancel subscription {} in the wrapped model {}. That model may still hold the subscription.",
                subscriptionId, named.getClass().getName(), throwable));
        return cancelled;
    }

    // Removes only the record the cancel read, so a subscribe of the id after the cancel keeps its own
    private void forget(String subscriptionId, @Nullable Owned record) {
        if (record != null) {
            owned.remove(subscriptionId, record);
        }
    }

    void shutdown() {
        // Before the ids are read below, which the second check in subscribeWithCatchup depends on.
        shutdown = true;
        catchupListeners.clear();
        handedOver.clear();
        if (historyLossReporting != null) {
            historyLossReporting.removeHistoryLossListener(historyLossListener);
        }
        owned.clear();
        new ArrayList<>(catchingUp.keySet()).forEach(subscriptionId -> {
            CatchupState state = catchingUp.remove(subscriptionId);
            if (state != null) {
                synchronized (state) {
                    state.cancelled.set(true);
                    Disposable replaying = state.replaying.get();
                    if (replaying != null) {
                        replaying.dispose();
                    }
                    // Disposing the replay runs none of its callbacks, so without this waitUntilStarted() never completes.
                    if (!state.handedOver.get()) {
                        state.started.tryEmitError(new SubscriptionModelShutdownException());
                    }
                }
            }
        });
        // Safe no-op on a cold-only composition: a Spring context close (destroyMethod = "shutdown") must not throw.
        if (!managesNamedSubscriptions()) {
            return;
        }
        named.shutdown();
    }

    private static final class CatchupState {
        // The handover whose live start the wrapped model lost, for a replay that takes it over, which resumes the
        // subscription the wrapped model holds paused instead of subscribing it
        final @Nullable HandedOver takesOver;
        final AtomicReference<@Nullable Disposable> replaying = new AtomicReference<>();
        final AtomicBoolean pendingPause = new AtomicBoolean(false);
        // Set while the replay waits for start(true) or a resume, because stop() interrupted it or it was made while
        // the model was stopped. Such a subscription is paused.
        final AtomicBoolean parked = new AtomicBoolean(false);
        final AtomicBoolean cancelled = new AtomicBoolean(false);
        final AtomicBoolean handedOver = new AtomicBoolean(false);
        final Sinks.Empty<Void> started = Sinks.empty();
        // The episode each launch announces. A stop parks this state and a resume runs its launcher again, reading
        // the history from the beginning, which is a different catch-up and gets a value of its own.
        final AtomicReference<Object> episode = new AtomicReference<>(new Object());
        // The replay, relaunchable: assigned once in subscribeWithCatchup before the state is published, run under
        // the state monitor by the initial subscribe, and by start(true) or a resume for a parked subscription.
        volatile Runnable launcher = () -> {
        };

        CatchupState(@Nullable HandedOver takesOver) {
            this.takesOver = takesOver;
        }
    }

    // A subscription this model made by name
    private static final class Owned {
        // The position the wrapped model lost the history from when no replay could take the loss over, after which a
        // resume refuses with CatchupCannotResumeException until the subscription is cancelled
        volatile @Nullable Checkpoint historyLost;
    }

    private static final class IdLock {
        final ReentrantLock lock = new ReentrantLock();
        // Threads holding or waiting for the lock, changed only inside the idLocks map operations
        int users;
    }

    private record NamedCatchupSubscription(String id, Mono<Void> started) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
