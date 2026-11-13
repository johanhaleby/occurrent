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
import org.occurrent.subscription.CatchupCannotResumeException;
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
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.reactor.RepositionableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelCapability;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.api.reactor.internal.CancellableRegistration;
import org.occurrent.subscription.internal.BoundedIdCache;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.internal.LongestReplay;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
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
 * <p>
 * This class runs an action, a cancel hook and a {@code waitUntilStarted()} handler with none of its locks held, so that
 * code can subscribe, cancel, stop or shut down any subscription. A subscribe that goes straight to live runs the
 * wrapped {@code subscribe(..)} with no lock held, since {@code ReactorMongoSubscriptionModel} can evaluate a dynamic
 * {@link StartAt} that answered {@code null} here again on the calling thread. A cancel of the id meanwhile goes to the
 * wrapped model at once. A wrapped model whose {@code CancellableRegistration.standsInForSubscribe(Object)} answers
 * {@code true} for the reference this class holds registers nothing for the subscribe when the cancel reaches it before
 * it took the id. Of Occurrent's models, only a {@code ReactorMongoSubscriptionModel} or
 * {@code ReactorDurableSubscriptionModel} of that exact class answers {@code true}. The subscribe then returns a
 * {@link Subscription} whose {@code waitUntilStarted()} fails with an {@link IllegalStateException}. On a wrapped model
 * shut down by the time it asks whether the subscribe was cancelled, the subscribe throws
 * {@code SubscriptionModelShutdownException} instead and returns nothing. A cancel that reaches
 * {@code ReactorDurableSubscriptionModel} after it took the id fails {@code waitUntilStarted()} with a
 * {@code CancellationException}. Any other wrapped model can
 * register the subscribe after the cancel, so the id is cancelled there again once the wrapped {@code subscribe(..)}
 * returned holding it, unless a later subscribe took the id there. A subclass or a proxy of either model has its
 * {@code subscribe(String, SubscriptionFilter, StartAt, Function)} called, and gets that second cancel too. These calls still run with a lock held.
 * <ul>
 * <li>The handover runs the wrapped {@code subscribe(..)}, or {@code resumeSubscription(id, startAt)} for a takeover,
 * and {@code isRunning(id)} and {@code pauseSubscription(id)} under the monitor of the subscription's state. So the
 * wrapped model must run no action and report no history loss on the calling thread before those calls return, and
 * those calls must not wait for a thread that reports a history loss or runs the takeover of a lost live start, since a
 * cancel, a pause or a resume holding the lock of the id can be waiting for that monitor.</li>
 * <li>A {@link CatchupListener} is told that a catch-up started inside an operation on a {@link ConcurrentHashMap},
 * which holds a lock of that map. A listener that calls this model can deadlock with another thread.</li>
 * <li>Whether the wrapped model holds an id is asked, through {@code subscriptionIds()} or {@code isRunning(id)} and
 * {@code isPaused(id)}, with the lock of that id held. A report of a history loss takes that lock while it is
 * answered, and the takeover that the wrapped model runs once it paused the subscription can take it too. So the
 * wrapped model must not hold a lock those calls take
 * while it runs code that can call this model, such as an action, a dynamic start, a report of a history loss, that
 * takeover, or what runs once a cancel or a start completes.</li>
 * <li>The wrapped {@code start(..)} runs under a lock that {@code shutdown()} also takes, so the wrapped model gets no
 * start after its shutdown. So code that the wrapped model runs on the calling thread while it starts must not wait for
 * another thread that starts or shuts down this model.</li>
 * </ul>
 */
@NullMarked
final class NamedCatchupSupport {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NamedCatchupSupport.class);

    private final CheckpointAwareSubscriptionModel wrapped;
    private final @Nullable SubscriptionModel named;
    // The wrapped model when it answers true to standsInForSubscribe(..) for this reference, which a Spring AOP proxy of
    // either shipped model doesn't do
    private final @Nullable CancellableRegistration cancellable;
    private final Class<?> modelClass;
    private final ConcurrentMap<String, CatchupState> catchingUp = new ConcurrentHashMap<>();
    // Set by stop(), cleared by start(..). Gates new replays, truncates in-flight ones (takeWhile), and makes
    // handOver park instead of subscribing the delegate on a stopped model. Same role as the blocking
    // AbstractCatchupSubscriptionModel's stopped flag.
    private volatile boolean stopped = false;
    // Set by beginShutdown() and never cleared, since a shut-down model cannot be started again. Refuses every named
    // subscribe, so none replays history and then fails at the handover to a wrapped model that is shut down. Refuses a
    // pause or a resume that reads it set, see requireNotShutdown, unless the wrapped model manages no named
    // subscriptions, and keeps every start from reaching the wrapped model.
    private volatile boolean shutdown = false;
    // Held by start(..) while it starts the wrapped model and by beginShutdown() while it sets shutdown, so the
    // wrapped model gets no start after the shutdown this model sends it
    private final Object startLock = new Object();
    // Who to tell about each id's catch-up boundaries. Kept until this model shuts down, since the registration
    // outlives any one catch-up and a recorder that stopped being told would record the next one's history as
    // though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // Present when the wrapped model reports lost history and resumes a subscription at a given position, so this
    // model takes over a subscription whose live start the wrapped model loses as it opens
    private final @Nullable HistoryLossReportingSubscriptions historyLossReporting;
    private final @Nullable RepositionableSubscriptions repositionable;
    private final HistoryLossListener historyLossListener = this::takeOverIfHandedOverAt;
    // The live start of each subscription whose replay handed over last, kept until the next handover replaces it, or
    // a cancel, a shutdown or a subscribe that replaces the subscription once it is gone removes it, so a loss of
    // exactly that live start is told apart from a loss after the subscription read on
    private final ConcurrentMap<String, HandedOver> handedOver = new ConcurrentHashMap<>();
    // Each subscription this model made by name, whether it replayed or went straight to live, from its subscribe until
    // its cancel ended and the wrapped model no longer holds it, so every history loss the wrapped model reports for it
    // is this model's to answer
    private final ConcurrentMap<String, Owned> owned = new ConcurrentHashMap<>();
    // One lock per id, held while this model reads and changes what it keeps for that id together, see underIdLock, and
    // removed once no thread holds or waits for it
    private final ConcurrentMap<String, IdLock> idLocks = new ConcurrentHashMap<>();
    // How many cancels of each id have not had the wrapped model cancel yet. A subscribe of such an id is refused, since
    // that cancel runs after the per-id lock is released and would cancel the subscription the subscribe makes. A resume
    // of such an id that would go to the wrapped model is refused too, see resumeHoldingIdLock.
    private final ConcurrentMap<String, Integer> cancelsUnderWay = new ConcurrentHashMap<>();
    // What each id's cancelled subscribes can still leave in the wrapped model, see Leftovers
    private final ConcurrentMap<String, Leftovers> leftovers = new ConcurrentHashMap<>();

    NamedCatchupSupport(CheckpointAwareSubscriptionModel wrapped, Class<?> modelClass) {
        this.wrapped = requireNonNull(wrapped);
        this.modelClass = requireNonNull(modelClass);
        this.named = wrapped instanceof SubscriptionModel subscriptionModel ? subscriptionModel : null;
        this.cancellable = named instanceof CancellableRegistration registration && registration.standsInForSubscribe(registration) ? registration : null;
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

    // Whether a replay of the id is in flight here, a subscribe of the id is still inside the wrapped subscribe(..) or
    // can still have taken the id there after its cancel, a cancel of the id here has not ended, or the wrapped model
    // holds the id. For such a subscribe this model cancels the id in the wrapped model once it returns, unless a later
    // subscribe here took the id.
    boolean holds(String subscriptionId) {
        Owned record = owned.get(subscriptionId);
        return catchingUp.containsKey(subscriptionId) || record != null && record.subscribing || leftovers.containsKey(subscriptionId)
                || cancelsUnderWay.containsKey(subscriptionId) || named != null && holds(named, subscriptionId);
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

    // A call refused by its first check never reaches the wrapped model. A pause or a resume that reads a catch-up in
    // flight for the id checks again once it holds the monitor of that catch-up, and a resume checks again once it has
    // read the handover, so either is refused when the flag was set by then. A resume refused after its first check may
    // already have started the wrapped model, when this model was stopped. A pause that reaches the wrapped model after
    // the flag is set, and a resume that read the flag just before it was set, get what that model answers.
    private void requireNotShutdown(Supplier<SubscriptionModelShutdownException> refusal) {
        if (shutdown) {
            throw refusal.get();
        }
    }

    private static SubscriptionModelShutdownException shutDownRefusal(String call, String subscriptionId) {
        return new SubscriptionModelShutdownException("Cannot " + call + " subscription " + subscriptionId + " because the subscription model is shutdown.");
    }

    // What every named subscribe checks, run by the models before they evaluate a dynamic StartAt, so a subscribe
    // this model refuses runs none of the caller's code.
    void requireNamedAndNotShutdown() {
        requireNamed();
        requireNotShutdown(SubscriptionModelShutdownException::new);
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
        requireNotShutdown(SubscriptionModelShutdownException::new);
        BoundedIdCache<CatchupEventKey> cache = new BoundedIdCache<>(handoverCacheSize);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(scopes, windowSize, handoverCacheSize);
        CatchupState state = new CatchupState(null);

        LiveGate live = new LiveGate();
        Function<CloudEvent, Mono<Void>> liveAction = cloudEvent -> live.apply(() ->
                livePredicate.test(cloudEvent) && !cache.contains(CatchupEventKey.of(cloudEvent)) ? action.apply(cloudEvent) : Mono.empty());

        // The replay is relaunchable. stop() aborts and parks it, and start(true) or a resume runs this again from the
        // same start position and resolves its live token again. Re-adding ids to the cache is a no-op, and
        // delivering replayed events again is at-least-once.
        // Made before the launcher, whose error callback removes it
        Owned record = new Owned();
        Replay replay = new Replay(subscriptionId, delegate, pipeline, cache, action, liveSubscriptionFilter, liveAction, live, new LongestReplay(), record);
        state.launcher = launcher(replay, state, PositionCatchupPipeline.notBefore(start), scope -> pipeline.resolveStart(wrapped, start, subscriptionId, scope.liveStart()), 0, null);
        // Read before the put, since once the replay is held a resume gives the state a sink of its own, which this
        // subscribe's handle must not wait for
        Mono<Void> started = state.started.asMono();

        underIdLock(subscriptionId, () -> {
            // The wrapped model already knowing the id means an earlier catch-up handed it over (or someone subscribed it
            // directly). Refuse synchronously, like every other subscribe path, instead of replaying history a second
            // time and failing asynchronously at the handover.
            if (holds(delegate, subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            forgetATakeoverNeverLaunched(subscriptionId);
            // Before the put, since a stop() and a start(true) take no lock of the id and launch every replay in
            // catchingUp, so a replay put for a subscribe that own(..) then refuses could run
            if (!own(subscriptionId, delegate, record)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (catchingUp.putIfAbsent(subscriptionId, state) != null) {
                // Undoes own(..), which put this record rather than replacing one, since it replaces one only while no
                // replay of the id is in flight here
                owned.remove(subscriptionId, record);
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
            launchIfPending(state);
        } catch (RuntimeException | Error e) {
            // Before the release, which cancels nothing in the wrapped model while a replay of the id is in flight
            catchingUp.remove(subscriptionId, state);
            release(subscriptionId, record);
            throw e;
        }
        return new NamedCatchupSubscription(subscriptionId, started);
    }

    // Runs the first launch of the replay, or parks the replay while this model is stopped. Run by subscribeWithCatchup
    // and by the wrapped model once it has paused the subscription a takeover was registered for. Runs nothing when
    // the replay was cancelled or launched already. A start(true) unparks and launches every parked replay in
    // catchingUp, which holds no replay of a subscribe that own(..) has not accepted, so a stop() and a start(true)
    // between the put and this run the first launch themselves.
    private void launchIfPending(CatchupState state) {
        Object launched;
        synchronized (state) {
            launched = launch(state, false);
        }
        // Outside the monitor, since a replay whose start, reader and action complete on this thread runs the action here
        if (launched != null) {
            state.launcher.accept(launched);
        }
    }

    // Called with the state monitor held. Every launch of a replay is named here, and the caller runs it once the
    // monitor is released. Null means no launch. A state launches its replay at its first launch, and again each time
    // start(true) or a resume unparks it. A stop() parks a replay that has not handed over, and a first launch while
    // this model is stopped parks the replay instead. registerTakeOver parks a takeover it registers while this model
    // is stopped, so that takeover's first launch is an unpark. Nothing launches once the replay has handed over or was
    // cancelled, or once shutdown() has set its flag, even before that shutdown cancels the state.
    private @Nullable Object launch(CatchupState state, boolean unparking) {
        if (shutdown || state.cancelled.get() || state.handedOver.get()) {
            return null;
        }
        if (unparking) {
            if (state.heldForLostLiveStart && state.resumeSkipsEvents) {
                return null;
            }
            if (stopped || !state.parked.getAndSet(false)) {
                return null;
            }
            if (state.heldForLostLiveStart) {
                state.heldForLostLiveStart = false;
                state.started = Sinks.empty();
            }
            return nextLaunch(state);
        }
        if (!state.launchPending || state.parked.get()) {
            return null;
        }
        if (stopped) {
            state.parked.set(true);
            return null;
        }
        return nextLaunch(state);
    }

    // Called with the state monitor held, for a replay that runs once the monitor is released. The launch is named here
    // rather than when it runs, so a launch that a stop parked never counts as current again after a start unparks it.
    private static Object nextLaunch(CatchupState state) {
        state.launchPending = false;
        Object launched = new Object();
        state.episode.set(launched);
        return launched;
    }

    // What a replay needs to run, and to run again after the wrapped model lost the live start it handed over.
    // longestReplay is the longest any of its runs took from reading the live start to checking it, which the failure
    // once the live start was lost each time names. owned is the record the subscribe that made it put.
    private record Replay(String subscriptionId, SubscriptionModel delegate, PositionCatchupPipeline pipeline, BoundedIdCache<CatchupEventKey> cache,
                          Function<CloudEvent, Mono<Void>> action, @Nullable SubscriptionFilter liveSubscriptionFilter,
                          Function<CloudEvent, Mono<Void>> liveAction, LiveGate live, LongestReplay longestReplay, Owned owned) {
    }

    // Runs the replay from the start firstStart resolves in the scope of the attempt, through the caller's action without
    // retry (failure is loud), then hands over to the wrapped model. notBefore is the live start an earlier attempt
    // stored, which the scope's reads must see everything up to, or null. replaysAgain is how many times the replay
    // already ran again. takenOver is the handover whose lost live start this replay takes over, or null for a replay a
    // subscribe started.
    private Consumer<Object> launcher(Replay replay, CatchupState state, @Nullable String notBefore, Function<CatchupScope, Mono<PositionCatchupPipeline.ReplayStart>> firstStart, int replaysAgain, @Nullable HandedOver takenOver) {
        String subscriptionId = replay.subscriptionId();
        return launched -> {
            // A relaunch reads the history again from the same start position, so it is a different catch-up and
            // announces itself as one. Sent before the subscribe below, so it precedes anything this launch
            // delivers.
            announceCatchupStarted(subscriptionId, state, launched);
            Disposable replaying = replayThenHandOver(replay, state, launched, notBefore, firstStart, replaysAgain)
                    .subscribe(unused -> {
                    }, throwable -> {
                        if (HistoryLossMessages.isHistoryLost(throwable) && holdForLostLiveStart(replay, state, launched, takenOver, throwable)) {
                            return;
                        }
                        // Under the lock of the id, so a resume or a report of the loss finds both the state gone and
                        // the handover counting its replays again from 0. The removal comes first for the readers that
                        // take no lock.
                        // Identity-checked, since a cancel and a subscribe of the id can have put a new state by the time
                        // this failure reaches here.
                        // Under the state monitor too, where a stop and a start or a resume name a newer launch, which
                        // runs on the same state and owns it, so the failure of this launch changes nothing of it
                        boolean replaced = underIdLock(subscriptionId, () -> {
                            synchronized (state) {
                                if (state.episode.get() != launched) {
                                    return true;
                                }
                                catchingUp.remove(subscriptionId, state);
                                if (takenOver != null) {
                                    // So a resume replays again, counting its replays again from 0
                                    handedOver.computeIfPresent(subscriptionId, (id, made) -> made == takenOver ? made.withTheReplaysAgainOf(0) : made);
                                }
                                return false;
                            }
                        });
                        if (replaced) {
                            log.warn("The catch-up replay for subscription {} failed after a stop and a start or a resume ran a newer replay, which keeps running.", subscriptionId, throwable);
                            return;
                        }
                        if (takenOver != null) {
                            replay.live().open(state);
                        }
                        // Only a replay that never handed over leaves no subscription behind, so a subscribe of the id
                        // replays again, unless a takeover in flight took over the subscription the wrapped model holds
                        boolean takenOverInFlight = takenOver == null && !state.handedOver.get() && !release(subscriptionId, replay.owned());
                        if (state.handedOver.get()) {
                            // Only the pause asked for during the replay can fail once the wrapped model has the subscription
                            log.error("Subscription {} went live once its catch-up replay was done, but pausing it as asked for during the replay failed, so it may not be paused.",
                                    subscriptionId, throwable);
                        } else if (takenOverInFlight) {
                            log.error("The catch-up of subscription {} failed, so its start fails, but the subscription model lost the history from its live start meanwhile and a takeover replay in flight takes the subscription over.",
                                    subscriptionId, throwable);
                        } else if (takenOver == null) {
                            // A failed replay is a dead subscription, reported to whoever waits AND logged: a caller
                            // that never blocks on waitUntilStarted() would otherwise get a dead subscription with no trace.
                            log.error("The catch-up replay for subscription {} failed; the subscription is dead until it is subscribed again.", subscriptionId, throwable);
                        } else if (replay.owned().historyLost != null) {
                            String lost = requireNonNull(replay.owned().historyLost).asString();
                            log.error(HistoryLossMessages.TERMINAL_FAILURE_LOG, subscriptionId, lost, lost, throwable);
                        } else if (requireNonNull(named).isPaused(subscriptionId)) {
                            log.error("The catch-up replay for subscription {} after the subscription model lost the history from its live start failed, so the subscription stays paused, and resuming it replays again.",
                                    subscriptionId, throwable);
                        } else {
                            log.error("The catch-up replay for subscription {} after the subscription model lost the history from its live start failed, and the subscription model no longer holds it paused. The catch-up replays again when the subscription model reports that live start lost again, or when the subscription is resumed once it is paused.",
                                    subscriptionId, throwable);
                        }
                        failStart(state, subscriptionId, throwable);
                    });
            // A stop(), a start(..) or a cancel since this launch was named, on this thread or another, may have parked
            // it, run a newer one or cancelled it, and then the field isn't this launch's
            boolean current;
            synchronized (state) {
                current = isCurrent(state, launched);
                if (current) {
                    state.replaying.set(replaying);
                }
            }
            if (!current) {
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
        // Also checked for an event the replay read before that, so a launch that a stop, a start or a cancel has
        // replaced starts the action for none of the events it still has queued
        Function<CloudEvent, Mono<Void>> action = cloudEvent -> keepReplaying.getAsBoolean() ? replay.action().apply(cloudEvent) : Mono.empty();
        return Flux.defer(() -> {
            AtomicReference<PositionCatchupPipeline.ReplayStart> resolved = new AtomicReference<>();
            return pipeline.<Void>inScope(notBefore, firstStart, (replayStart, reader) -> pipeline.replayApplying(reader, replayStart, replay.cache(), keepReplaying, action,
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
    // catch-up that is already over. The listener runs while the map operation holds a lock of the map, so a listener
    // that calls this model can deadlock with another thread, since a pause, resume, stop or shutdown takes a state
    // monitor that a handover holds while it takes that lock.
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
        requireNotShutdown(SubscriptionModelShutdownException::new);
        // Before the wrapped model opens, since it can report a loss before its subscribe returns
        Owned record = new Owned();
        underIdLock(subscriptionId, () -> {
            if (holds(delegate, subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            forgetATakeoverNeverLaunched(subscriptionId);
            // own(..) reads catchingUp only when it replaces a record, and a takeover can be in flight with no record
            // after a cancel of the id was refused
            if (catchingUp.containsKey(subscriptionId) || !own(subscriptionId, delegate, record)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            record.subscribing = true;
            return null;
        });
        // With no lock held, since the wrapped model can run a dynamic start of the caller's on this thread
        Function<CloudEvent, Mono<Void>> liveAction = cloudEvent -> !record.cancelled && livePredicate.test(cloudEvent) ? action.apply(cloudEvent) : Mono.empty();
        @Nullable Subscription subscription;
        try {
            subscription = cancellable != null
                    ? cancellable.subscribe(subscriptionId, liveSubscriptionFilter, startAt, liveAction, () -> record.cancelled)
                    : delegate.subscribe(subscriptionId, liveSubscriptionFilter, startAt, liveAction);
        } catch (RuntimeException | Error e) {
            subscribeReturned(subscriptionId, record, false, e);
            throw e;
        }
        subscribeReturned(subscriptionId, record, subscription != null, null);
        if (subscription == null) {
            return new NamedCatchupSubscription(subscriptionId, Mono.error(new IllegalStateException("Subscription " + subscriptionId + " was cancelled before it started.")));
        }
        return subscription;
    }

    // A cancel during the wrapped subscribe cancelled what the wrapped model held by then, and what that subscribe took
    // after it is cancelled once no live record owns the id, see settleHoldingIdLock. A subscribe that took the id
    // shows that no cancelled subscribe that returned before it holds the id.
    private void subscribeReturned(String subscriptionId, Owned record, boolean tookTheId, @Nullable Throwable failure) {
        settle(subscriptionId, failure, () -> {
            record.subscribing = false;
            Leftovers left = leftovers.get(subscriptionId);
            // None for a cancelled record after a shutdown, which forgets them all
            if (record.cancelled) {
                if (left != null) {
                    left.underWay--;
                }
            } else if (!tookTheId) {
                owned.remove(subscriptionId, record);
            } else if (left != null && left.underWay == 0) {
                leftovers.remove(subscriptionId);
            }
        });
    }

    // Removes the record of a subscribe with catch-up that failed, and the handover of that record. A takeover in flight
    // here that took over that subscribe's handover keeps both, since that takeover resumes the subscription the
    // wrapped model holds paused, a loss reported for the subscription is taken over only while the record is there,
    // and a resume after that takeover failed replays again only while the handover is there. False when a takeover
    // kept them.
    private boolean release(String subscriptionId, Owned record) {
        AtomicBoolean released = new AtomicBoolean();
        settle(subscriptionId, null, () -> {
            CatchupState inFlight = catchingUp.get(subscriptionId);
            if (inFlight == null || inFlight.takesOver == null || inFlight.takesOver.replay().owned() != record) {
                owned.remove(subscriptionId, record);
                forgetTheHandoverOf(subscriptionId, record);
                released.set(true);
            }
        });
        return released.get();
    }

    // A cancel that threw, or whose Mono from the wrapped model failed. A live record whose subscription the wrapped
    // model can run stays while the wrapped model holds the id or can't tell, so that subscription keeps reaching this
    // model, and goes with its handover as in own(..) once the wrapped model holds nothing. A record whose replay this
    // cancel stopped before it handed over goes at once, since what the wrapped model holds under the id then is what a
    // cancelled subscribe left. That is settled as when a cancel ends, since with no settle the id can stay held for
    // good.
    private void cancelRefused(String subscriptionId, @Nullable Owned record, boolean runsInWrappedModel, @Nullable Throwable failure) {
        settle(subscriptionId, failure, () -> {
            if (record != null && !record.cancelled && (!runsInWrappedModel || !holdsOrCannotTell(subscriptionId, failure))) {
                owned.remove(subscriptionId, record);
                forgetTheHandoverOf(subscriptionId, record);
            }
        });
    }

    // Runs change under the per-id lock, then what settleHoldingIdLock says. failure is what the call that settles
    // failed with, or null
    private void settle(String subscriptionId, @Nullable Throwable failure, Runnable change) {
        boolean cancel = underIdLock(subscriptionId, () -> {
            change.run();
            return settleHoldingIdLock(subscriptionId, failure);
        });
        if (cancel) {
            cancelLeftover(subscriptionId);
        }
    }

    // Called under the per-id lock once a cancelled subscribe returned, a subscribe failed, or a cancel ended or was
    // refused. While a cancelled subscribe is under way, and while the cancel of what it left runs, the id has a
    // record, so a loss the wrapped model reports for what that subscribe takes pauses it, and isRunning(..) answers
    // false. Once none is under way and no live record owns the id, true says the caller is to cancel the id in the
    // wrapped model, counted under way, when a cancelled subscribe returned since the last such cancel and the wrapped
    // model holds the id or can't tell. A cancelled record is forgotten once no cancel is under way and the wrapped
    // model tells it no longer holds the id, so isRunning(..) answers false while the wrapped model runs what that
    // subscribe left. A later subscribe of the id still inside the wrapped model puts a live record meanwhile, and
    // isRunning(..) then answers what the wrapped model runs under the id.
    private boolean settleHoldingIdLock(String subscriptionId, @Nullable Throwable failure) {
        Leftovers left = leftovers.get(subscriptionId);
        if (left != null && left.underWay > 0) {
            owned.putIfAbsent(subscriptionId, left.standIn);
            return false;
        }
        Owned current = owned.get(subscriptionId);
        if (catchingUp.containsKey(subscriptionId) || current != null && !current.cancelled) {
            return false;
        }
        if (left != null) {
            leftovers.remove(subscriptionId);
            if (holdsOrCannotTell(subscriptionId, failure)) {
                // Forgotten by cancelEnded(..) once that cancel ended
                owned.putIfAbsent(subscriptionId, left.standIn);
                cancelsUnderWay.merge(subscriptionId, 1, Integer::sum);
                return true;
            }
        }
        if (current != null && !cancelsUnderWay.containsKey(subscriptionId) && !holdsOrCannotTell(subscriptionId, failure)) {
            owned.remove(subscriptionId, current);
        }
        return false;
    }

    private void cancelLeftover(String subscriptionId) {
        try {
            cancelInWrappedModel(subscriptionId, new Cancelled(null, null, null, null), cancelUnderWayEnds(subscriptionId));
        } catch (RuntimeException | Error e) {
            log.warn("Could not cancel subscription {} in the wrapped model {}. That model may still hold the subscription.", subscriptionId, requireNonNull(named).getClass().getName(), e);
        }
    }

    // Called under the lock of the id, once the wrapped model was found to hold no subscription of the id. Forgets a
    // takeover registered for a subscription the wrapped model ended before it paused the subscription for it, so a
    // subscribe of the id goes through as it does when no takeover was registered. Cancelled under its monitor, so the
    // launch the wrapped model runs once it paused the subscription finds it cancelled and runs nothing. The gate that
    // registerTakeOver closed stays closed, since it holds only events of the subscription that ended.
    private void forgetATakeoverNeverLaunched(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state == null || state.takesOver == null) {
            return;
        }
        synchronized (state) {
            if (!state.launchPending || state.cancelled.get()) {
                return;
            }
            state.cancelled.set(true);
        }
        catchingUp.remove(subscriptionId, state);
    }

    // Called under the per-id lock. Puts record for the id, replacing a record whose subscription is gone, which is one
    // with no replay in flight here that the wrapped model neither runs nor holds paused. False when
    // the id has a live record, or a cancel of it has not had the wrapped model cancel yet. catchingUp is read first,
    // since a handover registers the id in the wrapped model before it removes the id from catchingUp.
    private boolean own(String subscriptionId, SubscriptionModel delegate, Owned record) {
        if (cancelsUnderWay.containsKey(subscriptionId)) {
            return false;
        }
        Owned existing = owned.putIfAbsent(subscriptionId, record);
        if (existing == null) {
            return true;
        }
        if (existing.subscribing && !existing.cancelled) {
            return false;
        }
        boolean gone = !catchingUp.containsKey(subscriptionId) && !holds(delegate, subscriptionId);
        if (!gone || !owned.replace(subscriptionId, existing, record)) {
            return false;
        }
        // Forgets the handover of the subscription replaced here
        forgetTheHandoverOf(subscriptionId, existing);
        return true;
    }

    // Called under the per-id lock once record no longer owns the id, so a later subscription of the id at the same
    // live start never finds that handover
    private void forgetTheHandoverOf(String subscriptionId, Owned record) {
        handedOver.computeIfPresent(subscriptionId, (id, made) -> made.replay().owned() == record ? null : made);
    }

    // Whether the wrapped model runs the id or holds it paused. A model that lists its ids answers from that list, since
    // ReactorMongoSubscriptionModel moves an id between its running and paused maps in two steps, and isRunning(..) and
    // isPaused(..) read between them both answer false. Its list is read under its monitor, which it never holds while it
    // calls the catch-up.
    private static boolean holds(SubscriptionModel delegate, String subscriptionId) {
        if (delegate instanceof IntrospectableSubscriptions introspectable) {
            return introspectable.subscriptionIds().contains(subscriptionId);
        }
        return delegate.isRunning(subscriptionId) || delegate.isPaused(subscriptionId);
    }

    // Whether the wrapped model holds the id, for a cancel or a subscribe that settles. True when asking throws, so the
    // id is settled as one the wrapped model holds. What asking threw is added to failure, or logged when there is no
    // failure.
    private boolean holdsOrCannotTell(String subscriptionId, @Nullable Throwable failure) {
        if (named == null) {
            return false;
        }
        try {
            return holds(named, subscriptionId);
        } catch (Throwable e) {
            if (failure == null) {
                log.warn("Could not tell whether the wrapped model {} holds subscription {}, so this model keeps the subscription as one that model holds.", named.getClass().getName(), subscriptionId, e);
            } else if (failure != e) {
                failure.addSuppressed(e);
            }
            return true;
        }
    }

    // Runs work holding the lock of subscriptionId, which is never held across anything asynchronous. Locks are taken in
    // the order startLock, the lock of an id, the monitor of a state, the wrapped model's own locks, and no code holding
    // one of them takes one before it. The wrapped model's locks are taken under the lock of an id by holds(..), and by
    // isPaused(..) in the answer to a loss and in a resume, under the monitor of a state by the handover's subscribe,
    // resumeAt, isRunning and pause, and under startLock by the wrapped start. A wrapped start that ran a takeover on the
    // calling thread would take the lock of an id under startLock, which this order allows only while that start holds
    // none of its own locks.
    //
    // A thread waiting under the lock of an id for a lock of the wrapped model deadlocks only when the wrapped model
    // holds that lock while it calls this model on a thread that then waits for the lock of the id, as the answer to a
    // loss does. ReactorMongoSubscriptionModel asks the listener and runs what runs once it paused the subscription with
    // none of its locks held. Its isPaused(..) takes no lock, and its subscriptionIds() takes its monitor, under which it
    // calls no listener. ReactorDurableSubscriptionModel as the wrapped model passes the listener on to the model it
    // wraps and calls no listener itself. Its isPaused(..) and subscriptionIds() take its positionLock and ask the
    // model it wraps outside it, and no block of it that holds a monitor calls the model it wraps, an action or a
    // listener. Neither can call this model holding a lock that a thread under the lock of an id waits for. A wrapped
    // model that reports a loss on the thread of a handover breaks the order instead, since the answer then takes the
    // lock of the id under the monitor of a state, which the class javadoc rules out.
    private <T> T underIdLock(String subscriptionId, Supplier<T> work) {
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
        Subscription delegated;
        boolean handing = false;
        try {
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
                        : new HandedOver(replayStart.liveFrom(), takenOver == null ? null : takenOver.liveStart(), replay, replayStart, replaysAgain, state, new AtomicBoolean());
                if (made != null) {
                    handedOver.put(subscriptionId, made);
                }
                // A pause of the wrapped model cancels the Mono of the action it runs, so the live action holds every
                // event until the pause below has returned, and the pause cancels none of the caller's code. A
                // takeover closed the gate for its own state when it started, and one that replaces this state
                // meanwhile keeps it closed until its own handover.
                handing = true;
                if (state.pendingPause.get()) {
                    replay.live().close(state);
                }
                try {
                    delegated = takenOver == null ? delegate.subscribe(subscriptionId, replay.liveSubscriptionFilter(), liveStartAt, replay.liveAction()) : resumeAt(subscriptionId, delegate, liveStartAt);
                } catch (RuntimeException | Error e) {
                    // A takeover puts back the record it took over, so a resume, or a loss of that live start reported
                    // again, replays again. A subscribe keeps its own handover here, and release(..) removes it unless a
                    // takeover was registered meanwhile, under the lock of the id that registering a takeover takes.
                    if (made != null && takenOver != null) {
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
            }
        } finally {
            // Outside the monitor, since an event the live action held runs the caller's action now
            if (handing) {
                replay.live().open(state);
            }
        }
        delegated.waitUntilStarted().subscribe(unused -> {
        }, failure -> failStart(state, subscriptionId, failure), state.started::tryEmitEmpty);
    }

    // Resumes the subscription the wrapped model holds paused at liveStartAt. A start(true) of the wrapped model, or of
    // the catch-up model, can resume it at the lost live start until the resume here returns, and it is paused again
    // each time. Throws when the resume is refused while the wrapped model doesn't run the subscription, and when the pause
    // finds the id gone.
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
     * over, which stays in flight until its handover returns. {@code takeOverAsked} is set once this model has answered a
     * loss of {@code liveStart} by taking the subscription over, so a resume with no takeover in flight replays again
     * instead of resuming the wrapped model there. {@code lostLiveStart} is the live start the wrapped model lost when a
     * takeover replay handed over, and {@code null} otherwise. The wrapped model reports a loss of it only for a
     * subscription a start(true) resumed there before the handover resumed it at {@code liveStart}, and taking that loss
     * over has the wrapped model pause the subscription again.
     */
    private record HandedOver(Checkpoint liveStart, @Nullable Checkpoint lostLiveStart, Replay replay, PositionCatchupPipeline.ReplayStart replayStart, int replaysAgain,
                              CatchupState handingOver, AtomicBoolean takeOverAsked) {
        HandedOver withTheReplaysAgainOf(int replaysAgain) {
            return new HandedOver(liveStart, lostLiveStart, replay, replayStart, replaysAgain, handingOver, takeOverAsked);
        }
    }

    // Asked by the wrapped model on a thread that reads subscriptions, holding none of its locks, and answered under the
    // lock of the id, so a pause, a resume or a cancel of the id that takes that lock comes before the answer or finds
    // the takeover it registers. A subscription this model didn't make is left to the wrapped model. A loss of the live
    // start a takeover handing over replaces is taken over with nothing to run, and that handover resumes the
    // subscription at its own live start. A loss of the live start the last replay of the id's current subscription
    // handed over registers a takeover replay in catchingUp. What the wrapped model runs once it paused the subscription
    // launches that replay unless it is parked or cancelled, or a resume or a start(true) launched it first. When a
    // takeover is in flight already, the loss is taken over for it, so the wrapped model keeps the subscription paused.
    // When the wrapped model already holds the subscription paused, the loss is taken over with nothing to run, and a
    // resume registers the takeover and runs it. Any other loss pauses the subscription for good.
    private HistoryLossHandling takeOverIfHandedOverAt(String subscriptionId, Checkpoint lostFrom) {
        return underIdLock(subscriptionId, () -> {
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
                    made.takeOverAsked().set(true);
                    CatchupState inFlight = catchingUp.get(subscriptionId);
                    if (inFlight != null && inFlight != made.handingOver()) {
                        return HistoryLossHandling.takeOver(() -> launchIfPending(inFlight));
                    }
                    if (requireNonNull(named).isPaused(subscriptionId)) {
                        return HistoryLossHandling.takeOver(() -> {
                        });
                    }
                    CatchupState taking = registerTakeOver(subscriptionId, made);
                    return HistoryLossHandling.takeOver(() -> {
                        if (taking != null) {
                            launchIfPending(taking);
                        }
                    });
                }
            }
            // No replay of this model can take the loss over without skipping events, so the subscription stays paused.
            // Recorded in afterPause, which the wrapped model runs only for a run that is still current, so a late report
            // never reaches a subscription made since. A resume before it is recorded reopens at the lost position and is
            // paused again.
            return HistoryLossHandling.takeOver(() -> {
                record.historyLost = lostFrom;
                String lost = lostFrom.asString();
                log.error(HistoryLossMessages.TERMINAL_FAILURE_LOG, subscriptionId, lost, lost);
            });
        });
    }

    // Called under the lock of the id. Registers a replay that runs again from the position the replay that handed over
    // started from, with a live start read when it runs, as the one in flight for the id. It replaces the replay that
    // handed over when that handover has not returned yet, and keeps a pause asked for during it. Registered parked
    // while this model is stopped. Null when it registers none, because a subscribe replaced the subscription, a cancel
    // of it forgot the handover, another replay of the id is in flight or this model shut down.
    private @Nullable CatchupState registerTakeOver(String subscriptionId, HandedOver made) {
        Replay replay = made.replay();
        if (shutdown || handedOver.get(subscriptionId) != made || owned.get(subscriptionId) != replay.owned()) {
            return null;
        }
        CatchupState state = new CatchupState(made);
        state.launcher = launcher(replay, state, null, replay.pipeline().replayAgainAfterLiveStartLost(wrapped, made.replayStart(), subscriptionId, made.replaysAgain(), replay.longestReplay()),
                made.replaysAgain() + 1, made);
        CatchupState inFlight = catchingUp.compute(subscriptionId, (id, current) -> {
            if (current != null && current != made.handingOver()) {
                return current;
            }
            if (current != null) {
                state.pendingPause.set(current.pendingPause.get());
            }
            // Closed before the put, since a stop() and a start(true) take no lock of the id and can launch the takeover
            // as soon as it is in the map, and its handover opens only a gate this state already holds. Opened by that
            // handover, by a failure of the takeover or by a cancel of it that fails. A start(true) can resume the
            // subscription in the wrapped model meanwhile, and the handover pauses it again before it resumes it at the
            // new live start.
            replay.live().close(state);
            return state;
        });
        if (inFlight != state) {
            return null;
        }
        // Read again after the put, since shutdown() and stop() set their flag and then read the ids in flight without
        // this lock. Either this sees the flag, or that read finds this state. The gate stays closed, as it does for a
        // takeover that shutdown() cancels.
        if (shutdown) {
            catchingUp.remove(subscriptionId, state);
            return null;
        }
        synchronized (state) {
            if (stopped) {
                state.parked.set(true);
            }
        }
        return state;
    }

    // --- Life cycle. Each call forwards to the wrapped model, except a start, pause or resume that finds this model shut
    // --- down. The extra bookkeeping is only for subscriptions whose replay has not handed over yet, which the wrapped
    // --- model does not know about.

    void stop() {
        // Safe no-op on a cold-only composition: nothing named can be running, and a Spring context stop must not
        // throw for a capability the application never used.
        if (!managesNamedSubscriptions()) {
            return;
        }
        stopped = true;
        catchingUp.values().forEach(state -> {
            // The per-state monitor closes the race with handOver: either the handover completed first (the wrapped
            // model's stop() below covers the live subscription), or the replay is parked before it can hand over.
            Disposable replaying = null;
            synchronized (state) {
                if (!state.handedOver.get() && !state.cancelled.get()) {
                    replaying = state.replaying.getAndSet(null);
                    state.parked.set(true);
                }
            }
            // Outside the monitor, since disposing the replay cancels its action, which runs the caller's code
            if (replaying != null) {
                replaying.dispose();
            }
        });
        named.stop();
    }

    void start(boolean resumeSubscriptionsAutomatically) {
        if (managesNamedSubscriptions()) {
            startUnlessShutdown(resumeSubscriptionsAutomatically);
        }
    }

    // False when this model was shut down, which starts nothing
    private boolean startUnlessShutdown(boolean resumeSubscriptionsAutomatically) {
        SubscriptionModel delegate = requireNamed();
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
        synchronized (startLock) {
            if (shutdown) {
                return false;
            }
            delegate.start(resumeSubscriptionsAutomatically);
            stopped = false;
        }
        if (!resumeSubscriptionsAutomatically) {
            return true;
        }
        catchingUp.values().forEach(state -> {
            Object launched;
            synchronized (state) {
                launched = launch(state, true);
            }
            if (launched != null) {
                state.launcher.accept(launched);
            }
        });
        return true;
    }

    boolean isStopped() {
        return stopped;
    }

    // Whether launched is the replay that owns the subscription right now. A stop() parks the subscription and a
    // start(true) or resume runs a new launch, so an older launch still running past its last check is not current.
    private static boolean isCurrent(CatchupState state, Object launched) {
        return !state.cancelled.get() && !state.parked.get() && state.episode.get() == launched;
    }


    // Not running until start(..) has cleared stopped, even when the wrapped model already runs, since a subscribe
    // before that is paused
    boolean isRunning() {
        return managesNamedSubscriptions() && !stopped && named.isRunning();
    }

    boolean isRunning(String subscriptionId) {
        if (cancelledUnderWay(subscriptionId)) {
            return false;
        }
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return !stopped && !state.parked.get() && !state.pendingPause.get() && !state.cancelled.get();
        }
        return managesNamedSubscriptions() && named.isRunning(subscriptionId);
    }

    private boolean cancelledUnderWay(String subscriptionId) {
        Owned record = owned.get(subscriptionId);
        return record != null && record.cancelled;
    }

    boolean isPaused(String subscriptionId) {
        if (cancelledUnderWay(subscriptionId)) {
            return false;
        }
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return (state.parked.get() || state.pendingPause.get()) && !state.cancelled.get();
        }
        return managesNamedSubscriptions() && named.isPaused(subscriptionId);
    }

    void pauseSubscription(String subscriptionId) {
        // A cold-only composition knows no subscription by id, whether it is shut down or not
        if (!managesNamedSubscriptions()) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        requireNotShutdown(() -> shutDownRefusal("pause", subscriptionId));
        // A cancelled subscribe whose subscription the wrapped model still runs, which isRunning(..) and isPaused(..)
        // answer false for
        if (cancelledUnderWay(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                requireNotShutdown(() -> shutDownRefusal("pause", subscriptionId));
                if (!state.handedOver.get()) {
                    if (state.parked.get() || state.pendingPause.getAndSet(true)) {
                        throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
                    }
                    return;
                }
            }
        }
        try {
            named.pauseSubscription(subscriptionId);
        } catch (SubscriptionNotRunningException e) {
            // The wrapped model paused the subscription for a takeover registered since the read above
            if (!pauseTakeOverRegisteredSince(subscriptionId)) {
                throw e;
            }
            return;
        }
        // Marks a takeover registered since the read above paused, as the branch above marks a replay in flight.
        // ReactorMongoSubscriptionModel never runs a takeover when this pause reached it before its own pause for that
        // takeover, so that takeover runs once the subscription is resumed.
        pauseTakeOverRegisteredSince(subscriptionId);
    }

    // Marks the takeover in flight for the id paused, unless it handed over, is paused or parked already, or was
    // cancelled. Under the lock of the id, so a report of the loss that pauses the subscription either registered its
    // takeover before this or finds the wrapped model holding the subscription paused and registers none.
    private boolean pauseTakeOverRegisteredSince(String subscriptionId) {
        return underIdLock(subscriptionId, () -> {
            CatchupState state = catchingUp.get(subscriptionId);
            if (state == null) {
                return false;
            }
            synchronized (state) {
                return !state.handedOver.get() && !state.cancelled.get() && !state.parked.get() && !state.pendingPause.getAndSet(true);
            }
        });
    }

    Subscription resumeSubscription(String subscriptionId) {
        // A cold-only composition knows no subscription by id, whether it is shut down or not
        if (!managesNamedSubscriptions()) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        requireNotShutdown(() -> shutDownRefusal("resume", subscriptionId));
        // Before anything starts, so a resume refused here changes nothing
        Owned record = owned.get(subscriptionId);
        if (record != null && record.cancelled) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        Checkpoint historyLost = record == null ? null : record.historyLost;
        if (historyLost != null) {
            throw HistoryLossMessages.historyLost(subscriptionId, historyLost);
        }
        // Resuming one subscription starts a stopped model without resuming the others, as the life-cycle contract
        // says, whether its replay is parked or it has handed over. A start that throws resumes nothing.
        if (stopped && isPaused(subscriptionId) && !startUnlessShutdown(false)) {
            throw shutDownRefusal("resume", subscriptionId);
        }
        Resumed resumed = underIdLock(subscriptionId, () -> resumeHoldingIdLock(subscriptionId));
        // The rest runs the caller's code, so it runs with no lock held
        if (resumed == null) {
            return named.resumeSubscription(subscriptionId);
        }
        // Read before the launch, since a replay it holds and a resume after it give the state a sink of its own
        Mono<Void> started = resumed.state().started.asMono();
        if (resumed.launched() != null) {
            resumed.state().launcher.accept(resumed.launched());
        }
        return new NamedCatchupSubscription(subscriptionId, started);
    }

    // The replay a resume runs, with the launch it runs once the lock of the id is released, or null for none
    private record Resumed(CatchupState state, @Nullable Object launched) {
    }

    // Called under the lock of the id. The replay a resume runs, or null when the resume goes to the wrapped model. A
    // replay in flight that hasn't handed over is resumed, or refuses the resume when it isn't paused. The wrapped model
    // can hold paused, with no takeover in flight, a subscription whose loss of the live start this model answered by
    // taking it over, for instance after the takeover replay failed or a cancel of the takeover was refused. Resuming
    // the wrapped model would open the subscription where it lost the history, so the takeover is registered instead,
    // and runs unless this model is stopped or shut down meanwhile.
    private @Nullable Resumed resumeHoldingIdLock(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                requireNotShutdown(() -> shutDownRefusal("resume", subscriptionId));
                if (!state.handedOver.get()) {
                    boolean pausedDuringReplay = state.pendingPause.getAndSet(false);
                    if (!state.parked.get() && !pausedDuringReplay) {
                        throw new SubscriptionAlreadyRunningException(subscriptionId);
                    }
                    // Launches nothing while this model is stopped. The replay is parked then, as a stop right after this
                    // resume would park it.
                    return new Resumed(state, launch(state, state.parked.get()));
                }
            }
        }
        HandedOver made = handedOver.get(subscriptionId);
        if (made != null && made.takeOverAsked().get() && requireNonNull(named).isPaused(subscriptionId)) {
            CatchupState taking = registerTakeOver(subscriptionId, made);
            if (taking != null) {
                synchronized (taking) {
                    return new Resumed(taking, launch(taking, false));
                }
            }
        }
        // Read after the handover. A shutdown sets the flag before it clears the handovers, and a cancel counts itself
        // under way before it removes the handover, so a resume that found the handover of a lost live start gone is
        // refused here instead of resuming the wrapped model where it lost the history. A resume of an id a cancel is
        // under way for is refused as one of a cancelled subscription.
        requireNotShutdown(() -> shutDownRefusal("resume", subscriptionId));
        if (cancelsUnderWay.containsKey(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        return null;
    }

    Mono<Void> cancelSubscription(String subscriptionId) {
        Cancelled cancel = underIdLock(subscriptionId, () -> cancelHoldingIdLock(subscriptionId));
        // The rest runs the caller's code, so it runs with no lock held. A subscribe of the id is refused until the
        // wrapped model has cancelled, so that cancel can't reach a subscription made since.
        Runnable cancelUnderWayEnds = cancelUnderWayEnds(subscriptionId);
        try {
            if (cancel.replaying() != null) {
                cancel.replaying().dispose();
            }
            if (cancel.failsTheStartOf() != null) {
                // Waiters are failed rather than completed, since completing would claim the subscription started
                // and doing nothing would hang them. The blocking twin answers false, which a Mono<Void> has no
                // room for. The wrapped model is still asked to cancel below, since it can hold what an earlier
                // process stored for this id even though this process never handed the id over.
                failStart(cancel.failsTheStartOf(), subscriptionId, new IllegalStateException("Subscription " + subscriptionId + " was cancelled before it started."));
            }
        } catch (RuntimeException | Error e) {
            cancelThrew(subscriptionId, cancel, cancelUnderWayEnds, e);
            throw e;
        }
        // A cold-only composition runs nothing by name and stores nothing by id, so there is nothing to stop or delete
        if (!managesNamedSubscriptions()) {
            cancelUnderWayEnds.run();
            cancelEnded(subscriptionId, cancel.record());
            return Mono.empty();
        }
        return cancelInWrappedModel(subscriptionId, cancel, cancelUnderWayEnds);
    }

    private Mono<Void> cancelInWrappedModel(String subscriptionId, Cancelled cancel, Runnable cancelUnderWayEnds) {
        Mono<Void> cancelled;
        try {
            // Ended before a subscriber of the cancel is told it completed, so a subscribe of the id from there goes
            // through. The handover is put back before that too, so a resume from there replays again.
            AtomicReference<@Nullable Throwable> refusal = new AtomicReference<>();
            cancelled = requireNonNull(named).cancelSubscription(subscriptionId).doOnError(failure -> {
                refusal.set(failure);
                putBackTheHandover(subscriptionId, cancel, failure);
            }).doOnTerminate(cancelUnderWayEnds).doFinally(signal -> {
                cancelUnderWayEnds.run();
                if (signal == SignalType.ON_ERROR) {
                    cancelRefused(subscriptionId, cancel.record(), cancel.runsInWrappedModel(), refusal.get());
                } else {
                    cancelEnded(subscriptionId, cancel.record());
                }
            }).cache();
        } catch (RuntimeException | Error e) {
            cancelThrew(subscriptionId, cancel, cancelUnderWayEnds, e);
            throw e;
        }
        // Subscribed here and cached, so ownership is forgotten even when nothing subscribes to what this returns,
        // and a wrapped cancel that does nothing until subscribed is not run twice
        cancelled.subscribe(unused -> {
        }, throwable -> log.warn("Could not cancel subscription {} in the wrapped model {}. That model may still hold the subscription.",
                subscriptionId, requireNonNull(named).getClass().getName(), throwable));
        return cancelled;
    }

    // A cancel that threw, or whose Mono from the wrapped model failed, puts back the handover it removed, while the
    // record it read still owns the id and the wrapped model still holds the id or can't tell, so a resume, or a loss
    // of that live start reported again, replays again instead of resuming the wrapped model where it lost the history.
    // Called before the cancel ends, so a resume after it finds the handover. Opens the gate that a takeover the cancel
    // stopped closed, since no handover of that takeover opens it now.
    private void putBackTheHandover(String subscriptionId, Cancelled cancel, Throwable failure) {
        HandedOver handover = cancel.handover();
        Owned record = cancel.record();
        if (handover != null && record != null) {
            underIdLock(subscriptionId, () -> {
                if (!shutdown && !record.cancelled && owned.get(subscriptionId) == record && handover.replay().owned() == record && holdsOrCannotTell(subscriptionId, failure)) {
                    handedOver.putIfAbsent(subscriptionId, handover);
                }
                return null;
            });
        }
        CatchupState stopped = cancel.failsTheStartOf();
        if (stopped != null && stopped.takesOver != null) {
            stopped.takesOver.replay().live().open(stopped);
        }
    }

    // A cancel that threw. The cancel ends, and its record is settled, even when putting the handover back throws. A
    // RuntimeException from putting it back is added to failure, so the caller still throws what the cancel threw. An
    // Error from putting it back is thrown instead, with failure added to it.
    private void cancelThrew(String subscriptionId, Cancelled cancel, Runnable cancelUnderWayEnds, Throwable failure) {
        try {
            putBackTheHandover(subscriptionId, cancel, failure);
        } catch (RuntimeException e) {
            if (e != failure) {
                failure.addSuppressed(e);
            }
        } catch (Error e) {
            if (e != failure) {
                e.addSuppressed(failure);
            }
            throw e;
        } finally {
            cancelUnderWayEnds.run();
            cancelRefused(subscriptionId, cancel.record(), cancel.runsInWrappedModel(), failure);
        }
    }

    // What a cancel took under the per-id lock, for the work it does once that lock is released. failsTheStartOf is the
    // state whose waiters it fails, since the subscription never handed over, or null. handover is the handover it
    // removed, or null.
    private record Cancelled(@Nullable Owned record, @Nullable Disposable replaying, @Nullable CatchupState failsTheStartOf, @Nullable HandedOver handover) {
        // False for a replay this cancel stopped before it handed over, unless it takes over a subscription the wrapped
        // model holds paused
        boolean runsInWrappedModel() {
            return failsTheStartOf == null || failsTheStartOf.takesOver != null;
        }
    }

    private Cancelled cancelHoldingIdLock(String subscriptionId) {
        // Before the handover is removed, so a resume that finds no handover sees this cancel under way and is refused
        cancelsUnderWay.merge(subscriptionId, 1, Integer::sum);
        // A loss reported after this removal finds no handover of this subscription to take over, unless a handover in
        // flight puts its record before the removal below. registerTakeOver checks the handover under this lock.
        HandedOver removed = handedOver.remove(subscriptionId);
        // Forgotten only once the wrapped model has cancelled, so a loss it reports meanwhile still pauses the
        // subscription rather than letting the wrapped model restart it from now
        Owned record = owned.get(subscriptionId);
        if (record != null && record.subscribing && !record.cancelled) {
            record.cancelled = true;
            leftovers.computeIfAbsent(subscriptionId, id -> new Leftovers(record)).underWay++;
        }
        CatchupState state = catchingUp.remove(subscriptionId);
        Disposable replaying = null;
        boolean failsStart = false;
        if (state != null) {
            // A handover either went first, and the wrapped model's cancel cancels what it subscribed, or it finds the
            // state cancelled and hands nothing over
            synchronized (state) {
                state.cancelled.set(true);
                replaying = state.replaying.get();
                failsStart = !state.handedOver.get();
            }
        }
        // A handover that held the state monitor between the removal at the top and the block above put its record then
        HandedOver putMeanwhile = handedOver.remove(subscriptionId);
        return new Cancelled(record, replaying, failsStart ? state : null, putMeanwhile != null ? putMeanwhile : removed);
    }

    // Ends one cancel of subscriptionId in cancelsUnderWay, however often it runs
    private Runnable cancelUnderWayEnds(String subscriptionId) {
        AtomicBoolean ended = new AtomicBoolean();
        return () -> {
            if (ended.compareAndSet(false, true)) {
                cancelsUnderWay.computeIfPresent(subscriptionId, (id, cancels) -> cancels == 1 ? null : cancels - 1);
            }
        };
    }

    // Removes the record the cancel read, unless a subscribe of it was under way, so a subscribe of the id after the
    // cancel keeps its own. A record whose subscribe was under way stays, marked, and settleHoldingIdLock forgets it.
    private void cancelEnded(String subscriptionId, @Nullable Owned record) {
        settle(subscriptionId, null, () -> {
            if (record != null && !record.cancelled) {
                owned.remove(subscriptionId, record);
            }
        });
    }

    void shutdown() {
        // Before the ids are read below, which the second check in subscribeWithCatchup depends on.
        beginShutdown();
        catchupListeners.clear();
        handedOver.clear();
        if (historyLossReporting != null) {
            historyLossReporting.removeHistoryLossListener(historyLossListener);
        }
        owned.clear();
        leftovers.clear();
        new ArrayList<>(catchingUp.keySet()).forEach(subscriptionId -> {
            CatchupState state = catchingUp.remove(subscriptionId);
            if (state != null) {
                Disposable replaying;
                boolean failsStart;
                synchronized (state) {
                    state.cancelled.set(true);
                    replaying = state.replaying.get();
                    failsStart = !state.handedOver.get();
                }
                // Outside the monitor, since both run the caller's code
                if (replaying != null) {
                    replaying.dispose();
                }
                // Disposing the replay runs none of its callbacks, so without this waitUntilStarted() never completes.
                if (failsStart) {
                    failStart(state, subscriptionId, new SubscriptionModelShutdownException());
                }
            }
        });
        // Safe no-op on a cold-only composition: a Spring context close (destroyMethod = "shutdown") must not throw.
        if (!managesNamedSubscriptions()) {
            return;
        }
        named.shutdown();
    }

    // Refuses every later start, and waits for a start under way to return from the wrapped model. Called by
    // ReactorCatchupSubscriptionModel on each inner model before it shuts any of them down, since they share one
    // wrapped model.
    void beginShutdown() {
        synchronized (startLock) {
            shutdown = true;
        }
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
        // Replaced once a resume or a start(true) runs a replay again that was held because the wrapped model lost its live
        // start, since that hold failed the one before
        volatile Sinks.Empty<Void> started = Sinks.empty();
        // Set under the state monitor while the replay is held because the wrapped model lost the live start it would go
        // live from. resumeSkipsEvents says the replay started after the beginning, so it never runs again.
        volatile boolean heldForLostLiveStart;
        volatile boolean resumeSkipsEvents;
        // The episode each launch announces. A stop parks this state and a resume runs its launcher again, reading
        // the history from the beginning, which is a different catch-up and gets a value of its own.
        final AtomicReference<Object> episode = new AtomicReference<>(new Object());
        // Runs the replay, and runs it again. Assigned once, in subscribeWithCatchup or registerTakeOver, before the
        // state is published. Run only with a launch that launch(..) named, once the state monitor is released.
        volatile Consumer<Object> launcher = launched -> {
        };
        // Read and written under the state monitor. Set until nextLaunch names the first launch.
        boolean launchPending = true;

        CatchupState(@Nullable HandedOver takesOver) {
            this.takesOver = takesOver;
        }
    }

    // Holds the events the live action is given while closed, and runs them once opened. A held event's
    // Mono runs none of the caller's code until then, so a pause of the wrapped model that cancels it cancels nothing
    // the caller wrote. Closed while any state that closed it holds it, so a handover can't open the gate a takeover
    // of its subscription closed.
    private static final class LiveGate {
        private final Set<CatchupState> holders = new HashSet<>();
        private volatile Sinks.@Nullable Empty<Void> closed;

        void close(CatchupState holder) {
            synchronized (holders) {
                holders.add(holder);
                if (closed == null) {
                    closed = Sinks.empty();
                }
            }
        }

        // Emits once the monitor is released, since an opened gate runs the caller's action for each event it held
        void open(CatchupState holder) {
            Sinks.Empty<Void> opened;
            synchronized (holders) {
                if (!holders.remove(holder) || !holders.isEmpty()) {
                    return;
                }
                opened = closed;
                closed = null;
            }
            if (opened != null) {
                opened.tryEmitEmpty();
            }
        }

        Mono<Void> apply(Supplier<Mono<Void>> action) {
            Sinks.Empty<Void> gate = closed;
            return gate == null ? Mono.defer(action) : gate.asMono().then(Mono.defer(action));
        }
    }

    /**
     * Holds a replay paused once the wrapped model lost the live start it would go live from, instead of ending it, so the
     * subscription neither runs nor disappears without a trace. When the replay started after the beginning, a replay
     * again from its origin with a live start read now would skip events, so the loss is recorded in the subscription's
     * record, and a resume refuses with {@link CatchupCannotResumeException} until the subscription is cancelled. A
     * replay that started at the beginning skips nothing when it runs again, so a resume or a {@code start(true)} runs it
     * again. A takeover replay leaves the subscription the wrapped model holds paused as it is, and only records the
     * loss. Returns false, so the failure takes the path of any other failure, when the replay is no longer the one in
     * flight for the id, a later launch of it runs since, it was cancelled or handed over, or this model shut down.
     */
    private boolean holdForLostLiveStart(Replay replay, CatchupState state, Object launched, @Nullable HandedOver takenOver, Throwable failure) {
        String subscriptionId = replay.subscriptionId();
        Checkpoint lost = HistoryLossMessages.lostFromWhereAResumeSkipsEvents(failure);
        if (takenOver != null) {
            if (lost != null) {
                // Only while this takeover is still the one in flight for the handover it took over, so a late failure of
                // a cancelled or replaced takeover doesn't refuse the resume of a subscription a newer run took over
                underIdLock(subscriptionId, () -> {
                    synchronized (state) {
                        if (!shutdown && !state.cancelled.get() && state.episode.get() == launched && catchingUp.get(subscriptionId) == state && handedOver.get(subscriptionId) == takenOver) {
                            replay.owned().historyLost = lost;
                        }
                    }
                    return null;
                });
            }
            return false;
        }
        // Read under the monitor, since a resume or a start(true) after the hold gives the state a sink of its own
        AtomicReference<Sinks.@Nullable Empty<Void>> failing = new AtomicReference<>();
        boolean held = underIdLock(subscriptionId, () -> {
            synchronized (state) {
                if (shutdown || state.cancelled.get() || state.handedOver.get() || state.episode.get() != launched || catchingUp.get(subscriptionId) != state) {
                    return false;
                }
                failing.set(state.started);
                state.pendingPause.set(false);
                state.parked.set(true);
                state.heldForLostLiveStart = true;
                state.resumeSkipsEvents = lost != null;
                if (lost != null) {
                    replay.owned().historyLost = lost;
                }
                return true;
            }
        });
        if (!held) {
            return false;
        }
        if (lost != null) {
            String at = lost.asString();
            log.error(HistoryLossMessages.TERMINAL_FAILURE_LOG, subscriptionId, at, at, failure);
        } else {
            log.error("The catch-up replay for subscription {} failed, so the subscription is held paused, and resuming it or start(true) on this model replays again from the beginning.", subscriptionId, failure);
        }
        failStart(requireNonNull(failing.get()), subscriptionId, failure);
        return true;
    }

    // A waiter whose error handler throws would otherwise abort the cancel or shutdown that fails it
    private static void failStart(CatchupState state, String subscriptionId, Throwable failure) {
        failStart(state.started, subscriptionId, failure);
    }

    private static void failStart(Sinks.Empty<Void> started, String subscriptionId, Throwable failure) {
        try {
            started.tryEmitError(failure);
        } catch (RuntimeException e) {
            log.error("A waiter for subscription {} to start threw when told it failed with {}.", subscriptionId, failure.toString(), e);
        }
    }

    // A subscription this model made by name
    private static final class Owned {
        // The position the wrapped model lost the history from, recorded only where a resume would skip events, which is
        // when no replay could take the loss over, or when a replay that started above position 0 lost its live start. A resume
        // then refuses with CatchupCannotResumeException until the subscription is cancelled
        volatile @Nullable Checkpoint historyLost;
        // Set while the wrapped subscribe(..) of a straight-to-live subscribe runs, changed under the per-id lock
        volatile boolean subscribing;
        // A cancel came while that subscribe ran, set under the per-id lock
        volatile boolean cancelled;
    }

    // The cancelled subscribes of an id whose wrapped subscribe(..) can have taken the id after their cancel, changed
    // under the per-id lock. underWay counts those still in the wrapped subscribe(..). standIn is the first of them,
    // the record of the id while one is under way or the wrapped model cancels what they left, and no other record owns
    // the id. A wrapped model that this model calls through CancellableRegistration registers nothing for a subscribe
    // cancelled first, so the second cancel finds nothing there to end.
    private static final class Leftovers {
        final Owned standIn;
        int underWay;

        Leftovers(Owned standIn) {
            this.standIn = standIn;
        }
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
