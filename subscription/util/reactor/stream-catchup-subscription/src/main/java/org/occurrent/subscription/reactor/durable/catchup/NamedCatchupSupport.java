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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.internal.BoundedIdCache;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

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
 * over to the wrapped model paused (blocking parity). A stop aborts the replay WITHOUT handing over, and parks the
 * subscription. {@code start(..)} relaunches the replay from its original start position, so replayed events may be
 * delivered again (the composition is at-least-once anyway). A subscription created while the model is stopped parks
 * the same way and replays only once the model starts. This is deliberately safer than the blocking catch-up model,
 * which abandons a stop-interrupted replay outright. The blocking composition never notices, because its durable
 * model parks subscriptions before the catch-up model sees them, a gate the delegating path here does not run
 * through. Cancelling or shutting down aborts in-flight replays. Waiting on a subscription that was cancelled before
 * its handover fails, since that subscription never started and nothing will start it, and the blocking
 * {@code CancelledSubscription} answers {@code false} for the same case. Model-wide calls forward to the wrapped
 * model, so give each composition its own wrapped model rather than sharing one.
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
    // Numbers every catch-up this JVM starts, so two of them for the same id are never confused by a caller that only
    // samples the model. Static because the number only has to differ, not mean anything.
    private static final java.util.concurrent.atomic.AtomicLong CATCHUP_GENERATIONS = new java.util.concurrent.atomic.AtomicLong();

    NamedCatchupSupport(CheckpointAwareSubscriptionModel wrapped, Class<?> modelClass) {
        this.wrapped = requireNonNull(wrapped);
        this.modelClass = requireNonNull(modelClass);
        this.named = wrapped instanceof SubscriptionModel subscriptionModel ? subscriptionModel : null;
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

    /**
     * Whether the replay for {@code subscriptionId} is still reading the history that was there when it started,
     * rather than the events written since. False for an id with no replay in flight, so a handed-over subscription
     * and one this model never saw read the same, matching {@link #isCatchingUp(String)}.
     */
    long catchupGeneration(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        return state == null ? 0L : state.generation;
    }

    boolean isReplayingHistory(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        return state != null && state.replayingHistory.get();
    }

    private SubscriptionModel requireNamed() {
        if (named == null) {
            throw new IllegalStateException(modelClass.getSimpleName() + " can only manage named subscriptions when the model it wraps manages them itself (implements " + SubscriptionModel.class.getSimpleName() + "). The wrapped " + wrapped.getClass().getName() + " only offers the plain (cold) subscribe(filter, startAt) primitive, so use that primitive directly, or wrap a model that manages named subscriptions.");
        }
        return named;
    }

    /**
     * Subscribes with a catch-up phase. It replays from {@code startPosition} through {@code reader}, applies the
     * caller's {@code action} to each replayed event without retry, then hands the live half to the wrapped model's
     * named {@code subscribe(..)} resuming from a token captured before the replay, deduped against the replayed ids.
     */
    Subscription subscribeWithCatchup(String subscriptionId, @Nullable SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                      CatchupReader reader, long windowSize, int handoverCacheSize, long startPosition,
                                      Function<CloudEvent, Mono<Void>> action) {
        SubscriptionModel delegate = requireNamed();
        // The wrapped model already knowing the id means an earlier catch-up handed it over (or someone subscribed it
        // directly). Refuse synchronously, like every other subscribe path, instead of replaying history a second
        // time and failing asynchronously at the handover.
        if (delegate.isRunning(subscriptionId) || delegate.isPaused(subscriptionId)) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        BoundedIdCache cache = new BoundedIdCache(handoverCacheSize);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, windowSize, handoverCacheSize);
        CatchupState state = new CatchupState();

        Function<CloudEvent, Mono<Void>> liveAction = cloudEvent ->
                livePredicate.test(cloudEvent) && !cache.contains(cloudEvent.getId()) ? action.apply(cloudEvent) : Mono.empty();

        // The replay is relaunchable: stop() aborts and parks it, start(..) runs this again from the same start
        // position (re-adding ids to the cache is a no-op; re-delivering replayed events is at-least-once).
        state.launcher = () -> {
            // A relaunch reads the history again from the same start position, so this attempt starts in the history
            // part of its catch-up however far the previous run got.
            state.replayingHistory.set(true);
            // Token before replay, replay through the caller's action (no retry, failure is loud), then delegate live.
            Disposable replaying = pipeline.captureLiveToken(wrapped)
                    .flatMapMany(liveToken -> pipeline.replayApplying(startPosition, cache,
                                    // A stop between dispose landing and this event truncates here, before the action runs.
                                    () -> !stopped && !state.cancelled.get(), action,
                                    () -> state.replayingHistory.set(false))
                            .thenMany(Flux.defer(() -> {
                                handOver(subscriptionId, state, delegate, liveSubscriptionFilter, StartAt.checkpoint(liveToken), liveAction);
                                return Flux.empty();
                            })))
                    .subscribe(unused -> {
                    }, throwable -> {
                        // A failed replay is a dead subscription, reported to whoever waits AND logged: a caller
                        // that never blocks on waitUntilStarted() would otherwise get a dead subscription with no trace.
                        log.error("The catch-up replay for subscription {} failed; the subscription is dead until it is subscribed again.", subscriptionId, throwable);
                        // Identity-checked: unlike handOver and cancelSubscription, this callback runs outside the
                        // state monitor, so a concurrent cancel-then-resubscribe for the same id can already have
                        // installed a new state by the time a stale failure reaches here. A plain remove(id) would
                        // delete that new attempt's entry instead of this dead one's.
                        catchingUp.remove(subscriptionId, state);
                        state.started.tryEmitError(throwable);
                    });
            state.replaying.set(replaying);
            // A synchronous replay failure (or instant hand-over) may already have removed the state.
            if (!catchingUp.containsKey(subscriptionId) && !state.handedOver.get()) {
                replaying.dispose();
            }
        };

        if (catchingUp.putIfAbsent(subscriptionId, state) != null) {
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        synchronized (state) {
            if (!stopped) {
                state.launcher.run();
            }
            // else parked: start(..) launches the replay once the model runs again.
        }
        return new NamedCatchupSubscription(subscriptionId, state.started.asMono());
    }

    /**
     * Subscribes with no catch-up phase. It is pure delegation to the wrapped model's named {@code subscribe(..)},
     * filtered in-process by {@code livePredicate} so a backend that does not honor the filter server-side still
     * only delivers matching events.
     */
    Subscription subscribeStraightToLive(String subscriptionId, @Nullable SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                         StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        SubscriptionModel delegate = requireNamed();
        return delegate.subscribe(subscriptionId, liveSubscriptionFilter, startAt,
                cloudEvent -> livePredicate.test(cloudEvent) ? action.apply(cloudEvent) : Mono.empty());
    }

    // Registers the delegated live subscription once the replay has drained. Runs inside the replay pipeline, so a
    // cancellation that disposed the replay never reaches here. A pause that arrived during the replay is applied to
    // the delegated subscription immediately after it is created, mirroring the blocking catch-up models. A stop that
    // truncated the replay parks instead: handing over to a stopped wrapped model would immediately conflict with its
    // own stop bookkeeping (the pre-fix behavior errored waitUntilStarted with "already paused").
    private void handOver(String subscriptionId, CatchupState state, SubscriptionModel delegate,
                          @Nullable SubscriptionFilter liveSubscriptionFilter, StartAt liveStartAt, Function<CloudEvent, Mono<Void>> liveAction) {
        synchronized (state) {
            if (state.cancelled.get()) {
                return;
            }
            if (stopped) {
                // Parked. The takeWhile truncated the replay, so this completion is not a finished replay; start(..)
                // relaunches from the original start position.
                state.replaying.set(null);
                return;
            }
            Subscription delegated = delegate.subscribe(subscriptionId, liveSubscriptionFilter, liveStartAt, liveAction);
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
                if (!state.handedOver.get()) {
                    Disposable replaying = state.replaying.getAndSet(null);
                    if (replaying != null) {
                        replaying.dispose();
                    }
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
            catchingUp.values().forEach(state -> state.pendingPause.set(false));
        }
        stopped = false;
        named.start(resumeSubscriptionsAutomatically);
        // Relaunch the parked replays: subscriptions created while stopped, and replays a stop() aborted.
        catchingUp.values().forEach(state -> {
            synchronized (state) {
                if (!state.handedOver.get() && !state.cancelled.get() && state.replaying.get() == null) {
                    state.launcher.run();
                }
            }
        });
    }

    boolean isRunning() {
        return managesNamedSubscriptions() && named.isRunning();
    }

    boolean isRunning(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return !stopped && !state.pendingPause.get() && !state.cancelled.get();
        }
        return managesNamedSubscriptions() && named.isRunning(subscriptionId);
    }

    boolean isPaused(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return state.pendingPause.get() && !state.cancelled.get();
        }
        return managesNamedSubscriptions() && named.isPaused(subscriptionId);
    }

    void pauseSubscription(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                if (!state.handedOver.get()) {
                    if (state.pendingPause.getAndSet(true)) {
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
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                if (!state.handedOver.get()) {
                    if (!state.pendingPause.getAndSet(false)) {
                        throw new SubscriptionAlreadyRunningException(subscriptionId);
                    }
                    return new NamedCatchupSubscription(subscriptionId, state.started.asMono());
                }
            }
        }
        if (!managesNamedSubscriptions()) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        return named.resumeSubscription(subscriptionId);
    }

    void cancelSubscription(String subscriptionId) {
        CatchupState state = catchingUp.remove(subscriptionId);
        if (state != null) {
            synchronized (state) {
                state.cancelled.set(true);
                Disposable replaying = state.replaying.get();
                if (replaying != null) {
                    replaying.dispose();
                }
                if (!state.handedOver.get()) {
                    // The id never reached the wrapped model, so there is nothing to cancel there. Waiters are failed
                    // rather than completed, since completing would claim the subscription started and doing nothing
                    // would hang them. The blocking twin answers false, which a Mono<Void> has no room for.
                    state.started.tryEmitError(new IllegalStateException("Subscription " + subscriptionId + " was cancelled before it started."));
                    return;
                }
            }
        }
        // Cancelling an id a cold-only composition never knew is an idempotent no-op, like cancelling any unknown id.
        if (!managesNamedSubscriptions()) {
            return;
        }
        named.cancelSubscription(subscriptionId);
    }

    void shutdown() {
        new ArrayList<>(catchingUp.keySet()).forEach(subscriptionId -> {
            CatchupState state = catchingUp.remove(subscriptionId);
            if (state != null) {
                state.cancelled.set(true);
                Disposable replaying = state.replaying.get();
                if (replaying != null) {
                    replaying.dispose();
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
        final AtomicReference<@Nullable Disposable> replaying = new AtomicReference<>();
        final AtomicBoolean pendingPause = new AtomicBoolean(false);
        final AtomicBoolean cancelled = new AtomicBoolean(false);
        final AtomicBoolean handedOver = new AtomicBoolean(false);
        final Sinks.Empty<Void> started = Sinks.empty();
        // False once the history read has handed everything it read to the action, so what follows is the events
        // written since the replay started. Back to true whenever the replay is relaunched, since that reads the
        // history again from the same start position.
        final AtomicBoolean replayingHistory = new AtomicBoolean(true);
        // Numbers this catch-up so a caller that only samples the model can tell it from the next one for the same id.
        final long generation = CATCHUP_GENERATIONS.incrementAndGet();
        // The replay, relaunchable: assigned once in subscribeWithCatchup before the state is published, run under
        // the state monitor by the initial subscribe and by start(..) for parked subscriptions.
        volatile Runnable launcher = () -> {
        };
    }

    private record NamedCatchupSubscription(String id, Mono<Void> started) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
