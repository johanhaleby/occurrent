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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
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
 * directly; this class composes the same replay with a <em>delegated</em> live half instead of a cold one: the live
 * subscription is handed to the wrapped model's own named {@code subscribe(..)}, so everything the wrapped model does
 * for a named subscription is inherited, the retry of a failing action and the synchronous refusal of an unsupported
 * filter included. That is the point of the promotion (issues #547 and #550).
 * <p>
 * A failing action during the replay is deliberately not retried: the blocking catch-up models do not retry there
 * either, and the replay failure reaches whoever waits on {@link Subscription#waitUntilStarted()} rather than being
 * logged and swallowed. Live delivery, where retry matters, is the wrapped model's.
 * <p>
 * The wrapped model must itself manage named subscriptions (implement {@link SubscriptionModel}). A catch-up model
 * over a cold-only wrapped model refuses the named path loudly, because the alternative is a second copy of the
 * named-over-cold driver that {@code ReactorDurableSubscriptionModel} already owns.
 * <p>
 * Life-cycle semantics mirror the blocking catch-up models: a pause or stop arriving while a subscription is still
 * replaying does not abort the replay; the subscription hands over to the wrapped model paused. Cancelling or shutting
 * down aborts in-flight replays. Model-wide calls forward to the wrapped model, so give each composition its own
 * wrapped model rather than sharing one.
 */
@NullMarked
final class NamedCatchupSupport {

    private final CheckpointAwareSubscriptionModel wrapped;
    private final @Nullable SubscriptionModel named;
    private final Class<?> modelClass;
    private final ConcurrentMap<String, CatchupState> catchingUp = new ConcurrentHashMap<>();

    NamedCatchupSupport(CheckpointAwareSubscriptionModel wrapped, Class<?> modelClass) {
        this.wrapped = requireNonNull(wrapped);
        this.modelClass = requireNonNull(modelClass);
        this.named = wrapped instanceof SubscriptionModel subscriptionModel ? subscriptionModel : null;
    }

    boolean managesNamedSubscriptions() {
        return named != null;
    }

    private SubscriptionModel requireNamed() {
        if (named == null) {
            throw new IllegalStateException(modelClass.getSimpleName() + " can only manage named subscriptions when the model it wraps manages them itself (implements " + SubscriptionModel.class.getSimpleName() + "). The wrapped " + wrapped.getClass().getName() + " only offers the plain (cold) subscribe(filter, startAt) primitive, so use that primitive directly, or wrap a model that manages named subscriptions.");
        }
        return named;
    }

    /**
     * Subscribe with a catch-up phase: replay from {@code startPosition} through {@code reader}, applying the caller's
     * {@code action} to each replayed event without retry, then hand the live half to the wrapped model's named
     * {@code subscribe(..)} resuming from a token captured before the replay, deduped against the replayed ids.
     */
    Subscription subscribeWithCatchup(String subscriptionId, @Nullable SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                      CatchupReader reader, long windowSize, int handoverCacheSize, long startPosition,
                                      Function<CloudEvent, Mono<Void>> action) {
        SubscriptionModel delegate = requireNamed();
        if (catchingUp.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }
        BoundedIdCache cache = new BoundedIdCache(handoverCacheSize);
        PositionCatchupPipeline pipeline = new PositionCatchupPipeline(reader, windowSize, handoverCacheSize);
        CatchupState state = new CatchupState();
        if (catchingUp.putIfAbsent(subscriptionId, state) != null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }

        Function<CloudEvent, Mono<Void>> liveAction = cloudEvent ->
                livePredicate.test(cloudEvent) && !cache.contains(cloudEvent.getId()) ? action.apply(cloudEvent) : Mono.empty();

        // Token before replay, replay through the caller's action (no retry, failure is loud), then delegate live.
        Disposable replaying = pipeline.captureLiveToken(wrapped)
                .flatMapMany(liveToken -> pipeline.replay(startPosition, cache)
                        .concatMap(action)
                        .thenMany(Flux.defer(() -> {
                            handOver(subscriptionId, state, delegate, liveSubscriptionFilter, StartAt.checkpoint(liveToken), liveAction);
                            return Flux.empty();
                        })))
                .subscribe(unused -> {
                }, throwable -> {
                    // A failed replay is a dead subscription, reported to whoever waits, never logged-and-swallowed.
                    catchingUp.remove(subscriptionId);
                    state.started.tryEmitError(throwable);
                });
        state.replaying.set(replaying);
        // A synchronous replay failure (or instant hand-over) may already have removed the state.
        if (!catchingUp.containsKey(subscriptionId) && !state.handedOver.get()) {
            replaying.dispose();
        }
        return new NamedCatchupSubscription(subscriptionId, state.started.asMono());
    }

    /**
     * Subscribe with no catch-up phase: pure delegation to the wrapped model's named {@code subscribe(..)}, filtered
     * in-process by {@code livePredicate} so a backend that does not honor the filter server-side still only delivers
     * matching events.
     */
    Subscription subscribeStraightToLive(String subscriptionId, @Nullable SubscriptionFilter liveSubscriptionFilter, Predicate<CloudEvent> livePredicate,
                                         StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        SubscriptionModel delegate = requireNamed();
        return delegate.subscribe(subscriptionId, liveSubscriptionFilter, startAt,
                cloudEvent -> livePredicate.test(cloudEvent) ? action.apply(cloudEvent) : Mono.empty());
    }

    // Registers the delegated live subscription once the replay has drained. Runs inside the replay pipeline, so a
    // cancellation that disposed the replay never reaches here. A pause or stop that arrived during the replay is
    // applied to the delegated subscription immediately after it is created, mirroring the blocking catch-up models.
    private void handOver(String subscriptionId, CatchupState state, SubscriptionModel delegate,
                          @Nullable SubscriptionFilter liveSubscriptionFilter, StartAt liveStartAt, Function<CloudEvent, Mono<Void>> liveAction) {
        synchronized (state) {
            if (state.cancelled.get()) {
                return;
            }
            Subscription delegated = delegate.subscribe(subscriptionId, liveSubscriptionFilter, liveStartAt, liveAction);
            state.handedOver.set(true);
            if (state.pendingPause.get()) {
                delegate.pauseSubscription(subscriptionId);
            }
            catchingUp.remove(subscriptionId);
            delegated.waitUntilStarted().subscribe(unused -> {
            }, state.started::tryEmitError, state.started::tryEmitEmpty);
        }
    }

    // --- Life cycle. Every call forwards to the wrapped model; the extra bookkeeping is only for subscriptions whose
    // --- replay has not handed over yet, which the wrapped model does not know about.

    void stop() {
        catchingUp.values().forEach(state -> state.pendingPause.set(true));
        requireNamed().stop();
    }

    void start(boolean resumeSubscriptionsAutomatically) {
        if (resumeSubscriptionsAutomatically) {
            catchingUp.values().forEach(state -> state.pendingPause.set(false));
        }
        requireNamed().start(resumeSubscriptionsAutomatically);
    }

    boolean isRunning() {
        return requireNamed().isRunning();
    }

    boolean isRunning(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return !state.pendingPause.get() && !state.cancelled.get();
        }
        return requireNamed().isRunning(subscriptionId);
    }

    boolean isPaused(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            return state.pendingPause.get() && !state.cancelled.get();
        }
        return requireNamed().isPaused(subscriptionId);
    }

    void pauseSubscription(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                if (!state.handedOver.get()) {
                    if (state.pendingPause.getAndSet(true)) {
                        throw new IllegalArgumentException("Subscription " + subscriptionId + " is already paused");
                    }
                    return;
                }
            }
        }
        requireNamed().pauseSubscription(subscriptionId);
    }

    Subscription resumeSubscription(String subscriptionId) {
        CatchupState state = catchingUp.get(subscriptionId);
        if (state != null) {
            synchronized (state) {
                if (!state.handedOver.get()) {
                    if (!state.pendingPause.getAndSet(false)) {
                        throw new IllegalArgumentException("Subscription " + subscriptionId + " is already running");
                    }
                    return new NamedCatchupSubscription(subscriptionId, state.started.asMono());
                }
            }
        }
        return requireNamed().resumeSubscription(subscriptionId);
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
                    // The id never reached the wrapped model, so there is nothing to cancel there.
                    state.started.tryEmitEmpty();
                    return;
                }
            }
        }
        requireNamed().cancelSubscription(subscriptionId);
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
        requireNamed().shutdown();
    }

    private static final class CatchupState {
        final AtomicReference<@Nullable Disposable> replaying = new AtomicReference<>();
        final AtomicBoolean pendingPause = new AtomicBoolean(false);
        final AtomicBoolean cancelled = new AtomicBoolean(false);
        final AtomicBoolean handedOver = new AtomicBoolean(false);
        final Sinks.Empty<Void> started = Sinks.empty();
    }

    private record NamedCatchupSubscription(String id, Mono<Void> started) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
