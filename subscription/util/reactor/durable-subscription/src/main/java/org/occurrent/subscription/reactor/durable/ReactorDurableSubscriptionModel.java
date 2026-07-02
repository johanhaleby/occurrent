/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.PositionAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModelLifeCycle;
import org.occurrent.subscription.api.reactor.SubscriptionPositionStorage;
import org.occurrent.subscription.util.predicate.EveryN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.subscription.PositionAwareCloudEvent.getSubscriptionPositionOrThrowIAE;

/**
 * Wraps a {@link PositionAwareSubscriptionModel} and adds persistent subscription position support, making a
 * subscription durable: it resumes from the last stored position across restarts and stores the position after each
 * successful {@code action}.
 * <p>
 * It is a transparent decorator that itself implements {@link Subscribable}, {@link PositionAwareSubscriptionModel},
 * and {@link SubscriptionModelLifeCycle}, so a {@code Durable(delegate)} chain composes uniformly and can be handed to
 * the reactive subscription DSLs and to lifecycle management, mirroring the blocking {@code DurableSubscriptionModel}.
 * The named {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} method reads events from the wrapped
 * model's plain (cold) {@link PositionAwareSubscriptionModel#subscribe(SubscriptionFilter, StartAt)} primitive,
 * resolving the start position from storage when the caller asks for the subscription-model default, and persisting the
 * position after each event per {@link ReactorDurableSubscriptionModelConfig}.
 * <p>
 * Note that this implementation stores the subscription position after _every_ action by default. If you have a lot of
 * events and duplication is not that much of a deal, consider changing this behavior by supplying an instance of
 * {@link ReactorDurableSubscriptionModelConfig}.
 */
@NullMarked
public class ReactorDurableSubscriptionModel implements PositionAwareSubscriptionModel, Subscribable, SubscriptionModelLifeCycle {
    private static final Logger log = LoggerFactory.getLogger(ReactorDurableSubscriptionModel.class);

    private final PositionAwareSubscriptionModel subscription;
    private final SubscriptionPositionStorage storage;
    private final ReactorDurableSubscriptionModelConfig config;
    private final ConcurrentMap<String, InternalSubscription> runningSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, InternalSubscription> pausedSubscriptions = new ConcurrentHashMap<>();

    private volatile boolean shutdown = false;
    private volatile boolean running = true;

    /**
     * Create a durable subscription model that stores the subscription position after each successful call to the action.
     *
     * @param subscription The subscription model that will read events from the event store
     * @param storage      The {@link SubscriptionPositionStorage} that'll be used to persist the stream position
     */
    public ReactorDurableSubscriptionModel(PositionAwareSubscriptionModel subscription, SubscriptionPositionStorage storage) {
        this(subscription, storage, new ReactorDurableSubscriptionModelConfig(EveryN.everyEvent()));
    }

    /**
     * Create a durable subscription model that stores the subscription position when the predicate defined in
     * {@link ReactorDurableSubscriptionModelConfig#persistCloudEventPositionPredicate} is fulfilled.
     *
     * @param subscription The subscription model that will read events from the event store
     * @param storage      The {@link SubscriptionPositionStorage} that'll be used to persist the stream position
     * @param config       Configures when the subscription position is persisted
     */
    public ReactorDurableSubscriptionModel(PositionAwareSubscriptionModel subscription, SubscriptionPositionStorage storage,
                                           ReactorDurableSubscriptionModelConfig config) {
        this.subscription = requireNonNull(subscription, PositionAwareSubscriptionModel.class.getSimpleName() + " cannot be null");
        this.storage = requireNonNull(storage, SubscriptionPositionStorage.class.getSimpleName() + " cannot be null");
        this.config = requireNonNull(config, ReactorDurableSubscriptionModelConfig.class.getSimpleName() + " cannot be null");
    }

    /**
     * The plain (cold) subscription-model primitive. It is a straight pass-through to the wrapped model and does
     * <em>not</em> persist the subscription position, since position storage is keyed by subscription id and this
     * primitive has none. Use the named {@link #subscribe(String, SubscriptionFilter, StartAt, Function)} method for a
     * durable subscription.
     */
    @Override
    public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
        return subscription.subscribe(filter, startAt);
    }

    @Override
    public Mono<SubscriptionPosition> globalSubscriptionPosition() {
        return subscription.globalSubscriptionPosition();
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(action, "Action cannot be null");
        requireNonNull(startAt, StartAt.class.getSimpleName() + " cannot be null");

        if (runningSubscriptions.containsKey(subscriptionId) || pausedSubscriptions.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }
        if (shutdown) {
            throw new IllegalStateException("Cannot start subscription because the subscription model is shutdown.");
        }
        return startInternalSubscription(subscriptionId, filter, new AtomicReference<>(startAt), action);
    }

    private Subscription startInternalSubscription(String subscriptionId, @Nullable SubscriptionFilter filter, AtomicReference<StartAt> currentStartAt, Function<CloudEvent, Mono<Void>> action) {
        if (!running) {
            // The model is stopped: don't subscribe at all, so waitUntilStarted() doesn't complete for a subscription
            // that won't deliver anything until start(true)/resumeSubscription actually starts it.
            InternalSubscription internalSubscription = new InternalSubscription(Disposables.disposed(), currentStartAt, filter, action, Mono.never());
            pausedSubscriptions.put(subscriptionId, internalSubscription);
            return new ReactorDurableSubscription(subscriptionId, internalSubscription.started);
        }
        Sinks.Empty<Void> startedSink = Sinks.empty();
        runningSubscriptions.put(subscriptionId, new InternalSubscription(Disposables.disposed(), currentStartAt, filter, action, startedSink.asMono()));
        Disposable disposable = resolveStartAt(subscriptionId, currentStartAt.get())
                .flatMapMany(resolvedStartAt -> {
                    currentStartAt.set(resolvedStartAt);
                    return source(subscriptionId, filter, resolvedStartAt, action, currentStartAt, true, startedSink);
                })
                // An empty resolveStartAt means a dynamic StartAt opted out of starting (its function returned null),
                // so read from the original StartAt without durable position handling, mirroring the blocking model's
                // "delegate to the wrapped model" branch.
                .switchIfEmpty(Flux.defer(() -> source(subscriptionId, filter, currentStartAt.get(), action, currentStartAt, false, startedSink)))
                .subscribe(unused -> {
                        }, throwable -> {
                            log.error("Subscription {} terminated with an unrecoverable error", subscriptionId, throwable);
                            startedSink.tryEmitError(throwable);
                            runningSubscriptions.remove(subscriptionId);
                        });
        InternalSubscription internalSubscription = new InternalSubscription(disposable, currentStartAt, filter, action, startedSink.asMono());
        if (runningSubscriptions.replace(subscriptionId, internalSubscription) == null) {
            // The placeholder was already removed by a synchronous error, so this subscription is already dead.
            disposable.dispose();
        }
        return new ReactorDurableSubscription(subscriptionId, internalSubscription.started);
    }

    // Reads events from the wrapped model's cold primitive, applies the action, then persists the position after each
    // event (per the config predicate) when persist is true. currentStartAt is advanced only after the action
    // completes so that pause/resume continues from the last delivered event rather than replaying or skipping.
    private Flux<Void> source(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action, AtomicReference<StartAt> currentStartAt, boolean persist, Sinks.Empty<Void> startedSink) {
        return subscription.subscribe(filter, startAt)
                .doOnSubscribe(__ -> startedSink.tryEmitEmpty())
                .concatMap(cloudEvent -> action.apply(cloudEvent)
                        .then(Mono.defer(() -> persist && config.persistCloudEventPositionPredicate.test(cloudEvent)
                                ? storage.save(subscriptionId, getSubscriptionPositionOrThrowIAE(cloudEvent)).then()
                                : Mono.empty()))
                        .doOnSuccess(unused -> currentStartAt.set(StartAt.subscriptionPosition(getSubscriptionPositionOrThrowIAE(cloudEvent)))));
    }

    // Resolve the effective StartAt, mirroring the blocking DurableSubscriptionModel#generateStartAtPositionFrom:
    // the subscription-model default reads the last stored position (initializing it from the global position when
    // absent); a dynamic StartAt is resolved against this model's context and recursed, an empty result meaning "opt
    // out"; any concrete StartAt passes through unchanged.
    private Mono<StartAt> resolveStartAt(String subscriptionId, StartAt startAt) {
        if (startAt.isDefault()) {
            return storage.read(subscriptionId)
                    .switchIfEmpty(Mono.defer(() -> subscription.globalSubscriptionPosition()
                            .flatMap(globalSubscriptionPosition -> storage.save(subscriptionId, globalSubscriptionPosition))))
                    .map(StartAt::subscriptionPosition)
                    .switchIfEmpty(Mono.fromSupplier(StartAt::now));
        } else if (startAt.isDynamic()) {
            StartAt nextStartAt = startAt.get(new SubscriptionModelContext(ReactorDurableSubscriptionModel.class));
            if (nextStartAt == null) {
                return Mono.empty();
            }
            return resolveStartAt(subscriptionId, nextStartAt);
        }
        return Mono.just(startAt);
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(ReactorDurableSubscriptionModel.class.getSimpleName() + " is shutdown");
        } else if (isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already paused");
        } else if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        }

        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
            pausedSubscriptions.put(subscriptionId, internalSubscription);
        }
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        if (shutdown) {
            throw new IllegalStateException(ReactorDurableSubscriptionModel.class.getSimpleName() + " is shutdown");
        } else if (isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already running");
        }

        InternalSubscription internalSubscription = pausedSubscriptions.remove(subscriptionId);
        if (internalSubscription == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }

        running = true;
        // Reuse the same currentStartAt reference so resume continues from the position of the last event delivered
        // before the subscription was paused, rather than replaying (or skipping) from the original StartAt.
        return startInternalSubscription(subscriptionId, internalSubscription.filter, internalSubscription.currentStartAt, internalSubscription.action);
    }

    /**
     * Cancel a subscription. It'll no longer receive events, and its persisted subscription position is removed.
     *
     * @param subscriptionId The subscription id to cancel
     */
    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        InternalSubscription internalSubscription = runningSubscriptions.remove(subscriptionId);
        if (internalSubscription != null) {
            internalSubscription.disposable.dispose();
        }
        pausedSubscriptions.remove(subscriptionId);
        // Best-effort asynchronous cleanup of the stored position. cancelSubscription is void (fire-and-forget), so the
        // delete runs on its own without blocking the caller.
        storage.delete(subscriptionId).subscribe(unused -> {
        }, throwable -> log.warn("Failed to delete stored subscription position for cancelled subscription {}", subscriptionId, throwable));
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        running = false;
        runningSubscriptions.values().forEach(internalSubscription -> internalSubscription.disposable.dispose());
        runningSubscriptions.clear();
        pausedSubscriptions.values().forEach(internalSubscription -> internalSubscription.disposable.dispose());
        pausedSubscriptions.clear();
    }

    @Override
    public synchronized void stop() {
        if (!shutdown) {
            running = false;
            runningSubscriptions.forEach((subscriptionId, __) -> pauseSubscription(subscriptionId));
        }
    }

    @Override
    public synchronized void start(boolean resumeSubscriptionsAutomatically) {
        if (!shutdown) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                pausedSubscriptions.forEach((subscriptionId, __) -> resumeSubscription(subscriptionId));
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !shutdown && runningSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return !shutdown && pausedSubscriptions.containsKey(subscriptionId);
    }

    private static final class InternalSubscription {
        final Disposable disposable;
        final AtomicReference<StartAt> currentStartAt;
        final @Nullable SubscriptionFilter filter;
        final Function<CloudEvent, Mono<Void>> action;
        final Mono<Void> started;

        private InternalSubscription(Disposable disposable, AtomicReference<StartAt> currentStartAt, @Nullable SubscriptionFilter filter, Function<CloudEvent, Mono<Void>> action, Mono<Void> started) {
            this.disposable = disposable;
            this.currentStartAt = currentStartAt;
            this.filter = filter;
            this.action = action;
            this.started = started;
        }
    }

    private static final class ReactorDurableSubscription implements Subscription {
        private final String subscriptionId;
        private final Mono<Void> started;

        private ReactorDurableSubscription(String subscriptionId, Mono<Void> started) {
            this.subscriptionId = subscriptionId;
            this.started = started;
        }

        @Override
        public String id() {
            return subscriptionId;
        }

        @Override
        public Mono<Void> waitUntilStarted() {
            return started;
        }
    }
}
