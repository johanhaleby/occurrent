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

package org.occurrent.subscription.api.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The reactive counterpart of the blocking {@code RegisteringSubscribable}: shared base for register-only reactive
 * {@link Subscribable}s that compose their handlers into the caller's reactive chain rather than driving them off a
 * change stream. Handlers register with a {@link SubscriptionFilter}, and events fed in by the subclass are routed to
 * every handler whose filter matches, sequentially (the next handler does not start until the previous handler's
 * {@link Mono} completes).
 * <p>
 * It owns id uniqueness, the filter-to-{@link Predicate} translation (via {@link SubscriptionFilterMatcher}), and
 * ordered dispatch. It has no start position, checkpoint, catch-up, or replay. {@link StartAt} is accepted for
 * interface compatibility but ignored.
 * <p>
 * It does implement {@link SubscriptionModelLifeCycle}, so a stopped model or a paused subscription is skipped by
 * {@link #route(CloudEvent)}. Read that as <i>dropped, not deferred</i>: nothing is holding the events back, so an
 * event fed in while a subscription is paused never reaches that handler, and resuming does not replay it.
 */
@NullMarked
public abstract class RegisteringSubscribable implements Subscribable, SubscriptionModelLifeCycle {

    private record Registration(String id, Predicate<CloudEvent> matcher, Function<CloudEvent, Mono<Void>> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedSubscriptions = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    @Override
    public final Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        // Build the matcher before reserving the id, so an unsupported filter does not leave the id permanently taken.
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter);
        if (!subscriptionIds.add(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already registered");
        }
        registrations.add(new Registration(subscriptionId, matcher, action));
        // Registering on a stopped model yields a paused subscription, so a caller that stopped the model before
        // wiring its handlers can resume them one at a time.
        if (!running) {
            pausedSubscriptions.add(subscriptionId);
        }
        return new AlreadyStartedSubscription(subscriptionId);
    }

    @Override
    public final void cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        // Drop the registration before releasing the id, so the id is never free while its handler can still be routed to.
        registrations.removeIf(registration -> registration.id().equals(subscriptionId));
        subscriptionIds.remove(subscriptionId);
        pausedSubscriptions.remove(subscriptionId);
    }

    @Override
    public final void stop() {
        running = false;
        pausedSubscriptions.addAll(subscriptionIds);
    }

    @Override
    public final void start(boolean resumeSubscriptionsAutomatically) {
        running = true;
        if (resumeSubscriptionsAutomatically) {
            pausedSubscriptions.clear();
        }
    }

    @Override
    public final boolean isRunning() {
        return running;
    }

    @Override
    public final boolean isRunning(String subscriptionId) {
        return running && subscriptionIds.contains(subscriptionId) && !pausedSubscriptions.contains(subscriptionId);
    }

    @Override
    public final boolean isPaused(String subscriptionId) {
        return pausedSubscriptions.contains(subscriptionId);
    }

    @Override
    public final Subscription resumeSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (!isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        }
        running = true;
        pausedSubscriptions.remove(subscriptionId);
        return new AlreadyStartedSubscription(subscriptionId);
    }

    @Override
    public final void pauseSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't running.");
        }
        pausedSubscriptions.add(subscriptionId);
    }

    /**
     * @return {@code true} if at least one handler is registered.
     */
    public final boolean hasSubscriptions() {
        return !registrations.isEmpty();
    }

    /**
     * Route a single event to every registered handler whose filter matches, in registration order and sequentially.
     * A handler error propagates through the returned {@link Mono}.
     *
     * @param cloudEvent The event to dispatch.
     * @return A {@link Mono} that completes when every matching handler has completed.
     */
    protected final Mono<Void> route(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        // Deferred so the running check happens on subscribe, not when the Mono is assembled.
        return Mono.defer(() -> {
            if (!running) {
                return Mono.empty();
            }
            return Flux.fromIterable(registrations)
                    .filter(registration -> !pausedSubscriptions.contains(registration.id()) && registration.matcher().test(cloudEvent))
                    .concatMap(registration -> registration.action().apply(cloudEvent))
                    .then();
        });
    }

    /**
     * Route each event in turn via {@link #route(CloudEvent)}, in iteration order and sequentially.
     *
     * @param cloudEvents The events to dispatch.
     * @return A {@link Mono} that completes when every event has been dispatched.
     */
    protected final Mono<Void> route(Iterable<CloudEvent> cloudEvents) {
        Objects.requireNonNull(cloudEvents, "cloudEvents cannot be null");
        return Flux.fromIterable(cloudEvents)
                .concatMap(this::route)
                .then();
    }

    private record AlreadyStartedSubscription(String id) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            // There is no background subscription to wait for: registration completes synchronously in subscribe.
            return Mono.empty();
        }
    }
}
