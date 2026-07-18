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
 * ordered dispatch. It has no lifecycle, start position, checkpoint, catch-up, or replay. {@link StartAt} is accepted
 * for interface compatibility but ignored.
 */
@NullMarked
public abstract class RegisteringSubscribable implements Subscribable {

    private record Registration(String id, Predicate<CloudEvent> matcher, Function<CloudEvent, Mono<Void>> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();

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
        return new AlreadyStartedSubscription(subscriptionId);
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
        return Flux.fromIterable(registrations)
                .filter(registration -> registration.matcher().test(cloudEvent))
                .concatMap(registration -> registration.action().apply(cloudEvent))
                .then();
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
