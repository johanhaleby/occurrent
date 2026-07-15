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

package org.occurrent.subscription.synchronous.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.reactor.ReactiveSynchronousEventDispatcher;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The reactive counterpart of the blocking {@code SynchronousSubscriptionModel}: a register-only reactive
 * {@link Subscribable} whose handlers are composed <strong>synchronously</strong> into the writer's reactive chain,
 * before {@code execute} completes, rather than being driven asynchronously off a change stream.
 * <p>
 * Driven by the reactive application service: after a successful write it hands the just-written cloud events to
 * {@link #dispatch(List)}, which routes each event to the registered handlers whose {@link SubscriptionFilter}
 * matches, invoking them in registration order and sequentially (the next handler does not start until the previous
 * one's {@link Mono} completes). A handler error propagates, so under a reactive transaction it rolls the write back.
 * <p>
 * It has no lifecycle, start position, checkpoint, catch-up, or replay. {@link StartAt} is accepted for interface
 * compatibility but ignored.
 */
@NullMarked
public class SynchronousSubscriptionModel implements Subscribable, ReactiveSynchronousEventDispatcher {

    private record Registration(String id, Predicate<CloudEvent> matcher, Function<CloudEvent, Mono<Void>> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        if (!subscriptionIds.add(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already registered");
        }
        registrations.add(new Registration(subscriptionId, SubscriptionFilterMatcher.matcherFor(filter), action));
        return new SynchronousSubscription(subscriptionId);
    }

    @Override
    public Mono<Void> dispatch(List<CloudEvent> writtenCloudEvents) {
        Objects.requireNonNull(writtenCloudEvents, "writtenCloudEvents cannot be null");
        return Flux.fromIterable(writtenCloudEvents)
                .concatMap(cloudEvent -> Flux.fromIterable(registrations)
                        .filter(registration -> registration.matcher().test(cloudEvent))
                        .concatMap(registration -> registration.action().apply(cloudEvent)))
                .then();
    }

    @Override
    public boolean hasSubscriptions() {
        return !registrations.isEmpty();
    }

    private record SynchronousSubscription(String id) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            // Synchronous subscriptions are always "started": there is no background subscription to wait for.
            return Mono.empty();
        }
    }
}
