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

package org.occurrent.subscription.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Shared base for register-only {@link Subscribable}s that dispatch in-process: handlers register with a
 * {@link SubscriptionFilter}, and events fed in by the subclass are routed to every handler whose filter matches.
 * <p>
 * It owns id uniqueness, the filter-to-{@link Predicate} translation (via {@link SubscriptionFilterMatcher}), and
 * ordered dispatch. It has no start position, checkpoint, catch-up, or replay, and no
 * {@link SubscriptionModelLifeCycle}: there is nothing to start or stop when the events arrive from the caller, and a
 * pause would drop them rather than defer them, since there is no feed holding them back. Cancellation is the one
 * life-cycle operation that does apply, so it implements {@link CancellableSubscriptions}. {@link StartAt} is accepted
 * for interface compatibility but ignored, since "where to start" is meaningless when the subclass decides which
 * events reach {@link #route(CloudEvent)} and when.
 * <p>
 * Subclasses expose their own ingestion API (for example a synchronous at-write-time {@code dispatch(List)} or an
 * externally driven {@code accept(CloudEvent)}) and delegate to {@link #route(CloudEvent)} to deliver each event.
 */
@NullMarked
public abstract class RegisteringSubscribable implements Subscribable, CancellableSubscriptions {

    private record Registration(String id, Predicate<CloudEvent> matcher, Consumer<CloudEvent> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();

    @Override
    public final Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
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

    @Override
    public final void cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        // Drop the registration before releasing the id, so the id is never free while its handler can still be routed to.
        registrations.removeIf(registration -> registration.id().equals(subscriptionId));
        subscriptionIds.remove(subscriptionId);
    }

    /**
     * @return {@code true} if at least one handler is registered.
     */
    public final boolean hasSubscriptions() {
        return !registrations.isEmpty();
    }

    /**
     * Route a single event to every registered handler whose filter matches, in registration order, on the calling
     * thread. A handler exception propagates to the caller.
     *
     * @param cloudEvent The event to dispatch.
     */
    protected final void route(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        for (Registration registration : registrations) {
            if (registration.matcher().test(cloudEvent)) {
                registration.action().accept(cloudEvent);
            }
        }
    }

    /**
     * Route each event in turn via {@link #route(CloudEvent)}, in iteration order.
     *
     * @param cloudEvents The events to dispatch.
     */
    protected final void route(Iterable<CloudEvent> cloudEvents) {
        Objects.requireNonNull(cloudEvents, "cloudEvents cannot be null");
        for (CloudEvent cloudEvent : cloudEvents) {
            route(cloudEvent);
        }
    }

    private record AlreadyStartedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            // There is no background thread to wait for: registration completes synchronously in subscribe.
            return true;
        }
    }
}
