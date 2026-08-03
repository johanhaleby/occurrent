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
import org.occurrent.inmemory.filtermatching.DataFieldReader;
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
 * ordered dispatch. It has no start position, checkpoint, catch-up, or replay. {@link StartAt} is accepted for
 * interface compatibility but ignored, since "where to start" is meaningless when the subclass decides which events
 * reach {@link #route(CloudEvent)} and when.
 * <p>
 * It does implement {@link SubscriptionModelLifeCycle}, so a stopped model or a paused subscription is skipped by
 * {@link #route(CloudEvent)}. Read that as <i>dropped, not deferred</i>: nothing is holding the events back, so an
 * event fed in while a subscription is paused never reaches that handler, and resuming does not replay it. This is
 * how {@code InMemorySubscriptionModel} already behaves for events fed to it while stopped, and it is what lets a
 * test stop everything and opt back in per subscription.
 * <p>
 * Subclasses expose their own ingestion API (for example a synchronous at-write-time {@code dispatch(List)} or an
 * externally driven {@code accept(CloudEvent)}) and delegate to {@link #route(CloudEvent)} to deliver each event.
 */
@NullMarked
public abstract class RegisteringSubscribable implements Subscribable, SubscriptionModelLifeCycle, IntrospectableSubscriptionModel {

    private record Registration(String id, Predicate<CloudEvent> matcher, Consumer<CloudEvent> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedSubscriptions = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;
    private final DataFieldReader dataFieldReader;

    /**
     * Creates a model that refuses a subscription filter on a {@code data} payload field, which is what a subclass
     * built without a reader has always done.
     */
    protected RegisteringSubscribable() {
        this(DataFieldReader.refusing());
    }

    /**
     * Creates a model that can answer a subscription filter on a {@code data} payload field by reading it through
     * {@code dataFieldReader}. Occurrent ships a Jackson-backed one in
     * {@code occurrent-common-inmemory-filter-matching-jackson}.
     */
    protected RegisteringSubscribable(DataFieldReader dataFieldReader) {
        this.dataFieldReader = Objects.requireNonNull(dataFieldReader, DataFieldReader.class.getSimpleName() + " cannot be null");
    }

    @Override
    public final Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        // Build the matcher before reserving the id, so an unsupported filter does not leave the id permanently taken.
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter, dataFieldReader);
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
    public final Set<String> subscriptionIds() {
        return Set.copyOf(subscriptionIds);
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
     * Route a single event to every registered handler whose filter matches, in registration order, on the calling
     * thread. A handler exception propagates to the caller.
     * <p>
     * A stopped model routes nothing, and a paused subscription is skipped. The event is dropped for that handler
     * rather than held, so resuming later does not deliver it.
     *
     * @param cloudEvent The event to dispatch.
     */
    protected final void route(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        if (!running) {
            return;
        }
        for (Registration registration : registrations) {
            if (!pausedSubscriptions.contains(registration.id()) && registration.matcher().test(cloudEvent)) {
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
