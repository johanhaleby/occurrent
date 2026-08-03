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
import org.occurrent.subscription.internal.SingleConsumerMessages;

import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
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
 * <p>
 * <strong>One consumer unless a subclass says otherwise.</strong> A subclass declares its {@link Consumers} through
 * the constructor, and the no-argument constructor means {@link Consumers#ONE}. That default is the safe one: an
 * externally driven sink delivers one message under one acknowledgement, so several consumers on it would mean one
 * failing consumer holding up the rest, which the isolation rule in {@code AGENTS.md} forbids. Only a subclass that
 * has an argument for why fan-out is safe for it, as the synchronous write-path dispatcher does, passes
 * {@link Consumers#MANY}. See ADR 88.
 */
@NullMarked
public abstract class RegisteringSubscribable implements Subscribable, SubscriptionModelLifeCycle, IntrospectableSubscriptionModel {

    /**
     * How many consumers a subclass accepts, fixed at construction.
     */
    public enum Consumers {
        /**
         * One consumer. A second {@code subscribe} is refused, naming the consumer already registered. Cancelling
         * the registered one frees the sink for another, since this counts what is registered now rather than
         * whether anything ever was.
         */
        ONE,
        /**
         * Several consumers, each receiving every matching event. Safe only where a failure cannot strand a sibling:
         * the synchronous models qualify because there is no broker and no acknowledgement, so a handler failure
         * fails the write itself.
         */
        MANY
    }

    private record Registration(String id, Predicate<CloudEvent> matcher, Consumer<CloudEvent> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedSubscriptions = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();
    private final Consumers consumers;
    // The sole consumer's id under Consumers.ONE, or null while the sink is free. An AtomicReference rather than a
    // registrations.isEmpty() check so claiming the slot is one atomic step, and cleared on cancel so the id can be
    // re-subscribed (which a failed push catch-up relies on).
    private final AtomicReference<@Nullable String> soleSubscriptionId = new AtomicReference<>();
    private volatile boolean running = true;

    /**
     * Accepts a single consumer. The default because a sink that fans out cannot keep its consumers isolated from one
     * another, so opting into that has to be deliberate.
     */
    protected RegisteringSubscribable() {
        this(Consumers.ONE);
    }

    /**
     * @param consumers How many consumers this subclass accepts. Pass {@link Consumers#MANY} only with a reason why
     *                  one consumer's failure cannot strand another.
     */
    protected RegisteringSubscribable(Consumers consumers) {
        this.consumers = Objects.requireNonNull(consumers, "consumers cannot be null");
    }

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
        if (consumers == Consumers.ONE && !soleSubscriptionId.compareAndSet(null, subscriptionId)) {
            // Release the id again: the duplicate-id check above took it, and this registration is not happening.
            subscriptionIds.remove(subscriptionId);
            throw new IllegalArgumentException(SingleConsumerMessages.singleConsumerOnly(
                    getClass().getSimpleName(), "subscription", String.valueOf(soleSubscriptionId.get()), subscriptionId));
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
        soleSubscriptionId.compareAndSet(subscriptionId, null);
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
     * Drop every registration and stop routing. Unlike {@link #stop()} this is not reversible: the ids are released
     * and the handlers are gone, so a shut-down model delivers nothing even after {@link #start(boolean)}.
     * <p>
     * Overridden because the interface default does nothing, which left a shut-down model still delivering every
     * event fed to it.
     */
    @Override
    public final void shutdown() {
        running = false;
        registrations.clear();
        subscriptionIds.clear();
        pausedSubscriptions.clear();
        soleSubscriptionId.set(null);
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
