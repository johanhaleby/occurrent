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
import org.occurrent.inmemory.filtermatching.DataFieldReader;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.internal.HandlerFailures;
import org.occurrent.subscription.internal.SingleConsumerMessages;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
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
 * <p>
 * <strong>One consumer unless a subclass says otherwise.</strong> A subclass declares its {@link Consumers} through
 * the constructor, and the no-argument constructor means {@link Consumers#ONE}. That default is the safe one: an
 * externally driven sink delivers one message under one acknowledgement, so several consumers on it would mean one
 * failing consumer holding up the rest, which the isolation rule in {@code AGENTS.md} forbids. Only a subclass that
 * has an argument for why fan-out is safe for it, as the synchronous write-path dispatcher does, passes
 * {@link Consumers#MANY}. See ADR 90.
 */
@NullMarked
public abstract class RegisteringSubscribable implements Subscribable, SubscriptionModelLifeCycle {

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
         * Several consumers, each receiving every matching event. Safe only where a failure cannot strand a sibling.
         * The synchronous models qualify two ways: inside a transaction a handler failure fails the write, so no handler's
         * work survives, and outside one {@link #routeIsolated(Iterable)} gives every handler the event anyway.
         */
        MANY
    }

    private record Registration(String id, Predicate<CloudEvent> matcher, Function<CloudEvent, Mono<Void>> action) {
    }

    private final Set<String> subscriptionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedSubscriptions = ConcurrentHashMap.newKeySet();
    private final CopyOnWriteArrayList<Registration> registrations = new CopyOnWriteArrayList<>();
    private final Consumers consumers;
    // The sole consumer's id under Consumers.ONE, or null while the sink is free. An AtomicReference rather than a
    // registrations.isEmpty() check so claiming the slot is one atomic step, and cleared on cancel so the id can be
    // re-subscribed (which a failed push catch-up relies on).
    private final AtomicReference<@Nullable String> soleSubscriptionId = new AtomicReference<>();
    // Held only while subscribe and cancelSubscription rearrange the four collections above, never by route, which
    // reads a CopyOnWriteArrayList snapshot. Without it a cancel landing between the slot claim and the registration
    // frees the slot while leaving the handler registered, and a second id could then claim it and fan out.
    private final Object registrationLock = new Object();
    private volatile boolean running = true;
    private final DataFieldReader dataFieldReader;

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
        this(consumers, DataFieldReader.refusing());
    }

    /**
     * @param consumers       How many consumers this subclass accepts. Pass {@link Consumers#MANY} only with a reason
     *                        why one consumer's failure cannot strand another.
     * @param dataFieldReader Reads a field out of an event's payload, so a subscription can filter on one. Occurrent
     *                        ships a Jackson-backed reader in {@code occurrent-common-inmemory-filter-matching-jackson}.
     *                        {@link DataFieldReader#refusing()} refuses such a filter, which is the default.
     */
    protected RegisteringSubscribable(Consumers consumers, DataFieldReader dataFieldReader) {
        this.consumers = Objects.requireNonNull(consumers, "consumers cannot be null");
        this.dataFieldReader = Objects.requireNonNull(dataFieldReader, DataFieldReader.class.getSimpleName() + " cannot be null");
    }

    @Override
    public final Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        // Build the matcher before reserving the id, so an unsupported filter does not leave the id permanently taken.
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter, dataFieldReader);
        synchronized (registrationLock) {
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
        }
        return new AlreadyStartedSubscription(subscriptionId);
    }

    @Override
    public final void cancelSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        synchronized (registrationLock) {
            // Drop the registration before releasing the id, so the id is never free while its handler can still be routed to.
            registrations.removeIf(registration -> registration.id().equals(subscriptionId));
            subscriptionIds.remove(subscriptionId);
            pausedSubscriptions.remove(subscriptionId);
            soleSubscriptionId.compareAndSet(subscriptionId, null);
        }
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
        synchronized (registrationLock) {
            registrations.clear();
            subscriptionIds.clear();
            pausedSubscriptions.clear();
            soleSubscriptionId.set(null);
        }
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
     * Route every event to every matching handler, like {@link #route(Iterable)}, except that one handler erroring does
     * not stop the others. Each error is collected and the returned {@link Mono} errors once the whole batch has been
     * offered.
     * <p>
     * A handler that errors is skipped for the rest of this batch, so isolation is between handlers and never within
     * one handler's own event order. One error is emitted exactly as it was, several as the first with the rest in
     * {@link Throwable#addSuppressed(Throwable)}.
     * <p>
     * See the 2026-08-04 amendment to ADR 57 for why a dispatch without a transaction works this way.
     *
     * @param cloudEvents The events to dispatch.
     * @return A {@link Mono} that completes when every handler has been offered every event, or errors if any failed.
     */
    protected final Mono<Void> routeIsolated(Iterable<CloudEvent> cloudEvents) {
        Objects.requireNonNull(cloudEvents, "cloudEvents cannot be null");
        return Mono.defer(() -> {
            // Created per subscription rather than per model, and every stage below is sequential through concatMap,
            // so these need no synchronisation. Which handlers have failed is tracked by identity, not by id and not by
            // Registration equality: cancelling frees an id for re-subscription, and a handler registered under a freed
            // id must not inherit the failure of the one that released it. The failures themselves go in a list, so
            // they are reported in the order they happened.
            Set<Registration> failed = Collections.newSetFromMap(new IdentityHashMap<>());
            List<Throwable> failures = new ArrayList<>();
            return Flux.fromIterable(cloudEvents)
                    .takeWhile(ignored -> running)
                    .concatMap(cloudEvent -> Flux.fromIterable(registrations)
                            .filter(registration -> !failed.contains(registration)
                                    && !pausedSubscriptions.contains(registration.id()))
                            // The matcher and the apply both go inside the defer. Outside it, a throw happens while
                            // concatMap is invoking the mapper, which terminates the whole batch and records nothing,
                            // and a filter on a payload field does throw from the matcher when the model was given no
                            // DataFieldReader.
                            .concatMap(registration -> Mono.defer(() -> registration.matcher().test(cloudEvent)
                                            ? registration.action().apply(cloudEvent)
                                            : Mono.<Void>empty())
                                    .onErrorResume(error -> {
                                        // An Error is not a recoverable situation, so it keeps going the way it does on
                                        // the blocking stack. A checked exception is an ordinary handler failure and is
                                        // collected, which only this stack can see, since a Consumer cannot throw one.
                                        if (error instanceof Error) {
                                            return Mono.error(error);
                                        }
                                        failed.add(registration);
                                        failures.add(error);
                                        return Mono.empty();
                                    }))
                            .then())
                    // Deferred so the failures are read after the batch has run, not when this chain is assembled.
                    .then(Mono.defer(() -> HandlerFailures.combined(failures)
                            .map(Mono::<Void>error)
                            .orElseGet(Mono::empty)));
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
