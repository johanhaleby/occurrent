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
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.internal.HandlerFailures;
import org.occurrent.subscription.internal.SingleConsumerMessages;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
 * It is a full {@link SubscriptionModel}, so a stopped model or a paused subscription is skipped by
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
 * {@link Consumers#MANY}. See ADR 90.
 */
@NullMarked
public abstract class RegisteringSubscribable implements SubscriptionModel, IntrospectableSubscriptions {

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

    private record Registration(String id, Predicate<CloudEvent> matcher, RoutingAction action) {
    }

    /**
     * What a registration does with a matched event, and whether it genuinely landed rather than only being
     * offered. {@link #routeReportingMatch(CloudEvent, boolean, BiConsumer)} reports
     * {@link RoutingOutcome#DELIVERED} or {@link RoutingOutcome#DEFERRED} from this return value, evaluated after
     * this method runs rather than guessed beforehand, so a caller wrapping a catch-up-then-live engine can refuse
     * without buffering when {@code bufferIfNotLive} is {@code false} and report that refusal accurately.
     * <p>
     * Public so a same-package-but-not-a-subclass caller reached through a {@code protected} pass-through, the
     * shape {@link org.occurrent.subscription.push.blocking.PushSubscriptionModel} exposes to
     * {@code CatchupThenPushSubscriptionModel} for exactly this, can still name the type. The registration entry
     * point this feeds, {@link #subscribeReportingDelivery(String, SubscriptionFilter, StartAt, RoutingAction)},
     * stays {@code protected}; only the shape of the action is public.
     */
    public interface RoutingAction {
        /**
         * @param cloudEvent      The matched event to route.
         * @param bufferIfNotLive What to do when the target this action feeds is not ready right now: buffer the
         *                        event for later if {@code true}, refuse it without buffering if {@code false}.
         *                        An implementation with only one behavior, no buffering distinction to make, is
         *                        free to ignore this parameter.
         * @return {@code true} once {@code cloudEvent} has genuinely landed, {@code false} when this call declined
         *         to hand it over at all (never when it was accepted and then failed; an exception is how that
         *         propagates instead).
         */
        boolean route(CloudEvent cloudEvent, boolean bufferIfNotLive);
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
    public final Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Objects.requireNonNull(action, "action cannot be null");
        return doSubscribe(subscriptionId, filter, startAt, (cloudEvent, bufferIfNotLive) -> {
            action.accept(cloudEvent);
            return true;
        });
    }

    /**
     * As {@link #subscribe(String, SubscriptionFilter, StartAt, Consumer)}, except the registered action reports
     * back whether the event it was given genuinely landed, so {@link #routeReportingMatch(CloudEvent, boolean, BiConsumer)}
     * can report {@link RoutingOutcome#DELIVERED} or {@link RoutingOutcome#DEFERRED} accurately instead of assuming
     * delivery ahead of it. {@link Consumers#ONE} only, the same restriction
     * {@link #routeReportingMatch(CloudEvent, boolean, BiConsumer)} itself already enforces at routing time.
     */
    protected final Subscription subscribeReportingDelivery(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, RoutingAction action) {
        return doSubscribe(subscriptionId, filter, startAt, action);
    }

    private Subscription doSubscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, RoutingAction action) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(startAt, "startAt cannot be null");
        Objects.requireNonNull(action, "action cannot be null");
        // Build the matcher before reserving the id, so an unsupported filter does not leave the id permanently taken.
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter, dataFieldReader);
        synchronized (registrationLock) {
            if (!subscriptionIds.add(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
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
                return new RegisteredSubscription(subscriptionId, false);
            }
        }
        return new RegisteredSubscription(subscriptionId, true);
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

    /**
     * Synchronized on {@link #registrationLock} because {@link #subscribe}, {@link #cancelSubscription} and
     * {@link #shutdown()} rearrange {@code subscriptionIds} and {@code registrations} in more than one step under
     * that same lock, so a reader taking it too cannot land between those steps and disagree with
     * {@link #hasSubscriptions()} about whether a given id is currently registered.
     */
    @Override
    public final Set<String> subscriptionIds() {
        synchronized (registrationLock) {
            return Set.copyOf(subscriptionIds);
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
        requireKnown(subscriptionId);
        if (!isPaused(subscriptionId)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }
        running = true;
        pausedSubscriptions.remove(subscriptionId);
        return new RegisteredSubscription(subscriptionId, true);
    }

    @Override
    public final void pauseSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireKnown(subscriptionId);
        if (!isRunning(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }
        pausedSubscriptions.add(subscriptionId);
    }

    // Separates "no such subscription here" from "wrong state for this call", which a caller holding several models
    // needs in order to tell "keep looking" from "this is the owner and the answer is no".
    private void requireKnown(String subscriptionId) {
        if (!subscriptionIds.contains(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
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
     * Synchronized on {@link #registrationLock} for the same reason as {@link #subscriptionIds()}: it keeps this
     * answer from disagreeing with that one about an id whose registration is mid-flight.
     *
     * @return {@code true} if at least one handler is registered.
     */
    public final boolean hasSubscriptions() {
        synchronized (registrationLock) {
            return !registrations.isEmpty();
        }
    }

    /**
     * For a subclass declared {@link Consumers#ONE}: evaluate its at-most-one registration's eligibility exactly
     * once, dispatch that registration's action if the matcher accepted, then tell {@code matchObserver} the
     * {@link RoutingOutcome}, deciding between {@link RoutingOutcome#DELIVERED} and {@link RoutingOutcome#DEFERRED}
     * from what the action itself reports rather than assuming delivery ahead of it.
     * <p>
     * Sharing one evaluation between the matcher and the report, unlike a separate pre-check ahead of
     * {@link #route(CloudEvent)}, means the two can never disagree about whether the event matched, even for a
     * matcher that is not a deterministic pure function of the event, and means no lifecycle transition (a
     * concurrent {@code stop()}, a {@code pauseSubscription} or a {@code resumeSubscription}) can land between the
     * decision and the report. The model not running and the sole subscription being paused both report
     * {@link RoutingOutcome#NOT_DELIVERABLE}, the same way {@link #route(CloudEvent)} already treats them for
     * dispatch. A filter that declines the event reports {@link RoutingOutcome#FILTERED}. The matcher itself
     * throwing reports {@link RoutingOutcome#NOT_DELIVERABLE}, never {@link RoutingOutcome#FILTERED}, since a
     * filter that failed to answer did not decline the event, and that throwing matcher's exception still
     * propagates to the caller once {@code matchObserver} has been told. If {@code matchObserver} itself then
     * throws a {@link RuntimeException} or an {@link Error} while being told, that failure is suppressed onto the
     * matcher's original exception rather than replacing it, so a badly behaved {@code matchObserver} can never
     * change which exception, or whose, a caller sees.
     * <p>
     * A matched registration's {@link RoutingAction} is always told this event was matched, even when it later
     * throws: {@code matchObserver} is told {@link RoutingOutcome#DELIVERED}, since the action was genuinely
     * invoked, which is what {@link RoutingOutcome#DELIVERED} has always meant regardless of what the action does
     * with the event afterward, and the original {@link RuntimeException} then still propagates to the caller once
     * {@code matchObserver} has been told. An engine-level refusal a {@link RoutingAction} makes deliberately, by
     * returning {@code false} rather than throwing, is a different thing entirely and is what decides
     * {@link RoutingOutcome#DEFERRED} instead.
     * <p>
     * Restricted to {@link Consumers#ONE} because sharing one evaluation across more than one registration would
     * mean deciding every registration's eligibility before dispatching any of them, changing which registration's
     * exception a caller sees first. With at most one registration that reordering cannot happen, which is what
     * makes this safe where restructuring {@link #route(CloudEvent)} itself would not be.
     *
     * @param cloudEvent      The event to route.
     * @param bufferIfNotLive Passed through to the matched registration's {@link RoutingAction#route(CloudEvent, boolean)}
     *                        unchanged; this method itself has no opinion on what it means.
     * @param matchObserver   Told, once, this event's {@link RoutingOutcome}, after its registration's action (if
     *                        any) has run, whether that action returned or threw.
     */
    protected final void routeReportingMatch(CloudEvent cloudEvent, boolean bufferIfNotLive, BiConsumer<CloudEvent, RoutingOutcome> matchObserver) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Objects.requireNonNull(matchObserver, "matchObserver cannot be null");
        if (consumers != Consumers.ONE) {
            throw new IllegalStateException(getClass().getSimpleName() + " must declare Consumers.ONE to call routeReportingMatch(..)");
        }
        if (running) {
            for (Registration registration : registrations) {
                // Paused is a lifecycle state, not a filter answer, so it is checked before the matcher runs at all.
                // Reporting FILTERED here would tell the caller the event was this subscription's and it declined
                // it, when in truth the filter was never asked.
                if (pausedSubscriptions.contains(registration.id())) {
                    matchObserver.accept(cloudEvent, RoutingOutcome.NOT_DELIVERABLE);
                    return;
                }
                boolean eligible;
                try {
                    eligible = registration.matcher().test(cloudEvent);
                } catch (RuntimeException | AssertionError e) {
                    try {
                        matchObserver.accept(cloudEvent, RoutingOutcome.NOT_DELIVERABLE);
                    } catch (RuntimeException | Error observerFailure) {
                        // Skip the instance itself. A shared exception object thrown by both the matcher and the
                        // observer would otherwise hit addSuppressed's self-suppression guard, an
                        // IllegalArgumentException that would replace the matcher failure this is here to protect.
                        // Same hazard HandlerFailures.combined(..) already guards against.
                        if (observerFailure != e) {
                            e.addSuppressed(observerFailure);
                        }
                    }
                    throw e;
                }
                if (!eligible) {
                    matchObserver.accept(cloudEvent, RoutingOutcome.FILTERED);
                    return;
                }
                boolean landed;
                RuntimeException actionFailure = null;
                try {
                    landed = registration.action().route(cloudEvent, bufferIfNotLive);
                } catch (RuntimeException e) {
                    // The action was invoked, which is what DELIVERED has always meant; whether the eventual fold
                    // succeeds or throws is a separate signal (RoutingOutcome's own javadoc says so). An
                    // engine-level refusal (BlockingHandover's catchUpFailure, say) never reaches this catch,
                    // because it is thrown before any dispatch is attempted and is expected to propagate as the
                    // real failure it is, not be reinterpreted as a delivery.
                    landed = true;
                    actionFailure = e;
                }
                matchObserver.accept(cloudEvent, landed ? RoutingOutcome.DELIVERED : RoutingOutcome.DEFERRED);
                if (actionFailure != null) {
                    throw actionFailure;
                }
                return;
            }
        }
        matchObserver.accept(cloudEvent, RoutingOutcome.NOT_DELIVERABLE);
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
                registration.action().route(cloudEvent, true);
            }
        }
    }

    /**
     * Route every event to every matching handler, like {@link #route(Iterable)}, except that one handler throwing does
     * not stop the others. Each failure is collected and rethrown once the whole batch has been offered.
     * <p>
     * A handler that throws is skipped for the rest of this batch, so isolation is between handlers and never within
     * one handler's own event order. One failure is rethrown exactly as it was, several as the first with the rest in
     * {@link Throwable#addSuppressed(Throwable)}. Only a {@link RuntimeException} is caught, which is all a
     * {@link Consumer} can throw, so an {@link Error} still propagates immediately.
     * <p>
     * See the 2026-08-04 amendment to ADR 57 for why a dispatch without a transaction works this way.
     *
     * @param cloudEvents The events to dispatch.
     */
    protected final void routeIsolated(Iterable<CloudEvent> cloudEvents) {
        Objects.requireNonNull(cloudEvents, "cloudEvents cannot be null");
        // Which handlers have failed is tracked by identity, not by id and not by Registration equality: cancelling
        // frees an id for re-subscription, and a handler registered under a freed id must not inherit the failure of
        // the one that released it. The failures themselves go in a list, so they are reported in the order they
        // happened.
        Set<Registration> failed = Collections.newSetFromMap(new IdentityHashMap<>());
        List<RuntimeException> failures = new ArrayList<>();
        for (CloudEvent cloudEvent : cloudEvents) {
            if (!running) {
                break;
            }
            for (Registration registration : registrations) {
                if (failed.contains(registration) || pausedSubscriptions.contains(registration.id())) {
                    continue;
                }
                // The matcher is inside the try, not just the action: a supplied DataFieldReader that itself fails to
                // read can still throw from here, and one subscription's filter must not cost the others theirs. A
                // model given no reader at all refuses a payload filter earlier, at subscribe time.
                try {
                    if (registration.matcher().test(cloudEvent)) {
                        registration.action().route(cloudEvent, true);
                    }
                } catch (RuntimeException e) {
                    failed.add(registration);
                    failures.add(e);
                }
            }
        }
        HandlerFailures.combined(failures).ifPresent(failure -> {
            throw failure;
        });
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

    // Answers for the one registration it was created for. There is no background thread to wait for, so registering
    // on a running model starts the subscription there and then, and registering on a stopped model does not.
    // A subscription started later takes its handle from resumeSubscription.
    private record RegisteredSubscription(String id, boolean started) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return started;
        }
    }
}
