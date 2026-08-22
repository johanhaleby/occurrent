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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
     * offered. {@link #routeReportingMatch(CloudEvent, BiConsumer)} reports {@link RoutingOutcome#DELIVERED} or
     * {@link RoutingOutcome#DEFERRED} from this return value, evaluated after this method runs rather than guessed
     * beforehand, so a caller wrapping a catch-up-then-live engine can refuse without buffering and report that
     * refusal accurately. Mirrors the blocking {@code RegisteringSubscribable.RoutingAction}, except this one has no
     * {@code bufferIfNotLive} flag. The reactor stack has one caller shape,
     * {@code PushSubscriptionModel.accept(CloudEvent)}, so there is no second behaviour for a flag to select between.
     * <p>
     * Public so a same-package-but-not-a-subclass caller reached through a {@code protected} pass-through, the
     * shape {@code org.occurrent.subscription.push.reactor.PushSubscriptionModel} exposes to
     * {@code CatchupThenPushSubscriptionModel} for exactly this, can still name the type. The registration entry
     * point this feeds, {@link #subscribeReportingDelivery(String, SubscriptionFilter, StartAt, RoutingAction)},
     * stays {@code protected}; only the shape of the action is public.
     */
    public interface RoutingAction {
        /**
         * @param cloudEvent The matched event to route.
         * @return A {@link Mono} that completes with {@code true} once {@code cloudEvent} has genuinely landed,
         *         {@code false} when this call declined to hand it over at all (never when it was accepted and then
         *         failed; an error is how that propagates instead), or errors with {@link Refusal} to report a
         *         refusal decided before any dispatch was attempted (an engine-level guard, not a handler running
         *         at all), so {@link #routeReportingMatch(CloudEvent, BiConsumer)} can tell it apart from a handler
         *         that errored after genuinely being invoked. Any other error is taken to mean the opposite:
         *         dispatch was attempted and the handler behind it failed.
         */
        Mono<Boolean> route(CloudEvent cloudEvent);

        /**
         * Thrown or emitted by {@link #route(CloudEvent)} to report a refusal decided before any dispatch was
         * attempted, wrapping the real failure as {@link #getCause()}. {@code routeReportingMatch} never reports
         * {@link RoutingOutcome#DELIVERED} for one of these, and propagates the wrapped cause unchanged, exactly as
         * it would have propagated without this wrapper.
         * <p>
         * {@code permanent} is the action's promise about its own refusal, and it decides which outcome is
         * reported. Pass {@code true} only when offering the same event to this same registration again is certain
         * to be refused the same way, which reports {@link RoutingOutcome#REFUSED} and tells a caller to stop.
         * A refusal that clears on its own, a catch-up-then-live engine whose live buffer is full while its replay
         * is still running, say, passes {@code false} and reports {@link RoutingOutcome#NOT_DELIVERABLE}, which
         * sends the event through the caller's failure policy instead.
         */
        final class Refusal extends RuntimeException {
            private final RuntimeException refusal;
            private final boolean permanent;

            /**
             * @param refusal   The real failure, propagated unchanged once the outcome has been reported.
             * @param permanent Whether offering the same event to this same registration again is certain to be
             *                  refused the same way.
             */
            public Refusal(RuntimeException refusal, boolean permanent) {
                super(refusal);
                this.refusal = refusal;
                this.permanent = permanent;
            }

            RuntimeException unwrap() {
                return refusal;
            }

            RoutingOutcome outcome() {
                return permanent ? RoutingOutcome.REFUSED : RoutingOutcome.NOT_DELIVERABLE;
            }
        }
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
        Objects.requireNonNull(action, "action cannot be null");
        // Every event this released signature's action handles is reported delivered, matching what DELIVERED has
        // always meant here. The handler was genuinely invoked, whether it then completes or errors.
        return doSubscribe(subscriptionId, filter, startAt, cloudEvent -> action.apply(cloudEvent).thenReturn(true));
    }

    /**
     * As {@link #subscribe(String, SubscriptionFilter, StartAt, Function)}, except the registered action reports
     * back whether the event it was given genuinely landed, so {@link #routeReportingMatch(CloudEvent, BiConsumer)}
     * can report {@link RoutingOutcome#DELIVERED} or {@link RoutingOutcome#DEFERRED} accurately instead of assuming
     * delivery ahead of it. {@link Consumers#ONE} only, the same restriction
     * {@link #routeReportingMatch(CloudEvent, BiConsumer)} itself already enforces at routing time.
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
     * decision and the report. Nothing registered, the model not running and the sole subscription being paused all
     * report {@link RoutingOutcome#UNAVAILABLE} and throw nothing, the same three states
     * {@link #route(CloudEvent)} already skips for dispatch. A filter that declines the event reports
     * {@link RoutingOutcome#FILTERED}. The matcher itself
     * throwing reports {@link RoutingOutcome#NOT_DELIVERABLE}, never {@link RoutingOutcome#FILTERED}, since a
     * filter that failed to answer did not decline the event, and that throwing matcher's exception still
     * propagates once {@code matchObserver} has been told. If {@code matchObserver} itself then throws a
     * {@link RuntimeException} or an {@link Error} while being told, that failure is suppressed onto the matcher's
     * original exception rather than replacing it, so a badly behaved {@code matchObserver} can never change which
     * exception, or whose, a caller sees.
     * <p>
     * A matched registration's {@link RoutingAction} is always told this event was matched, even when it later
     * errors: {@code matchObserver} is told {@link RoutingOutcome#DELIVERED}, since the action was genuinely
     * invoked, which is what {@link RoutingOutcome#DELIVERED} has always meant regardless of what the action does
     * with the event afterward, and the original error then still propagates once {@code matchObserver} has been
     * told.
     * <p>
     * A {@link RoutingAction.Refusal} is a different thing again. The action was reached but refused before
     * attempting any dispatch, so it is never {@link RoutingOutcome#DELIVERED}, and the wrapped cause propagates
     * rather than the refusal itself. Which outcome is reported comes from the refusal,
     * {@link RoutingOutcome#REFUSED} when the action promises refusing is permanent for that registration and
     * {@link RoutingOutcome#NOT_DELIVERABLE} when it does not.
     * <p>
     * Restricted to {@link Consumers#ONE} because sharing one evaluation across more than one registration would
     * mean deciding every registration's eligibility before dispatching any of them, changing which registration's
     * error a caller sees first. With at most one registration that reordering cannot happen, which is what makes
     * this safe where restructuring {@link #route(CloudEvent)} itself would not be. That check runs eagerly, not
     * deferred, since it is a caller error rather than model state.
     *
     * @param cloudEvent    The event to route.
     * @param matchObserver Told, once, this event's {@link RoutingOutcome}, after its registration's action (if
     *                      any) has run, whether that action completed, declined, or errored.
     * @return A {@link Mono} that completes when the action, if any ran, has completed.
     */
    protected final Mono<Void> routeReportingMatch(CloudEvent cloudEvent, BiConsumer<CloudEvent, RoutingOutcome> matchObserver) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        Objects.requireNonNull(matchObserver, "matchObserver cannot be null");
        if (consumers != Consumers.ONE) {
            throw new IllegalStateException(getClass().getSimpleName() + " must declare Consumers.ONE to call routeReportingMatch(..)");
        }
        return Mono.defer(() -> {
            if (running) {
                for (Registration registration : registrations) {
                    // Paused is a lifecycle state, not a filter answer, so it is checked before the matcher runs at
                    // all. Reporting FILTERED here would tell the caller the event was this subscription's and it
                    // declined it, when in truth the filter was never asked.
                    if (pausedSubscriptions.contains(registration.id())) {
                        matchObserver.accept(cloudEvent, RoutingOutcome.UNAVAILABLE);
                        return Mono.<Void>empty();
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
                            // IllegalArgumentException that would replace the matcher failure this is here to
                            // protect. Same hazard HandlerFailures.combined(..) already guards against.
                            if (observerFailure != e) {
                                e.addSuppressed(observerFailure);
                            }
                        }
                        throw e;
                    }
                    if (!eligible) {
                        matchObserver.accept(cloudEvent, RoutingOutcome.FILTERED);
                        return Mono.<Void>empty();
                    }
                    // Deferred so a synchronous error from route(..) itself, not just one signalled on the returned
                    // Mono, is still caught by the classification below rather than escaping this assembly step.
                    // Scoped to the action call alone, before the observer notification is attached, so a
                    // matchObserver failure on the success path below can never re-enter this same handler and be
                    // misclassified as an action failure.
                    return Mono.defer(() -> registration.action().route(cloudEvent))
                            .onErrorResume(error -> {
                                // A RoutingAction.Refusal is decided before any dispatch was attempted
                                // (ReactiveHandover's catch-up failure, say), never a delivery, so this is
                                // NOT_DELIVERABLE, the same outcome a matcher that failed to answer reports, not
                                // DELIVERED, and the wrapped cause is what propagates, unchanged. Any other error
                                // means the action was invoked, which is what DELIVERED has always meant; whether the
                                // eventual fold succeeds or errors is a separate signal (RoutingOutcome's own javadoc
                                // says so).
                                RoutingOutcome outcome;
                                Throwable propagate;
                                if (error instanceof RoutingAction.Refusal refusal) {
                                    outcome = refusal.outcome();
                                    propagate = refusal.unwrap();
                                } else {
                                    outcome = RoutingOutcome.DELIVERED;
                                    propagate = error;
                                }
                                try {
                                    matchObserver.accept(cloudEvent, outcome);
                                } catch (RuntimeException | Error observerFailure) {
                                    // Same self-suppression guard as the matcher-throw branch above. Skip the
                                    // instance itself.
                                    if (observerFailure != propagate) {
                                        propagate.addSuppressed(observerFailure);
                                    }
                                }
                                return Mono.<Boolean>error(propagate);
                            })
                            .flatMap(landed -> {
                                // Told once, on the success path, with no suppression guard. A failure here is the
                                // observer's own, propagating as itself rather than being attached to anything,
                                // mirroring the blocking stack's success path.
                                matchObserver.accept(cloudEvent, Boolean.TRUE.equals(landed) ? RoutingOutcome.DELIVERED : RoutingOutcome.DEFERRED);
                                return Mono.<Void>empty();
                            });
                }
            }
            matchObserver.accept(cloudEvent, RoutingOutcome.UNAVAILABLE);
            return Mono.<Void>empty();
        });
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
                    // Unwrapped exactly as it always has been, even for an action that errors with
                    // RoutingAction.Refusal. That wrapper only ever matters to routeReportingMatch(..), which can act
                    // on it before propagating the same unwrapped cause, and a caller here gets that cause directly
                    // since this path has no observer to tell first.
                    .concatMap(registration -> registration.action().route(cloudEvent)
                            .onErrorMap(RoutingAction.Refusal.class, RoutingAction.Refusal::unwrap))
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
                            // and a supplied DataFieldReader that itself fails to read can still throw from the
                            // matcher. A model given no reader at all refuses a payload filter earlier, at subscribe
                            // time.
                            .concatMap(registration -> Mono.defer(() -> registration.matcher().test(cloudEvent)
                                            ? registration.action().route(cloudEvent)
                                            : Mono.<Boolean>empty())
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

    // Answers for the one registration it was created for. Nothing runs in the background, so registering on a running
    // model starts the subscription there and then. A registration made while the model was stopped never completes,
    // the way the reactor Mongo and durable models park theirs, and a later start hands back its own handle.
    private record RegisteredSubscription(String id, boolean started) implements Subscription {
        @Override
        public Mono<Void> waitUntilStarted() {
            return started ? Mono.empty() : Mono.never();
        }
    }
}
