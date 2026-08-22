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

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

/**
 * Defines life-cycle methods for reactive subscription models and subscriptions. Mirrors the blocking
 * {@code SubscriptionModelLifeCycle}. These methods return synchronously, updating in-memory bookkeeping (which
 * named subscriptions are running, paused, or gone) without waiting for the asynchronous I/O that starting or
 * stopping a subscription can trigger in the background, which is why they return {@code void} rather than a
 * {@code Mono}/{@code Flux}. Cancellation lives in {@link CancellableSubscriptions}, which this extends, because a
 * register-only model can cancel a subscription without having anything to start, stop, or pause.
 */
@NullMarked
public interface SubscriptionModelLifeCycle extends CancellableSubscriptions {

    /**
     * Temporarily stop the subscription model so that none of its subscriptions will receive any events.
     * It can be started again using {@link #start}.
     * <p>
     * Every subscription that was running is left <i>paused</i>, so {@link #isPaused(String)} returns {@code true} for it,
     * and {@link #resumeSubscription(String)} can bring it back on its own without starting the rest: every other
     * subscription {@code stop()} paused stays paused (its own {@link #isPaused(String)} and {@link #isRunning(String)}
     * are unaffected) until it too is resumed or {@link #start} is called.
     * <p>
     * For a model that owns a single running/stopped flag, resuming that one subscription also reopens the model as
     * a whole: {@link #isRunning()} reports {@code true} again as soon as the first subscription is resumed, since
     * such a model has no state between "everything is stopped" and "the gate is open, and each subscription's own
     * paused/running flag decides whether it uses it". Do not read a {@code true} {@link #isRunning()} after a
     * partial resume as "every subscription that was running before {@code stop()} is delivering again" on such a
     * model, check each one individually with {@link #isRunning(String)}. The blocking stack's
     * {@code ManualStartSubscriptionModel} answers the same way, even though it holds registrations back on top of the
     * model it wraps. Resuming one subscription after {@code stop()} makes its {@link #isRunning()} report
     * {@code true} again.
     */
    void stop();

    /**
     * Start a subscription model that was previously stopped, and resume all its subscriptions.
     * <p>
     * Calling this on a model that is already started is allowed. It brings up whatever is not running yet and leaves
     * everything else as it is, so a caller that cannot observe the current state, a leader election or a health check
     * for example, can call it without checking {@link #isRunning()} first.
     *
     * @see #stop()
     * @see #start(boolean)
     */
    default void start() {
        start(true);
    }

    /**
     * Start a subscription model that was previously stopped.
     * <p>
     * Calling this on a model that is already started is allowed, and brings up whatever is not running yet.
     *
     * @param resumeSubscriptionsAutomatically Whether to automatically resume all subscriptions when starting. If <code>false</code>, then the subscriptions must be resumed manually using {@link #resumeSubscription(String)}.
     * @see #stop()
     * @see #start()
     */
    void start(boolean resumeSubscriptionsAutomatically);

    /**
     * @return {@code true} if the subscription model is running, {@code false} otherwise.
     */
    boolean isRunning();

    /**
     * Check if a particular subscription is running.
     *
     * @param subscriptionId The id of the subscription to check whether it's running or not
     * @return {@code true} if the subscription is running, {@code false} otherwise.
     */
    boolean isRunning(String subscriptionId);

    /**
     * Check if a particular subscription is paused.
     *
     * @param subscriptionId The id of the subscription to check whether it's paused or not
     * @return {@code true} if the subscription is paused, {@code false} otherwise.
     */
    boolean isPaused(String subscriptionId);

    /**
     * Resume a paused ({@link #pauseSubscription(String)} or {@link #stop()}) subscription. This is useful for testing purposes when you want
     * to write events to an event store and you want a particular subscription to receive these events (but you may have paused
     * others). Resumes from the position of the last event delivered before the subscription was paused.
     * <p>
     * Resuming a subscription that {@link #stop()} paused makes {@link #isRunning()} report {@code true} again, even
     * though every other subscription {@code stop()} paused is left exactly as {@code stop()} left it, individually
     * paused and not running, until it too is resumed or {@link #start} is called. See {@link #stop()} for why a
     * model has no state in between.
     *
     * @param subscriptionId The id of the subscription to resume.
     * @throws UnknownSubscriptionException       If this subscription model has no subscription with that id.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running. Resuming is a transition of
     *                                             one subscription rather than a goal, so a redundant call is a
     *                                             mistake worth reporting. Starting the whole model is the opposite
     *                                             and accepts being called twice.
     * @throws IllegalStateException              If the subscription cannot be resumed right now for a reason that is
     *                                             not the caller's doing, which on a competing consumer model means
     *                                             another node currently holds the subscription.
     */
    SubscriptionHandle resumeSubscription(String subscriptionId);

    /**
     * Pause an individual subscription. It'll be paused <i>temporarily</i>, which means that it can be
     * resumed later ({@link #resumeSubscription(String)}). This is useful for testing purposes when you want
     * to write events to an event store without triggering this particular subscription.
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws UnknownSubscriptionException   If this subscription model has no subscription with that id.
     * @throws SubscriptionNotRunningException If the subscription exists here but is not running, because it is
     *                                         already paused, was never started, or the whole model is stopped.
     */
    void pauseSubscription(String subscriptionId);

    /**
     * Shutdown the subscription model and dispose all subscriptions (they can be resumed later if you start from a durable checkpoint).
     * A subscription model that is shutdown cannot be started again, since it releases resources such as database connections.
     */
    default void shutdown() {
    }
}
