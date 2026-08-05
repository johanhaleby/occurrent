package org.occurrent.subscription.api.blocking;

import org.jspecify.annotations.NullMarked;

/**
 * Defines life-cycle methods for subscription models and subscriptions. Cancellation lives in
 * {@link CancellableSubscriptions}, which this extends, because a register-only model can cancel a subscription
 * without having anything to start, stop, or pause.
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
     * model; check each one individually with {@link #isRunning(String)}. A model that layers its own gate on top of
     * a delegate, like {@link ManualStartSubscriptionModel}, is not bound by this and documents its own
     * {@link #isRunning()} answer.
     */
    void stop();

    /**
     * Start a subscription model if it as previously stopped and resume all subscriptions.
     *
     * @see #stop()
     * @see #start(boolean)
     */
    default void start() {
        start(true);
    }

    /**
     * Start a subscription model if it as previously stopped
     *
     * @param resumeSubscriptionsAutomatically Whether to automatically resume all subscriptions when starting. If <code>false</code>, then the subscriptions must be resumed manually using {@link #resumeSubscription(String)}.
     * @see #stop()
     * @see #start(boolean)
     */
    void start(boolean resumeSubscriptionsAutomatically);

    /**
     * @return {@code true} if the subscription model is running, {@code false} otherwise.
     */
    boolean isRunning();

    /**
     * Check if a particular subscription is running.
     *
     * @param subscriptionId The id of the  subscription to check whether it's running or not
     * @return {@code true} if the subscription is running, {@code false} otherwise.
     */
    boolean isRunning(String subscriptionId);

    /**
     * Check if a particular subscription is paused.
     *
     * @param subscriptionId The id of the  subscription to check whether it's paused or not
     * @return {@code true} if the subscription is paused, {@code false} otherwise.
     */
    boolean isPaused(String subscriptionId);

    /**
     * Resume a paused ({@link #pauseSubscription(String)} or {@link #stop()}) subscription. This is useful for testing purposes when you want
     * to write events to an event store and you want a particular subscription to receive these events (but you may have paused
     * others).
     * <p>
     * On a model that owns a single running/stopped flag, resuming a subscription that {@link #stop()} paused reopens
     * the model-wide gate: {@link #isRunning()} reports {@code true} again, even though every other subscription
     * {@code stop()} paused is left exactly as {@code stop()} left it, individually paused and not running, until it
     * too is resumed or {@link #start} is called. See {@link #stop()} for why such a model has no state in between,
     * and for the one documented exception.
     *
     * @param subscriptionId The id of the subscription to resume.
     * @throws IllegalArgumentException If subscription is not paused
     */
    Subscription resumeSubscription(String subscriptionId);

    /**
     * Pause an individual subscription. It'll be paused <i>temporarily</i>, which means that it can be
     * resumed later ({@link #resumeSubscription(String)}). This is useful for testing purposes when you want
     * to write events to an event store without triggering this particular subscription.
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws IllegalArgumentException If subscription is not running
     */
    void pauseSubscription(String subscriptionId);

    /**
     * Shutdown the subscription model and close all subscriptions (they can be resumed later if you start from a durable checkpoint).
     * A subscription model that is shutdown cannot be started again, since it closes resources such as database connections,
     * thread pools etc.
     */
    default void shutdown() {
    }
}
