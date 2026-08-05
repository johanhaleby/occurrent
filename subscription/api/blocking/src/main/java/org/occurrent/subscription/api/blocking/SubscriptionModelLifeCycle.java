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
     * and {@link #resumeSubscription(String)} can bring it back on its own without starting the rest.
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
     * Resume a paused ({@link #pauseSubscription(String)}) subscription. This is useful for testing purposes when you want
     * to write events to an event store and you want a particular subscription to receive these events (but you may have paused
     * others).
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws IllegalArgumentException If subscription is not paused
     */
    Subscription resumeSubscription(String subscriptionId);

    /**
     * Pause an individual subscription. It'll be paused <i>temporarily</i>, which means that it can be
     * resumed later ({@link #resumeSubscription(String)}). This is useful for testing purposes when you want
     * to write events to an event store without triggering this particular subscription.
     * <p>
     * What happens to an event published while a subscription is paused is up to the model, so read the one you
     * use rather than assuming either answer. A model reading a log or a change stream can resume from the
     * position it had reached and deliver that event, at the price of handing over an event a second time if
     * something else consumed it in the meantime. A model that dispatches events as they arrive has nowhere to
     * hold them, so the event never reaches that handler at all.
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
