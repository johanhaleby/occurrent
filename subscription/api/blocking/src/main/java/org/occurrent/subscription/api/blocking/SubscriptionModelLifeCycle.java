package org.occurrent.subscription.api.blocking;

import java.util.List;
import java.util.Optional;

/**
 * Defines life-cycle methods for subscription models and subscriptions.
 */
public interface SubscriptionModelLifeCycle extends SubscriptionModelCancelSubscription {

    /**
     * Temporary stop the subscription model so that none of its subscriptions will receive any events.
     * It can be started again using {@link #start}.
     */
    void stop();

    /**
     * Start a subscription model if it as previously stopped
     *
     * @see #stop()
     */
    void start();

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
     *
     * @param subscriptionId The id of the subscription to pause.
     * @throws IllegalArgumentException If subscription is not running
     */
    void pauseSubscription(String subscriptionId);
}
