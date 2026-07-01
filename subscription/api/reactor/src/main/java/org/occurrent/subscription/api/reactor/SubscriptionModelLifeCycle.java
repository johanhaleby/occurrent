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

/**
 * Defines life-cycle methods for reactive subscription models and subscriptions. Mirrors the blocking
 * {@code SubscriptionModelLifeCycle}. The methods here manage in-memory bookkeeping (which named subscriptions are
 * running, paused, or gone) rather than doing I/O themselves, so they are synchronous rather than returning a
 * {@code Mono}/{@code Flux}.
 */
@NullMarked
public interface SubscriptionModelLifeCycle {

    /**
     * Temporary stop the subscription model so that none of its subscriptions will receive any events.
     * It can be started again using {@link #start}.
     */
    void stop();

    /**
     * Start a subscription model if it was previously stopped and resume all subscriptions.
     *
     * @see #stop()
     * @see #start(boolean)
     */
    default void start() {
        start(true);
    }

    /**
     * Start a subscription model if it was previously stopped
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
     * Resume a paused ({@link #pauseSubscription(String)}) subscription. This is useful for testing purposes when you want
     * to write events to an event store and you want a particular subscription to receive these events (but you may have paused
     * others). Resumes from the position of the last event delivered before the subscription was paused.
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
     * Cancel a subscription, this will remove the position from position storage (if used),
     * and you cannot restart it from its current position again. Cancelling a subscription id that is unknown or
     * already cancelled is a no-op.
     */
    void cancelSubscription(String subscriptionId);

    /**
     * Shutdown the subscription model and dispose all subscriptions (they can be resumed later if you start from a durable subscription position).
     * A subscription model that is shutdown cannot be started again, since it releases resources such as database connections.
     */
    default void shutdown() {
    }
}
