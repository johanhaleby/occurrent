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

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.UnsupportedStartAtException;

import java.util.Optional;

/**
 * A subscription model that can refuse a subscription's start position after {@code subscribe} has returned, on a
 * thread of its own, and tells a listener when it does.
 * <p>
 * The MongoDB subscription models work out a {@link org.occurrent.subscription.StartAt#dynamic(java.util.function.Function)}
 * start position when they open the change stream, and refuse a catch-up's checkpoint they have no change stream
 * position for. They refuse a start at the present too when MongoDB gives them no position for it, and remove the
 * subscription. By then {@code subscribe} has returned, so the caller only learns about it from
 * {@link Subscription#waitUntilStarted(java.time.Duration)}. A model that passes its subscriptions to such a model and
 * keeps state for them, such as a lease, needs to know so it can let go of that state. A restart from the present
 * after a loss of history, and a resume at the present, that gets no position isn't refused. The model holds the
 * subscription paused and keeps its id instead, and tells the listeners through
 * {@link StartRefusalListener#handleStartHeldPaused(String, UnsupportedStartAtException)}.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface StartRefusalReportingSubscriptions extends SubscriptionModelCapability {

    /**
     * Adds a listener that is told about each start position this model refuses after {@code subscribe} has returned.
     *
     * @param listener The listener to add.
     */
    void addStartRefusalListener(StartRefusalListener listener);

    /**
     * Removes a listener added with {@link #addStartRefusalListener(StartRefusalListener)}, so the model no longer
     * tells it anything or keeps a reference to it. Removing a listener that was never added does nothing.
     *
     * @param listener The listener to remove, the same instance that was added.
     */
    void removeStartRefusalListener(StartRefusalListener listener);

    /**
     * Finds the {@link StartRefusalReportingSubscriptions} capability behind {@code subscriptionModel}, unwrapping a
     * {@link SubscriptionModelWrapper} chain until one is found.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if nothing in the chain implements it.
     */
    static Optional<StartRefusalReportingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(StartRefusalReportingSubscriptions.class);
    }

    /**
     * Told about a start position the model refused after {@code subscribe} returned.
     */
    @FunctionalInterface
    interface StartRefusalListener {
        /**
         * Called once the model has refused the start position of {@code subscriptionId}. The model removed the
         * subscription and freed its id before calling, so a {@code subscribe} with the same id and a start position the
         * model accepts makes a new subscription, and one may already have done so by the time the listener runs. The
         * model doesn't try the refused start again, and a handle of the refused subscription that waits for it to start
         * throws the refusal.
         * <p>
         * Called on the thread that would have read the subscription, without any lock of the model. The model calls
         * each listener in the order they were added. A listener that throws a {@link RuntimeException} is logged, and
         * the model goes on to the next listener. What the model did about the refusal stays as it is either way.
         *
         * @param subscriptionId The subscription whose start position was refused.
         * @param refusal        What the model refused the start position with.
         */
        void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal);

        /**
         * Called once the model has held {@code subscriptionId} paused because it couldn't open at the present, after
         * a loss of history or at a resume. The model keeps the subscription and its id, a
         * {@link SubscriptionModelLifeCycle#resumeSubscription(String)} asks for the present again, and a handle that
         * still waits for the held run to start throws {@code refusal}. Nothing is freed, so a listener that keeps
         * state for the subscription keeps it. A resume or a cancel of the subscription may already have run by the
         * time the listener runs. Does nothing unless overridden.
         * <p>
         * Called as {@link #handleStartRefusal(String, UnsupportedStartAtException)} is, on the thread that would have
         * read the subscription, without any lock of the model, and a listener that throws is logged.
         *
         * @param subscriptionId The subscription the model holds paused.
         * @param refusal        What a handle that waits for the held run to start throws.
         */
        default void handleStartHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
        }
    }
}
