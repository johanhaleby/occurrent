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
import org.occurrent.subscription.Checkpoint;

import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * A subscription model that restarts a subscription from a new position when the position it had read to is no
 * longer in the history it reads from, and tells a listener which position that is before the restart.
 * <p>
 * A model that passes its subscriptions to such a model and stores their checkpoints needs to know. Until the
 * restarted subscription delivers an event, the stored checkpoint is still the position that was lost, so a process that stops
 * in between restarts from that lost position again. It then restarts from wherever the history has reached by that
 * time, and skips every event written in between even though those events are still there to be read.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface HistoryLossReportingSubscriptions extends SubscriptionModelCapability {

    /**
     * Adds a listener that is told the position a subscription restarts from after its history was lost, before the
     * subscription is restarted from it.
     * <p>
     * While a listener whose {@link HistoryLossListener#storesRestartPositionOf(String)} answers {@code true} for a
     * subscription is added, a model whose reply to {@code ping} has no operation time doesn't restart that
     * subscription after its history was lost, and tries again as its {@code RetryStrategy} says. Every other
     * subscription restarts from the present.
     *
     * @param listener The listener to add.
     */
    void addHistoryLossListener(HistoryLossListener listener);

    /**
     * Removes a listener added with {@link #addHistoryLossListener(HistoryLossListener)}, so the model no longer
     * tells it anything or keeps a reference to it. Removing a listener that was never added does nothing.
     *
     * @param listener The listener to remove, the same instance that was added.
     */
    void removeHistoryLossListener(HistoryLossListener listener);

    /**
     * Finds the {@link HistoryLossReportingSubscriptions} capability behind {@code subscriptionModel}, unwrapping a
     * {@link SubscriptionModelWrapper} chain until one is found.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if nothing in the chain implements it.
     */
    static Optional<HistoryLossReportingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(HistoryLossReportingSubscriptions.class);
    }

    /**
     * Told the position a subscription restarts from after its history was lost.
     */
    @FunctionalInterface
    interface HistoryLossListener {
        /**
         * Called before the subscription restarts from {@code restartedFrom}. A listener that throws makes the model
         * try the restart again later, as it does for any other failure to restart, and ask again for the position
         * to restart from.
         * <p>
         * The model asks MongoDB for {@code restartedFrom} before it calls this, and the subscription can be resumed,
         * or cancelled and subscribed again, while it asks. {@code stillCurrent} returns {@code false} once either has
         * happened, and a pause alone leaves it {@code true}. A listener that stores {@code restartedFrom} as the
         * subscription's checkpoint calls it under the same lock as its own resume and cancel of the subscription,
         * and stores nothing when it returns {@code false}. Otherwise a subscription restarted from that checkpoint
         * can skip the events the resumed or new subscription had not received yet.
         *
         * @param subscriptionId The subscription that lost its history.
         * @param restartedFrom  The position the subscription restarts from.
         * @param stillCurrent   Returns {@code false} once the subscription has been resumed or cancelled since it
         *                       lost its history.
         */
        void restartingAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent);

        /**
         * Whether this listener stores the position {@code subscriptionId} restarts from after its history was lost, as
         * the checkpoint the subscription resumes from. The model asks this when it can't find out where the present
         * is, so it has no position to pass to
         * {@link #restartingAfterHistoryLoss(String, Checkpoint, BooleanSupplier)}.
         * While a listener answers {@code true}, the model doesn't restart the subscription, since the lost position
         * would stay stored and a process that starts from it later would skip the events written in between. It tries
         * again as its retry strategy says. When every listener answers {@code false}, the model restarts the
         * subscription from the present. A listener that throws makes the model try the restart again later, as it
         * does for any other failure to restart.
         * <p>
         * Answers {@code true} unless overridden. Override it to answer {@code false} for a subscription whose position
         * this listener doesn't store, so a listener that only counts or logs lost history doesn't keep that
         * subscription from restarting.
         *
         * @param subscriptionId The subscription that lost its history.
         * @return {@code true} if this listener stores the position {@code subscriptionId} restarts from.
         */
        default boolean storesRestartPositionOf(String subscriptionId) {
            return true;
        }
    }
}
