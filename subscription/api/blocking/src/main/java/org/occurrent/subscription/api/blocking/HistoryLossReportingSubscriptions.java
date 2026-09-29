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
     *
     * @param listener The listener to add.
     */
    void addHistoryLossListener(HistoryLossListener listener);

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
         *
         * @param subscriptionId The subscription that lost its history.
         * @param restartedFrom  The position the subscription restarts from.
         */
        void restartingAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom);
    }
}
