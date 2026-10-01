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
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * A subscription model that tells a listener the position a subscription has read to when a read returned no event
 * for it. That position is the subscription's quiet position. The subscription's action has completed for every
 * event before it, so the subscription can start again from there without missing an event.
 * <p>
 * A model that passes its subscriptions to such a model and stores their checkpoints needs to know. A subscription
 * whose filter matches no event for a long time otherwise keeps the checkpoint of the last event that did match, and
 * the history the wrapped model reads from can drop that position while the subscription is still up to date.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface QuietPositionReportingSubscriptions extends SubscriptionModelCapability {

    /**
     * Adds a listener that is asked before each read whether it wants the quiet position of the subscription.
     *
     * @param listener The listener to add.
     */
    void addQuietPositionListener(QuietPositionListener listener);

    /**
     * Removes a listener added with {@link #addQuietPositionListener(QuietPositionListener)}, so the model no
     * longer asks it anything or keeps a reference to it. Removing a listener that was never added does nothing.
     *
     * @param listener The listener to remove, the same instance that was added.
     */
    void removeQuietPositionListener(QuietPositionListener listener);

    /**
     * Finds the {@link QuietPositionReportingSubscriptions} capability behind {@code subscriptionModel}, unwrapping
     * a {@link SubscriptionModelWrapper} chain until one is found.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if nothing in the chain implements it.
     */
    static Optional<QuietPositionReportingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(QuietPositionReportingSubscriptions.class);
    }

    /**
     * Asked before each read whether it wants the quiet position of a subscription.
     */
    @FunctionalInterface
    interface QuietPositionListener {
        /**
         * Called on the thread that delivers the subscription's events, before the model reads more of them. The
         * model calls the returned consumer with the quiet position when that read returned no event for the
         * subscription, after it read them, and drops the consumer when the read returned one.
         * <p>
         * An exception from this method or from the consumer is handled like an exception from the subscription's
         * action that its retries didn't get past, so a
         * {@link org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException} ends delivery on this
         * node.
         *
         * @param subscriptionId The subscription about to be read.
         * @return A consumer for the quiet position, or {@code null} when the listener doesn't want it this time.
         */
        @Nullable Consumer<Checkpoint> beforeReading(String subscriptionId);
    }
}
