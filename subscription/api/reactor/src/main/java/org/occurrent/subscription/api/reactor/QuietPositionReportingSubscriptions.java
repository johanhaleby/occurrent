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
import org.occurrent.subscription.Checkpoint;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

/**
 * A subscription model that tells a listener the position a subscription has read to when a read returned no event
 * for it. That position is the subscription's quiet position. The {@code Mono} of the subscription's action has
 * completed for every event before it, so the subscription can start again from there without missing an event.
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
     * Finds the {@link QuietPositionReportingSubscriptions} capability of {@code subscriptionModel}. A
     * {@code ReactorCatchupSubscriptionModel} or {@code ReactorStreamCatchupSubscriptionModel} has it when the model it
     * wraps has it, and this then returns the capability of the wrapped model. A model of your own that wraps another
     * one has it only when it overrides {@link SubscriptionModelCapability#capability(Class)} the same way.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if the model doesn't have it.
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
         * Called before the model waits for more events for the subscription, and the model waits for the returned
         * {@code Mono} before that. When the wait ends with no event for the subscription and the model has found a
         * new quiet position meanwhile, the model calls the function the {@code Mono} completed with, passing the
         * quiet position, and hands the subscription nothing more until the {@code Mono} that function returns has
         * completed. Otherwise the model drops the function.
         * <p>
         * The model calls the action, or such a function, for one subscription at a time. After a pause, the resumed
         * subscription reads nothing until every {@code Mono} the model subscribed to for it before the pause has
         * completed, failed or been cancelled, and a pause cancels the ones still running.
         * <p>
         * An error from either {@code Mono} is handled like an error reading the subscription's events. The model
         * reads again from the subscription's position after its backoff, and keeps doing so while the error comes
         * back, as it keeps calling an action that fails. A
         * {@link org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException} is handled the same way.
         *
         * @param subscriptionId The subscription about to be read.
         * @return A {@code Mono} with a function for the quiet position, or an empty {@code Mono} when the listener
         * doesn't want it this time.
         */
        Mono<Function<Checkpoint, Mono<Void>>> beforeReading(String subscriptionId);
    }
}
