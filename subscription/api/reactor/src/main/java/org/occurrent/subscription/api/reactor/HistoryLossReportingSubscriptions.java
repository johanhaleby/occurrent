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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import reactor.core.publisher.Flux;

import java.util.Optional;

/**
 * A subscription model that asks a listener what to do with a subscription whose history was lost, meaning that the
 * position the subscription opens at is no longer in the history the model reads from, such as a MongoDB change
 * stream position the oplog no longer reaches back to. The listener can take the subscription over, so it is paused
 * instead of restarted from the present or ended.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface HistoryLossReportingSubscriptions extends SubscriptionModelCapability {

    /**
     * Adds a listener that is asked what the model does with a subscription with an id whose history was lost.
     *
     * @param listener The listener to add.
     */
    void addHistoryLossListener(HistoryLossListener listener);

    /**
     * Removes a listener added with {@link #addHistoryLossListener(HistoryLossListener)}, so the model no longer asks
     * it anything or keeps a reference to it. Removing a listener that was never added does nothing.
     *
     * @param listener The listener to remove, the same instance that was added.
     */
    void removeHistoryLossListener(HistoryLossListener listener);

    /**
     * Does what {@link Subscribable#subscribe(SubscriptionFilter, StartAt)} does, except when the history no longer
     * reaches back to {@code startAt} before the {@code Flux} has read anything. The {@code Flux} then fails with
     * {@link HistoryLostException}, whose {@link HistoryLostException#lostFrom()} is the position {@code startAt}
     * holds, instead of starting from the present or failing with what the model got. A history lost after the
     * {@code Flux} has read past {@code startAt} is handled as {@code subscribe} handles it.
     *
     * @param filter  The filter to use, or {@code null} for no filter.
     * @param startAt The position to start from.
     * @return The events from {@code startAt} on.
     */
    Flux<CloudEvent> subscribeFailingIfStartLost(@Nullable SubscriptionFilter filter, StartAt startAt);

    /**
     * Subscribes from {@code startAt}, and fails the returned {@code Flux} with {@link HistoryLostException} whenever
     * this model loses the history it reads from, as it opens at {@code startAt} or after it has delivered events. It
     * never restarts the subscription from the current time, whatever this model is configured to do.
     * {@link #subscribeFailingIfStartLost} fails only on a loss of {@code startAt} itself.
     * {@link HistoryLostException#lostFrom()} is the position the subscription had read to when the history was lost.
     * While nothing has been read it is {@code startAt}'s position, or, for a start that names no position such as
     * {@link StartAt#now()}, the position the subscription opened at.
     *
     * @param filter  The filter to use, or {@code null} for no filter.
     * @param startAt The position to start from.
     * @return The events from {@code startAt} on.
     */
    Flux<CloudEvent> subscribeFailingOnHistoryLoss(@Nullable SubscriptionFilter filter, StartAt startAt);

    /**
     * Finds the {@link HistoryLossReportingSubscriptions} capability of {@code subscriptionModel}.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if the model doesn't have it.
     */
    static Optional<HistoryLossReportingSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(HistoryLossReportingSubscriptions.class);
    }

    /**
     * Asked what the model does with a subscription with an id whose history was lost.
     */
    @FunctionalInterface
    interface HistoryLossListener {
        /**
         * Asked what the model does with {@code subscriptionId} once its history is lost, before the model does
         * anything else about the loss, whether or not the model restarts subscriptions after lost history. The model
         * asks each listener in the order they were added, and the first that answers
         * {@link HistoryLossHandling#takeOver(Runnable)} takes the subscription over. The model then pauses it and
         * runs the task as that method describes, and doesn't restart it. When every listener answers
         * {@link HistoryLossHandling#leaveToSubscriptionModel()}, the model goes on as it would without them.
         * <p>
         * Called on a thread that reads subscriptions, without any lock of the model, so answer from what you already
         * hold in memory and don't block. When a listener throws, the model logs it and reads again from the
         * subscription's position after its backoff, and asks again if the history is still lost.
         *
         * @param subscriptionId The subscription that lost its history.
         * @param lostFrom       The position the subscription opened at, or the position it had read to when it had
         *                       read past that one.
         * @return What the model does with the subscription.
         */
        HistoryLossHandling handleHistoryLoss(String subscriptionId, Checkpoint lostFrom);
    }
}
