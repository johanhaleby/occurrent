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

import java.util.Optional;

/**
 * A subscription model that can say whether it delivers the events of a subscription in the order they committed.
 * <p>
 * That order matters to a caller that builds up state from the events one by one, such as a saga, where an event applied before an
 * earlier one gives a different result. A model answers per subscription, and its answer can change over time.
 * <p>
 * A model answers {@code true} for a subscription only when no event is delivered to it before every event matching
 * it that committed earlier has been delivered and its listener call returned normally. A delivery whose listener
 * throws is retried in place before anything after it is delivered.
 * <p>
 * A caller that finds no implementation, which is an empty result from {@link #findIn(SubscriptionModelCapability)},
 * treats the model as not delivering in commit order.
 *
 * @see #deliversInCommitOrder(String)
 */
@NullMarked
public interface CommitOrderedSubscriptions extends SubscriptionModelCapability {

    /**
     * Whether the subscription {@code subscriptionId} is delivered in commit order. The answer covers the delivery in
     * progress and every later one, for as long as the model keeps answering {@code true}.
     * <p>
     * Ask it on the delivering thread, from inside the listener. Asked from any other thread, it may speak for a
     * delivery that is already over.
     *
     * @param subscriptionId The id of the subscription.
     * @return {@code true} when no event is delivered before every earlier committed event matching the subscription
     * has been delivered and its listener call returned normally, {@code false} otherwise, and for an id this model
     * does not know.
     */
    boolean deliversInCommitOrder(String subscriptionId);

    /**
     * Whether {@link #deliversInCommitOrder(String)} answers {@code true} for every subscription id at every moment.
     * <p>
     * Declared by the model rather than worked out by the caller, because a caller cannot conclude it from the
     * {@code true} answers it happened to get. The default is {@code false}, which is the right answer for a model that
     * says nothing.
     *
     * @return {@code true} when this model delivers every subscription in commit order, always.
     */
    default boolean deliversEveryEventInCommitOrder() {
        return false;
    }

    /**
     * The commit-ordered model behind {@code subscriptionModel}. A {@link SubscriptionModelWrapper} is not unwrapped
     * for this capability, so a wrapper is found only when it implements it itself. Commit order is a property of the
     * whole delivery path, and a wrapper can break it, for example with a catch-up handover, a thread pool or a
     * listener that parks deliveries.
     * <p>
     * An empty result means the model does not declare it, and a caller treats that as not in commit order.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these.
     * @return The model that declares commit order, or empty when this object doesn't implement it.
     */
    static Optional<CommitOrderedSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(CommitOrderedSubscriptions.class);
    }
}
