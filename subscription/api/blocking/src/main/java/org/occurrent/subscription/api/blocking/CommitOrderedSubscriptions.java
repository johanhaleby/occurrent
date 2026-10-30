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
 * A subscription model that says whether it delivers the events of every subscription in the order they committed.
 * <p>
 * That order matters to a caller that builds up state from the events one by one, such as a saga, where an event applied before an
 * earlier one gives a different result.
 * <p>
 * A model that answers {@code true} delivers no event to a subscription before every earlier event matching it has been
 * delivered and its listener call returned normally. An event is earlier when it committed earlier, or when it was
 * written earlier in the same commit, so the events of one commit are delivered in the order they were written. Earlier
 * events count from the position the subscription was started or resumed at, so a subscription started at the present
 * waits for nothing written before it. A model that can move a subscription past events it never delivered, without
 * being asked to, answers {@code false}. A delivery whose listener throws is retried in place before anything after it
 * is delivered.
 * <p>
 * A caller that finds no implementation, which is an empty result from {@link #findIn(SubscriptionModelCapability)},
 * treats the model as not delivering in commit order.
 *
 * @see #deliversEveryEventInCommitOrder()
 */
@NullMarked
public interface CommitOrderedSubscriptions extends SubscriptionModelCapability {

    /**
     * Whether this model delivers every one of its subscriptions in commit order. The answer is the same for the whole
     * life of the model instance, so a caller can read it once.
     * <p>
     * Declared by the model rather than worked out by the caller, because a caller can't conclude it from the
     * deliveries it happened to see.
     *
     * @return {@code true} when this model delivers every one of its subscriptions in commit order.
     */
    boolean deliversEveryEventInCommitOrder();

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
