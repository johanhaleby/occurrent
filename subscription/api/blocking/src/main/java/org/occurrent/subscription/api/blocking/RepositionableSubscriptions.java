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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

import java.util.Optional;

/**
 * A subscription model that can resume a paused subscription at an explicit {@link StartAt}, rather than only at
 * the position it had read to. This is what lets a caller holding a better position, for example a checkpoint
 * another node advanced while this one was paused, hand it over on resume instead of continuing from a stale one.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface RepositionableSubscriptions extends SubscriptionModelCapability {

    /**
     * Resume a paused subscription at {@code startAt}, instead of the position {@link SubscriptionModelLifeCycle#resumeSubscription(String)}
     * would have continued from.
     *
     * @param subscriptionId The id of the subscription to resume.
     * @param startAt        The position to resume from.
     * @return The resumed subscription.
     * @throws UnknownSubscriptionException        If this subscription model has no subscription with that id.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running.
     */
    Subscription resumeSubscription(String subscriptionId, StartAt startAt);

    /**
     * Whether a subscription this model subscribes at {@code checkpoint}, as it does when a stored checkpoint is read
     * back after a restart, starts reading there. A model that stores positions on top of this one, such as a durable
     * subscription model, stores the position of a resume at a position only when this answers {@code true}. A stored
     * position that this model opens somewhere else, for example at the present, would make a restart skip the events
     * written in between.
     * <p>
     * The default answers {@code false}, so nothing is stored for a model that doesn't override this. A restart then
     * resumes from the checkpoint stored before the resume, which can deliver events again but skips none. Override
     * it to answer {@code true} for every checkpoint this model starts reading at.
     *
     * @param checkpoint The position a subscription would be subscribed at.
     * @return {@code true} if a subscription subscribed at {@code checkpoint} starts reading there, {@code false} by default.
     */
    default boolean restartsAt(Checkpoint checkpoint) {
        return false;
    }

    /**
     * {@code subscriptionModel} when it can be resumed at an explicit position. A {@link SubscriptionModelWrapper} that
     * doesn't implement this is not looked past, because a resume at a position through the model beneath it would go
     * around the state the wrapper keeps for the subscription. An empty result means the model cannot be resumed at an
     * explicit position.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these. Typed as {@link SubscriptionModelCapability}
     *                          because callers hold different subsets of a subscription model's capabilities, and no
     *                          existing type names their union.
     * @return {@code subscriptionModel} as a repositionable model, or empty if it doesn't implement this.
     */
    static Optional<RepositionableSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(RepositionableSubscriptions.class);
    }
}
