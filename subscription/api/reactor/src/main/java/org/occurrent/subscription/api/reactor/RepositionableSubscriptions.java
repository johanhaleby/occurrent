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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;

import java.util.Optional;

/**
 * A subscription model that can resume a paused subscription at a given {@link StartAt}, instead of only at the
 * position it had read to.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface RepositionableSubscriptions extends SubscriptionModelCapability {

    /**
     * Resumes a paused subscription at {@code startAt}, instead of the position
     * {@link SubscriptionModelLifeCycle#resumeSubscription(String)} would have resumed it from.
     *
     * @param subscriptionId The id of the subscription to resume.
     * @param startAt        The position to resume from.
     * @return The resumed subscription.
     * @throws UnknownSubscriptionException        If this subscription model has no subscription with that id.
     * @throws SubscriptionAlreadyRunningException If the subscription is already running.
     */
    Subscription resumeSubscription(String subscriptionId, StartAt startAt);

    /**
     * Finds the {@link RepositionableSubscriptions} capability of {@code subscriptionModel}.
     *
     * @param subscriptionModel The subscription model to look in.
     * @return The capability, or empty if the model doesn't have it.
     */
    static Optional<RepositionableSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(RepositionableSubscriptions.class);
    }
}
