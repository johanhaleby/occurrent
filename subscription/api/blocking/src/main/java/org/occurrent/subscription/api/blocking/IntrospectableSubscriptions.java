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
import java.util.Set;

/**
 * A subscription model that can list the subscriptions it knows about, so a caller can name one that does not exist
 * rather than only repeating the id it was given.
 * <p>
 * Not every subscription model implements this, so reach it with {@link #findIn(SubscriptionModelCapability)}.
 */
@NullMarked
public interface IntrospectableSubscriptions extends SubscriptionModelCapability {

    /**
     * @return Every subscription id this model knows, whether running or paused.
     */
    Set<String> subscriptionIds();

    /**
     * The introspectable model behind {@code subscriptionModel}, unwrapping a {@link SubscriptionModelWrapper} until
     * one is found. An empty result means the model cannot list its subscriptions, which is not the same as having
     * none.
     *
     * @param subscriptionModel A {@link Subscribable}, a {@link SubscriptionModelLifeCycle}, a whole {@link SubscriptionModel},
     *                          or a {@link SubscriptionModelWrapper} around one of these. Typed as {@link SubscriptionModelCapability}
     *                          because callers hold different subsets of a subscription model's capabilities, and no
     *                          existing type names their union.
     * @return The introspectable model, or empty if nothing in the chain implements this.
     */
    static Optional<IntrospectableSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
        return subscriptionModel.capability(IntrospectableSubscriptions.class);
    }
}
