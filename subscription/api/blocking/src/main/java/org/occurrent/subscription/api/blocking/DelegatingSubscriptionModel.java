/*
 * Copyright 2020 Johan Haleby
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

/**
 * A delegating subscription model is a subscription model that wraps another subscription model
 * and delegates to it when {@code subscribe} methods are called. Sometimes it's useful to get the underlying
 * subscription model (mainly for testing purposes), since it may support more features than the {@code DelegatingSubscriptionModel} instance,
 * such as implementing {@link SubscriptionModelLifeCycle}.
 */
public interface DelegatingSubscriptionModel {

    /**
     * @return The wrapped {@link SubscriptionModel} that this {@code SubscriptionModel} delegates to.
     */
    SubscriptionModel getDelegatedSubscriptionModel();

    /**
     * Get the first {@link SubscriptionModel} that is not wrapped. For example, if
     * this {@code SubscriptionModel} wraps another {@code SubscriptionModel} (S) that
     * is also a {@code DelegatingSubscriptionModel}, then this method will return the
     * {@code SubscriptionModel} that (S) is wrapping.
     *
     * @return The first {@link SubscriptionModel} that is not wrapped.
     */
    default SubscriptionModel getDelegatedSubscriptionModelRecursively() {
        SubscriptionModel delegatedSubscriptionModel = getDelegatedSubscriptionModel();
        if (delegatedSubscriptionModel instanceof DelegatingSubscriptionModel) {
            return ((DelegatingSubscriptionModel) delegatedSubscriptionModel).getDelegatedSubscriptionModelRecursively();
        }
        return delegatedSubscriptionModel;
    }
}
