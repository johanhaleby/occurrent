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

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.StartAt;

/**
 * A subscription model wrapper wraps another subscription model and delegates to it when {@code subscribe}
 * methods are called. Sometimes it's useful to get the underlying
 * subscription model (mainly for testing purposes), since it may support more features than the {@code SubscriptionModelWrapper} instance,
 * such as implementing {@link SubscriptionModelLifeCycle}.
 */
@NullMarked
public interface SubscriptionModelWrapper extends SubscriptionModelCapability {

    /**
     * @return The wrapped {@link SubscriptionModel} that this {@code SubscriptionModel} delegates to.
     */
    SubscriptionModel getWrappedSubscriptionModel();

    /**
     * Get the first {@link SubscriptionModel} that is not wrapped. For example, if
     * this {@code SubscriptionModel} wraps another {@code SubscriptionModel} (S) that
     * is also a {@code SubscriptionModelWrapper}, then this method will return the
     * {@code SubscriptionModel} that (S) is wrapping.
     *
     * @return The first {@link SubscriptionModel} that is not wrapped.
     */
    default SubscriptionModel getWrappedSubscriptionModelRecursively() {
        SubscriptionModel wrappedSubscriptionModel = getWrappedSubscriptionModel();
        if (wrappedSubscriptionModel instanceof SubscriptionModelWrapper) {
            return ((SubscriptionModelWrapper) wrappedSubscriptionModel).getWrappedSubscriptionModelRecursively();
        }
        return wrappedSubscriptionModel;
    }

    /**
     * Whether the wrapped {@link SubscriptionModel} receives the {@link StartAt} the caller passed, rather than one
     * resolved here. A wrapper answering {@code true} may still resolve the position for a decision of its own, such
     * as a competing consumer model working out whether to compete for the subscription, as long as it hands the
     * caller's own object down and lets the model below resolve it again.
     * <p>
     * {@link ManualStartSubscriptionModel} asks this when it works out where a registration will start from. It goes
     * down the wrapped models asking each of them what the caller's position resolves to, and passes over a wrapper
     * that answers {@code true} here, since nothing such a wrapper answers changes where the subscription starts.
     *
     * @return {@code true} if the caller's own {@code StartAt} reaches the wrapped model. {@code false}, the default,
     * for a wrapper that resolves the position and passes down what came out of that.
     */
    default boolean forwardsStartAtUnresolved() {
        return false;
    }
}
