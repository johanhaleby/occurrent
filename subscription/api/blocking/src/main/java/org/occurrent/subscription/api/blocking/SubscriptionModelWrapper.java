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
     * Whether what the caller's {@link StartAt} resolves to under this wrapper's own class is what decides where the
     * subscription starts. Answer {@code false} only when that resolution decides something else, the way a competing
     * consumer model resolves the position to work out whether to compete for the subscription and leaves where the
     * subscription starts to the model below it.
     * <p>
     * Handing the caller's own {@code StartAt} object down is not what this asks about. A catch-up model does that
     * and still answers {@code true}, because the model it hands the object to resolves it under the catch-up model's
     * class rather than its own, so the answer given for this class is the one acted on. So does a durable model that
     * resolves the position for itself and passes the caller's object on only when its own answer was nothing.
     * <p>
     * {@link ManualStartSubscriptionModel} asks this when it works out where a registration will start from. It goes
     * down the wrapped models asking each of them what the caller's position resolves to, and passes over one that
     * answers {@code false} here, since nothing that model answers changes where the subscription starts.
     *
     * @return {@code true}, the default, for a wrapper whose own resolution settles where the subscription starts.
     * {@code false} for one that resolves the position for a decision of its own and leaves the start to the model
     * below it.
     */
    default boolean decidesWhereTheSubscriptionStarts() {
        return true;
    }
}
