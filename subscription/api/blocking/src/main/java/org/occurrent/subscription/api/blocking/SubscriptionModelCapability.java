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

import java.util.Optional;

/**
 * Marker supertype for every blocking subscription model capability. {@link Subscribable}, {@link CancellableSubscriptions},
 * {@link Pushable}, {@link RepositionableSubscriptions}, {@link ReplayAwareSubscriptions}, {@link IntrospectableSubscriptions},
 * {@link HistoryRetainingSubscriptions}, {@link HistoryLossReportingSubscriptions} and {@link SubscriptionModelWrapper} all extend it, so a whole {@link SubscriptionModel} is one transitively, without
 * declaring it directly.
 * <p>
 * It exists so a method that accepts "whatever partial or complete subscription model a caller happens to hold" has a
 * real type to declare, rather than {@link Object}. A caller may hold a bare {@link Subscribable}, a
 * {@link SubscriptionModelLifeCycle}, a full {@link SubscriptionModel}, or a {@link SubscriptionModelWrapper} around one
 * of these, and {@link SubscriptionModel} itself is the intersection of {@link Subscribable} and
 * {@link SubscriptionModelLifeCycle}, not their union. Java has no syntax for that union, and overloading on it is not
 * possible either, since a {@link SubscriptionModel} argument would satisfy every overload at once. Declaring the
 * parameter as this common supertype is the only typed way to accept all of them.
 *
 * @see RepositionableSubscriptions#findIn(SubscriptionModelCapability)
 * @see ReplayAwareSubscriptions#findIn(SubscriptionModelCapability)
 * @see IntrospectableSubscriptions#findIn(SubscriptionModelCapability)
 * @see HistoryRetainingSubscriptions#findIn(SubscriptionModelCapability)
 * @see HistoryLossReportingSubscriptions#findIn(SubscriptionModelCapability)
 */
public interface SubscriptionModelCapability {

    /**
     * The capability of type {@code type} behind this object. This is the instance-side counterpart to a facet's own
     * static {@code findIn} method, for a caller that has a {@link Class} in hand rather than a single facet named at the
     * call site.
     * <p>
     * This object answers when it implements {@code type}. Past that, a {@link SubscriptionModelWrapper} is unwrapped
     * only for {@link IntrospectableSubscriptions}, {@link ReplayAwareSubscriptions}, {@link HistoryRetainingSubscriptions},
     * {@link QuietPositionReportingSubscriptions} and {@link Pushable}, which observe subscriptions or take events in at
     * the bottom of the stack, and for {@link HistoryLossReportingSubscriptions}, which a catch-up model reaches under a
     * durable model. Every other capability acts on a subscription, such as {@link Subscribable},
     * {@link CancellableSubscriptions} and {@link RepositionableSubscriptions}, and is answered only by a model that
     * implements it, since a wrapper that keeps state for a subscription would otherwise never hear about the action
     * taken on the model beneath it. A wrapper is found for such a capability by implementing it.
     *
     * @param type The capability to look for.
     * @param <T>  The capability type.
     * @return The capability, or empty if this object doesn't implement {@code type} and, for a capability a wrapper is
     * unwrapped for, nothing in the wrapper chain does either.
     */
    default <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
        if (type.isInstance(this)) {
            return Optional.of(type.cast(this));
        } else if (this instanceof SubscriptionModelWrapper wrapper && isUnwrappedFor(type)) {
            return wrapper.getWrappedSubscriptionModel().capability(type);
        }
        return Optional.empty();
    }

    /**
     * Whether the capability of type {@code type} exists behind this object, without returning it.
     *
     * @param type The capability to look for.
     * @return {@code true} if {@link #capability(Class)} would return a non-empty result for {@code type}.
     */
    default boolean hasCapability(Class<? extends SubscriptionModelCapability> type) {
        return capability(type).isPresent();
    }

    // Each of these only observes subscriptions or takes events in, except HistoryLossReportingSubscriptions, whose
    // listener can take a subscription over, but a catch-up model has to reach it under a durable model (ADR 151)
    private static boolean isUnwrappedFor(Class<?> type) {
        return type == IntrospectableSubscriptions.class || type == ReplayAwareSubscriptions.class
                || type == HistoryRetainingSubscriptions.class || type == QuietPositionReportingSubscriptions.class
                || type == Pushable.class || type == HistoryLossReportingSubscriptions.class;
    }
}
