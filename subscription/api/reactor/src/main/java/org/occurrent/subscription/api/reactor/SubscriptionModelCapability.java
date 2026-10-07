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

import java.util.Optional;

/**
 * Marker supertype for every reactive subscription model capability. {@link Subscribable}, {@link CancellableSubscriptions},
 * {@link Pushable}, {@link IntrospectableSubscriptions} and {@link ReplayAwareSubscriptions} all extend it, so a whole
 * {@link SubscriptionModel} is one transitively, without declaring it directly. Mirrors the blocking stack's
 * {@code org.occurrent.subscription.api.blocking.SubscriptionModelCapability}.
 * <p>
 * This stack has no {@code SubscriptionModelWrapper} to unwrap and no recursive {@code of(...)} lookup. Callers check
 * the model they hold with {@code instanceof} directly, as {@link IntrospectableSubscriptions} and
 * {@link ReplayAwareSubscriptions} already document. {@link QuietPositionReportingSubscriptions} is the one exception.
 * {@code ReactorCatchupSubscriptionModel} and {@code ReactorStreamCatchupSubscriptionModel} answer
 * {@link #capability(Class)} for it with the capability of the model they wrap, so callers find it with
 * {@link QuietPositionReportingSubscriptions#findIn(SubscriptionModelCapability)}. A whole {@link SubscriptionModel} is
 * the intersection of {@link Subscribable} and {@link SubscriptionModelLifeCycle}, not their union, so a method like
 * {@code findIn} that accepts any partial or complete capability set on this stack has a supertype to declare
 * instead of {@link Object}.
 */
public interface SubscriptionModelCapability {

    /**
     * The capability of type {@code type} behind this object. By default the check is a direct {@code instanceof}
     * against this object. {@code ReactorCatchupSubscriptionModel} and {@code ReactorStreamCatchupSubscriptionModel}
     * answer {@link QuietPositionReportingSubscriptions} with the capability of the model they wrap instead, and no
     * other capability.
     *
     * @param type The capability to look for.
     * @param <T>  The capability type.
     * @return The capability, or empty if this object doesn't have it.
     */
    default <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) {
        return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
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
}
