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

/**
 * Marker supertype for every reactive subscription model capability. {@link Subscribable}, {@link CancellableSubscriptions},
 * {@link Pushable}, {@link IntrospectableSubscriptions} and {@link ReplayAwareSubscriptions} all extend it, so a whole
 * {@link SubscriptionModel} is one transitively, without declaring it directly. Mirrors the blocking stack's
 * {@code org.occurrent.subscription.api.blocking.SubscriptionModelCapability}.
 * <p>
 * This stack has no {@code SubscriptionModelWrapper} to unwrap and no recursive {@code of(...)} lookup, so nothing here
 * declares a parameter of this type yet. Callers check the model they hold with {@code instanceof} directly, as
 * {@link IntrospectableSubscriptions} and {@link ReplayAwareSubscriptions} already document. The type exists for the
 * same reason the blocking one does even without a current caller. A whole {@link SubscriptionModel} is the
 * intersection of {@link Subscribable} and {@link SubscriptionModelLifeCycle}, not their union, so a future method that
 * needs to accept any partial or complete capability set on this stack has a real supertype to declare instead of
 * {@link Object}.
 */
public interface SubscriptionModelCapability {
}
