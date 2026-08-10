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

import java.util.Set;

/**
 * A reactive subscription model that can list the subscriptions it knows about, so a caller can name one that does not
 * exist rather than only repeating the id it was given.
 * <p>
 * Not every reactive subscription model implements this, so check the model you hold with {@code instanceof}. The
 * subscription DSL wrappers do not forward it, so ask the subscription model itself. See ADR 89.
 */
@NullMarked
public interface IntrospectableSubscriptions extends SubscriptionModelCapability {

    /**
     * @return Every subscription id this model knows, whether running or paused.
     */
    Set<String> subscriptionIds();
}
