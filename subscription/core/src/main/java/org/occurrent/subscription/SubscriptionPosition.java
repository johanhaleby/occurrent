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

package org.occurrent.subscription;

import org.jspecify.annotations.NonNull;

/**
 * Represents the position for a subscription. Note that this is not the same as a CloudEvent position in an event stream.
 * Rather the subscription position is the position for a particular subscriber that streams events from a subscription.
 * <p>
 * There can be multiple implementations of a {@code SubscriptionPosition} for a database but each implementation
 * must be able to return the subscription position as a {@link String}. Implementations can provide optimized data structures
 * for a particular database. For example if you're streaming changes from a MongoDB event store <i>and</i> persists the position in
 * MongoDB one could return the MongoDB "Document" after having casted the {@code SubscriptionPosition} to a MongoDB specific implementation.
 * Thus there's no need to serialize the "Document" representing the subscription position to a String and back to a Document again.
 * </p>
 */
public interface SubscriptionPosition {
    @NonNull
    String asString();
}