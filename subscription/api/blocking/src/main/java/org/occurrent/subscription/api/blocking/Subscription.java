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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Represents a unique subscription to a subscription. Subscriptions are typically started in a background thread
 * and you may wish to wait ({@link #waitUntilStarted(Duration)} for them to start before continuing.
 */
@NullMarked
public interface Subscription {

    /**
     * @return The id of the subscription
     */
    String id();

    /**
     * Synchronous, <strong>blocking</strong> call returns once the {@link Subscription} has started.
     */
    default void waitUntilStarted() {
        waitUntilStarted(ChronoUnit.FOREVER.getDuration());
    }

    /**
     * Synchronous, <strong>blocking</strong> call returns once the {@link Subscription} has started or
     * {@link Duration timeout} exceeds.
     *
     * @param timeout must not be <code>null</code>
     * @return <code>true</code> if the subscription was started within the given Duration, <code>false</code> otherwise.
     */
    boolean waitUntilStarted(Duration timeout);
}
