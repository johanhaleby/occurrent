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
     * <p>
     * This overload waits forever and throws away the answer, so a subscription that never started looks exactly like
     * one that did. Use {@link #waitUntilStarted(Duration)} when you need to know which happened.
     */
    default void waitUntilStarted() {
        waitUntilStarted(ChronoUnit.FOREVER.getDuration());
    }

    /**
     * Synchronous, <strong>blocking</strong> call returns once the {@link Subscription} has started or
     * {@link Duration timeout} exceeds.
     * <p>
     * This handle answers for the one start it was created for, and it reports started once nothing further is
     * required of you for the subscription to deliver. That is not a claim about the present moment. A subscription
     * that has started can afterwards be paused, be stopped, or be waiting for another node to release a competing
     * consumer lock, and this keeps answering {@code true}. Ask the subscription model's {@code isRunning(id)} and
     * {@code isPaused(id)} about the present.
     * <p>
     * A subscription you still have to start yourself has not started, so it answers {@code false}. That covers one
     * registered while its model was stopped, one withheld under {@code occurrent.subscription.mode=manual}, and a
     * catch-up replay that {@code stop()} interrupted. A start that failed and will not be retried throws rather than
     * answering.
     *
     * @param timeout must not be <code>null</code>
     * @return <code>true</code> if the subscription was started within the given Duration, <code>false</code> otherwise.
     */
    boolean waitUntilStarted(Duration timeout);
}
