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
import reactor.core.publisher.Mono;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Represents a subscription instance started by a subscription model's {@code subscribe(...)} call. It's typically
 * started in a background thread and you may wish to wait ({@link #waitUntilStarted()}) for it to start before
 * continuing.
 * <p>
 * Unlike the blocking {@code SubscriptionHandle}, {@link #waitUntilStarted()} returns a {@link Mono} rather than blocking
 * the calling thread. "Started" means the underlying change stream has been subscribed to, not that the server has
 * acknowledged the command and the cursor is positioned. This is weaker than the blocking and native subscription
 * models, whose equivalent signal only fires after that blocking round trip has already completed.
 */
@NullMarked
public interface SubscriptionHandle {

    /**
     * @return The id of the subscription
     */
    String id();

    /**
     * This handle answers for the one start it was created for, and it completes once nothing further is required of
     * you for the subscription to deliver. That is not a claim about the present moment. A subscription that has
     * started can afterwards be paused or stopped, and this stays completed. Ask the subscription model's
     * {@code isRunning(id)} and {@code isPaused(id)} about the present.
     * <p>
     * A subscription you still have to start yourself has not started, so this does not complete. That covers one
     * registered while its model was stopped, one whose replay was parked because the model was not running, and a
     * replay that {@code stop()} interrupted. Unlike the blocking stack, a parked replay that a later
     * {@code start(..)} relaunches does complete here, because the relaunch runs through this same signal.
     * <p>
     * A start that failed and will not be retried errors the {@link Mono} rather than leaving it incomplete.
     *
     * @return A {@link Mono} that completes once the subscription has started.
     */
    Mono<Void> waitUntilStarted();

    /**
     * @param timeout must not be <code>null</code>
     * @return A {@link Mono} that emits {@code true} if the subscription started within the given duration, {@code false} otherwise.
     */
    default Mono<Boolean> waitUntilStarted(Duration timeout) {
        requireNonNull(timeout, "timeout cannot be null");
        return waitUntilStarted().thenReturn(true).timeout(timeout, Mono.just(false));
    }
}
