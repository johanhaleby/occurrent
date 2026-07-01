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
 * Represents a unique subscription to a subscription. Subscriptions are typically started in a background thread
 * and you may wish to wait ({@link #waitUntilStarted()}) for them to start before continuing.
 * <p>
 * Unlike the blocking {@code Subscription}, {@link #waitUntilStarted()} returns a {@link Mono} rather than blocking
 * the calling thread. "Started" means the underlying change stream has been subscribed to, not that the server has
 * acknowledged the command and the cursor is positioned. This is weaker than the blocking and native subscription
 * models, whose equivalent signal only fires after that blocking round trip has already completed.
 */
@NullMarked
public interface Subscription {

    /**
     * @return The id of the subscription
     */
    String id();

    /**
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
