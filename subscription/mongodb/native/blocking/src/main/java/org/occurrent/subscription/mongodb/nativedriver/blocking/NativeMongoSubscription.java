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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

@NullMarked
public record NativeMongoSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch, Supplier<@Nullable RuntimeException> startFailure) implements Subscription {

    public NativeMongoSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch) {
        this(subscriptionId, subscriptionStartedLatch, () -> null);
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    /**
     * Note that this only signals that the change stream cursor has been acquired, not that MongoDB has confirmed
     * the cursor is healthy; a failure right after this returns is still possible and will trigger the subscription
     * model's own restart-on-error handling.
     * <p>
     * Throws the {@link org.occurrent.subscription.UnsupportedStartAtException} when the model refused the start
     * position, since the change stream then never opens.
     * <p>
     * It answers {@code true} also for a resumed subscription that waits for an action call from before its pause to
     * return, so a caller isn't held back by a call that doesn't return. When the change stream of that subscription
     * lost its history as it opened, the subscription can still stop after this answered {@code true}, or be held
     * paused when its restart from the present gets no position.
     * <a href="https://github.com/johanhaleby/occurrent/issues/1502">#1502</a> tracks a subscription that stops after
     * this answered {@code true}.
     */
    @Override
    public boolean waitUntilStarted(Duration timeout) {
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        try {
            boolean released = subscriptionStartedLatch.await(safeTimeout.timeout(), safeTimeout.timeUnit());
            RuntimeException failure = startFailure.get();
            if (failure != null) {
                throw failure;
            }
            return released;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}