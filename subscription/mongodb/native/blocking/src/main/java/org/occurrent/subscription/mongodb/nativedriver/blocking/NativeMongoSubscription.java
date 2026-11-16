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
     * Answers {@code true} once the change stream of the subscription has opened. Through a mongos, a subscription
     * that starts at a stored position, a checkpoint, an operation time or a resume token, counts as opened only once
     * its change stream's first reply came, which can take up to the model's {@code maxAwaitTime}, or the server's
     * default when none is set. So does a subscription at the present whose position {@code subscribe(..)} read before
     * the change stream began to open, as one made while the model was stopped and started later, or one resumed
     * after a pause before its first reply, since that position can have left the oplog by then.
     * <p>
     * An open change stream doesn't mean that MongoDB has confirmed it is healthy. After a failure the model restarts
     * the change stream, apart from a loss of its history with {@code restartSubscriptionsOnChangeStreamHistoryLost}
     * off, which refuses the subscription as below.
     * <p>
     * Throws the {@link org.occurrent.subscription.UnsupportedStartAtException} when the model refused the
     * subscription, at its start position or after a loss of its change stream's history. A refusal that comes after
     * this answered {@code true} is thrown from the next call on, since the model no longer runs the subscription. A
     * subscription held paused, because its restart from the present after a loss of history got no position, throws
     * its refusal the same way. The handle {@code subscribe(..)} returned stops throwing it once a resume starts the
     * subscription again.
     * <p>
     * It answers {@code true} also for a resumed subscription that waits for an action call from before its pause to
     * return, so a caller isn't held back by a call that doesn't return. A refusal of that subscription is thrown from
     * the next call on, as for any other.
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