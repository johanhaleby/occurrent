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
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

@NullMarked
public record NativeMongoSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch) implements Subscription {

    @Override
    public String id() {
        return subscriptionId;
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        try {
            return subscriptionStartedLatch.await(safeTimeout.timeout(), safeTimeout.timeUnit());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}