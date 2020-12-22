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

import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NativeMongoSubscription implements Subscription {
    public final String subscriptionId;
    final CountDownLatch subscriptionStartedLatch;

    NativeMongoSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch) {
        this.subscriptionId = subscriptionId;
        this.subscriptionStartedLatch = subscriptionStartedLatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NativeMongoSubscription)) return false;
        NativeMongoSubscription that = (NativeMongoSubscription) o;
        return Objects.equals(subscriptionId, that.subscriptionId) &&
                Objects.equals(subscriptionStartedLatch, that.subscriptionStartedLatch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, subscriptionStartedLatch);
    }

    @Override
    public String toString() {
        return "NativeMongoDBSubscription{" +
                "subscriptionId='" + subscriptionId + '\'' +
                ", subscriptionStartedLatch=" + subscriptionStartedLatch +
                '}';
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    @Override
    public void waitUntilStarted() {
        try {
            subscriptionStartedLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        try {
            return subscriptionStartedLatch.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}