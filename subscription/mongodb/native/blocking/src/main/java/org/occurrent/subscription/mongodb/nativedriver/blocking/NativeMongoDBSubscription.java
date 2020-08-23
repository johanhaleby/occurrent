package org.occurrent.subscription.mongodb.nativedriver.blocking;

import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NativeMongoDBSubscription implements Subscription {
    public final String subscriptionId;
    final CountDownLatch subscriptionStartedLatch;

    NativeMongoDBSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch) {
        this.subscriptionId = subscriptionId;
        this.subscriptionStartedLatch = subscriptionStartedLatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NativeMongoDBSubscription)) return false;
        NativeMongoDBSubscription that = (NativeMongoDBSubscription) o;
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