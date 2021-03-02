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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;

public class SpringMongoSubscription implements Subscription {

    private final String subscriptionId;
    private final AtomicReference<org.springframework.data.mongodb.core.messaging.Subscription> subscriptionReference;
    private volatile boolean shutdown = false;

    protected SpringMongoSubscription(String subscriptionId, org.springframework.data.mongodb.core.messaging.Subscription subscriptionReference) {
        this.subscriptionId = subscriptionId;
        this.subscriptionReference = new AtomicReference<>(subscriptionReference);
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    @Override
    public void waitUntilStarted() {
        waitUntilStarted(Duration.of(100000, ChronoUnit.DAYS)); // "Forever" :)
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        boolean continueWaiting = true;
        final long startTime = System.currentTimeMillis();
        while (!shutdown && continueWaiting) {
            final long currentTime = System.currentTimeMillis();
            if ((currentTime - startTime) >= timeout.toMillis()) {
                return false;
            }
            try {
                continueWaiting = !subscriptionReference.get().await(Duration.ofMillis(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return !continueWaiting;
    }

    AtomicReference<org.springframework.data.mongodb.core.messaging.Subscription> getSubscriptionReference() {
        return subscriptionReference;
    }

    void changeSubscription(org.springframework.data.mongodb.core.messaging.Subscription subscription) {
        subscriptionReference.set(subscription);
    }

    void shutdown() {
        shutdown = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpringMongoSubscription)) return false;
        SpringMongoSubscription that = (SpringMongoSubscription) o;
        return shutdown == that.shutdown && Objects.equals(subscriptionId, that.subscriptionId) && Objects.equals(subscriptionReference, that.subscriptionReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, subscriptionReference, shutdown);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SpringMongoSubscription.class.getSimpleName() + "[", "]")
                .add("subscriptionId='" + subscriptionId + "'")
                .add("subscriptionReference=" + subscriptionReference)
                .add("shutdown=" + shutdown)
                .toString();
    }
}