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

public class SpringMongoDBSubscription implements Subscription {

    private final String subscriptionId;
    private final org.springframework.data.mongodb.core.messaging.Subscription subscription;

    public SpringMongoDBSubscription(String subscriptionId, org.springframework.data.mongodb.core.messaging.Subscription subscription) {
        this.subscriptionId = subscriptionId;
        this.subscription = subscription;
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    @Override
    public void waitUntilStarted() {
        try {
            subscription.await(Duration.of(100000, ChronoUnit.DAYS)); // "Forever" :)
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        try {
            return subscription.await(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpringMongoDBSubscription)) return false;
        SpringMongoDBSubscription that = (SpringMongoDBSubscription) o;
        return Objects.equals(subscriptionId, that.subscriptionId) &&
                Objects.equals(subscription, that.subscription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, subscription);
    }

    @Override
    public String toString() {
        return "MongoDBSpringSubscription{" +
                "subscriptionId='" + subscriptionId + '\'' +
                ", subscription=" + subscription +
                '}';
    }
}