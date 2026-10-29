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

package org.occurrent.dsl.saga.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * A subscription model that keeps the listeners it is given, so a test decides which events are delivered and when.
 * It declares no capability. Events passed to one {@link #deliver} call reach the listener in the order given, which
 * is the order of a commit.
 */
class StubSubscriptionModel implements SubscriptionModel {

    private final Map<String, Consumer<CloudEvent>> listeners = new ConcurrentHashMap<>();

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        listeners.put(subscriptionId, action);
        return new StartedSubscription(subscriptionId);
    }

    void deliver(String subscriptionId, CloudEvent... events) {
        Consumer<CloudEvent> listener = listeners.get(subscriptionId);
        if (listener == null) {
            throw new IllegalStateException("Nothing is subscribed as '" + subscriptionId + "'");
        }
        for (CloudEvent event : events) {
            listener.accept(event);
        }
    }

    boolean isSubscribed(String subscriptionId) {
        return listeners.containsKey(subscriptionId);
    }

    Set<String> subscriptionIds() {
        return Set.copyOf(listeners.keySet());
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        listeners.remove(subscriptionId);
    }

    @Override
    public void stop() {
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
    }

    @Override
    public boolean isRunning() {
        return true;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return isSubscribed(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return false;
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        return new StartedSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
    }

    private record StartedSubscription(String id) implements Subscription {
        @Override
        public void waitUntilStarted() {
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
