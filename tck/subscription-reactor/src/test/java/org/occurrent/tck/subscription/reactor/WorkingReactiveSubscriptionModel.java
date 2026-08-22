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

package org.occurrent.tck.subscription.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The smallest reactive subscription model that honours the reactive contract, so
 * {@link ReactiveSuiteNeverSkipsTest} has something to run the suite green against: no reactive in-memory model exists
 * in this repository to borrow, and the real models' wirings live in their own modules, invisible from here.
 * <p>
 * Honouring the contract means: the action's {@code Mono} is subscribed per delivered event (here by blocking on it,
 * which for an in-memory test double is subscription plus completion in one call), registration runs no action, an
 * action failure propagates to the publisher and leaves the model running (the propagate half of the
 * retry-or-propagate choice), and {@code waitUntilStarted()} completes immediately and repeatably.
 */
@NullMarked
final class WorkingReactiveSubscriptionModel implements SubscriptionModel {

    private final Map<String, Function<CloudEvent, Mono<Void>>> running = new ConcurrentHashMap<>();
    private final Map<String, Function<CloudEvent, Mono<Void>>> paused = new ConcurrentHashMap<>();
    private volatile boolean modelRunning = true;

    void deliver(List<CloudEvent> events) {
        for (CloudEvent event : events) {
            for (Function<CloudEvent, Mono<Void>> action : running.values()) {
                // block() subscribes, which is the whole point: the work inside the Mono runs. An error propagates to
                // the caller (the publisher), which is one of the two documented answers.
                action.apply(event).block();
            }
        }
    }

    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
        if (running.containsKey(subscriptionId) || paused.containsKey(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }
        running.put(subscriptionId, action);
        return new SubscriptionHandle() {
            @Override
            public String id() {
                return subscriptionId;
            }

            @Override
            public Mono<Void> waitUntilStarted() {
                // Started the moment subscribe returned, and Mono.empty() answers every subscriber, so asking twice
                // answers twice and a disposed wait costs nothing.
                return Mono.empty();
            }
        };
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        Function<CloudEvent, Mono<Void>> action = running.remove(subscriptionId);
        if (action == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        }
        paused.put(subscriptionId, action);
    }

    @Override
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        Function<CloudEvent, Mono<Void>> action = paused.remove(subscriptionId);
        if (action == null) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " isn't paused.");
        }
        return subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), action);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        running.remove(subscriptionId);
        paused.remove(subscriptionId);
    }

    @Override
    public void stop() {
        modelRunning = false;
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        modelRunning = true;
        if (resumeSubscriptionsAutomatically) {
            paused.keySet().forEach(this::resumeSubscription);
        }
    }

    @Override
    public boolean isRunning() {
        return modelRunning;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return running.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return paused.containsKey(subscriptionId);
    }

    @Override
    public void shutdown() {
        modelRunning = false;
        running.clear();
        paused.clear();
    }
}
