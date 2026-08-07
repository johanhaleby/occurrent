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

package org.occurrent.tck.subscription.blocking.outofpackage;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A model that honours the whole {@code SubscriptionModelConformance} contract, so
 * {@link DeliveryTimeoutIsUsableFromASubclassOutsideThePackageTest} can run that suite green from this package.
 * <p>
 * A trimmed copy of {@code org.occurrent.tck.subscription.blocking.WorkingSubscriptionModel}'s shape rather than a
 * reuse of it, because that class is package private on purpose. That is exactly why this class exists here. An
 * implementer outside {@code org.occurrent.tck.subscription.blocking} has to be able to satisfy the whole suite using
 * only what this package can see, and this proves it rather than assuming it.
 */
final class OutOfPackageWorkingSubscriptionModel implements SubscriptionModel {

    private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
    private final Set<String> paused = ConcurrentHashMap.newKeySet();

    private volatile boolean running = true;

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter);
        Registration registration = new Registration(matcher, action, Executors.newSingleThreadExecutor());
        if (registrations.putIfAbsent(subscriptionId, registration) != null) {
            registration.dispatcher.shutdownNow();
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
        }
        return new StartedSubscription(subscriptionId);
    }

    void feed(List<CloudEvent> cloudEvents) {
        if (!running) {
            return;
        }
        registrations.forEach((subscriptionId, registration) -> {
            if (paused.contains(subscriptionId)) {
                return;
            }
            List<CloudEvent> matching = cloudEvents.stream().filter(registration.matcher).toList();
            if (matching.isEmpty()) {
                return;
            }
            registration.dispatcher.execute(() -> matching.forEach(cloudEvent -> deliverWithRetry(registration.action, cloudEvent)));
        });
    }

    // Two retries and then give up, rather than forever, so a handler installed to fail once does not spin the
    // dispatcher thread past what the caller's wait allows for.
    private static void deliverWithRetry(Consumer<CloudEvent> action, CloudEvent cloudEvent) {
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                action.accept(cloudEvent);
                return;
            } catch (RuntimeException retryable) {
                // Try again.
            }
        }
        try {
            action.accept(cloudEvent);
        } catch (RuntimeException givingUp) {
            // Nothing here can report it. The test that installed the handler notices, by never seeing the event.
        }
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        Registration removed = registrations.remove(subscriptionId);
        if (removed != null) {
            removed.dispatcher.shutdownNow();
        }
        paused.remove(subscriptionId);
    }

    @Override
    public void stop() {
        running = false;
        paused.addAll(registrations.keySet());
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        running = true;
        if (resumeSubscriptionsAutomatically) {
            paused.clear();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return registrations.containsKey(subscriptionId) && !paused.contains(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return registrations.containsKey(subscriptionId) && paused.contains(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        if (!isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        }
        paused.remove(subscriptionId);
        running = true;
        return new StartedSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        if (!isRunning(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not running");
        }
        paused.add(subscriptionId);
    }

    @Override
    public void shutdown() {
        registrations.values().forEach(registration -> registration.dispatcher.shutdownNow());
        registrations.clear();
        paused.clear();
    }

    private record Registration(Predicate<CloudEvent> matcher, Consumer<CloudEvent> action, ExecutorService dispatcher) {
    }

    private record StartedSubscription(String id) implements Subscription {

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
