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

package org.occurrent.tck.subscription.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
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
 * A subscription model that honours the whole contract, so that {@link SuiteNeverSkipsTest} can run the suite green and
 * reach every line of it.
 * <p>
 * <strong>Deliberately asynchronous</strong>, handing each event to a pool thread rather than calling the handler where
 * the event was published. A synchronous one would be less code, and it would leave the suite's whole waiting mechanism
 * untested by the green run, which is the part of the suite most likely to be wrong.
 * <p>
 * It drops events for a paused subscription rather than holding them, and it retries a failing handler, so the green run
 * exercises those two branches of the fixture's declarations. The opposite branches are exercised by Occurrent's own
 * models.
 */
public class WorkingSubscriptionModel implements SubscriptionModel, IntrospectableSubscriptions, CheckpointAwareSubscriptionModel {

    private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
    private final Set<String> paused = ConcurrentHashMap.newKeySet();
    private final Duration deliveryDelay;

    private volatile boolean running = true;

    public WorkingSubscriptionModel() {
        this(Duration.ZERO);
    }

    /**
     * @param deliveryDelay How long a pool thread waits before calling the handler. Zero for the green runs, where the
     *                      only thing that matters is that delivery happens on another thread. A real delay is for the
     *                      run that has to prove {@link InProcessDeliveryConformance} fails against an asynchronous
     *                      model: with no delay a pool thread sometimes finishes before the assertion reads the list,
     *                      and that run would pass by luck every so often.
     */
    public WorkingSubscriptionModel(Duration deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
    }

    /**
     * This model has no history, so every start position means the same thing as starting live, and {@code subscribe}
     * ignores {@link StartAt} accordingly. That is why a fixed checkpoint is an honest answer here rather than a stub:
     * a write after the subscription starts arrives either way, which is the property the suite asserts. Do not read
     * this as a model with real position semantics.
     */
    @Override
    public Checkpoint globalCheckpoint() {
        return new StringBasedCheckpoint("working-model-has-no-history");
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        // Built before the id is reserved, so a filter this model cannot apply is refused without leaving a half
        // registered subscription behind.
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter);
        // One thread per subscription, so this subscription's events are delivered in the order they were published.
        Registration registration = new Registration(matcher, action, Executors.newSingleThreadExecutor());
        if (registrations.putIfAbsent(subscriptionId, registration) != null) {
            registration.dispatcher.shutdownNow();
            throw new DuplicateSubscriptionIdException(subscriptionId);
        }
        return new StartedSubscription(subscriptionId);
    }

    /**
     * Hands the events to every running subscription whose filter matches them.
     * <p>
     * One task per subscription, not one per event, and each subscription's tasks run on a thread of its own. A pool
     * shared across events delivers them in whatever order the threads happen to run, which broke the suite's
     * order assertion about one time in ten. Every real model has one cursor or one queue per subscription, so a double
     * that fans out per event is the wrong shape rather than merely unlucky.
     */
    public void feed(List<CloudEvent> cloudEvents) {
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
            registration.dispatcher.execute(() -> {
                sleepQuietly(deliveryDelay);
                matching.forEach(cloudEvent -> deliverWithRetry(registration.action, cloudEvent));
            });
        });
    }

    private static void sleepQuietly(Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Two retries and then give up, rather than forever, so a handler that always throws fails the test that installed
    // it instead of spinning until the suite's timeout. The last attempt runs outside the try, so giving up is a line
    // you can see rather than a condition hidden in a catch block.
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
            // Nothing here can report it: no caller is on this thread. The test that installed the handler is what
            // notices, by never seeing the event it expected.
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
        requireKnown(subscriptionId);
        if (!isPaused(subscriptionId)) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }
        paused.remove(subscriptionId);
        // Resuming one subscription after stop() has to make it deliver again, which is what the life cycle promises,
        // so the model-wide flag has to come back too. Occurrent's own three models do the same thing. The alternative
        // is for delivery to ignore the model-wide flag entirely and look only at the per-subscription one, and nothing
        // says which of the two the contract means.
        running = true;
        return new StartedSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        requireKnown(subscriptionId);
        if (!isRunning(subscriptionId)) {
            throw new SubscriptionNotRunningException(subscriptionId);
        }
        paused.add(subscriptionId);
    }

    private void requireKnown(String subscriptionId) {
        if (!registrations.containsKey(subscriptionId)) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
    }

    @Override
    public Set<String> subscriptionIds() {
        return Set.copyOf(registrations.keySet());
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
