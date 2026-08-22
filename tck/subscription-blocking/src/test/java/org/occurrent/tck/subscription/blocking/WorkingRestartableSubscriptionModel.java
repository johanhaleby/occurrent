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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilterMatcher;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A model with state that outlives it, so {@link RestartConformance} can be run green as well as red.
 * <p>
 * {@link Storage} is the part a restart keeps: a log of everything published, and how far each subscription got
 * through it. Building a second model over the same {@code Storage} is what a restart is here, which is the same shape
 * a real fixture uses when it points a fresh model at the database the old one was reading.
 * <p>
 * One flag, {@code resumesAfterARestart}, decides which side of the fixture's declaration this model sits on, so both
 * branches of it get a green run rather than only the one Occurrent's own models happen to take. A model answering
 * {@code false} still keeps its log, since events published while it was down have to exist somewhere for the suite to
 * assert they are <em>not</em> delivered.
 * <p>
 * Delivery is synchronous, unlike {@link WorkingSubscriptionModel}. The waiting machinery is already exercised by that
 * model's green run, and what matters here is where a subscription starts reading, which is easier to get right and to
 * read when nothing is racing.
 */
final class WorkingRestartableSubscriptionModel implements SubscriptionModel {

    /**
     * What survives a restart. Guarded by its own monitor, since the suite publishes from the test thread while
     * delivery runs on it too.
     */
    static final class Storage {
        private final List<CloudEvent> log = new ArrayList<>();
        private final Map<String, Integer> howFarEachSubscriptionGot = new HashMap<>();
    }

    private final Storage storage;
    private final boolean resumesAfterARestart;
    private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
    private final Set<String> paused = ConcurrentHashMap.newKeySet();

    private volatile boolean running = true;

    WorkingRestartableSubscriptionModel(Storage storage, boolean resumesAfterARestart) {
        this.storage = storage;
        this.resumesAfterARestart = resumesAfterARestart;
    }

    @Override
    public SubscriptionHandle subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        Predicate<CloudEvent> matcher = SubscriptionFilterMatcher.matcherFor(filter);
        // Registering and recording where this subscription starts happen under one lock. Split apart, an append
        // landing between them would see the id registered with no starting position recorded yet, default it to the
        // beginning and replay the whole log to a subscription that should have started live.
        synchronized (storage) {
            if (registrations.putIfAbsent(subscriptionId, new Registration(matcher, action)) != null) {
                throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
            }
            // A model that does not resume starts at the end of the log every time, and one that does starts at the end
            // only the first time it sees an id, which is what makes the first subscription in a test start live on both.
            if (!resumesAfterARestart || !storage.howFarEachSubscriptionGot.containsKey(subscriptionId)) {
                storage.howFarEachSubscriptionGot.put(subscriptionId, storage.log.size());
            }
        }
        deliverPending(subscriptionId);
        return new StartedSubscription(subscriptionId);
    }

    /**
     * Appends to the log and delivers to whoever is listening. Appending even while the model is stopped is the point:
     * that is the gap a restart has to answer for.
     */
    void feed(List<CloudEvent> cloudEvents) {
        synchronized (storage) {
            storage.log.addAll(cloudEvents);
        }
        registrations.keySet().forEach(this::deliverPending);
    }

    /**
     * Walks one subscription from where it got to up to the end of the log, one event at a time, recording progress
     * only after the handler has had the event. Recording it before would lose an event to a handler that threw, which
     * is the bug this whole suite exists to catch in a real model.
     */
    private void deliverPending(String subscriptionId) {
        Registration registration = registrations.get(subscriptionId);
        if (registration == null || !running || paused.contains(subscriptionId)) {
            return;
        }
        while (true) {
            final CloudEvent next;
            final int position;
            synchronized (storage) {
                position = storage.howFarEachSubscriptionGot.getOrDefault(subscriptionId, 0);
                if (position >= storage.log.size()) {
                    return;
                }
                next = storage.log.get(position);
            }
            if (registration.matcher.test(next)) {
                registration.action.accept(next);
            }
            synchronized (storage) {
                storage.howFarEachSubscriptionGot.put(subscriptionId, position + 1);
            }
        }
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        registrations.remove(subscriptionId);
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
            Set<String> resumed = Set.copyOf(paused);
            paused.clear();
            resumed.forEach(this::deliverPending);
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
    public SubscriptionHandle resumeSubscription(String subscriptionId) {
        if (!isPaused(subscriptionId)) {
            throw new IllegalArgumentException("Subscription " + subscriptionId + " is not paused");
        }
        paused.remove(subscriptionId);
        running = true;
        deliverPending(subscriptionId);
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
        running = false;
        registrations.clear();
        paused.clear();
    }

    private record Registration(Predicate<CloudEvent> matcher, Consumer<CloudEvent> action) {
    }

    private record StartedSubscription(String id) implements SubscriptionHandle {

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
