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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A node runs the action of a competing subscription only while the lease strategy reports its lease held, also while
 * a call for another subscription waits inside the wrapped model, and {@code stop()} and {@code shutdown()} do not wait
 * for a resume of a competing subscription that can no longer deliver.
 * <p>
 * Each test has s1 and s2 on one wrapped model, and a grant of s1 whose resume waits inside the wrapped model. Another
 * node then takes the lease of s2, and an event for s2 arrives while the wrapped model still runs s2.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerDeliversOnlyUnderItsLeaseTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Longer than the 200 ms an event waits at most between two looks at the lease
    private static final Duration NOT_DELIVERED_WITHIN = Duration.ofMillis(1500);
    private static final Duration RETURNS_WITHIN = Duration.ofMillis(500);

    private final WrappedModel wrapped = new WrappedModel();
    private final Strategy strategy = new Strategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
    private final ExecutorService otherThreads = Executors.newCachedThreadPool();
    private final Gate resumeOfS1 = new Gate();
    private final Map<String, List<String>> delivered = new ConcurrentHashMap<>();

    @AfterEach
    void shutdown() {
        resumeOfS1.open();
        otherThreads.shutdownNow();
        model.shutdown();
    }

    @Test
    void a_subscription_whose_lease_another_node_took_before_this_node_is_told_delivers_nothing_while_a_resume_of_another_subscription_waits() {
        theGrantOfS1WaitsInsideTheWrappedModel();

        strategy.anotherNodeTakesBeforeThisNodeIsTold("s2");
        wrapped.publish("s2", "e1");

        assertThat(deliveredWithin(NOT_DELIVERED_WITHIN, "s2"))
                .as("[s2 delivered an event after another node took its lease]")
                .isEmpty();
    }

    @Test
    void a_shutdown_while_a_resume_waits_returns_without_it_and_no_subscription_delivers_without_its_lease() {
        theGrantOfS1WaitsInsideTheWrappedModel();

        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(model::shutdown, otherThreads);
        happensWithin(RETURNS_WITHIN, shutdown::isDone);
        assertThat(shutdown).as("[shutdown() returned while the resume of s1 waits inside the wrapped model]").isDone();
        strategy.anotherNodeTakes("s2");
        wrapped.publish("s2", "e1");

        assertThat(deliveredWithin(NOT_DELIVERED_WITHIN, "s2"))
                .as("[s2 delivered an event after another node took its lease while this node shut down]")
                .isEmpty();

        resumeOfS1.open();
        await().atMost(EVENTUALLY).until(() -> !wrapped.isRunning("s1"));
        wrapped.publish("s1", "e2");
        assertThat(deliveredWithin(NOT_DELIVERED_WITHIN, "s1"))
                .as("[s1 delivered an event after the resume that shutdown() did not wait for returned]")
                .isEmpty();
    }

    @Test
    void a_stop_while_a_resume_waits_returns_without_it_and_the_late_resume_is_paused_again() {
        theGrantOfS1WaitsInsideTheWrappedModel();

        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop, otherThreads);
        happensWithin(RETURNS_WITHIN, stop::isDone);
        assertThat(stop).as("[stop() returned while the resume of s1 waits inside the wrapped model]").isDone();

        resumeOfS1.open();
        await().atMost(EVENTUALLY).until(() -> wrapped.resumesReturned.contains("s1"));
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("[s1 paused again in the wrapped model]").isFalse());
        wrapped.publish("s1", "e1");
        assertThat(deliveredWithin(NOT_DELIVERED_WITHIN, "s1"))
                .as("[s1 delivered an event after stop() returned and the late resume was paused again]")
                .isEmpty();
    }

    // s1 and s2 run on this node. Another node takes s1 and gives it back, and the grant of s1 that follows resumes s1
    // in the wrapped model, where the resume waits.
    private void theGrantOfS1WaitsInsideTheWrappedModel() {
        for (String subscriptionId : List.of("s1", "s2")) {
            delivered.put(subscriptionId, new CopyOnWriteArrayList<>());
            model.subscribe(NODE, subscriptionId, null, StartAt.subscriptionModelDefault(), event -> delivered.get(subscriptionId).add(event.getId()));
        }
        wrapped.publish("s2", "e0");
        await().atMost(EVENTUALLY).until(() -> delivered.get("s2").contains("e0"));

        strategy.anotherNodeTakes("s1");
        await().atMost(EVENTUALLY).until(() -> wrapped.isPaused("s1"));
        strategy.anotherNodeGivesUp("s1");
        wrapped.resumeGates.put("s1", resumeOfS1);
        strategy.grantWhatIsFree();
        assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("the grant's resume of s1 waits inside the wrapped model").isTrue();
        assertThat(wrapped.isRunning("s2")).as("s2 runs in the wrapped model").isTrue();
    }

    private List<String> deliveredWithin(Duration window, String subscriptionId) {
        List<String> before = List.copyOf(delivered.get(subscriptionId));
        happensWithin(window, () -> delivered.get(subscriptionId).size() > before.size());
        List<String> all = delivered.get(subscriptionId);
        return all.subList(before.size(), all.size());
    }

    // Returns as soon as the condition holds, or when the window has passed without it
    private static void happensWithin(Duration window, BooleanSupplier condition) {
        try {
            await().atMost(window).pollInterval(1, MILLISECONDS).until(condition::getAsBoolean);
        } catch (ConditionTimeoutException ignored) {
            // The window passed, which the assertions that follow are about
        }
    }

    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);
        private volatile @Nullable Thread enteredOn;

        private void pass() {
            enteredOn = Thread.currentThread();
            entered.countDown();
            try {
                open.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private boolean awaitEnteredOnAnotherThread() {
            try {
                return entered.await(5, SECONDS) && enteredOn != Thread.currentThread();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private void open() {
            open.countDown();
        }
    }

    // Holds the lease of each subscription for this node or for another one. Like the MongoDB lease strategies, it tells
    // this node of a lease another node took, and of a lease it won in a refresh, on a thread of each subscription's own.
    private static final class Strategy implements CompetingConsumerStrategy {
        private static final String OTHER_NODE = "other-node";
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final Set<String> candidates = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final Map<String, ExecutorService> notifiers = new ConcurrentHashMap<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            candidates.add(subscriptionId);
            return NODE.equals(holders.computeIfAbsent(subscriptionId, __ -> NODE));
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            candidates.remove(subscriptionId);
            holders.remove(subscriptionId, NODE);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, NODE);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return NODE.equals(holders.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        @Override
        public void shutdown() {
            notifiers.values().forEach(ExecutorService::shutdownNow);
        }

        private void anotherNodeTakes(String subscriptionId) {
            if (NODE.equals(holders.put(subscriptionId, OTHER_NODE))) {
                notifyLater(subscriptionId, listener -> listener.onConsumeProhibited(subscriptionId, NODE));
            }
        }

        // As when the lease expired, and the refresh that tells this node has not run yet
        private void anotherNodeTakesBeforeThisNodeIsTold(String subscriptionId) {
            holders.put(subscriptionId, OTHER_NODE);
        }

        private void anotherNodeGivesUp(String subscriptionId) {
            holders.remove(subscriptionId, OTHER_NODE);
        }

        private void grantWhatIsFree() {
            for (String subscriptionId : candidates) {
                if (holders.putIfAbsent(subscriptionId, NODE) == null) {
                    notifyLater(subscriptionId, listener -> listener.onConsumeGranted(subscriptionId, NODE));
                }
            }
        }

        private void notifyLater(String subscriptionId, Consumer<CompetingConsumerListener> notification) {
            notifiers.computeIfAbsent(subscriptionId, __ -> Executors.newSingleThreadExecutor())
                    .execute(() -> listeners.forEach(notification));
        }
    }

    // A model of a user's own that runs each event on a thread of its own, as the MongoDB models run it on the thread
    // reading the change stream, and whose resume of a subscription waits at its gate once, outside its monitor
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final Set<String> resumesReturned = ConcurrentHashMap.newKeySet();
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        private void publish(String subscriptionId, String eventId) {
            Consumer<CloudEvent> action;
            synchronized (this) {
                if (!running || !runningIds.contains(subscriptionId)) {
                    return;
                }
                action = actions.get(subscriptionId);
            }
            CloudEvent event = CloudEventBuilder.v1().withId(eventId).withSource(URI.create("urn:test")).withType("Tested").build();
            Thread.ofPlatform().daemon().start(() -> {
                try {
                    action.accept(event);
                } catch (RuntimeException ignored) {
                    // Refused, and a resume would deliver it again from the position this model keeps
                }
            });
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, action, false);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, action, true);
        }

        private synchronized Subscription make(String subscriptionId, Consumer<CloudEvent> action, boolean paused) {
            actions.put(subscriptionId, action);
            (running && !paused ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Set<String> subscriptionIds() {
            Set<String> ids = new HashSet<>(runningIds);
            ids.addAll(pausedIds);
            return ids;
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            running = false;
            pausedIds.addAll(runningIds);
            runningIds.clear();
        }

        @Override
        public synchronized void shutdown() {
            stop();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
            }
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public synchronized boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            Gate gate = resumeGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                if (!pausedIds.remove(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
                }
                runningIds.add(subscriptionId);
                // Started by a resume, as a wrapped model that starts itself on a resume is
                running = true;
            }
            resumesReturned.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }
    }

    private record WrappedSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
