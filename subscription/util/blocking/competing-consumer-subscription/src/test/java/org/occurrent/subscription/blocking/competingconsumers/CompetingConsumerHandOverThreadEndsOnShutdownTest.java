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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A stop() that finds another call holding a subscription's lock hands the subscription to a thread of its own, which
 * applies the stop once that call returns. Shutting the model down ends that thread, also while the call holding the
 * lock is stuck, since nothing is left to apply the stop to.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerHandOverThreadEndsOnShutdownTest {

    private static final String NODE = "node";
    private static final String HAND_OVER_THREAD = "occurrent-competing-consumer-lifecycle-s1";

    @Test
    void a_thread_that_waits_to_apply_a_stop_to_a_subscription_ends_when_the_model_is_shut_down_while_the_call_holding_its_lock_is_stuck() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = Executors.newCachedThreadPool();
        Gate unregisterOfS1 = new Gate();
        CompletableFuture<?> pausing = null;
        try {
            model.subscribe(NODE, "s1", null, StartAt.subscriptionModelDefault(), __ -> {
            });
            // The user's pause holds the lock of s1 while it waits inside the unregister, as it does through a database outage
            strategy.firstUnregister.set(unregisterOfS1);
            pausing = CompletableFuture.runAsync(() -> model.pauseSubscription("s1"), otherThreads);
            assertThat(unregisterOfS1.awaitEnteredOnAnotherThread()).as("pauseSubscription(s1) waits inside the unregister of s1, holding its lock").isTrue();

            model.stop();
            model.shutdown();

            assertThat(pausing.isDone()).as("the pause of s1 is still stuck when shutdown() returns").isFalse();
            // The thread checks for a shutdown every 100 milliseconds, and the pause stays stuck for as long as this waits,
            // so only a thread that never ends runs out the wait
            await().atMost(Duration.ofSeconds(10)).pollInterval(10, MILLISECONDS).untilAsserted(() -> assertThat(liveThreadsNamed(HAND_OVER_THREAD))
                    .as("[threads named %s still alive 10 seconds after shutdown() returned, while the pause of s1 is stuck]", HAND_OVER_THREAD).isEmpty());
        } finally {
            unregisterOfS1.open();
            awaitBounded(pausing);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    private static List<String> liveThreadsNamed(String name) {
        return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive).map(Thread::getName).filter(name::equals).toList();
    }

    private static void awaitBounded(@Nullable CompletableFuture<?> future) {
        if (future != null) {
            try {
                future.get(5, SECONDS);
            } catch (Exception ignored) {
                // The test already reports what matters
            }
        }
    }

    // A point that a thread waits at until the test opens it, which tells the test that a thread got there
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

    // Grants every lease asked for. The first unregister after a gate is set waits at it.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final AtomicReference<@Nullable Gate> firstUnregister = new AtomicReference<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            Gate gate = firstUnregister.getAndSet(null);
            if (gate != null) {
                gate.pass();
            }
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holders.contains(subscriptionId);
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
        }
    }

    // A model of a user's own that holds a subscription paused when asked to
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, false);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, true);
        }

        private synchronized Subscription make(String subscriptionId, boolean paused) {
            if (runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId)) {
                throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
            }
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
            synchronized (this) {
                if (!pausedIds.remove(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
                }
                runningIds.add(subscriptionId);
                return new WrappedSubscription(subscriptionId);
            }
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
