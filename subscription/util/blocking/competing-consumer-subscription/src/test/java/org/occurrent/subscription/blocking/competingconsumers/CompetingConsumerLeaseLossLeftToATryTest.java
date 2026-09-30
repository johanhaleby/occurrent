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
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A lease loss that finds its subscription's lock taken is left to a try, which acts on it as soon as the call holding
 * the lock has returned. Once a try has ended on an interrupt, the next failure for its subscription starts a new try.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseLossLeftToATryTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    private static final int ROUNDS = 5;
    // The fastest of the rounds, so one slow thread start on a loaded machine does not decide the outcome. A try that
    // first waits for a backoff takes at least that backoff, which is 100 milliseconds.
    private static final Duration AT_ONCE = Duration.ofMillis(50);

    @Test
    void a_lease_loss_that_finds_its_subscription_busy_is_acted_on_as_soon_as_the_call_holding_it_returns() {
        List<Duration> latencies = new ArrayList<>();
        for (int round = 1; round <= ROUNDS; round++) {
            latencies.add(aLeaseLossWhileAResumeHoldsTheSubscription());
        }
        Duration fastest = latencies.stream().min(Duration::compareTo).orElseThrow();
        assertThat(fastest).as("[the fastest of %d losses left to a try was acted on %d ms after the call holding the subscription returned, all of them %s]",
                ROUNDS, fastest.toMillis(), latencies).isLessThan(AT_ONCE);
    }

    private static Duration aLeaseLossWhileAResumeHoldsTheSubscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        Gate isRunningOfS1 = new Gate();
        try {
            subscribe(model, "s1");
            assertThat(wrapped.isRunning("s1")).as("s1 runs once this node holds its lease").isTrue();
            // A resume of a subscription that runs asks the wrapped model whether it runs, holding the subscription,
            // and then refuses
            wrapped.isRunningGates.put("s1", isRunningOfS1);
            CompletableFuture<Void> resuming = CompletableFuture.runAsync(() -> model.resumeSubscription("s1"));
            assertThat(isRunningOfS1.awaitEnteredOnAnotherThread()).as("the resume holds s1").isTrue();

            strategy.holders.remove("s1");
            model.onConsumeProhibited("s1", NODE);
            long returnedAt = System.nanoTime();
            isRunningOfS1.open();
            assertThat(resuming).as("the resume of a subscription that runs").failsWithin(EVENTUALLY);

            await().atMost(EVENTUALLY).untilAsserted(() ->
                    assertThat(wrapped.isRunning("s1")).as("[s1 runs in the wrapped model although this node lost its lease]").isFalse());
            return Duration.ofNanos(wrapped.pausedAt.get("s1") - returnedAt);
        } finally {
            isRunningOfS1.open();
            model.shutdown();
        }
    }

    @Test
    void after_a_try_ended_on_an_interrupt_the_next_failure_for_its_subscription_starts_a_new_try() throws InterruptedException {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            subscribe(model, "s1");
            wrapped.pauseFails = true;
            strategy.holders.remove("s1");
            model.onConsumeProhibited("s1", NODE);
            Thread firstTry = await().atMost(EVENTUALLY).until(() -> tryOf("s1"), Optional::isPresent).orElseThrow();

            firstTry.interrupt();
            firstTry.join(EVENTUALLY.toMillis());
            assertThat(firstTry.isAlive()).as("the try that was interrupted ends").isFalse();

            model.onConsumeProhibited("s1", NODE);
            wrapped.pauseFails = false;

            await().atMost(EVENTUALLY).untilAsserted(() ->
                    assertThat(wrapped.isRunning("s1")).as("[s1 runs in the wrapped model although this node lost its lease, and its pause failed after a try was interrupted]").isFalse());
        } finally {
            model.shutdown();
        }
    }

    private static Optional<Thread> tryOf(String subscriptionId) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals("occurrent-competing-consumer-reconcile-" + subscriptionId))
                .findFirst();
    }

    private static Subscription subscribe(CompetingConsumerSubscriptionModel model, String subscriptionId) {
        return model.subscribe(NODE, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
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
                if (!open.await(5, SECONDS)) {
                    throw new IllegalStateException("The gate was never opened");
                }
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

    // Grants every lease asked for
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
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

    // A model of a user's own that records when it pauses a subscription. Its next isRunning(id) for an id waits at
    // that id's gate once, outside the monitor of this model, and its pause throws while the test says so.
    private static final class WrappedModel implements SubscriptionModel {
        private final Map<String, Gate> isRunningGates = new ConcurrentHashMap<>();
        private final Map<String, Long> pausedAt = new ConcurrentHashMap<>();
        private volatile boolean pauseFails;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            (running ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            pausedIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            running = false;
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            Gate gate = isRunningGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                return runningIds.contains(subscriptionId);
            }
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            pausedIds.remove(subscriptionId);
            runningIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            if (pauseFails) {
                throw new IllegalStateException("Pausing " + subscriptionId + " failed");
            }
            if (runningIds.remove(subscriptionId)) {
                pausedAt.put(subscriptionId, System.nanoTime());
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
