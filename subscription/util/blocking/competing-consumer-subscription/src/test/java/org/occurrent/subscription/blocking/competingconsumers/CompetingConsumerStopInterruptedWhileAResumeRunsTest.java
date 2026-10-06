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
import org.awaitility.core.ConditionTimeoutException;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * stop() waits for a resume of a subscription that does not compete before it stops the wrapped model, since nothing
 * stops that subscription from delivering once it runs there. Interrupting the thread that waits must not cut that wait
 * short, and the interrupt must still be there once stop() returns, for the caller to act on. A resume of a competing
 * subscription stop() does not wait for, see {@link CompetingConsumerDeliversOnlyUnderItsLeaseTest}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStopInterruptedWhileAResumeRunsTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Long enough for a stop() that gives up its wait on an interrupt to have returned, which takes microseconds
    private static final Duration DOES_NOT_RETURN_WITHIN = Duration.ofMillis(500);

    @Test
    void a_stop_interrupted_while_it_waits_for_a_resume_of_a_subscription_that_does_not_compete_keeps_waiting_and_keeps_the_interrupt_for_its_caller() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = Executors.newCachedThreadPool();
        Gate resumeOfN1 = new Gate();
        CompletableFuture<?> resuming = null;
        try {
            model.subscribe(NODE, "n1", null, StartAt.dynamic(__ -> null), __ -> {
            });
            model.pauseSubscription("n1");
            assertThat(wrapped.isPaused("n1")).as("n1 paused in the wrapped model by the user").isTrue();
            wrapped.events.clear();
            wrapped.resumeGates.put("n1", resumeOfN1);
            resuming = CompletableFuture.runAsync(() -> model.resumeSubscription("n1"), otherThreads);
            assertThat(resumeOfN1.awaitEnteredOnAnotherThread()).as("the resume of n1 waits inside the wrapped model").isTrue();

            CompletableFuture<Boolean> stopReturnedWithInterruptFlag = new CompletableFuture<>();
            Thread stopping = new Thread(() -> {
                try {
                    model.stop();
                    stopReturnedWithInterruptFlag.complete(Thread.currentThread().isInterrupted());
                } catch (Throwable e) {
                    stopReturnedWithInterruptFlag.completeExceptionally(e);
                }
            }, "test-stopping-thread");
            stopping.setDaemon(true);
            stopping.start();
            await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> stopping.getState() == Thread.State.WAITING);
            assertThat(wrapped.stopCalls.get()).as("the wrapped model is stopped before the stopping thread is interrupted").isZero();

            stopping.interrupt();
            happensWithin(DOES_NOT_RETURN_WITHIN, () -> stopReturnedWithInterruptFlag.isDone() || wrapped.stopCalls.get() > 0);

            assertThat(stopReturnedWithInterruptFlag.isDone()).as("[stop() returned while a resume is still inside the wrapped model]").isFalse();
            assertThat(wrapped.stopCalls.get()).as("[the wrapped model is stopped while a resume is still inside it]").isZero();

            resumeOfN1.open();
            assertThat(stopReturnedWithInterruptFlag).as("stop() once the resume returned").succeedsWithin(EVENTUALLY);
            assertThat(stopReturnedWithInterruptFlag.join()).as("[the interrupt of the stopping thread is still set when stop() returns]").isTrue();
            assertThat(wrapped.stopCalls.get()).as("the wrapped model was stopped once").isEqualTo(1);
            assertThat(wrapped.events).as("[the wrapped model is stopped after the resume returned]").containsSubsequence("resumed:n1", "stop");
        } finally {
            resumeOfN1.open();
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    // Returns as soon as the condition holds, or when the window has passed without it
    private static void happensWithin(Duration window, BooleanSupplier condition) {
        try {
            await().atMost(window).pollInterval(1, MILLISECONDS).until(condition::getAsBoolean);
        } catch (ConditionTimeoutException ignored) {
            // The window passed, which the assertions that follow are about
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

    // A model of a user's own that holds a subscription paused when asked to, and whose resume of a subscription waits at
    // its gate once. The wait is outside the monitor of this model, so it holds up nothing but the call that waits. It
    // records when a resume returns and when it is stopped, in that order.
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final AtomicInteger stopCalls = new AtomicInteger();
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
            stopCalls.incrementAndGet();
            events.add("stop");
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
            Gate gate = resumeGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                if (!pausedIds.remove(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
                }
                runningIds.add(subscriptionId);
                events.add("resumed:" + subscriptionId);
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
