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
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A start(..) or stop() decides for every subscription this model knows, also one that a subscribe(..) records while
 * it runs, and a stop() does not wait for a resume of a competing subscription, also when another stop() begins
 * behind it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStartAndStopReachEverySubscriptionTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // How long a stop() takes the subscriptions it stops depends on the machine, so the subscribe records s1 at each of
    // these delays in turn, and one of them falls in the middle of it
    private static final int MAX_DELAY_MICROS = 300;
    private static final int DELAY_STEP_MICROS = 10;
    private static final int OTHER_SUBSCRIPTIONS = 3000;

    @Test
    void a_subscription_that_a_subscribe_made_while_stopped_records_while_start_runs_competes_once_start_returned() {
        WrappedModel wrapped = WrappedModel.pausingWhatItRunsOnStop();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            model.stop();
            CompletableFuture<Void> started = new CompletableFuture<>();
            // The subscribe holds s1 here, having found this model stopped, and asks the wrapped model whether it runs
            // s1 before it records s1 as waiting. start(true) runs to its end in between.
            wrapped.isRunningHooks.put("s1", () -> runOnAnotherThreadAndWait(() -> model.start(true), started));

            model.subscribe(NODE, "s1", null, StartAt.subscriptionModelDefault(), __ -> {
            });

            assertThat(started).as("start(true) while the subscribe held s1").isCompleted();
            await().atMost(EVENTUALLY).untilAsserted(() ->
                    assertThat(wrapped.isRunning("s1")).as("[s1 runs once start(true) returned, this node being the only one competing for it]").isTrue());
        } finally {
            model.shutdown();
        }
    }

    // Neither stop() waits for the grant's resume of s1, since the resume of a competing subscription can no longer
    // deliver once a stop() has begun
    @Test
    void a_stop_that_another_stop_begins_behind_returns_without_waiting_for_a_resume_that_can_no_longer_deliver() {
        WrappedModel wrapped = WrappedModel.pausingWhatItRunsOnStop();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = Executors.newCachedThreadPool();
        Gate resumeOfS1 = new Gate();
        try {
            subscribe(model, "s1");
            subscribe(model, "s2");
            strategy.holders.remove("s1");
            model.onConsumeProhibited("s1", NODE);
            assertThat(wrapped.isRunning("s1")).as("s1 paused in the wrapped model once this node lost its lease").isFalse();
            wrapped.resumeGates.put("s1", resumeOfS1);
            strategy.holders.add("s1");
            otherThreads.execute(() -> model.onConsumeGranted("s1", NODE));
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("the grant's resume of s1 waits inside the wrapped model").isTrue();

            CompletableFuture<Boolean> s2RanWhenTheFirstStopReturned = CompletableFuture.supplyAsync(() -> {
                model.stop();
                return wrapped.isRunning("s2");
            }, otherThreads);
            assertThat(s2RanWhenTheFirstStopReturned).as("[the first stop() returned while the resume of s1 waits]").succeedsWithin(EVENTUALLY);
            assertThat(s2RanWhenTheFirstStopReturned.join()).as("[s2 runs in the wrapped model after the first stop() returned]").isFalse();
            CompletableFuture<Void> secondStop = CompletableFuture.runAsync(model::stop, otherThreads);
            assertThat(secondStop).as("[the second stop() returned while the resume of s1 waits]").succeedsWithin(EVENTUALLY);

            resumeOfS1.open();
            await().atMost(EVENTUALLY).untilAsserted(() ->
                    assertThat(wrapped.isPaused("s1")).as("[s1 paused again in the wrapped model once the resume the stop() calls did not wait for returned]").isTrue());
        } finally {
            resumeOfS1.open();
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_stop_while_a_subscribe_records_its_subscription_leaves_that_subscription_holding_no_lease() {
        for (int delayMicros = 0; delayMicros <= MAX_DELAY_MICROS; delayMicros += DELAY_STEP_MICROS) {
            aStopWhileASubscribeRecordsItsSubscription(delayMicros);
        }
    }

    // The subscribe records s1 the given number of microseconds after the stop() has stopped the wrapped model, while
    // stop() goes on to take the subscriptions it stops. Other subscriptions make that take long enough for the
    // subscribe to record s1 in the middle of it.
    private static void aStopWhileASubscribeRecordsItsSubscription(int delayMicros) {
        WrappedModel wrapped = WrappedModel.refusingSubscribePausedAndKeepingItsSubscriptionsOnStop();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        Gate stopOfTheWrappedModel = new Gate();
        CompletableFuture<Void> stopped = new CompletableFuture<>();
        try {
            for (int i = 0; i < OTHER_SUBSCRIPTIONS; i++) {
                subscribe(model, "other-" + i);
            }
            // The last time the subscribe asks whether this node holds the lease of s1, holding s1, before it records
            // s1 as running
            strategy.hasLockHooks.put(3, () -> {
                wrapped.stopGate = stopOfTheWrappedModel;
                Thread stopping = new Thread(() -> {
                    try {
                        model.stop();
                        stopped.complete(null);
                    } catch (Throwable e) {
                        stopped.completeExceptionally(e);
                    }
                }, "test-stopping-thread");
                stopping.setDaemon(true);
                stopping.start();
                stopOfTheWrappedModel.awaitEnteredOnAnotherThread();
                stopOfTheWrappedModel.open();
                spinFor(delayMicros);
            });

            subscribe(model, "s1");

            assertThat(stopped).as("stop() with the subscribe %d microseconds behind", delayMicros).succeedsWithin(EVENTUALLY);
            await().atMost(EVENTUALLY).untilAsserted(() ->
                    assertThat(strategy.holders).as("[s1 holds its lease after stop() returned, recorded %d microseconds after the wrapped model was stopped]", delayMicros)
                            .doesNotContain("s1"));
        } finally {
            stopOfTheWrappedModel.open();
            model.shutdown();
        }
    }

    private static void spinFor(int micros) {
        long until = System.nanoTime() + micros * 1_000L;
        while (System.nanoTime() < until) {
            Thread.onSpinWait();
        }
    }

    private static Subscription subscribe(CompetingConsumerSubscriptionModel model, String subscriptionId) {
        return model.subscribe(NODE, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    private static void runOnAnotherThreadAndWait(Runnable call, CompletableFuture<Void> done) {
        Thread thread = new Thread(() -> {
            try {
                call.run();
                done.complete(null);
            } catch (Throwable e) {
                done.completeExceptionally(e);
            }
        }, "test-calling-thread");
        thread.setDaemon(true);
        thread.start();
        try {
            done.get(EVENTUALLY.toMillis(), MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("The call on another thread did not return", e);
        }
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            if (!latch.await(EVENTUALLY.toMillis(), MILLISECONDS)) {
                throw new IllegalStateException("Timed out waiting for a latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
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
            awaitOrFail(open);
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

    // Grants every lease asked for. A hook runs on the given call of hasLock for s1, before it answers.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final Map<Integer, Runnable> hasLockHooks = new ConcurrentHashMap<>();
        private final AtomicInteger hasLockCallsForS1 = new AtomicInteger();

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
            if (subscriptionId.equals("s1")) {
                Runnable hook = hasLockHooks.remove(hasLockCallsForS1.incrementAndGet());
                if (hook != null) {
                    hook.run();
                }
            }
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

    // A model of a user's own, which runs nothing while it is stopped. One kind pauses what it runs when it is stopped,
    // and the other keeps its subscriptions as they are and refuses subscribePaused. A hook runs once on the next
    // isRunning(id) for an id, a resume waits at its gate once, and so does the next stop() once the test sets a gate
    // for it, all outside the monitor of this model.
    private static final class WrappedModel implements SubscriptionModel {
        private final boolean refusesSubscribePaused;
        private final boolean pausesWhatItRunsOnStop;
        private final Map<String, Runnable> isRunningHooks = new ConcurrentHashMap<>();
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private volatile @Nullable Gate stopGate;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        private WrappedModel(boolean refusesSubscribePaused, boolean pausesWhatItRunsOnStop) {
            this.refusesSubscribePaused = refusesSubscribePaused;
            this.pausesWhatItRunsOnStop = pausesWhatItRunsOnStop;
        }

        private static WrappedModel pausingWhatItRunsOnStop() {
            return new WrappedModel(false, true);
        }

        private static WrappedModel refusingSubscribePausedAndKeepingItsSubscriptionsOnStop() {
            return new WrappedModel(true, false);
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            (running ? runningIds : pausedIds).add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (refusesSubscribePaused) {
                throw new UnsupportedOperationException("subscribePaused");
            }
            pausedIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public void stop() {
            Gate gate = stopGate;
            stopGate = null;
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                running = false;
                if (pausesWhatItRunsOnStop) {
                    pausedIds.addAll(runningIds);
                    runningIds.clear();
                }
            }
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
        public boolean isRunning(String subscriptionId) {
            Runnable hook = isRunningHooks.remove(subscriptionId);
            if (hook != null) {
                hook.run();
            }
            synchronized (this) {
                return running && runningIds.contains(subscriptionId);
            }
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
                pausedIds.remove(subscriptionId);
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
