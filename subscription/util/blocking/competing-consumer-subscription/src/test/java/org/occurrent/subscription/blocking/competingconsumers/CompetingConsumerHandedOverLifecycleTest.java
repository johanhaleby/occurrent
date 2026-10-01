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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A subscription whose lock another call holds when a start(..) or stop() is applied to it is handed over to a thread
 * of its own. Once that thread is done, the subscription is where it would be had each start(..) and stop() found its
 * lock free. That holds for every order of two calls, from a running and from a stopped model, both when the second
 * call comes while the lock is still held and when it comes while that thread applies the first.
 */
class CompetingConsumerHandedOverLifecycleTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);

    @TestFactory
    Stream<DynamicTest> a_subscription_handed_over_ends_where_it_would_with_its_lock_free() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Initially initially : Initially.values()) {
            for (Call first : Call.values()) {
                for (Call second : Call.values()) {
                    for (When when : When.values()) {
                        String name = "from a " + initially.description + " model, " + first.description + " and then " + second.description + " " + when.description;
                        tests.add(DynamicTest.dynamicTest(name, () -> {
                            State expected = withTheLockFree(initially, first, second);
                            State actual = handedOver(initially, first, second, when);
                            assertThat(actual).as("[s1 once the thread it was handed over to is done, from a %s model, %s and then %s %s]",
                                    initially.description, first.description, second.description, when.description).isEqualTo(expected);
                        }));
                    }
                }
            }
        }
        return tests.stream();
    }

    private static State withTheLockFree(Initially initially, Call first, Call second) {
        Fixture fixture = new Fixture(initially);
        try {
            first.apply(fixture.model);
            second.apply(fixture.model);
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    private static State handedOver(Initially initially, Call first, Call second, When when) {
        Fixture fixture = new Fixture(initially);
        try {
            // A grant for s1 that waits inside hasLock holds the lock of s1, and then finds the lease gone, so the grant
            // itself changes nothing
            Gate grantHoldingTheLock = new Gate();
            fixture.strategy.nextHasLockForS1.set(grantHoldingTheLock);
            CompletableFuture<Void> grant = new CompletableFuture<>();
            Thread granting = new Thread(() -> {
                try {
                    fixture.model.onConsumeGranted("s1", NODE);
                    grant.complete(null);
                } catch (Throwable e) {
                    grant.completeExceptionally(e);
                }
            }, "test-granting-thread");
            granting.setDaemon(true);
            granting.start();
            assertThat(grantHoldingTheLock.awaitEntered()).as("the grant of s1 holds its lock").isTrue();

            first.apply(fixture.model);
            if (when == When.WHILE_THE_FIRST_IS_APPLIED) {
                // The first call is applied once the grant returns, and calls into the wrapped model unless it has
                // nothing to do there
                Gate applyingTheFirst = new Gate();
                fixture.wrapped.nextIsRunningOfS1OnAHandedOverThread.set(applyingTheFirst);
                grantHoldingTheLock.open();
                await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> applyingTheFirst.hasEntered() || noThreadLeftForS1());
                boolean takenByTheFirst = fixture.wrapped.nextIsRunningOfS1OnAHandedOverThread.getAndSet(null) == null;
                if (takenByTheFirst) {
                    assertThat(applyingTheFirst.awaitEntered()).as("the first call is being applied to s1").isTrue();
                }
                second.apply(fixture.model);
                applyingTheFirst.open();
            } else {
                second.apply(fixture.model);
                grantHoldingTheLock.open();
            }
            assertThat(grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    // Neither a thread applying a start(..) or stop() to s1, nor a try of s1, is left
    private static void awaitNothingLeftForS1() {
        await().atMost(EVENTUALLY).pollInterval(5, MILLISECONDS).until(CompetingConsumerHandedOverLifecycleTest::noThreadLeftForS1);
    }

    private static boolean noThreadLeftForS1() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .noneMatch(name -> name.equals("occurrent-competing-consumer-lifecycle-s1") || name.equals("occurrent-competing-consumer-reconcile-s1"));
    }

    private enum Initially {
        RUNNING("running"),
        STOPPED("stopped");

        private final String description;

        Initially(String description) {
            this.description = description;
        }
    }

    private enum Call {
        START_RESUMING("start(true)"),
        START_NOT_RESUMING("start(false)"),
        STOP("stop()");

        private final String description;

        Call(String description) {
            this.description = description;
        }

        private void apply(CompetingConsumerSubscriptionModel model) {
            switch (this) {
                case START_RESUMING -> model.start(true);
                case START_NOT_RESUMING -> model.start(false);
                case STOP -> model.stop();
            }
        }
    }

    private enum When {
        WHILE_THE_LOCK_IS_HELD("while the lock is still held"),
        WHILE_THE_FIRST_IS_APPLIED("while the first is being applied");

        private final String description;

        When(String description) {
            this.description = description;
        }
    }

    private record State(boolean pausedInThisModel, boolean runsInTheWrappedModel, boolean holdsTheLease) {
    }

    // A started model with s1 running, which a stop() stops when the model starts out stopped
    private static final class Fixture {
        private final WrappedModel wrapped = new WrappedModel();
        private final Strategy strategy = new Strategy();
        private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);

        private Fixture(Initially initially) {
            model.subscribe(NODE, "s1", null, StartAt.subscriptionModelDefault(), __ -> {
            });
            if (initially == Initially.STOPPED) {
                model.stop();
            }
        }

        private State state() {
            return new State(model.isPaused("s1"), wrapped.isRunning("s1"), strategy.holders.contains("s1"));
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

        private void pass() {
            entered.countDown();
            awaitOrFail(open);
        }

        private boolean hasEntered() {
            return entered.getCount() == 0;
        }

        private boolean awaitEntered() {
            try {
                return entered.await(EVENTUALLY.toMillis(), MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private void open() {
            open.countDown();
        }
    }

    // Grants every lease asked for. The next hasLock for s1 waits at a gate once the test sets one, and then answers
    // that the lease is not held.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Gate> nextHasLockForS1 = new AtomicReference<>();

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
                Gate gate = nextHasLockForS1.getAndSet(null);
                if (gate != null) {
                    gate.pass();
                    return false;
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

    // A model of a user's own, which pauses what it runs when it is stopped and runs nothing while it is stopped. The
    // next isRunning for s1 on the thread s1 was handed over to waits at a gate once the test sets one.
    private static final class WrappedModel implements SubscriptionModel {
        private final AtomicReference<@Nullable Gate> nextIsRunningOfS1OnAHandedOverThread = new AtomicReference<>();
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
        public boolean isRunning(String subscriptionId) {
            if (subscriptionId.equals("s1") && Thread.currentThread().getName().equals("occurrent-competing-consumer-lifecycle-s1")) {
                Gate gate = nextIsRunningOfS1OnAHandedOverThread.getAndSet(null);
                if (gate != null) {
                    gate.pass();
                }
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
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            pausedIds.remove(subscriptionId);
            runningIds.add(subscriptionId);
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
