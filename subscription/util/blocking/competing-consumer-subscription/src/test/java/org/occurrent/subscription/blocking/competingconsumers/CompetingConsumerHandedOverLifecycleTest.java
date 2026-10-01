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
import org.junit.jupiter.api.Test;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscription whose lock another call holds when a start(..) or stop() is applied to it is handed over to a thread
 * of its own. Once that thread is done, the subscription is where it would be had each start(..) and stop() found its
 * lock free, whether this model or the user paused it and whether the wrapped model runs included. That holds for
 * every order of two and of three calls, from a running and from a stopped model, both when the later calls come while
 * the lock is still held and when they come while that thread applies the first.
 */
class CompetingConsumerHandedOverLifecycleTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);

    @TestFactory
    Stream<DynamicTest> a_subscription_handed_over_ends_where_it_would_with_its_lock_free() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Initially initially : Initially.values()) {
            for (List<Call> calls : everyOrderOfTwoAndOfThreeCalls()) {
                for (When when : When.values()) {
                    String described = calls.stream().map(call -> call.description).collect(Collectors.joining(", then "));
                    String name = "from a " + initially.description + " model, " + described + " " + when.description;
                    tests.add(DynamicTest.dynamicTest(name, () -> {
                        State expected = withTheLockFree(initially, calls);
                        State actual = handedOver(initially, calls, when);
                        assertThat(actual).as("[s1 once the thread it was handed over to is done, from a %s model, %s %s]",
                                initially.description, described, when.description).isEqualTo(expected);
                    }));
                }
            }
        }
        return tests.stream();
    }

    private static List<List<Call>> everyOrderOfTwoAndOfThreeCalls() {
        List<List<Call>> orders = new ArrayList<>();
        for (Call first : Call.values()) {
            for (Call second : Call.values()) {
                orders.add(List.of(first, second));
                for (Call third : Call.values()) {
                    orders.add(List.of(first, second, third));
                }
            }
        }
        return orders;
    }

    // A stop() that begins after the thread s1 was handed to has failed to apply start(true) to it, and finds the lock
    // of s1 free, applies that start(true) itself before pausing s1
    @Test
    void a_stop_that_applies_a_start_handed_over_before_it_pauses_the_subscription_as_by_the_user() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            Gate backingOff = fixture.handedOverThreadAboutToBackOff();
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            // A RuntimeException would go to the try of s1 and count as applied, so the thread s1 is handed to gets an
            // Error, and then stands before its backoff with the lock of s1 free
            fixture.wrapped.errorsFromIsRunningOfS1OnALifecycleThread.set(1);
            fixture.model.start(true);
            grantHoldingTheLock.open();
            assertThat(backingOff.awaitEntered()).as("the thread s1 was handed to failed and let go of the lock").isTrue();

            fixture.model.stop();
            fixture.model.start(false);
            backingOff.open();
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once start(true), stop() and start(false) are applied")
                    .isEqualTo(new State(true, true, false, false, false));
        } finally {
            fixture.model.shutdown();
        }
    }

    // A grant that took the lock of s1 before stop() began comes before that stop(), also when a start(false) after
    // the stop() has begun by the time the grant runs s1
    @Test
    void a_grant_holding_the_lock_when_stop_begins_comes_before_that_stop() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.strategy.leaseHeldAfterTheGate.set(true);
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            fixture.model.start(true);
            fixture.model.stop();
            fixture.model.start(false);
            grantHoldingTheLock.open();
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once the grant, start(true), stop() and start(false) are applied")
                    .isEqualTo(new State(true, true, false, false, false));
        } finally {
            fixture.model.shutdown();
        }
    }

    // A failure of a start(..) handed over is tried again by the thread it was handed to, also when a later start(..)
    // finds the lock free first and fails to apply it
    @Test
    void a_start_handed_over_for_a_subscription_that_does_not_compete_is_tried_until_it_is_applied() {
        Fixture fixture = new Fixture(Initially.STOPPED);
        try {
            fixture.model.subscribe(NODE, "n1", null, StartAt.dynamic(__ -> null), __ -> {
            });
            Gate backingOff = fixture.handedOverThreadAboutToBackOff();
            Gate pauseHoldingTheLock = new Gate();
            fixture.wrapped.nextIsPausedOfN1OnTheTestThread.set(pauseHoldingTheLock);
            CompletableFuture<Void> pause = runOnTheTestThread(() -> fixture.model.pauseSubscription("n1"));
            assertThat(pauseHoldingTheLock.awaitEntered()).as("the pause of n1 holds its lock").isTrue();
            // Fails once on the thread n1 is handed to, which then stands before its backoff with the lock of n1 free,
            // and twice more, so the second start(true) fails to apply the first and, unless it hands both back to that
            // thread, its own
            fixture.wrapped.failuresFromResumingN1.set(3);
            fixture.model.start(true);
            pauseHoldingTheLock.open();
            assertThat(pause).as("the pause of n1").failsWithin(EVENTUALLY);
            assertThat(backingOff.awaitEntered()).as("the thread n1 was handed to failed and let go of the lock").isTrue();

            Throwable thrownBySecondStart = catchThrowable(() -> fixture.model.start(true));
            assertThat(fixture.wrapped.resumeFailuresOfN1.awaitCount(2)).as("the second start(true) failed to apply the first").isTrue();
            backingOff.open();

            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(fixture.wrapped.isRunning("n1")).as("n1 runs in the wrapped model").isTrue());
            assertThat(thrownBySecondStart).as("what the second start(true) threw").isNull();
        } finally {
            fixture.model.shutdown();
        }
    }

    // A resume that took the lock of s1 before stop() began runs first, and stop() pauses s1 after it
    @Test
    void a_resume_under_way_when_stop_begins_ends_paused_by_the_user() {
        Fixture fixture = new Fixture(Initially.RUNNING);
        try {
            fixture.model.pauseSubscription("s1");
            Gate resumeHoldingTheLock = new Gate();
            fixture.wrapped.nextIsRunningOfS1OnTheTestThread.set(resumeHoldingTheLock);
            CompletableFuture<Void> resume = runOnTheTestThread(() -> fixture.model.resumeSubscription("s1"));
            assertThat(resumeHoldingTheLock.awaitEntered()).as("the resume of s1 holds its lock").isTrue();

            fixture.model.stop();
            resumeHoldingTheLock.open();
            assertThat(resume).as("the resume of s1").succeedsWithin(EVENTUALLY);
            awaitNothingLeftForS1();

            assertThat(fixture.state()).as("s1 once the resume and stop() are applied")
                    .isEqualTo(new State(true, true, false, false, false));
        } finally {
            fixture.model.shutdown();
        }
    }

    private static CompletableFuture<Void> runOnTheTestThread(Runnable call) {
        CompletableFuture<Void> called = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                call.run();
                called.complete(null);
            } catch (Throwable e) {
                called.completeExceptionally(e);
            }
        }, WrappedModel.TEST_THREAD);
        thread.setDaemon(true);
        thread.start();
        return called;
    }

    private static State withTheLockFree(Initially initially, List<Call> calls) {
        Fixture fixture = new Fixture(initially);
        try {
            calls.forEach(call -> call.apply(fixture.model));
            awaitNothingLeftForS1();
            return fixture.state();
        } finally {
            fixture.model.shutdown();
        }
    }

    private static State handedOver(Initially initially, List<Call> calls, When when) {
        Fixture fixture = new Fixture(initially);
        try {
            Gate grantHoldingTheLock = fixture.grantHoldingTheLockOfS1();
            List<Call> later = calls.subList(1, calls.size());
            calls.getFirst().apply(fixture.model);
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
                later.forEach(call -> call.apply(fixture.model));
                applyingTheFirst.open();
            } else {
                later.forEach(call -> call.apply(fixture.model));
                grantHoldingTheLock.open();
            }
            assertThat(fixture.grant).as("the grant of s1").succeedsWithin(EVENTUALLY);
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

    // Paused by the user, or by stop(), is a pause that start(false) keeps
    private record State(boolean pausedInThisModel, boolean pausedByTheUser, boolean runsInTheWrappedModel, boolean holdsTheLease, boolean wrappedModelRunning) {
    }

    // A started model with s1 running, which a stop() stops when the model starts out stopped
    private static final class Fixture {
        private final WrappedModel wrapped = new WrappedModel();
        private final Strategy strategy = new Strategy();
        private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        private final CompletableFuture<Void> grant = new CompletableFuture<>();

        private Fixture(Initially initially) {
            model.subscribe(NODE, "s1", null, StartAt.subscriptionModelDefault(), __ -> {
            });
            if (initially == Initially.STOPPED) {
                model.stop();
            }
        }

        // A thread a subscription is handed to waits at the gate each time it has failed and let go of the lock, before
        // its backoff, until the test opens it
        private Gate handedOverThreadAboutToBackOff() {
            Gate backingOff = new Gate();
            model.runBeforeAHandedOverThreadBacksOff(backingOff::pass);
            return backingOff;
        }

        // A grant for s1 that waits inside hasLock holds the lock of s1, and then finds the lease gone, so the grant
        // itself changes nothing
        private Gate grantHoldingTheLockOfS1() {
            Gate grantHoldingTheLock = new Gate();
            strategy.nextHasLockForS1.set(grantHoldingTheLock);
            Thread granting = new Thread(() -> {
                try {
                    model.onConsumeGranted("s1", NODE);
                    grant.complete(null);
                } catch (Throwable e) {
                    grant.completeExceptionally(e);
                }
            }, "test-granting-thread");
            granting.setDaemon(true);
            granting.start();
            assertThat(grantHoldingTheLock.awaitEntered()).as("the grant of s1 holds its lock").isTrue();
            return grantHoldingTheLock;
        }

        // Applies start(false) last when s1 is paused, which tells whether the user paused it
        private State state() {
            boolean paused = model.isPaused("s1");
            boolean runs = wrapped.isRunning("s1");
            boolean holdsTheLease = strategy.holders.contains("s1");
            boolean wrappedModelRunning = wrapped.isRunning();
            boolean pausedByTheUser = false;
            if (paused) {
                model.start(false);
                awaitNothingLeftForS1();
                pausedByTheUser = model.isPaused("s1");
            }
            return new State(paused, pausedByTheUser, runs, holdsTheLease, wrappedModelRunning);
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

    // How often something happened, which a test can wait for
    private static final class Counter {
        private final AtomicInteger count = new AtomicInteger();

        private void increment() {
            synchronized (count) {
                count.incrementAndGet();
                count.notifyAll();
            }
        }

        private boolean awaitCount(int expected) {
            long deadline = System.nanoTime() + EVENTUALLY.toNanos();
            synchronized (count) {
                while (count.get() < expected) {
                    long left = deadline - System.nanoTime();
                    if (left <= 0) {
                        return false;
                    }
                    try {
                        count.wait(Math.max(1, left / 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                }
                return true;
            }
        }
    }

    // Grants every lease asked for. The next hasLock for s1 waits at a gate once the test sets one, and then answers
    // that the lease is not held, unless the test says it is.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Gate> nextHasLockForS1 = new AtomicReference<>();
        private final AtomicBoolean leaseHeldAfterTheGate = new AtomicBoolean();

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
                    return leaseHeldAfterTheGate.get();
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
    // calls a test waits at, or makes fail, are each set by a field of their own.
    private static final class WrappedModel implements SubscriptionModel {
        private static final String TEST_THREAD = "test-calling-thread";
        private static final String LIFECYCLE_THREAD_OF_S1 = "occurrent-competing-consumer-lifecycle-s1";

        private final AtomicReference<@Nullable Gate> nextIsRunningOfS1OnAHandedOverThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextIsRunningOfS1OnTheTestThread = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> nextIsPausedOfN1OnTheTestThread = new AtomicReference<>();
        private final AtomicInteger errorsFromIsRunningOfS1OnALifecycleThread = new AtomicInteger();
        private final AtomicInteger failuresFromResumingN1 = new AtomicInteger();
        private final Counter resumeFailuresOfN1 = new Counter();
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
            if (subscriptionId.equals("s1")) {
                String thread = Thread.currentThread().getName();
                if (thread.equals(LIFECYCLE_THREAD_OF_S1)) {
                    if (errorsFromIsRunningOfS1OnALifecycleThread.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                        throw new AssertionError("isRunning of s1 failed on " + thread);
                    }
                    passIfSet(nextIsRunningOfS1OnAHandedOverThread);
                } else if (thread.equals(TEST_THREAD)) {
                    passIfSet(nextIsRunningOfS1OnTheTestThread);
                }
            }
            synchronized (this) {
                return running && runningIds.contains(subscriptionId);
            }
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            if (subscriptionId.equals("n1") && Thread.currentThread().getName().equals(TEST_THREAD)) {
                passIfSet(nextIsPausedOfN1OnTheTestThread);
            }
            synchronized (this) {
                return pausedIds.contains(subscriptionId);
            }
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            if (subscriptionId.equals("n1") && failuresFromResumingN1.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                resumeFailuresOfN1.increment();
                throw new IllegalStateException("Resuming n1 failed");
            }
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

        private static void passIfSet(AtomicReference<@Nullable Gate> gate) {
            Gate set = gate.getAndSet(null);
            if (set != null) {
                set.pass();
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
