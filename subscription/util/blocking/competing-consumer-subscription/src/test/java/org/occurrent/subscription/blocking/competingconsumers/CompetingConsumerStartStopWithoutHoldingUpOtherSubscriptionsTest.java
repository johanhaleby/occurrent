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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * start(..) and stop() take every subscription while a call for one of them waits for the lease strategy, a start or
 * stop handed over to a thread of its own is tried again until it succeeds, a call that a stop() refuses is refused
 * without waiting for that stop(), a lease callback on the strategy's thread throws nothing into the strategy, and a
 * stop() waits for a subscribe that runs the subscription in the wrapped model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStartStopWithoutHoldingUpOtherSubscriptionsTest {

    private static final String SUBSCRIBER = "node";
    private static final Duration DOES_NOT_WAIT = Duration.ofSeconds(2);
    private static final List<String> SUBSCRIPTION_IDS = List.of("s1", "s2", "s3", "s4");

    @Test
    void stop_stops_every_other_subscription_while_the_lease_strategy_holds_up_the_unregister_of_one() {
        WrappedModel wrapped = new WrappedModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate firstUnregister = new Gate();
        CompletableFuture<?> stopping = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, SUBSCRIPTION_IDS.toArray(String[]::new));
            strategy.firstUnregister.set(firstUnregister);
            stopping = CompletableFuture.runAsync(model::stop, otherThreads);
            assertThat(firstUnregister.awaitEnteredOnAnotherThread()).as("stop() waits inside the first unregister").isTrue();
            String heldUp = firstUnregister.heldUpFor;

            List<String> others = SUBSCRIPTION_IDS.stream().filter(id -> !id.equals(heldUp)).toList();
            await().atMost(DOES_NOT_WAIT).untilAsserted(() -> assertThat(strategy.registered)
                    .as("registrations while stop() waits inside the unregister of %s", heldUp).doesNotContainAnyElementsOf(others));

            firstUnregister.open();
            assertThat(stopping).as("stop() once the unregister of %s returned", heldUp).succeedsWithin(Duration.ofSeconds(5));
            assertThat(wrapped.isRunning()).as("wrapped model running once stop() returned").isFalse();
            assertThat(wrapped.runningIds()).as("subscriptions running in the wrapped model once stop() returned").isEmpty();
        } finally {
            firstUnregister.open();
            awaitBounded(stopping);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void start_registers_every_other_subscription_while_the_lease_strategy_holds_up_the_register_of_one() {
        WrappedModel wrapped = new WrappedModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate firstRegister = new Gate();
        CompletableFuture<?> starting = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, SUBSCRIPTION_IDS.toArray(String[]::new));
            model.stop();
            assertThat(strategy.registered).as("registrations once stopped").isEmpty();
            strategy.firstRegister.set(firstRegister);
            starting = CompletableFuture.runAsync(() -> model.start(true), otherThreads);
            assertThat(firstRegister.awaitEnteredOnAnotherThread()).as("start(true) waits inside the first registration").isTrue();
            String heldUp = firstRegister.heldUpFor;

            List<String> others = SUBSCRIPTION_IDS.stream().filter(id -> !id.equals(heldUp)).toList();
            await().atMost(DOES_NOT_WAIT).untilAsserted(() -> assertThat(strategy.registered)
                    .as("registrations while start(true) waits inside the registration of %s", heldUp).containsAll(others));

            firstRegister.open();
            assertThat(starting).as("start(true) once the registration of %s returned", heldUp).succeedsWithin(Duration.ofSeconds(5));
            assertThat(strategy.registered).as("registrations once start(true) returned").containsExactlyInAnyOrderElementsOf(SUBSCRIPTION_IDS);
        } finally {
            firstRegister.open();
            awaitBounded(starting);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_start_handed_over_for_a_subscription_that_does_not_compete_is_tried_again_until_it_resumes() {
        WrappedModel wrapped = new WrappedModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate resumeOfNc = new Gate();
        CompletableFuture<?> resumingNc = null;
        CompletableFuture<?> starting = null;
        try {
            model.subscribe(SUBSCRIBER, "nc", null, StartAt.dynamic(__ -> null), __ -> {});
            model.stop();
            assertThat(wrapped.isPaused("nc")).as("nc paused in the wrapped model once stopped").isTrue();
            // The user's resume holds the lock of nc across start(true) and then fails, and so does the start handed over
            wrapped.resumeFailures.put("nc", new AtomicInteger(3));
            wrapped.resumeGates.put("nc", resumeOfNc);
            resumingNc = CompletableFuture.runAsync(() -> model.resumeSubscription("nc"), otherThreads);
            assertThat(resumeOfNc.awaitEnteredOnAnotherThread()).as("resumeSubscription(nc) waits inside the resume of nc in the wrapped model").isTrue();

            starting = CompletableFuture.runAsync(() -> model.start(true), otherThreads);
            assertThat(starting).as("start(true) while resumeSubscription(nc) holds the lock of nc").succeedsWithin(DOES_NOT_WAIT);

            resumeOfNc.open();
            await().atMost(10, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("nc"))
                    .as("nc runs in the wrapped model once a resume there succeeds, resumes failing still=" + wrapped.resumeFailures.get("nc")).isTrue());
        } finally {
            resumeOfNc.open();
            awaitBounded(resumingNc);
            awaitBounded(starting);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_grant_decided_before_a_stop_is_refused_at_once_instead_of_waiting_for_the_stop() {
        WrappedModel wrapped = new WrappedModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate resumeOfS1 = new Gate();
        AtomicReference<@Nullable Thread> stopThread = new AtomicReference<>();
        CompletableFuture<?> resumingS1 = null;
        CompletableFuture<?> grantingS2 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1");
            subscribeWaitingForARival(model, wrapped, strategy, "s2");
            // Paused by the user, so the resume runs s1 in the wrapped model, which the stop() below waits for
            model.pauseSubscription("s1");
            wrapped.resumeGates.put("s1", resumeOfS1);
            resumingS1 = CompletableFuture.runAsync(() -> model.resumeSubscription("s1"), otherThreads);
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("resumeSubscription(s1) waits inside the resume of s1 in the wrapped model").isTrue();

            strategy.rivals.remove("s2");
            strategy.holders.add("s2");
            // The grant has decided to start s2 when a stop() begins and waits for the resume of s1
            wrapped.isRunningHooks.put("s2", () -> stopThread.set(startedStopThatWaits(model)));
            grantingS2 = CompletableFuture.runAsync(() -> model.onConsumeGranted("s2", SUBSCRIBER), otherThreads);

            assertThat(grantingS2.handle((__, ___) -> null))
                    .as("onConsumeGranted for s2 once a stop() began that waits for the resume of s1").succeedsWithin(DOES_NOT_WAIT);
            assertThat(wrapped.isRunning("s2")).as("s2 runs in the wrapped model after a stop() overtook its grant").isFalse();
        } finally {
            resumeOfS1.open();
            awaitBounded(resumingS1);
            awaitBounded(grantingS2);
            joinBounded(stopThread.get());
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_stop_overtaking_a_grant_on_the_strategy_thread_does_not_throw_into_the_strategy() {
        WrappedModel wrapped = new WrappedModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        AtomicReference<@Nullable CompletableFuture<?>> stopping = new AtomicReference<>();
        try {
            subscribeWaitingForARival(model, wrapped, strategy, "s1", "s2");
            strategy.rivals.remove("s1");
            strategy.holders.add("s1");
            // The grant has decided to start s1 when a stop() runs to the end
            wrapped.isRunningHooks.put("s1", () -> {
                CompletableFuture<?> stop = CompletableFuture.runAsync(model::stop, otherThreads);
                stopping.set(stop);
                awaitBounded(stop);
            });

            Throwable thrown = catchThrowable(() -> model.onConsumeGranted("s1", SUBSCRIBER));

            assertThat(thrown).as("what onConsumeGranted for s1 threw into the strategy's thread once a stop() overtook it").isNull();
            assertThat(stopping.get()).as("stop() that overtook the grant of s1").succeedsWithin(Duration.ofSeconds(5));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held after stop()").isFalse());
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after stop()").isFalse();

            model.start(true);
            strategy.rivals.remove("s2");
            strategy.holders.add("s2");
            Throwable thrownByALaterGrant = catchThrowable(() -> model.onConsumeGranted("s2", SUBSCRIBER));
            assertThat(thrownByALaterGrant).as("what a later onConsumeGranted for s2 threw").isNull();
            assertThat(wrapped.isRunning("s2")).as("s2 runs in the wrapped model after a later grant").isTrue();
        } finally {
            awaitBounded(stopping.get());
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_second_stop_while_a_subscribe_made_during_the_first_is_in_the_wrapped_model_leaves_the_wrapped_model_stopped() {
        WrappedModel wrapped = new WrappedModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate subscribeOfS1 = new Gate();
        AtomicReference<@Nullable Throwable> secondStopFailure = new AtomicReference<>();
        Thread secondStop = new Thread(() -> {
            try {
                model.stop();
            } catch (Throwable e) {
                secondStopFailure.set(e);
            }
        }, "test-second-stop");
        secondStop.setDaemon(true);
        CompletableFuture<Subscription> subscribingS1 = null;
        try {
            model.stop();
            // The lease is free, so the subscribe wins it while stopped and subscribes s1 in the wrapped model
            wrapped.subscribeGates.put("s1", subscribeOfS1);
            subscribingS1 = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIBER, "s1", null, StartAt.subscriptionModelDefault(), __ -> {}), otherThreads);
            assertThat(subscribeOfS1.awaitEnteredOnAnotherThread()).as("the subscribe of s1 waits inside the subscribe of s1 in the wrapped model").isTrue();

            secondStop.start();
            await().atMost(5, SECONDS).until(() -> waitsOrHasEnded(secondStop));
            subscribeOfS1.open();

            assertThat(subscribingS1).as("the subscribe of s1 once the wrapped model returned").succeedsWithin(Duration.ofSeconds(5));
            joinBounded(secondStop);
            assertThat(secondStop.isAlive()).as("second stop() still running").isFalse();
            assertThat(secondStopFailure.get()).as("what the second stop() threw").isNull();
            assertThat(wrapped.isRunning()).as("wrapped model running once both stop() calls and the subscribe of s1 returned").isFalse();
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once both stop() calls returned").isFalse();
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held once both stop() calls returned").isFalse();
        } finally {
            subscribeOfS1.open();
            awaitBounded(subscribingS1);
            joinBounded(secondStop);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    private static void subscribeRunningWithLease(CompetingConsumerSubscriptionModel model, WrappedModel wrapped, Strategy strategy, String... subscriptionIds) {
        for (String subscriptionId : subscriptionIds) {
            model.subscribe(SUBSCRIBER, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {});
            assertThat(wrapped.isRunning(subscriptionId)).as("%s runs in the wrapped model once subscribed", subscriptionId).isTrue();
            assertThat(strategy.hasLock(subscriptionId, SUBSCRIBER)).as("lease of %s held once subscribed", subscriptionId).isTrue();
        }
    }

    // Registered while another node holds the lease, so a grant starts it
    private static void subscribeWaitingForARival(CompetingConsumerSubscriptionModel model, WrappedModel wrapped, Strategy strategy, String... subscriptionIds) {
        for (String subscriptionId : subscriptionIds) {
            strategy.rivals.add(subscriptionId);
            model.subscribe(SUBSCRIBER, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {});
            assertThat(wrapped.isRunning(subscriptionId)).as("%s runs in the wrapped model once subscribed while a rival holds its lease", subscriptionId).isFalse();
            assertThat(strategy.registered).as("registrations once %s was subscribed", subscriptionId).contains(subscriptionId);
        }
    }

    // A stop() on a thread of its own, returned once that thread waits for the calls the stop() has to wait for
    private static Thread startedStopThatWaits(CompetingConsumerSubscriptionModel model) {
        Thread stop = new Thread(model::stop, "test-stop");
        stop.setDaemon(true);
        stop.start();
        await().atMost(5, SECONDS).until(() -> stop.getState() == Thread.State.WAITING || !stop.isAlive());
        return stop;
    }

    private static boolean waitsOrHasEnded(Thread thread) {
        Thread.State state = thread.getState();
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING || state == Thread.State.TERMINATED;
    }

    private static ExecutorService otherThreads() {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "test-other-thread");
            thread.setDaemon(true);
            return thread;
        });
    }

    // Bounded, so a call still stuck after its gate opened cannot hang the run
    private static void awaitBounded(@Nullable CompletableFuture<?> call) {
        if (call != null) {
            call.orTimeout(5, SECONDS).exceptionally(__ -> null).join();
        }
    }

    private static void joinBounded(@Nullable Thread thread) {
        if (thread == null || thread.getState() == Thread.State.NEW) {
            return;
        }
        try {
            thread.join(SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // A point that a thread waits at until the test opens it, which tells the test that a thread got there and for
    // which subscription
    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);
        private volatile @Nullable Thread enteredOn;
        private volatile @Nullable String heldUpFor;

        private void pass(String subscriptionId) {
            heldUpFor = subscriptionId;
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

        private static void passIfPresent(@Nullable Gate gate, String subscriptionId) {
            if (gate != null) {
                gate.pass(subscriptionId);
            }
        }
    }

    // Grants a lease on register unless a rival node holds it, and records the registrations. The first register and
    // the first unregister after a gate is set wait at it, whatever the subscription.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Set<String> rivals = ConcurrentHashMap.newKeySet();
        private final AtomicReference<@Nullable Gate> firstRegister = new AtomicReference<>();
        private final AtomicReference<@Nullable Gate> firstUnregister = new AtomicReference<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            Gate.passIfPresent(firstRegister.getAndSet(null), subscriptionId);
            registered.add(subscriptionId);
            if (rivals.contains(subscriptionId)) {
                return false;
            }
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            Gate.passIfPresent(firstUnregister.getAndSet(null), subscriptionId);
            registered.remove(subscriptionId);
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

    // A model of a user's own that pauses its subscriptions when it is stopped. One that refuses subscribePaused starts
    // itself on subscribe, as Spring's does. A subscribe or resume of a subscription waits at its gate once, a resume
    // can fail a number of times, and the first isRunning for a subscription runs its hook. Each of these happens
    // outside the monitor of this model, so it holds up nothing but the call that waits.
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final boolean refusesSubscribePaused;
        private final Map<String, Gate> subscribeGates = new ConcurrentHashMap<>();
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> resumeFailures = new ConcurrentHashMap<>();
        private final Map<String, Runnable> isRunningHooks = new ConcurrentHashMap<>();
        private boolean running = true;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();

        private WrappedModel(boolean refusesSubscribePaused) {
            this.refusesSubscribePaused = refusesSubscribePaused;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Gate.passIfPresent(subscribeGates.remove(subscriptionId), subscriptionId);
            synchronized (this) {
                if (refusesSubscribePaused) {
                    running = true;
                }
                return make(subscriptionId, false);
            }
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (refusesSubscribePaused) {
                throw new UnsupportedOperationException("subscribePaused");
            }
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

        private synchronized Set<String> runningIds() {
            return new HashSet<>(runningIds);
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
            Runnable hook = isRunningHooks.remove(subscriptionId);
            if (hook != null) {
                hook.run();
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
        public Subscription resumeSubscription(String subscriptionId) {
            Gate.passIfPresent(resumeGates.remove(subscriptionId), subscriptionId);
            AtomicInteger failures = resumeFailures.get(subscriptionId);
            if (failures != null && failures.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                throw new IllegalStateException("transient resume failure of " + subscriptionId);
            }
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
