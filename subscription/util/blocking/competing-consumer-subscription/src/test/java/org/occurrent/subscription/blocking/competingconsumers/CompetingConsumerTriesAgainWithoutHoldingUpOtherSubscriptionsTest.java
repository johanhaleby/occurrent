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

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A call to the lease strategy or the wrapped model that fails for one subscription is tried again on a thread of its
 * own. Neither that try, nor a release of a lease that hangs or throws on shutdown, may hold up the calls for another
 * subscription, and a try that decides from what it finds keeps a user's pause and a stop() in force.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerTriesAgainWithoutHoldingUpOtherSubscriptionsTest {

    private static final String SUBSCRIBER = "node";
    private static final Duration DOES_NOT_WAIT = Duration.ofSeconds(2);

    @Test
    void a_try_that_waits_for_the_lease_strategy_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate registerOfS1 = new Gate();
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            model.pauseSubscription("s1");
            // Resuming s1 fails to register it, and the try that follows waits inside the registration
            strategy.registerFailsOnce.add("s1");
            strategy.registerGates.put("s1", registerOfS1);
            model.resumeSubscription("s1");
            assertThat(registerOfS1.awaitEnteredOnAnotherThread()).as("a try of s1 waits inside the registration of s1 on a thread of its own").isTrue();

            // A lease loss for s2, and then a grant, and then a pause
            strategy.holders.remove("s2");
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeProhibited("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeProhibited for s2 while a try of s1 waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);
            strategy.holders.add("s2");
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeGranted("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeGranted for s2 while a try of s1 waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);
            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while a try of s1 waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);

            registerOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once the registration returned, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            registerOfS1.open();
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_try_that_waits_for_the_wrapped_model_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate pauseOfS1 = new Gate();
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            // The first pause of s1 fails, which starts a try, and the pause that try makes waits
            wrapped.pauseFailsOnce.add("s1");
            wrapped.blockPauseOnCall.put("s1", new CallGate(2, pauseOfS1));
            strategy.holders.remove("s1");
            model.onConsumeProhibited("s1", SUBSCRIBER);
            assertThat(pauseOfS1.awaitEnteredOnAnotherThread()).as("a try of s1 waits inside the pause of s1 in the wrapped model on a thread of its own").isTrue();

            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while a try of s1 waits inside the wrapped model").succeedsWithin(DOES_NOT_WAIT);

            pauseOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isPaused("s1")).as("s1 is paused in the wrapped model once its pause returned, running=" + wrapped.isRunning("s1")).isTrue());
        } finally {
            pauseOfS1.open();
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_grant_for_a_subscription_whose_try_is_under_way_returns_and_the_subscription_runs_once_the_try_is_done() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate registerOfS1 = new Gate();
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1");
            model.pauseSubscription("s1");
            // Another node holds s1 by the time the registration returns, so the registration does not win it
            strategy.heldElsewhere.add("s1");
            strategy.registerFailsOnce.add("s1");
            strategy.registerGates.put("s1", registerOfS1);
            model.resumeSubscription("s1");
            assertThat(registerOfS1.awaitEnteredOnAnotherThread()).as("a try of s1 waits inside the registration of s1 on a thread of its own").isTrue();

            // The lease comes to this node while the try waits
            strategy.holders.add("s1");
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeGranted("s1", SUBSCRIBER), otherThreads))
                    .as("onConsumeGranted for s1 while a try of s1 waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);

            registerOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once the try is done, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            registerOfS1.open();
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void shutdown_returns_while_the_release_of_a_lease_hangs() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate unregisterOfS1 = new Gate();
        CompletableFuture<Void> shuttingDown = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2", "s3");
            strategy.unregisterGates.put("s1", unregisterOfS1);

            shuttingDown = CompletableFuture.runAsync(model::shutdown, otherThreads);

            assertThat(shuttingDown).as(() -> "shutdown() while the release of the lease of s1 hangs, unregister called for " + strategy.unregisterCalls).succeedsWithin(Duration.ofSeconds(10));
            assertThat(strategy.unregisterCalls).as("consumers whose lease shutdown() released").contains("s2", "s3");
            assertThat(strategy.holders).as("leases held once shutdown() had returned").doesNotContain("s2", "s3");
            assertThat(strategy.listeners).as("model still listening to the lease strategy once shutdown() had returned").isEmpty();
        } finally {
            unregisterOfS1.open();
            if (shuttingDown != null) {
                shuttingDown.orTimeout(5, SECONDS).exceptionally(__ -> null).join();
            }
            otherThreads.shutdownNow();
        }
    }

    @Test
    void shutdown_releases_every_other_lease_when_one_release_throws() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2", "s3", "s4", "s5");
        strategy.unregisterFailsFor.add("s3");

        Throwable failure = catchThrowable(model::shutdown);

        assertThat(failure).as("failure of shutdown() when the release of the lease of s3 throws").isNull();
        assertThat(strategy.registered).as("consumers still registered once shutdown() had returned").isSubsetOf("s3");
        assertThat(strategy.holders).as("leases held once shutdown() had returned").isSubsetOf("s3");
        assertThat(strategy.listeners).as("model still listening to the lease strategy once shutdown() had returned").isEmpty();
    }

    @Test
    void a_user_pause_that_takes_effect_and_then_throws_stays_paused_when_the_wrapped_model_cannot_say_whether_it_runs() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1");
            // The pause takes effect and throws, and the check of whether s1 runs, right after that, throws too
            wrapped.pauseFailsOnceAfterTakingEffect.add("s1");
            wrapped.isRunningFailsOnceAfterThatFailure.add("s1");

            Throwable failure = catchThrowable(() -> model.pauseSubscription("s1"));

            assertThat(failure).as("the pause of s1").isNotNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(threadsTryingAgain("s1")).as("threads trying s1 again").isEmpty());
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after the user paused it").isFalse();
            assertThat(wrapped.isPaused("s1")).as("s1 is paused in the wrapped model after the user paused it").isTrue();
            assertThat(strategy.registered).as("registrations of a subscription the user paused").doesNotContain("s1");
            assertThat(strategy.holders).as("leases held for a subscription the user paused").doesNotContain("s1");
            assertThat(model.isPaused("s1")).as("s1 is paused according to the model").isTrue();

            model.resumeSubscription("s1");
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once the user resumed it, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_stop_whose_check_of_the_wrapped_model_throws_gives_up_the_lease_and_start_resumes_the_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        try {
            subscribeRunningWithLease(model, wrapped, strategy, s1Received::add, "s1");
            // stop() first asks the wrapped model whether it runs at all, and then whether it runs s1
            wrapped.isRunningFailsOnce.add("s1");

            Throwable failure = catchThrowable(model::stop);

            assertThat(failure).as("stop() when the check of whether the wrapped model runs s1 throws").isNotNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered.contains("s1") || strategy.holders.contains("s1"))
                    .as("s1 still registered or holding its lease after stop(), registered=" + strategy.registered + ", holders=" + strategy.holders).isFalse());

            model.start(true);
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after start(true), holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
            wrapped.write("e1");
            assertThat(s1Received).as("events s1 received after start(true)").containsExactly("e1");
        } finally {
            model.shutdown();
        }
    }

    @Test
    void resume_returns_and_the_subscription_runs_when_asking_the_wrapped_model_whether_it_runs_throws() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1");
            model.pauseSubscription("s1");
            wrapped.isRunningFailsOnce.add("s1");

            Throwable failure = catchThrowable(() -> model.resumeSubscription("s1"));

            assertThat(failure).as("the resume of s1 when asking the wrapped model whether it runs s1 throws").isNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once the user resumed it, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_grant_whose_check_of_the_wrapped_model_throws_is_tried_again_until_the_subscription_runs() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1");
            // The wrapped model stops running s1 behind the back of the model, which still records it as running
            wrapped.stopRunningSilently("s1");
            wrapped.isRunningFailsOnce.add("s1");

            Throwable failure = catchThrowable(() -> model.onConsumeGranted("s1", SUBSCRIBER));

            assertThat(failure).as("onConsumeGranted for s1 when asking the wrapped model whether it runs s1 throws").isNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after its grant, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void stop_gives_up_the_registration_of_every_subscription_being_made_when_asking_the_wrapped_model_about_one_throws() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate makingS1 = new Gate();
        Gate makingS2 = new Gate();
        List<CompletableFuture<Subscription>> subscribing = new ArrayList<>();
        try {
            wrapped.subscribeGates.put("s1", makingS1);
            wrapped.subscribeGates.put("s2", makingS2);
            subscribing.add(CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIBER, "s1", null, StartAt.subscriptionModelDefault(), __ -> {}), otherThreads));
            subscribing.add(CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIBER, "s2", null, StartAt.subscriptionModelDefault(), __ -> {}), otherThreads));
            assertThat(makingS1.awaitEnteredOnAnotherThread()).as("the subscribe of s1 waits inside the wrapped model after its registration returned").isTrue();
            assertThat(makingS2.awaitEnteredOnAnotherThread()).as("the subscribe of s2 waits inside the wrapped model after its registration returned").isTrue();
            assertThat(strategy.registered).as("registrations while both subscribes wait inside the wrapped model").containsExactlyInAnyOrder("s1", "s2");
            // stop() asks whether the wrapped model runs s1 before it gets to s2, which is the order the subscriptions being made are kept in
            wrapped.isRunningFailsOnce.add("s1");

            catchThrowable(model::stop);

            assertThat(strategy.registered).as("registrations of s2 once stop() had returned, which a stopped model must not compete with").doesNotContain("s2");
        } finally {
            makingS1.open();
            makingS2.open();
            subscribing.forEach(subscribe -> subscribe.orTimeout(5, SECONDS).exceptionally(__ -> null).join());
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * Checks what a try that calls without the model's monitor could break. It also passes when a try holds the
     * monitor, since stop() then waits for the try.
     */
    @Test
    void a_stop_during_a_try_that_resumes_the_subscription_leaves_the_wrapped_model_stopped() throws Exception {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        Gate resumeOfS1 = new Gate();
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1");
            model.pauseSubscription("s1");
            // Resuming s1 fails to register it, and the try that follows waits inside the resume in the wrapped model
            strategy.registerFailsOnce.add("s1");
            wrapped.resumeGates.put("s1", resumeOfS1);
            model.resumeSubscription("s1");
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("a try of s1 waits inside the resume of s1 in the wrapped model on a thread of its own").isTrue();

            Thread stopping = runOnAnotherThreadUntilDoneOrBlocked(model::stop);
            resumeOfS1.open();
            stopping.join(SECONDS.toMillis(5));
            assertThat(stopping.isAlive()).as("stop() returned once the try was let go").isFalse();

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after stop() returned, registered=" + strategy.registered + ", holders=" + strategy.holders).isFalse());
            assertThat(wrapped.isRunning()).as("wrapped model running after stop() returned").isFalse();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once stop() returned").doesNotContain("s1"));
        } finally {
            resumeOfS1.open();
            model.shutdown();
        }
    }

    private static void subscribeRunningWithLease(CompetingConsumerSubscriptionModel model, WrappedModel wrapped, Strategy strategy, String... subscriptionIds) {
        subscribeRunningWithLease(model, wrapped, strategy, __ -> {}, subscriptionIds);
    }

    private static void subscribeRunningWithLease(CompetingConsumerSubscriptionModel model, WrappedModel wrapped, Strategy strategy, Consumer<String> eventIds, String... subscriptionIds) {
        for (String subscriptionId : subscriptionIds) {
            model.subscribe(SUBSCRIBER, subscriptionId, null, StartAt.subscriptionModelDefault(), e -> eventIds.accept(e.getId()));
            assertThat(wrapped.isRunning(subscriptionId)).as("%s runs in the wrapped model once subscribed", subscriptionId).isTrue();
            assertThat(strategy.hasLock(subscriptionId, SUBSCRIBER)).as("lease of %s held once subscribed", subscriptionId).isTrue();
        }
    }

    private static ExecutorService otherThreads() {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "test-other-thread");
            thread.setDaemon(true);
            return thread;
        });
    }

    // Threads of the model that try something for the subscription again
    private static List<String> threadsTryingAgain(String subscriptionId) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith("occurrent-competing-consumer-") && name.endsWith("-" + subscriptionId))
                .toList();
    }

    // Runs the call on a new thread and returns once it has returned, or once it waits for a monitor, so a call that
    // waits for a try that holds the monitor does not wait for good
    private static Thread runOnAnotherThreadUntilDoneOrBlocked(Runnable call) {
        Thread thread = new Thread(call, "test-other-thread");
        thread.setDaemon(true);
        thread.start();
        long deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return thread;
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

    // The call of a subscription, counted from one, that waits at the gate
    private record CallGate(int call, Gate gate) {
    }

    // Grants a lease unless another node holds it, and records the registrations and the unregisters it is asked for.
    // A registration or an unregister of a subscription can wait at a gate, and a registration can fail once while an
    // unregister can fail every time.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> heldElsewhere = ConcurrentHashMap.newKeySet();
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Set<String> registerFailsOnce = ConcurrentHashMap.newKeySet();
        private final Set<String> unregisterFailsFor = ConcurrentHashMap.newKeySet();
        // A registration waits at its gate once
        private final Map<String, Gate> registerGates = new ConcurrentHashMap<>();
        // An unregister waits at its gate every time, until the gate is opened
        private final Map<String, Gate> unregisterGates = new ConcurrentHashMap<>();
        private final List<String> unregisterCalls = new CopyOnWriteArrayList<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (registerFailsOnce.remove(subscriptionId)) {
                throw new IllegalStateException("transient register failure");
            }
            Gate gate = registerGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            registered.add(subscriptionId);
            if (heldElsewhere.contains(subscriptionId)) {
                return false;
            }
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            unregisterCalls.add(subscriptionId);
            Gate gate = unregisterGates.get(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            if (unregisterFailsFor.contains(subscriptionId)) {
                throw new IllegalStateException("unregister failure for " + subscriptionId);
            }
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

    // A model of a user's own that implements subscribePaused, holds a subscription made while it is stopped paused, pauses
    // its subscriptions when it is stopped and starts each subscription at the end of its log. A call of it can fail once,
    // also after it took effect, or wait at a gate before it does anything, and the answer to whether it runs a
    // subscription can throw once, or once the next time after a pause failed. It can also stop running a subscription
    // with no call.
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Set<String> pauseFailsOnce = ConcurrentHashMap.newKeySet();
        private final Set<String> pauseFailsOnceAfterTakingEffect = ConcurrentHashMap.newKeySet();
        private final Set<String> isRunningFailsOnce = ConcurrentHashMap.newKeySet();
        // Armed by the pause that failed after it took effect, and then works as isRunningFailsOnce
        private final Set<String> isRunningFailsOnceAfterThatFailure = ConcurrentHashMap.newKeySet();
        private final Map<String, Integer> pauseCalls = new ConcurrentHashMap<>();
        private final Map<String, CallGate> blockPauseOnCall = new ConcurrentHashMap<>();
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final Map<String, Gate> subscribeGates = new ConcurrentHashMap<>();
        private boolean running = true;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private final List<String> log = new ArrayList<>();
        private final Map<String, Consumer<CloudEvent>> actions = new HashMap<>();
        private final Map<String, Integer> positions = new HashMap<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, action, false);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, action, true);
        }

        private Subscription make(String subscriptionId, Consumer<CloudEvent> action, boolean paused) {
            Gate gate = subscribeGates.get(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                if (runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId)) {
                    throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
                }
                (running && !paused ? runningIds : pausedIds).add(subscriptionId);
                actions.put(subscriptionId, action);
                positions.put(subscriptionId, log.size());
            }
            return new WrappedSubscription(subscriptionId);
        }

        private synchronized void write(String eventId) {
            log.add(eventId);
            List.copyOf(runningIds).forEach(this::deliver);
        }

        private void deliver(String subscriptionId) {
            for (int position = positions.get(subscriptionId); position < log.size(); position++) {
                actions.get(subscriptionId).accept(CloudEventBuilder.v1().withId(log.get(position)).withSource(URI.create("urn:wrapped")).withType("written").build());
                positions.put(subscriptionId, position + 1);
            }
        }

        private synchronized void stopRunningSilently(String subscriptionId) {
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
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
                List.copyOf(runningIds).forEach(this::deliver);
            }
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (isRunningFailsOnce.remove(subscriptionId)) {
                throw new IllegalStateException("transient isRunning failure");
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
            Gate gate = resumeGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
            }
            synchronized (this) {
                if (!pausedIds.remove(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
                }
                runningIds.add(subscriptionId);
                deliver(subscriptionId);
                return new WrappedSubscription(subscriptionId);
            }
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            int call = pauseCalls.merge(subscriptionId, 1, Integer::sum);
            CallGate callGate = blockPauseOnCall.get(subscriptionId);
            if (callGate != null && callGate.call() == call) {
                callGate.gate().pass();
            }
            if (pauseFailsOnce.remove(subscriptionId)) {
                throw new IllegalStateException("transient pause failure");
            }
            boolean failsAfterTakingEffect = pauseFailsOnceAfterTakingEffect.remove(subscriptionId);
            synchronized (this) {
                if (runningIds.remove(subscriptionId)) {
                    pausedIds.add(subscriptionId);
                }
            }
            if (failsAfterTakingEffect) {
                if (isRunningFailsOnceAfterThatFailure.remove(subscriptionId)) {
                    isRunningFailsOnce.add(subscriptionId);
                }
                throw new IllegalStateException("transient pause failure after taking effect");
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
