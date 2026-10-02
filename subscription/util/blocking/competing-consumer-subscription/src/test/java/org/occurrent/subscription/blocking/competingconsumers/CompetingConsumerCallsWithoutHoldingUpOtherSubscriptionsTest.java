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
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A call for one subscription that waits for the lease strategy or the wrapped model holds up no call, grant or lease
 * loss for another subscription, and start() and stop() take every other subscription while a try for one of them
 * waits for the lease strategy.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerCallsWithoutHoldingUpOtherSubscriptionsTest {

    private static final String SUBSCRIBER = "node";
    private static final Duration DOES_NOT_WAIT = Duration.ofSeconds(2);

    @Test
    void a_resume_waiting_for_the_lease_strategy_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate registerOfS1 = new Gate();
        CompletableFuture<?> resumingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            // Paused by the user, so the resume registers s1 again
            model.pauseSubscription("s1");
            strategy.registerGates.put("s1", registerOfS1);
            resumingS1 = CompletableFuture.runAsync(() -> model.resumeSubscription("s1"), otherThreads);
            assertThat(registerOfS1.awaitEnteredOnAnotherThread()).as("resumeSubscription(s1) waits inside the registration of s1").isTrue();

            strategy.holders.remove("s2");
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeProhibited("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeProhibited for s2 while resumeSubscription(s1) waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);
            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while resumeSubscription(s1) waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);

            registerOfS1.open();
            assertThat(resumingS1).as("resumeSubscription(s1) once its registration returned").succeedsWithin(Duration.ofSeconds(5));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once the registration returned, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            registerOfS1.open();
            awaitBounded(resumingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_pause_waiting_for_the_lease_strategy_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate unregisterOfS1 = new Gate();
        CompletableFuture<?> pausingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            strategy.unregisterGates.put("s1", unregisterOfS1);
            pausingS1 = CompletableFuture.runAsync(() -> model.pauseSubscription("s1"), otherThreads);
            assertThat(unregisterOfS1.awaitEnteredOnAnotherThread()).as("pauseSubscription(s1) waits inside the unregister of s1").isTrue();

            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while pauseSubscription(s1) waits inside the unregister of s1").succeedsWithin(DOES_NOT_WAIT);

            unregisterOfS1.open();
            assertThat(pausingS1).as("pauseSubscription(s1) once its unregister returned").succeedsWithin(Duration.ofSeconds(5));
            assertThat(wrapped.isPaused("s1")).as("s1 is paused in the wrapped model once pauseSubscription(s1) returned").isTrue();
            assertThat(strategy.registered).as("registrations once pauseSubscription(s1) returned").doesNotContain("s1");
        } finally {
            unregisterOfS1.open();
            awaitBounded(pausingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_cancel_waiting_for_the_wrapped_model_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate cancelOfS1 = new Gate();
        CompletableFuture<?> cancellingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            wrapped.cancelGates.put("s1", cancelOfS1);
            cancellingS1 = CompletableFuture.runAsync(() -> model.cancelSubscription("s1"), otherThreads);
            assertThat(cancelOfS1.awaitEnteredOnAnotherThread()).as("cancelSubscription(s1) waits inside the cancel of s1 in the wrapped model").isTrue();

            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while cancelSubscription(s1) waits inside the wrapped model").succeedsWithin(DOES_NOT_WAIT);

            cancelOfS1.open();
            assertThat(cancellingS1).as("cancelSubscription(s1) once the wrapped model returned").succeedsWithin(Duration.ofSeconds(5));
            assertThat(wrapped.subscriptionIds()).as("subscriptions of the wrapped model once s1 was cancelled").doesNotContain("s1");
            assertThat(strategy.registered).as("registrations once s1 was cancelled").doesNotContain("s1");
            assertThat(model.subscriptionIds()).as("subscriptions of the model once s1 was cancelled").doesNotContain("s1");
        } finally {
            cancelOfS1.open();
            awaitBounded(cancellingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_grant_waiting_for_the_wrapped_model_to_resume_does_not_hold_up_the_grant_of_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate resumeOfS1 = new Gate();
        CompletableFuture<?> grantingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            loseTheLease(model, strategy, "s1", "s2");
            strategy.holders.add("s1");
            strategy.holders.add("s2");
            wrapped.resumeGates.put("s1", resumeOfS1);
            grantingS1 = CompletableFuture.runAsync(() -> model.onConsumeGranted("s1", SUBSCRIBER), otherThreads);
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("onConsumeGranted for s1 waits inside the resume of s1 in the wrapped model").isTrue();

            assertThat(CompletableFuture.runAsync(() -> model.onConsumeGranted("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeGranted for s2 while onConsumeGranted for s1 waits inside the wrapped model").succeedsWithin(DOES_NOT_WAIT);
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s2")).as("s2 runs in the wrapped model after its grant while the grant of s1 waits").isTrue());

            resumeOfS1.open();
            assertThat(grantingS1).as("onConsumeGranted for s1 once the wrapped model returned").succeedsWithin(Duration.ofSeconds(5));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once its resume returned, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            resumeOfS1.open();
            awaitBounded(grantingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_lease_loss_waiting_for_the_wrapped_model_to_pause_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate pauseOfS1 = new Gate();
        CompletableFuture<?> prohibitingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            wrapped.pauseGates.put("s1", pauseOfS1);
            strategy.holders.remove("s1");
            prohibitingS1 = CompletableFuture.runAsync(() -> model.onConsumeProhibited("s1", SUBSCRIBER), otherThreads);
            assertThat(pauseOfS1.awaitEnteredOnAnotherThread()).as("onConsumeProhibited for s1 waits inside the pause of s1 in the wrapped model").isTrue();

            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while onConsumeProhibited for s1 waits inside the wrapped model").succeedsWithin(DOES_NOT_WAIT);

            pauseOfS1.open();
            assertThat(prohibitingS1).as("onConsumeProhibited for s1 once the wrapped model returned").succeedsWithin(Duration.ofSeconds(5));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isPaused("s1")).as("s1 is paused in the wrapped model once its pause returned").isTrue());
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held after it was lost").isFalse();
        } finally {
            pauseOfS1.open();
            awaitBounded(prohibitingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_grant_waiting_for_the_lease_strategy_to_say_whether_the_lease_is_held_does_not_hold_up_the_lease_loss_of_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate hasLockOfS1 = new Gate();
        CompletableFuture<?> grantingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            loseTheLease(model, strategy, "s1");
            strategy.holders.add("s1");
            strategy.hasLockGates.put("s1", hasLockOfS1);
            grantingS1 = CompletableFuture.runAsync(() -> model.onConsumeGranted("s1", SUBSCRIBER), otherThreads);
            assertThat(hasLockOfS1.awaitEnteredOnAnotherThread()).as("onConsumeGranted for s1 waits inside hasLock for s1").isTrue();

            strategy.holders.remove("s2");
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeProhibited("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeProhibited for s2 while onConsumeGranted for s1 waits inside hasLock for s1").succeedsWithin(DOES_NOT_WAIT);

            hasLockOfS1.open();
            assertThat(grantingS1).as("onConsumeGranted for s1 once hasLock returned").succeedsWithin(Duration.ofSeconds(5));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once hasLock returned, holders=" + strategy.holders).isTrue());
        } finally {
            hasLockOfS1.open();
            awaitBounded(grantingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_subscribe_waiting_for_the_wrapped_model_to_resume_does_not_hold_up_calls_for_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate resumeOfS1 = new Gate();
        CompletableFuture<Subscription> subscribingS1 = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s2");
            // Granted on register and held paused by subscribePaused, so the subscribe resumes s1 in the wrapped model
            wrapped.resumeGates.put("s1", resumeOfS1);
            subscribingS1 = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIBER, "s1", null, StartAt.subscriptionModelDefault(), __ -> {}), otherThreads);
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("the subscribe of s1 waits inside the resume of s1 in the wrapped model").isTrue();

            assertThat(CompletableFuture.runAsync(() -> model.pauseSubscription("s2"), otherThreads))
                    .as("pauseSubscription(s2) while the subscribe of s1 waits inside the wrapped model").succeedsWithin(DOES_NOT_WAIT);

            resumeOfS1.open();
            assertThat(subscribingS1).as("the subscribe of s1 once the wrapped model returned").succeedsWithin(Duration.ofSeconds(5));
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once subscribed").isTrue();
            assertThat(strategy.hasLock("s1", SUBSCRIBER)).as("lease of s1 held while it runs").isTrue();
        } finally {
            resumeOfS1.open();
            awaitBounded(subscribingS1);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_stop_waiting_for_the_lease_strategy_does_not_hold_up_the_grant_of_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate unregisterOfS1 = new Gate();
        CompletableFuture<?> stopping = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            strategy.unregisterGates.put("s1", unregisterOfS1);
            stopping = CompletableFuture.runAsync(model::stop, otherThreads);
            assertThat(unregisterOfS1.awaitEnteredOnAnotherThread()).as("stop() waits inside the unregister of s1").isTrue();

            // A grant, since stop() may or may not have taken s2 already, and a pause of a paused s2 throws
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeGranted("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeGranted for s2 while stop() waits inside the unregister of s1").succeedsWithin(DOES_NOT_WAIT);

            unregisterOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the unregister of s1 returned").doesNotContain("s1"));
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after stop()").isFalse();
            assertThat(wrapped.isRunning()).as("wrapped model running after stop()").isFalse();
        } finally {
            unregisterOfS1.open();
            awaitBounded(stopping);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_start_waiting_for_the_lease_strategy_does_not_hold_up_the_grant_of_another_subscription() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate registerOfS1 = new Gate();
        CompletableFuture<?> starting = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            model.stop();
            strategy.registerGates.put("s1", registerOfS1);
            starting = CompletableFuture.runAsync(() -> model.start(true), otherThreads);
            assertThat(registerOfS1.awaitEnteredOnAnotherThread()).as("start(true) waits inside the registration of s1").isTrue();

            // A grant, since start(true) may or may not have taken s2 already, and a pause of a paused s2 throws
            assertThat(CompletableFuture.runAsync(() -> model.onConsumeGranted("s2", SUBSCRIBER), otherThreads))
                    .as("onConsumeGranted for s2 while start(true) waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);

            registerOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the registration of s1 returned").contains("s1", "s2"));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once its registration returned, holders=" + strategy.holders).isTrue());
        } finally {
            registerOfS1.open();
            awaitBounded(starting);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void stop_returns_and_stops_every_other_subscription_while_a_try_waits_for_the_lease_strategy() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate registerOfS1 = new Gate();
        CompletableFuture<?> stopping = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            model.pauseSubscription("s1");
            // Resuming s1 fails to register it, and the try that follows waits inside the registration
            strategy.registerFailsOnce.add("s1");
            strategy.registerGates.put("s1", registerOfS1);
            model.resumeSubscription("s1");
            assertThat(registerOfS1.awaitEnteredOnAnotherThread()).as("a try of s1 waits inside the registration of s1 on a thread of its own").isTrue();

            stopping = CompletableFuture.runAsync(model::stop, otherThreads);
            assertThat(stopping).as("stop() while a try of s1 waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);
            assertThat(wrapped.isRunning()).as("wrapped model running once stop() returned").isFalse();
            assertThat(wrapped.isRunning("s2")).as("s2 runs in the wrapped model once stop() returned").isFalse();
            assertThat(strategy.registered).as("registrations once stop() returned").doesNotContain("s2");

            registerOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the try of s1 returned from the registration").doesNotContain("s1"));
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after stop()").isFalse();
        } finally {
            registerOfS1.open();
            awaitBounded(stopping);
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void start_returns_and_starts_every_other_subscription_while_a_try_waits_for_the_lease_strategy() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = otherThreads();
        Gate registerOfS1 = new Gate();
        CompletableFuture<?> starting = null;
        try {
            subscribeRunningWithLease(model, wrapped, strategy, "s1", "s2");
            model.stop();
            // Resumed while stopped, s1 fails to register, and the try that follows waits inside the registration
            strategy.registerFailsOnce.add("s1");
            strategy.registerGates.put("s1", registerOfS1);
            model.resumeSubscription("s1");
            assertThat(registerOfS1.awaitEnteredOnAnotherThread()).as("a try of s1 waits inside the registration of s1 on a thread of its own").isTrue();

            starting = CompletableFuture.runAsync(() -> model.start(true), otherThreads);
            assertThat(starting).as("start(true) while a try of s1 waits inside the registration of s1").succeedsWithin(DOES_NOT_WAIT);
            assertThat(strategy.registered).as("registrations once start(true) returned").contains("s2");
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s2")).as("s2 runs in the wrapped model once granted, holders=" + strategy.holders).isTrue());

            registerOfS1.open();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the try of s1 returned from the registration").contains("s1"));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once registered, holders=" + strategy.holders).isTrue());
        } finally {
            registerOfS1.open();
            awaitBounded(starting);
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

    // Paused by the loss of its lease, so a grant resumes it
    private static void loseTheLease(CompetingConsumerSubscriptionModel model, Strategy strategy, String... subscriptionIds) {
        for (String subscriptionId : subscriptionIds) {
            strategy.holders.remove(subscriptionId);
            model.onConsumeProhibited(subscriptionId, SUBSCRIBER);
            assertThat(model.isPaused(subscriptionId)).as("%s paused once its lease was lost", subscriptionId).isTrue();
        }
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

        private static void passIfPresent(@Nullable Gate gate) {
            if (gate != null) {
                gate.pass();
            }
        }
    }

    // Grants a lease on register and records the registrations. A registration can fail once. A registration or a
    // hasLock of a subscription waits at its gate once, and an unregister waits at its gate every time until it opens.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Set<String> registerFailsOnce = ConcurrentHashMap.newKeySet();
        private final Map<String, Gate> registerGates = new ConcurrentHashMap<>();
        private final Map<String, Gate> unregisterGates = new ConcurrentHashMap<>();
        private final Map<String, Gate> hasLockGates = new ConcurrentHashMap<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (registerFailsOnce.remove(subscriptionId)) {
                throw new IllegalStateException("transient register failure");
            }
            Gate.passIfPresent(registerGates.remove(subscriptionId));
            registered.add(subscriptionId);
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            Gate.passIfPresent(unregisterGates.get(subscriptionId));
            registered.remove(subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            Gate.passIfPresent(hasLockGates.remove(subscriptionId));
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

    // A model of a user's own that implements subscribePaused, holds a subscription made while it is stopped paused and
    // pauses its subscriptions when it is stopped. A pause, resume or cancel of a subscription waits at its gate once,
    // outside the monitor of this model, so a gate holds up nothing but the call that waits at it.
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Map<String, Gate> pauseGates = new ConcurrentHashMap<>();
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final Map<String, Gate> cancelGates = new ConcurrentHashMap<>();
        private boolean running = true;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();

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
        public void cancelSubscription(String subscriptionId) {
            Gate.passIfPresent(cancelGates.remove(subscriptionId));
            synchronized (this) {
                runningIds.remove(subscriptionId);
                pausedIds.remove(subscriptionId);
            }
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
            Gate.passIfPresent(resumeGates.remove(subscriptionId));
            synchronized (this) {
                if (!pausedIds.remove(subscriptionId)) {
                    throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
                }
                runningIds.add(subscriptionId);
                return new WrappedSubscription(subscriptionId);
            }
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            Gate.passIfPresent(pauseGates.remove(subscriptionId));
            synchronized (this) {
                if (runningIds.remove(subscriptionId)) {
                    pausedIds.add(subscriptionId);
                }
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
