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
 * A lease callback that reaches the model while subscribe(..) holds the subscription's lock, and before the consumer is
 * recorded, finds nothing recorded to act on. The callback must not be lost, so the subscription ends up where the lease
 * is, whether the lease was lost or granted in that window.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerLeaseCallbackDuringSubscribeTest {

    private static final String NODE = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);

    @Test
    void a_lease_lost_while_subscribe_resumes_the_subscription_in_the_wrapped_model_leaves_it_not_running_there() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ExecutorService otherThreads = Executors.newCachedThreadPool();
        Gate resumeOfS1 = new Gate();
        wrapped.resumeGates.put("s1", resumeOfS1);
        CompletableFuture<Subscription> subscribing = null;
        try {
            subscribing = CompletableFuture.supplyAsync(() -> subscribe(model, "s1"), otherThreads);
            assertThat(resumeOfS1.awaitEnteredOnAnotherThread()).as("subscribe waits inside the resume of s1 in the wrapped model, holding the lock of s1").isTrue();

            // The lease moves to another node while subscribe holds the lock, and the strategy tells the model
            strategy.holders.remove("s1");
            deliverAndAwaitSettled(() -> model.onConsumeProhibited("s1", NODE));
        } finally {
            resumeOfS1.open();
        }
        try {
            assertThat(subscribing).as("subscribe of s1 once the wrapped model resumed it").succeedsWithin(EVENTUALLY);

            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrapped.isRunning("s1"))
                    .as("[s1 runs in the wrapped model although this node lost its lease]").isFalse());
        } finally {
            otherThreads.shutdownNow();
            model.shutdown();
        }
    }

    @Test
    void a_lease_granted_between_the_subscribe_finding_it_not_held_and_recording_the_subscription_as_waiting_leaves_it_running() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        strategy.grantOnRegister = false;
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        // subscribe asks whether this node holds the lease and is told no, while the lease is granted to it right then
        strategy.onTheFirstHasLock.set(() -> {
            strategy.holders.add("s1");
            deliverAndAwaitSettled(() -> model.onConsumeGranted("s1", NODE));
        });
        try {
            subscribe(model, "s1");

            await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrapped.isRunning("s1"))
                    .as("[s1 runs once this node holds its lease]").isTrue());
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_lease_lost_after_subscribe_returned_leaves_the_subscription_not_running_in_the_wrapped_model() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            subscribe(model, "s1");
            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model with the lease it won").isTrue();

            strategy.holders.remove("s1");
            model.onConsumeProhibited("s1", NODE);

            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model after this node lost its lease").isFalse();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_lease_granted_after_subscribe_returned_makes_a_subscription_that_waited_for_it_run() {
        WrappedModel wrapped = new WrappedModel();
        Strategy strategy = new Strategy();
        strategy.grantOnRegister = false;
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        try {
            subscribe(model, "s1");
            assertThat(wrapped.isRunning("s1")).as("s1 does not run in the wrapped model without the lease").isFalse();

            strategy.holders.add("s1");
            model.onConsumeGranted("s1", NODE);

            assertThat(wrapped.isRunning("s1")).as("s1 runs in the wrapped model once this node holds its lease").isTrue();
        } finally {
            model.shutdown();
        }
    }

    private static Subscription subscribe(CompetingConsumerSubscriptionModel model, String subscriptionId) {
        return model.subscribe(NODE, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    // Calls as the strategy's notifier would, from a thread of its own, and waits until the call has returned or is
    // waiting for something, which a callback that waits for the subscription's lock does
    private static void deliverAndAwaitSettled(Runnable callback) {
        CompletableFuture<Void> returned = new CompletableFuture<>();
        Thread notifier = new Thread(() -> {
            try {
                callback.run();
                returned.complete(null);
            } catch (Throwable e) {
                returned.completeExceptionally(e);
            }
        }, "test-lease-notifier");
        notifier.setDaemon(true);
        notifier.start();
        await().atMost(EVENTUALLY).pollInterval(1, MILLISECONDS).until(() -> returned.isDone() || notifier.getState() == Thread.State.WAITING);
        assertThat(returned.isCompletedExceptionally()).as("the lease callback threw").isFalse();
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

    // Grants a lease on register unless told not to, and answers hasLock from its holders. The first hasLock can run a hook
    // and is then answered no.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final AtomicReference<@Nullable Runnable> onTheFirstHasLock = new AtomicReference<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private volatile boolean grantOnRegister = true;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
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
            Runnable hook = onTheFirstHasLock.getAndSet(null);
            if (hook != null) {
                hook.run();
                return false;
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

    // A model of a user's own that holds a subscription paused when asked to, and whose resume of a subscription waits at
    // its gate once. The wait is outside the monitor of this model, so it holds up nothing but the call that waits.
    private static final class WrappedModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final Map<String, Gate> resumeGates = new ConcurrentHashMap<>();
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            synchronized (this) {
                return make(subscriptionId, false);
            }
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
            Gate gate = resumeGates.remove(subscriptionId);
            if (gate != null) {
                gate.pass();
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
