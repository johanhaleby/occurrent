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

/**
 * {@code subscribe(..)} registers with the lease strategy and subscribes in the wrapped model without holding the
 * model's monitor, while lifecycle calls and lease callbacks run on other threads. Whatever runs in between, a
 * subscription delivers only while this node holds its lease, and a failure that goes away does not keep the id
 * refused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscribeConcurrencyTest {

    @Test
    void a_resume_that_starts_the_wrapped_model_while_a_stopped_model_subscribes_delivers_nothing_without_the_lease() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s0", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();

        // Another thread resumes s0, which starts the wrapped model, when the wrapped model is about to subscribe s1
        List<Thread> resuming = new CopyOnWriteArrayList<>();
        delegate.beforeSubscribe = id -> {
            if (id.equals("s1")) {
                delegate.beforeSubscribe = __ -> {};
                resuming.add(runOnAnotherThreadUntilDoneOrBlocked(() -> model.resumeSubscription("s0")));
            }
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.write("e1");
        model.start(false);
        delegate.write("e2");
        strategy.heldElsewhere.remove("s1");
        strategy.holders.add("s1");
        Throwable grantFailure = catchThrowable(() -> strategy.listeners.forEach(l -> l.onConsumeGranted("s1", "node")));
        for (Thread thread : resuming) {
            thread.join(SECONDS.toMillis(5));
        }

        assertThat(s1Received).as("events s1 received on a node that never held its lease").isEmpty();
        assertThat(grantFailure).as("the grant once this node wins the lease of s1").isNull();
        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once this node holds its lease").isTrue();
    }

    @Test
    void a_subscribe_that_a_stop_overtook_does_not_start_the_wrapped_model_after_stop_returned() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CountDownLatch release = new CountDownLatch(1);
        strategy.blockRegister.put("s1", release);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        assertThat(strategy.registerEntered.await(5, SECONDS)).as("the registration of s1 began").isTrue();

        model.stop();
        delegate.afterSubscribe = id -> {
            if (id.equals("s1")) {
                delegate.afterSubscribe = __ -> {};
                delegate.write("written-after-stop-returned");
            }
        };
        release.countDown();
        assertThat(subscribing).succeedsWithin(Duration.ofSeconds(5));
        List<String> nonCompetingReceived = new CopyOnWriteArrayList<>();
        model.subscribe("n1", null, StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault()), e -> nonCompetingReceived.add(e.getId()));
        delegate.write("written-while-stopped");

        assertThat(model.isRunning()).as("model.isRunning() after stop() and the subscribe it overtook returned").isFalse();
        assertThat(s1Received).as("events s1 received after stop() returned").isEmpty();
        assertThat(nonCompetingReceived).as("events a non-competing subscription made while stopped received").isEmpty();
    }

    @Test
    void a_subscribe_that_a_stop_overtook_over_a_model_that_cannot_pause_delivers_nothing_without_its_lease() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CountDownLatch release = new CountDownLatch(1);
        strategy.blockRegister.put("s1", release);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        assertThat(strategy.registerEntered.await(5, SECONDS)).as("the registration of s1 began").isTrue();

        model.stop();
        release.countDown();
        Throwable failure = catchThrowable(() -> subscribing.get(5, SECONDS));
        delegate.write("e1");

        assertThat(failure).as("the subscribe of s1").isNull();
        assertThat(delegate.isRunning("s1") && !strategy.hasLock("s1", "node")).as("s1 runs in the wrapped model without its lease, received=" + s1Received).isFalse();
        assertThat(s1Received).as("events s1 received without its lease").isEmpty();
    }

    @Test
    void a_subscription_that_a_model_which_cannot_pause_runs_when_a_stop_overtakes_the_subscribe_keeps_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Another thread stops the model once the wrapped model runs s1
        delegate.afterSubscribe = id -> {
            if (id.equals("s1")) {
                delegate.afterSubscribe = __ -> {};
                runOnAnotherThreadUntilDoneOrBlocked(model::stop);
            }
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.write("e1");

        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model that cannot pause it").isTrue();
        assertThat(strategy.hasLock("s1", "node")).as("lease held while the wrapped model delivers s1, received=" + s1Received).isTrue();
        assertThat(model.isRunning("s1")).as("recorded as running").isTrue();
    }

    @Test
    void a_resume_that_fails_once_after_a_start_overtook_the_subscribe_lets_the_same_id_be_subscribed_again() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.stop();
        // Another thread starts the model once the wrapped model holds s1 paused
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(false));
        };
        delegate.resumeFailsOnce = true;

        Throwable first = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));
        Throwable retry = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        assertThat(first).as("the first subscribe, whose resume failed").hasMessage("transient resume failure");
        assertThat(retry).as("subscribing s1 again after the first subscribe failed, delegate.isPaused(s1)=" + delegate.isPaused("s1")).isNull();
        assertThat(delegate.isRunning("s1")).isTrue();
        assertThat(strategy.hasLock("s1", "node")).isTrue();
    }

    @Test
    void a_subscribe_after_shutdown_throws_and_neither_registers_nor_subscribes() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.shutdown();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        assertThat(failure).isInstanceOf(IllegalStateException.class);
        assertThat(strategy.registered).as("registrations after shutdown").isEmpty();
        assertThat(delegate.isRunning("s1") || delegate.isPaused("s1")).as("s1 in the wrapped model after shutdown").isFalse();
    }

    @Test
    void shutdown_returns_while_start_retries_a_registration_under_the_monitor() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();
        // Registering on start() waits until the strategy is shut down, as a registration retrying through an outage does
        strategy.blockRegister.put("s1", strategy.shutDown);
        CompletableFuture<Void> starting = CompletableFuture.runAsync(() -> model.start(false));
        try {
            assertThat(strategy.registerEntered.await(5, SECONDS)).as("start() began registering s1").isTrue();

            CompletableFuture<Void> shuttingDown = CompletableFuture.runAsync(model::shutdown);

            assertThat(shuttingDown).as("shutdown() while start() retries a registration").succeedsWithin(Duration.ofSeconds(5));
        } finally {
            strategy.shutDown.countDown();
        }
        assertThat(starting).succeedsWithin(Duration.ofSeconds(5));
    }

    // Runs the call on a new thread and returns once it has returned, or once it waits for a monitor that the calling
    // thread holds, so a hook the wrapped model calls with the monitor held does not wait for good
    private static Thread runOnAnotherThreadUntilDoneOrBlocked(Runnable call) {
        Thread thread = new Thread(call);
        thread.start();
        long deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return thread;
    }

    // Grants a lease unless another node holds it, and makes a registration wait on a latch when told to
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> heldElsewhere = ConcurrentHashMap.newKeySet();
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Map<String, CountDownLatch> blockRegister = new ConcurrentHashMap<>();
        private final CountDownLatch registerEntered = new CountDownLatch(1);
        private final CountDownLatch shutDown = new CountDownLatch(1);
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            CountDownLatch latch = blockRegister.remove(subscriptionId);
            if (latch != null) {
                registerEntered.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
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
            shutDown.countDown();
        }
    }

    // A model of a user's own, which does not implement subscribePaused, holds a subscription made while it is stopped
    // paused, and starts each subscription at the end of its log. One that cannot pause ignores pauseSubscription(..)
    // and keeps its subscriptions running when it is stopped.
    private static final class UserWrittenModel implements SubscriptionModel {
        private final boolean cannotPause;
        private volatile Consumer<String> beforeSubscribe = __ -> {};
        private volatile Consumer<String> afterSubscribe = __ -> {};
        private volatile boolean resumeFailsOnce;
        private boolean running = true;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private final List<String> log = new ArrayList<>();
        private final Map<String, Consumer<CloudEvent>> actions = new HashMap<>();
        private final Map<String, Integer> positions = new HashMap<>();

        private UserWrittenModel(boolean cannotPause) {
            this.cannotPause = cannotPause;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            beforeSubscribe.accept(subscriptionId);
            synchronized (this) {
                if (runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId)) {
                    throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
                }
                (running ? runningIds : pausedIds).add(subscriptionId);
                actions.put(subscriptionId, action);
                positions.put(subscriptionId, log.size());
            }
            afterSubscribe.accept(subscriptionId);
            return new UserWrittenSubscription(subscriptionId);
        }

        private synchronized void write(String eventId) {
            log.add(eventId);
            List.copyOf(runningIds).forEach(this::deliver);
        }

        private void deliver(String subscriptionId) {
            for (int position = positions.get(subscriptionId); position < log.size(); position++) {
                actions.get(subscriptionId).accept(CloudEventBuilder.v1().withId(log.get(position)).withSource(URI.create("urn:user-written")).withType("written").build());
                positions.put(subscriptionId, position + 1);
            }
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            running = false;
            if (!cannotPause) {
                pausedIds.addAll(runningIds);
                runningIds.clear();
            }
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
        public synchronized boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            if (resumeFailsOnce) {
                resumeFailsOnce = false;
                throw new IllegalStateException("transient resume failure");
            }
            if (!pausedIds.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            runningIds.add(subscriptionId);
            deliver(subscriptionId);
            return new UserWrittenSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            if (!cannotPause && runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }
    }

    private record UserWrittenSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
