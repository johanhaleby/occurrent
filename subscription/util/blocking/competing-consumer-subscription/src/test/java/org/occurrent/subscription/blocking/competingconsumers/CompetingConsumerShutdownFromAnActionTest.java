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
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A {@code shutdown()} called from the action of a subscription returns at once while another {@code shutdown()} waits
 * for that action to return. The other one waits in the wrapped model's own {@code shutdown()}, or for the call into
 * the wrapped model that runs the action.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerShutdownFromAnActionTest {

    // InMemorySubscriptionModel waits five seconds for its delivery thread before it interrupts it
    private static final Duration PROMPTLY = Duration.ofSeconds(2);
    private static final Duration EVENTUALLY = Duration.ofSeconds(15);
    // Far beyond EVENTUALLY, so a shutdown() that waits for it fails the test before the wait ends
    private static final Duration WAITS_FOR_THE_ACTION = Duration.ofSeconds(40);
    // Resolves to no position for the competing consumer model, which hands the subscription straight to the wrapped
    // model, and to the wrapped model's default there
    private static final StartAt DOES_NOT_COMPETE = StartAt.dynamic(context ->
            context.subscriptionModelType() == CompetingConsumerSubscriptionModel.class ? null : StartAt.subscriptionModelDefault());

    private final Leases strategy = new Leases();
    private final CountDownLatch inWrappedShutdown = new CountDownLatch(1);

    @Test
    void a_shutdown_from_the_action_of_a_competing_subscription_returns_promptly_while_another_waits_for_the_in_memory_model_to_shut_down() throws Exception {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                inWrappedShutdown.countDown();
                super.shutdown();
            }
        };

        ShutdownFromTheAction outcome = shutdownFromTheActionWhileAnotherShutdownWaitsForIt(wrapped, StartAt.subscriptionModelDefault());

        assertThat(outcome.actionReturned).as("the action returned").isTrue();
        assertThat(outcome.millisInShutdownFromTheAction.get()).as("milliseconds the shutdown() called from the action took").isLessThan(PROMPTLY.toMillis());
        assertThat(outcome.thrownByShutdownFromTheAction.get()).as("[what the shutdown() called from the action threw]").isNull();
        assertThat(outcome.otherShutdownReturned).as("the other shutdown() returned").isTrue();
        assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty();
    }

    @Test
    void a_shutdown_from_the_action_of_a_subscription_that_does_not_compete_returns_promptly_while_another_waits_for_the_in_memory_model_to_shut_down() throws Exception {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(RetryStrategy.none()) {
            @Override
            public void shutdown() {
                inWrappedShutdown.countDown();
                super.shutdown();
            }
        };

        ShutdownFromTheAction outcome = shutdownFromTheActionWhileAnotherShutdownWaitsForIt(wrapped, DOES_NOT_COMPETE);

        assertThat(outcome.actionReturned).as("the action returned").isTrue();
        assertThat(outcome.millisInShutdownFromTheAction.get()).as("milliseconds the shutdown() called from the action took").isLessThan(PROMPTLY.toMillis());
        assertThat(outcome.thrownByShutdownFromTheAction.get()).as("[what the shutdown() called from the action threw]").isNull();
        assertThat(outcome.otherShutdownReturned).as("the other shutdown() returned").isTrue();
    }

    @Test
    void a_shutdown_from_an_action_lets_another_whose_wrapped_model_waits_for_that_action_without_a_time_limit_return_and_give_the_lease_up() throws Exception {
        ExecutorService deliveryThreads = Executors.newCachedThreadPool();
        AtomicBoolean shutDownOnce = new AtomicBoolean();
        // Waits for its delivery threads to end, with no time limit, the first time it is shut down
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel(deliveryThreads, RetryStrategy.none()) {
            @Override
            public void shutdown() {
                inWrappedShutdown.countDown();
                if (!shutDownOnce.compareAndSet(false, true)) {
                    return;
                }
                super.shutdown();
                try {
                    deliveryThreads.awaitTermination(1, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        ShutdownFromTheAction outcome = shutdownFromTheActionWhileAnotherShutdownWaitsForIt(wrapped, StartAt.subscriptionModelDefault());

        assertThat(outcome.actionReturned).as("the action returned").isTrue();
        assertThat(outcome.otherShutdownReturned).as("the other shutdown() returned").isTrue();
        assertThat(outcome.millisInShutdownFromTheAction.get()).as("milliseconds the shutdown() called from the action took").isLessThan(PROMPTLY.toMillis());
        assertThat(outcome.thrownByShutdownFromTheAction.get()).as("[what the shutdown() called from the action threw]").isNull();
        assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty();
    }

    @Test
    void a_shutdown_from_an_action_that_starting_the_wrapped_model_runs_lets_another_that_waits_for_that_start_return() throws Exception {
        CallsTheActionInside wrapped = new CallsTheActionInside();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ShutdownFromTheAction outcome = new ShutdownFromTheAction();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), shuttingDownOnce(model, outcome)).waitUntilStarted();
        model.stop();
        wrapped.runsTheActionOnStart = "s1";

        Thread starting = Thread.ofPlatform().daemon().start(model::start);

        shutDownWhileTheActionWaits(model, wrapped, outcome);
        starting.join(EVENTUALLY.toMillis());

        assertThat(outcome.actionReturned).as("the action returned").isTrue();
        assertThat(outcome.millisInShutdownFromTheAction.get()).as("milliseconds the shutdown() called from the action took").isLessThan(PROMPTLY.toMillis());
        assertThat(outcome.thrownByShutdownFromTheAction.get()).as("[what the shutdown() called from the action threw]").isNull();
        assertThat(outcome.otherShutdownReturned).as("the other shutdown() returned").isTrue();
        assertThat(wrapped.shutDowns.get()).as("times the wrapped model was shut down").isEqualTo(1);
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty());
    }

    @Test
    void a_shutdown_from_an_action_that_resuming_a_subscription_that_does_not_compete_runs_lets_another_that_waits_for_that_resume_return_and_give_the_lease_up() throws Exception {
        CallsTheActionInside wrapped = new CallsTheActionInside();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        ShutdownFromTheAction outcome = new ShutdownFromTheAction();
        model.subscribe("node", "competing", null, StartAt.subscriptionModelDefault(), event -> {
        }).waitUntilStarted();
        model.subscribe("node", "s0", null, DOES_NOT_COMPETE, shuttingDownOnce(model, outcome)).waitUntilStarted();
        model.pauseSubscription("s0");
        assertThat(strategy.holders).as("[subscriptions with a lease held]").containsOnlyKeys("competing");
        wrapped.runsTheActionOnResume = "s0";

        Thread resuming = Thread.ofPlatform().daemon().start(() -> model.resumeSubscription("s0"));

        shutDownWhileTheActionWaits(model, wrapped, outcome);
        resuming.join(EVENTUALLY.toMillis());

        assertThat(outcome.actionReturned).as("the action returned").isTrue();
        assertThat(outcome.millisInShutdownFromTheAction.get()).as("milliseconds the shutdown() called from the action took").isLessThan(PROMPTLY.toMillis());
        assertThat(outcome.thrownByShutdownFromTheAction.get()).as("[what the shutdown() called from the action threw]").isNull();
        assertThat(outcome.otherShutdownReturned).as("the other shutdown() returned").isTrue();
        assertThat(wrapped.shutDowns.get()).as("times the wrapped model was shut down").isEqualTo(1);
        assertThat(strategy.holders).as("[subscriptions with a lease held]").isEmpty();
    }

    // Calls shutdown() for the first event, once the other shutdown() waits for the call that runs the action
    private static Consumer<CloudEvent> shuttingDownOnce(CompetingConsumerSubscriptionModel model, ShutdownFromTheAction outcome) {
        AtomicBoolean called = new AtomicBoolean();
        return event -> {
            if (!called.compareAndSet(false, true)) {
                return;
            }
            outcome.inAction.countDown();
            try {
                outcome.otherShutdownWaits.await();
                long began = System.nanoTime();
                try {
                    model.shutdown();
                } catch (Throwable e) {
                    outcome.thrownByShutdownFromTheAction.set(e);
                }
                outcome.millisInShutdownFromTheAction.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                outcome.returned.countDown();
            }
        };
    }

    private static void shutDownWhileTheActionWaits(CompetingConsumerSubscriptionModel model, CallsTheActionInside wrapped, ShutdownFromTheAction outcome) throws InterruptedException {
        assertThat(outcome.inAction.await(EVENTUALLY.toSeconds(), SECONDS)).as("the wrapped model runs the action inside the call").isTrue();
        wrapped.actionReturned = outcome.returned;
        Thread other = Thread.ofPlatform().daemon().start(model::shutdown);
        // The only wait before the wrapped model's own shutdown() is the one for the call that runs the action
        await().atMost(EVENTUALLY).until(() -> other.getState() == Thread.State.WAITING);
        outcome.otherShutdownWaits.countDown();

        outcome.actionReturned = outcome.returned.await(EVENTUALLY.toSeconds(), SECONDS);
        other.join(EVENTUALLY.toMillis());
        outcome.otherShutdownReturned = !other.isAlive();
    }

    // Runs the action of a subscription on the thread that starts this model or resumes that subscription, and its
    // shutdown() waits for that action to return, for longer than the test waits
    private static final class CallsTheActionInside extends InMemorySubscriptionModel {
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final AtomicLong shutDowns = new AtomicLong();
        private volatile @Nullable String runsTheActionOnStart;
        private volatile @Nullable String runsTheActionOnResume;
        private volatile CountDownLatch actionReturned = new CountDownLatch(0);

        private CallsTheActionInside() {
            super(RetryStrategy.none());
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            return super.subscribePaused(subscriptionId, filter, startAt, action);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            super.start(resumeSubscriptionsAutomatically);
            runTheActionOf(runsTheActionOnStart);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            Subscription resumed = super.resumeSubscription(subscriptionId);
            if (subscriptionId.equals(runsTheActionOnResume)) {
                runTheActionOf(subscriptionId);
            }
            return resumed;
        }

        private void runTheActionOf(@Nullable String subscriptionId) {
            Consumer<CloudEvent> action = subscriptionId == null ? null : actions.get(subscriptionId);
            if (action != null) {
                action.accept(event("e1"));
            }
        }

        @Override
        public void shutdown() {
            shutDowns.incrementAndGet();
            try {
                actionReturned.await(WAITS_FOR_THE_ACTION.toSeconds(), SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.shutdown();
        }
    }

    // s1's action calls shutdown() for e1 once another shutdown() has reached the wrapped model's own shutdown(), which
    // waits for the thread that runs the action
    private ShutdownFromTheAction shutdownFromTheActionWhileAnotherShutdownWaitsForIt(InMemorySubscriptionModel wrapped, StartAt startAt) throws Exception {
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        CountDownLatch inAction = new CountDownLatch(1);
        CountDownLatch otherShutdownInTheWrappedModel = new CountDownLatch(1);
        CountDownLatch actionReturned = new CountDownLatch(1);
        ShutdownFromTheAction outcome = new ShutdownFromTheAction();
        model.subscribe("node", "s1", null, startAt, event -> {
            inAction.countDown();
            try {
                otherShutdownInTheWrappedModel.await();
                long began = System.nanoTime();
                try {
                    model.shutdown();
                } catch (Throwable e) {
                    outcome.thrownByShutdownFromTheAction.set(e);
                }
                outcome.millisInShutdownFromTheAction.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                actionReturned.countDown();
            }
        }).waitUntilStarted();
        wrapped.accept(List.of(event("e1")));
        assertThat(inAction.await(EVENTUALLY.toSeconds(), SECONDS)).as("s1's action runs for e1").isTrue();

        Thread other = Thread.ofPlatform().daemon().start(model::shutdown);
        assertThat(inWrappedShutdown.await(EVENTUALLY.toSeconds(), SECONDS)).as("the other shutdown() reaches the wrapped model's own shutdown()").isTrue();
        otherShutdownInTheWrappedModel.countDown();

        outcome.actionReturned = actionReturned.await(EVENTUALLY.toSeconds(), SECONDS);
        other.join(EVENTUALLY.toMillis());
        outcome.otherShutdownReturned = !other.isAlive();
        return outcome;
    }

    private static final class ShutdownFromTheAction {
        private final CountDownLatch inAction = new CountDownLatch(1);
        private final CountDownLatch otherShutdownWaits = new CountDownLatch(1);
        private final CountDownLatch returned = new CountDownLatch(1);
        private final AtomicLong millisInShutdownFromTheAction = new AtomicLong(-1);
        private final AtomicReference<Throwable> thrownByShutdownFromTheAction = new AtomicReference<>();
        private boolean actionReturned;
        private boolean otherShutdownReturned;
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Grants each lease to the node that asks first, and reports it held while that node has it
    private static final class Leases implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return subscriberId.equals(holders.get(subscriptionId));
        }

        @Override
        public void addListener(CompetingConsumerListener listener) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listener) {
        }
    }
}
