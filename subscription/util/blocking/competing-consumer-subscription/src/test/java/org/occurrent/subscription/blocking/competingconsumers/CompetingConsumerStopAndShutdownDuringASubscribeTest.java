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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Neither stop() nor shutdown() waits for the wrapped model to make a subscription, since over a durable model that
 * reads the stored position, which takes as long as the database cannot be reached. Once the subscribe returns, a
 * competing subscription delivers nothing and holds no lease, with the wrapped model stopped, until start(..) has it
 * compete for its lease, as one whose registration a stop() gave up does. One that does not compete ends as one made
 * once the stop() had returned. A subscribe that a shutdown() overtook runs nothing in the wrapped model. A competing
 * subscription goes to the wrapped model through subscribe, which these wait at, only when that model refuses
 * subscribePaused, and such a model starts itself on a subscribe here. Once the wrapped model has made one that does
 * not compete, the subscribe neither asks whether it is paused there nor resumes it, unless a start(true) began while
 * it was being made.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerStopAndShutdownDuringASubscribeTest {

    private static final String SUBSCRIBER = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Longer than EVENTUALLY, so a stop() or shutdown() that waits for the subscribe is still waiting when the test
    // gives up on it
    private static final Duration SUBSCRIBE_HELD_AT_MOST = Duration.ofSeconds(30);

    @ParameterizedTest(name = "competes for a lease: {0}, wrapped model starts itself on a subscribe: {1}")
    @CsvSource({"true, true", "false, false", "false, true"})
    void stop_returns_while_the_wrapped_model_makes_a_subscription_which_then_waits_for_a_start(boolean competes, boolean startsItselfOnSubscribe) {
        States expected = competes
                ? new States(new State(true, false, false, false), new State(false, true, true, true))
                : madeOnceTheStopReturned(startsItselfOnSubscribe);
        Fixture fixture = new Fixture(startsItselfOnSubscribe);
        Gate subscribeInTheWrappedModel = new Gate();
        try {
            fixture.wrapped.nextSubscribeOfS2.set(subscribeInTheWrappedModel);
            CompletableFuture<Void> subscribing = CompletableFuture.runAsync(() -> fixture.subscribeS2(competes));
            assertThat(subscribeInTheWrappedModel.awaitEntered()).as("the wrapped model is making s2").isTrue();

            CompletableFuture<Void> stopping = CompletableFuture.runAsync(fixture.model::stop);
            assertThat(stopping).as("[stop() while the wrapped model makes s2]").succeedsWithin(EVENTUALLY);
            subscribeInTheWrappedModel.open();
            assertThat(subscribing).as("the subscribe of s2").succeedsWithin(EVENTUALLY);

            assertThat(fixture.states()).as("[s2 once the subscribe and stop() returned, and then once start(false) returned]").isEqualTo(expected);
        } finally {
            subscribeInTheWrappedModel.open();
            fixture.model.shutdown();
        }
    }

    @ParameterizedTest(name = "competes for a lease: {0}, wrapped model starts itself on a subscribe: {1}")
    @CsvSource({"true, true", "false, false", "false, true"})
    void shutdown_returns_while_the_wrapped_model_makes_a_subscription_which_then_runs_nothing_there(boolean competes, boolean startsItselfOnSubscribe) {
        Fixture fixture = new Fixture(startsItselfOnSubscribe);
        Gate subscribeInTheWrappedModel = new Gate();
        try {
            fixture.wrapped.nextSubscribeOfS2.set(subscribeInTheWrappedModel);
            // Throws when it finds this model shut down, which is one of the two ways it may end
            CompletableFuture<@Nullable Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> fixture.subscribeS2(competes)));
            assertThat(subscribeInTheWrappedModel.awaitEntered()).as("the wrapped model is making s2").isTrue();

            CompletableFuture<Void> shuttingDown = CompletableFuture.runAsync(fixture.model::shutdown);
            assertThat(shuttingDown).as("[shutdown() while the wrapped model makes s2]").succeedsWithin(EVENTUALLY);
            subscribeInTheWrappedModel.open();
            assertThat(subscribing).as("the subscribe of s2").succeedsWithin(EVENTUALLY);

            assertThat(fixture.wrapped.runs("s2")).as("[s2 runs in the wrapped model once the subscribe returned after shutdown()]").isFalse();
            assertThat(fixture.strategy.holders).as("[leases held once the subscribe returned after shutdown()]").isEmpty();
        } finally {
            subscribeInTheWrappedModel.open();
            fixture.model.shutdown();
        }
    }

    @ParameterizedTest(name = "called while the wrapped model makes s2: {0}, wrapped model running: {1}")
    @CsvSource({"nothing, true", "nothing, false", "start(false), true", "start(false), false", "stop(), true", "stop(), false", "shutdown(), true", "shutdown(), false"})
    void a_subscription_that_does_not_compete_is_left_as_the_wrapped_model_made_it_unless_a_start_that_resumes_began_meanwhile(String calledMeanwhile, boolean wrappedModelRunning) {
        List<String> calls = callsOnceTheWrappedModelMadeS2(calledMeanwhile, wrappedModelRunning);

        assertThat(calls).as("[what the subscribe of s2 asked of the wrapped model once that model had made s2, with %s called meanwhile]", calledMeanwhile).doesNotContain("isPaused s2", "resumeSubscription s2");
    }

    @ParameterizedTest(name = "wrapped model running: {0}")
    @CsvSource({"true", "false"})
    void a_subscription_that_does_not_compete_is_resumed_by_its_subscribe_when_a_start_that_resumes_began_meanwhile(boolean wrappedModelRunning) {
        List<String> calls = callsOnceTheWrappedModelMadeS2("start(true)", wrappedModelRunning);

        if (wrappedModelRunning) {
            assertThat(calls).as("[what the subscribe of s2 asked of the wrapped model once that model had made s2 running]").contains("isPaused s2").doesNotContain("resumeSubscription s2");
        } else {
            assertThat(calls).as("[what the subscribe of s2 asked of the wrapped model once that model had made s2 paused]").contains("isPaused s2", "resumeSubscription s2");
        }
    }

    private static List<String> callsOnceTheWrappedModelMadeS2(String calledMeanwhile, boolean wrappedModelRunning) {
        Fixture fixture = new Fixture(false);
        Gate subscribeInTheWrappedModel = new Gate();
        try {
            if (!wrappedModelRunning) {
                fixture.wrapped.stop();
            }
            fixture.wrapped.nextSubscribeOfS2.set(subscribeInTheWrappedModel);
            // Throws when it finds this model shut down
            CompletableFuture<@Nullable Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> fixture.subscribeS2(false)));
            assertThat(subscribeInTheWrappedModel.awaitEntered()).as("the wrapped model is making s2").isTrue();

            Runnable call = switch (calledMeanwhile) {
                case "nothing" -> () -> {
                };
                case "start(false)" -> () -> fixture.model.start(false);
                case "start(true)" -> () -> fixture.model.start(true);
                case "stop()" -> fixture.model::stop;
                case "shutdown()" -> fixture.model::shutdown;
                default -> throw new IllegalArgumentException(calledMeanwhile);
            };
            assertThat(CompletableFuture.runAsync(call)).as("[%s while the wrapped model makes s2]", calledMeanwhile).succeedsWithin(EVENTUALLY);
            subscribeInTheWrappedModel.open();
            assertThat(subscribing).as("the subscribe of s2").succeedsWithin(EVENTUALLY);
            return List.copyOf(fixture.wrapped.callsOnceS2WasMade);
        } finally {
            subscribeInTheWrappedModel.open();
            fixture.model.shutdown();
        }
    }

    private static States madeOnceTheStopReturned(boolean startsItselfOnSubscribe) {
        Fixture fixture = new Fixture(startsItselfOnSubscribe);
        try {
            fixture.model.stop();
            fixture.subscribeS2(false);
            return fixture.states();
        } finally {
            fixture.model.shutdown();
        }
    }

    // Where s2 is, in this model and in the wrapped model
    private record State(boolean paused, boolean runsInTheWrappedModel, boolean holdsTheLease, boolean wrappedModelRunning) {
    }

    private record States(State afterTheStop, State afterStartWithoutResuming) {
    }

    private static final class Fixture {
        private final WrappedModel wrapped;
        private final Strategy strategy = new Strategy();
        private final CompetingConsumerSubscriptionModel model;

        private Fixture(boolean startsItselfOnSubscribe) {
            wrapped = new WrappedModel(startsItselfOnSubscribe);
            model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        }

        // A start position that resolves to null here makes the model hand the subscription straight to the wrapped model
        private void subscribeS2(boolean competes) {
            model.subscribe(SUBSCRIBER, "s2", null, competes ? StartAt.subscriptionModelDefault() : StartAt.dynamic(__ -> null), __ -> {
            });
        }

        private States states() {
            State afterTheStop = state();
            model.start(false);
            return new States(afterTheStop, state());
        }

        private State state() {
            return new State(model.isPaused("s2"), wrapped.runs("s2"), strategy.holders.contains("s2"), wrapped.isRunning());
        }
    }

    private static void awaitOrFail(CountDownLatch latch, Duration atMost) {
        try {
            if (!latch.await(atMost.toMillis(), MILLISECONDS)) {
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
            awaitOrFail(open, SUBSCRIBE_HELD_AT_MOST);
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

    // Grants every lease on register
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

    // A model of a user's own, which pauses what it runs when it is stopped and runs nothing while it is stopped. One
    // that starts itself on a subscribe refuses subscribePaused. The next subscribe of s2, paused or not, waits at its
    // gate outside the monitor of this model, so it holds up nothing but itself. What the thread that made s2 asks of
    // it after that is kept in callsOnceS2WasMade.
    private static final class WrappedModel implements SubscriptionModel {
        private final boolean startsItselfOnSubscribe;
        private final AtomicReference<@Nullable Gate> nextSubscribeOfS2 = new AtomicReference<>();
        private final List<String> callsOnceS2WasMade = new CopyOnWriteArrayList<>();
        private volatile @Nullable Thread madeS2;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private boolean running = true;

        private WrappedModel(boolean startsItselfOnSubscribe) {
            this.startsItselfOnSubscribe = startsItselfOnSubscribe;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (subscriptionId.equals("s2")) {
                passIfSet(nextSubscribeOfS2);
            }
            synchronized (this) {
                if (startsItselfOnSubscribe) {
                    running = true;
                }
                (running ? runningIds : pausedIds).add(subscriptionId);
                if (subscriptionId.equals("s2")) {
                    madeS2 = Thread.currentThread();
                }
                return new WrappedSubscription(subscriptionId);
            }
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startsItselfOnSubscribe) {
                throw new UnsupportedOperationException("subscribePaused");
            }
            if (subscriptionId.equals("s2")) {
                passIfSet(nextSubscribeOfS2);
            }
            synchronized (this) {
                pausedIds.add(subscriptionId);
                return new WrappedSubscription(subscriptionId);
            }
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            record("cancelSubscription " + subscriptionId);
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
        }

        @Override
        public synchronized void stop() {
            record("stop");
            running = false;
            pausedIds.addAll(runningIds);
            runningIds.clear();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            record("start " + resumeSubscriptionsAutomatically);
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
            }
        }

        @Override
        public synchronized boolean isRunning() {
            record("isRunning");
            return running;
        }

        @Override
        public synchronized boolean isRunning(String subscriptionId) {
            record("isRunning " + subscriptionId);
            return running && runningIds.contains(subscriptionId);
        }

        // Delivers, which a subscription the wrapped model holds as running does only while that model runs
        private synchronized boolean runs(String subscriptionId) {
            return running && runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            record("isPaused " + subscriptionId);
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            record("resumeSubscription " + subscriptionId);
            running = true;
            pausedIds.remove(subscriptionId);
            runningIds.add(subscriptionId);
            return new WrappedSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            record("pauseSubscription " + subscriptionId);
            if (runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
        }

        private void record(String call) {
            if (Thread.currentThread() == madeS2) {
                callsOnceS2WasMade.add(call);
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
