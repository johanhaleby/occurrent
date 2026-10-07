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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * For a subscription registered with the model default, the evaluation of the start position, which the wrapped model
 * runs on a thread of its own, never waits for the subscribing thread to ask the wrapped model for its position. Each
 * test here gives the wrapped model a lock that its {@code globalCheckpoint()} takes on the subscribing thread, and
 * every wait gives up after at most 30 seconds, so threads that wait for each other fail the test instead of hanging
 * the build.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelLockOrderTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration SUBSCRIBE_BOUND = Duration.ofSeconds(10);

    /**
     * The wrapped model here evaluates the start position on a thread of its own while holding a lock, once the
     * subscribing thread has entered {@code globalCheckpoint()}, and that method takes the same lock on the
     * subscribing thread. The evaluation is parked inside this model before the subscribing thread asks for the lock,
     * so neither thread may wait for the other.
     */
    @Test
    void a_subscribe_returns_when_the_wrapped_model_evaluates_the_start_position_while_holding_the_lock_its_global_checkpoint_takes() {
        EvaluatesWhileHoldingTheLock wrapped = new EvaluatesWhileHoldingTheLock();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        DaemonThreads threads = new DaemonThreads();

        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }), threads);

        try {
            assertThat(subscribing).as("the subscribe").succeedsWithin(SUBSCRIBE_BOUND);
            assertThat(wrapped.evaluated).as("the start position the wrapped model evaluated")
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                            startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("present"));
            assertThat(storage.read(SUBSCRIPTION_ID)).as("the position stored for the subscription")
                    .extracting(Checkpoint::asString).isEqualTo("present");
        } finally {
            threads.interruptAll();
            wrapped.interruptTheEvaluation();
        }
    }

    /**
     * The wrapped model here evaluates the start position on a thread of its own while holding a lock, as in the test
     * above, and answers a different position to the subscribing thread than to the evaluation. The storage writes
     * unconditionally, so nothing but this model keeps the subscribing thread and the evaluation from each storing a
     * position. The subscribing thread is held inside {@code globalCheckpoint()} until the evaluation waits or has
     * finished, so the evaluation records first.
     */
    @Test
    void a_subscribe_and_an_evaluation_recording_at_the_same_time_store_one_first_position_and_the_evaluation_starts_from_it() {
        EvaluatesWhileHoldingTheLock wrapped = new EvaluatesWhileHoldingTheLock("seen by the subscribing thread", "seen by the evaluation");
        RecordsEverySaveWithoutEvaluatingWriteConditions storage = new RecordsEverySaveWithoutEvaluatingWriteConditions();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        DaemonThreads threads = new DaemonThreads();

        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }), threads);

        try {
            assertThat(subscribing).as("the subscribe").succeedsWithin(SUBSCRIBE_BOUND);
            assertThat(wrapped.evaluated).as("the start position the wrapped model evaluated")
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                            startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("seen by the evaluation"));
            assertThat(storage.saved).as("the positions saved for the subscription").containsExactly("seen by the evaluation");
            assertThat(storage.read(SUBSCRIPTION_ID)).as("the position stored for the subscription")
                    .extracting(Checkpoint::asString).isEqualTo("seen by the evaluation");
        } finally {
            threads.interruptAll();
            wrapped.interruptTheEvaluation();
        }
    }

    /**
     * The wrapped model here pauses a subscription by taking a lock and then waiting for the thread that evaluates
     * the start position, and its {@code globalCheckpoint()} takes the same lock on every thread. The pause comes only
     * once the evaluation waits or has finished, so an evaluation that asks for the position itself has it by then,
     * as it did in 0.33.0. The subscribing thread is held inside
     * {@code globalCheckpoint()} until the pause owns the lock, so the subscribe has to let the evaluation finish for
     * the pause to return, and may not wait for the pause itself.
     */
    @Test
    void a_subscribe_returns_when_the_wrapped_models_pause_holds_the_lock_its_global_checkpoint_takes_while_it_waits_for_the_evaluation() throws InterruptedException {
        PausesWhileWaitingForTheEvaluation wrapped = new PausesWhileWaitingForTheEvaluation();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
        DaemonThreads threads = new DaemonThreads();

        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }), threads);

        try {
            assertThat(wrapped.subscribingThreadEntered.await(10, TimeUnit.SECONDS))
                    .as("the subscribing thread asked the wrapped model for the global checkpoint").isTrue();
            wrapped.evaluateTheStartPosition();
            assertThat(wrapped.evaluationIsParkedOrDone(Duration.ofSeconds(5)))
                    .as("the evaluation waits or has finished").isTrue();
            CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> durable.pauseSubscription(SUBSCRIPTION_ID), threads);
            assertThat(wrapped.pauseHoldsTheLock.await(10, TimeUnit.SECONDS))
                    .as("the pause took the lock").isTrue();
            wrapped.letTheSubscribingThreadAskForTheLock();

            assertThat(subscribing).as("the subscribe").succeedsWithin(SUBSCRIBE_BOUND);
            assertThat(pausing).as("the pause").succeedsWithin(SUBSCRIBE_BOUND);
            assertThat(wrapped.evaluated).as("the start position the wrapped model evaluated")
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                            startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("present"));
            assertThat(storage.read(SUBSCRIPTION_ID)).as("the position stored for the subscription")
                    .extracting(Checkpoint::asString).isEqualTo("present");
        } finally {
            wrapped.letTheSubscribingThreadAskForTheLock();
            threads.interruptAll();
            wrapped.interruptTheEvaluation();
        }
    }

    /**
     * Writes unconditionally, the way a storage that cannot evaluate write conditions does, and remembers the
     * position of every save. Every save overload ends in the three-argument one, so overriding it sees them all.
     */
    private static final class RecordsEverySaveWithoutEvaluatingWriteConditions extends InMemoryCheckpointStorage {
        final List<String> saved = new CopyOnWriteArrayList<>();

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return false;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saved.add(checkpoint.asString());
            return super.save(subscriptionId, checkpoint, condition);
        }
    }

    private static final class DaemonThreads implements Executor {
        private final List<Thread> started = new CopyOnWriteArrayList<>();

        @Override
        public void execute(Runnable runnable) {
            Thread thread = Thread.ofPlatform().daemon().unstarted(runnable);
            started.add(thread);
            thread.start();
        }

        void interruptAll() {
            started.forEach(Thread::interrupt);
        }
    }

    /**
     * Holds the lock {@code globalCheckpoint()} takes, and evaluates the start position on one thread of its own,
     * the evaluator. A subclass says when the evaluator runs and what it does around the evaluation.
     */
    private abstract static class WrappedModelWithALock implements CheckpointAwareSubscriptionModel {
        static final Duration BOUND = Duration.ofSeconds(30);

        final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
        final CompletableFuture<@Nullable StartAt> evaluated = new CompletableFuture<>();
        final ReentrantLock lock = new ReentrantLock();
        final CountDownLatch subscribingThreadEntered = new CountDownLatch(1);
        private volatile boolean evaluating;
        volatile @Nullable Thread evaluator;

        abstract void runEvaluator(StartAt startAt) throws InterruptedException;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.add(subscriptionId);
            Thread thread = Thread.ofPlatform().daemon().unstarted(() -> {
                try {
                    runEvaluator(startAt);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    evaluated.completeExceptionally(e);
                }
            });
            evaluator = thread;
            thread.start();
            return dummySubscription(subscriptionId);
        }

        void evaluate(StartAt startAt) {
            evaluating = true;
            try {
                evaluated.complete(startAt.get(new SubscriptionModelContext(WrappedModelWithALock.class)));
            } catch (Throwable t) {
                evaluated.completeExceptionally(t);
            }
        }

        boolean isTheEvaluator() {
            return Thread.currentThread() == evaluator;
        }

        boolean evaluationIsParkedOrDone(Duration bound) {
            long until = System.nanoTime() + bound.toNanos();
            while (!evaluationWaitsOrIsDone() && System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            return evaluationWaitsOrIsDone();
        }

        private boolean evaluationWaitsOrIsDone() {
            Thread thread = evaluator;
            if (thread == null || !evaluating) {
                return false;
            }
            Thread.State state = thread.getState();
            return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING
                   || state == Thread.State.BLOCKED || state == Thread.State.TERMINATED;
        }

        Checkpoint presentPositionUnderTheLock() {
            return positionUnderTheLock("present");
        }

        Checkpoint positionUnderTheLock(String position) {
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the lock", e);
            }
            try {
                return new StringBasedCheckpoint(position);
            } finally {
                lock.unlock();
            }
        }

        void interruptTheEvaluation() {
            Thread thread = evaluator;
            if (thread != null) {
                thread.interrupt();
            }
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        private static Subscription dummySubscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }

    /**
     * Its evaluator takes the lock when it starts and evaluates the start position while still holding it, once the
     * subscribing thread has entered {@code globalCheckpoint()}. On the subscribing thread that method answers once the
     * evaluator waits or has finished, and then takes the lock.
     */
    private static final class EvaluatesWhileHoldingTheLock extends WrappedModelWithALock {
        private final String positionForTheSubscribingThread;
        private final String positionForTheEvaluation;

        EvaluatesWhileHoldingTheLock() {
            this("present", "present");
        }

        EvaluatesWhileHoldingTheLock(String positionForTheSubscribingThread, String positionForTheEvaluation) {
            this.positionForTheSubscribingThread = positionForTheSubscribingThread;
            this.positionForTheEvaluation = positionForTheEvaluation;
        }

        @Override
        void runEvaluator(StartAt startAt) throws InterruptedException {
            lock.lockInterruptibly();
            try {
                subscribingThreadEntered.await(BOUND.toSeconds(), TimeUnit.SECONDS);
                evaluate(startAt);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            if (isTheEvaluator()) {
                return positionUnderTheLock(positionForTheEvaluation);
            }
            subscribingThreadEntered.countDown();
            evaluationIsParkedOrDone(BOUND);
            return positionUnderTheLock(positionForTheSubscribingThread);
        }
    }

    /**
     * Its evaluator evaluates the start position when the test says so, and takes the lock for the global checkpoint
     * too. Its pause takes the lock and waits for the evaluator while holding it. On the subscribing thread
     * {@code globalCheckpoint()} waits for the test to let it go, and then takes the lock.
     */
    private static final class PausesWhileWaitingForTheEvaluation extends WrappedModelWithALock {
        final CountDownLatch pauseHoldsTheLock = new CountDownLatch(1);
        private final CountDownLatch evaluate = new CountDownLatch(1);
        private final CountDownLatch subscribingThreadMayAskForTheLock = new CountDownLatch(1);

        void evaluateTheStartPosition() {
            evaluate.countDown();
        }

        void letTheSubscribingThreadAskForTheLock() {
            subscribingThreadMayAskForTheLock.countDown();
        }

        @Override
        void runEvaluator(StartAt startAt) throws InterruptedException {
            evaluate.await(BOUND.toSeconds(), TimeUnit.SECONDS);
            evaluate(startAt);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            if (isTheEvaluator()) {
                return presentPositionUnderTheLock();
            }
            subscribingThreadEntered.countDown();
            try {
                subscribingThreadMayAskForTheLock.await(BOUND.toSeconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting to ask for the lock", e);
            }
            return presentPositionUnderTheLock();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            try {
                lock.lockInterruptibly();
                try {
                    pauseHoldsTheLock.countDown();
                    Thread thread = evaluator;
                    if (thread != null) {
                        thread.join(BOUND.toMillis());
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while pausing", e);
            }
        }
    }
}
