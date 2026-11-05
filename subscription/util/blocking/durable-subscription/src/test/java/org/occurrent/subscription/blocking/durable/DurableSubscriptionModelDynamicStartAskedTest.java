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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A {@code StartAt.dynamic} function is asked before the subscribe takes the lock for its id, so a cancel of the id on
 * another thread cannot deadlock against it. A resume of the id on another thread waits for such a subscribe. It gives
 * the function 10 seconds to return, and then waits for the lock with no limit, as a resume always has. Two of the
 * tests here wait 10 seconds or more by design, since that bound is a constant of the model.
 */
@Timeout(60)
@DisplayName("DurableSubscriptionModelDynamicStartAsked")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelDynamicStartAskedTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration LONGER_THAN_THE_FUNCTION_BOUND = Duration.ofSeconds(12);
    private static final Duration BOUND = Duration.ofSeconds(30);

    private final ExecutorService otherThreads = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        return thread;
    });
    private final CompletableFuture<Void> releaseTheHeldDelete = new CompletableFuture<>();
    private final CompletableFuture<Void> releaseTheFunction = new CompletableFuture<>();

    @AfterEach
    void let_every_held_thread_go_and_stop_the_threads() {
        releaseTheHeldDelete.complete(null);
        releaseTheFunction.complete(null);
        otherThreads.shutdownNow();
    }

    @Nested
    @DisplayName("when a resume is made while a cancel holds the lock and a subscribe whose function has returned waits for it")
    class When_a_resume_is_made_while_a_cancel_holds_the_lock_and_a_subscribe_whose_function_has_returned_waits_for_it {

        @Test
        @DisplayName("the resume is still waiting after 10 seconds and returns once the cancel lets go")
        void the_resume_is_still_waiting_after_10_seconds_and_returns_once_the_cancel_lets_go() {
            // Given
            HoldsDeletes storage = new HoldsDeletes(releaseTheHeldDelete);
            InMemoryWrappedModel wrapped = new InMemoryWrappedModel();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
            CompletableFuture<Void> cancel = CompletableFuture.runAsync(() -> durable.cancelSubscription(SUBSCRIPTION_ID), otherThreads);
            assertThat(storage.deleteStarted).as("the cancel holding the lock").succeedsWithin(BOUND);
            AtomicBoolean functionReturned = new AtomicBoolean();
            AtomicReference<Thread> subscribing = new AtomicReference<>();
            CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> {
                subscribing.set(Thread.currentThread());
                return durable.subscribePaused(SUBSCRIPTION_ID, null, StartAt.dynamic(context -> {
                    functionReturned.set(true);
                    return StartAt.checkpoint(new StringBasedCheckpoint("a-position"));
                }), event -> {
                });
            }, otherThreads);
            await().atMost(BOUND).until(() -> functionReturned.get() && waitsForALock(subscribing.get()));

            // When
            AtomicReference<Thread> resuming = new AtomicReference<>();
            CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> {
                resuming.set(Thread.currentThread());
                return durable.resumeSubscription(SUBSCRIPTION_ID);
            }, otherThreads);
            await().atMost(BOUND).until(() -> waits(resuming.get()));

            // Then the resume neither throws nor returns while the cancel still holds the lock
            assertThatThrownBy(() -> resume.get(LONGER_THAN_THE_FUNCTION_BOUND.toMillis(), TimeUnit.MILLISECONDS))
                    .as("the resume while the cancel holds the lock")
                    .isInstanceOf(TimeoutException.class);

            releaseTheHeldDelete.complete(null);
            assertAll(
                    () -> assertThat(cancel).as("the cancel").succeedsWithin(BOUND),
                    () -> assertThat(subscribe).as("the subscribe").succeedsWithin(BOUND),
                    () -> assertThat(resume).as("the resume").succeedsWithin(BOUND),
                    () -> assertThat(wrapped.resumed).as("the ids the wrapped model resumed").containsExactly(SUBSCRIPTION_ID)
            );
        }
    }

    @Nested
    @DisplayName("when a resume is made while the function of a subscribe of the id blocks on another thread for longer than 10 seconds")
    class When_a_resume_is_made_while_the_function_of_a_subscribe_blocks_for_longer_than_10_seconds {

        @Test
        @DisplayName("the resume throws IllegalStateException saying the function didn't return within 10000 ms and resumes nothing")
        void the_resume_throws_IllegalStateException_saying_the_function_did_not_return_within_10000_ms_and_resumes_nothing() {
            // Given
            InMemoryWrappedModel wrapped = new InMemoryWrappedModel();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
            CountDownLatch functionStarted = new CountDownLatch(1);
            CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> durable.subscribePaused(SUBSCRIPTION_ID, null, StartAt.dynamic(context -> {
                functionStarted.countDown();
                releaseTheFunction.orTimeout(BOUND.toSeconds(), TimeUnit.SECONDS).join();
                return StartAt.checkpoint(new StringBasedCheckpoint("a-position"));
            }), event -> {
            }), otherThreads);
            await().atMost(BOUND).until(() -> functionStarted.getCount() == 0);

            // When
            CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> durable.resumeSubscription(SUBSCRIPTION_ID), otherThreads);

            // Then
            assertThat(resume).as("the resume").failsWithin(BOUND)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(IllegalStateException.class)
                    .withMessageContaining("didn't return within 10000 ms");
            assertThat(wrapped.resumed).as("the ids the wrapped model resumed").isEmpty();

            releaseTheFunction.complete(null);
            assertAll(
                    () -> assertThat(subscribe).as("the subscribe").succeedsWithin(BOUND),
                    () -> assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("the id is paused").isTrue(),
                    () -> assertThat(wrapped.resumed).as("the ids the wrapped model resumed").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the function of a subscribe cancels its own id on the subscribing thread")
    class When_the_function_cancels_its_own_id_on_the_subscribing_thread {

        @Test
        @DisplayName("the subscribe still subscribes")
        void the_subscribe_still_subscribes() {
            // Given
            InMemoryWrappedModel wrapped = new InMemoryWrappedModel();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
            StartAt function = StartAt.dynamic(context -> {
                durable.cancelSubscription(SUBSCRIPTION_ID);
                return StartAt.checkpoint(new StringBasedCheckpoint("a-position"));
            });

            // When
            Subscription subscription = durable.subscribe(SUBSCRIPTION_ID, null, function, event -> {
            });

            // Then
            assertAll(
                    () -> assertThat(subscription.waitUntilStarted(Duration.ofSeconds(1))).as("the subscription started").isTrue(),
                    () -> assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("the wrapped model runs the id").isTrue(),
                    () -> assertThat(durable.isRunning(SUBSCRIPTION_ID)).as("the id is running").isTrue()
            );
        }
    }

    private static boolean waits(@Nullable Thread thread) {
        return thread != null && (thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING);
    }

    // Parked on a ReentrantLock, which the lock of an id is. A thread parked there is already queued for it
    private static boolean waitsForALock(@Nullable Thread thread) {
        return thread != null && LockSupport.getBlocker(thread) instanceof AbstractQueuedSynchronizer;
    }

    /**
     * The first delete waits until it is released, so the call that deletes holds the lock for its id meanwhile.
     */
    private static final class HoldsDeletes extends InMemoryCheckpointStorage {
        private final CompletableFuture<Void> deleteStarted = new CompletableFuture<>();
        private final CompletableFuture<Void> release;

        private HoldsDeletes(CompletableFuture<Void> release) {
            this.release = release;
        }

        @Override
        public void delete(String subscriptionId) {
            deleteStarted.complete(null);
            release.orTimeout(BOUND.toSeconds(), TimeUnit.SECONDS).join();
            super.delete(subscriptionId);
        }
    }

    private static final class InMemoryWrappedModel implements CheckpointAwareSubscriptionModel {
        private final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private final List<String> resumed = new CopyOnWriteArrayList<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.add(subscriptionId);
            return started(subscriptionId);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscriptions.add(subscriptionId);
            paused.add(subscriptionId);
            return started(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            resumed.add(subscriptionId);
            paused.remove(subscriptionId);
            return started(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("present");
        }

        private static Subscription started(String subscriptionId) {
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
}
