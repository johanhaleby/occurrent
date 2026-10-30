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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CatchupSubscriptionModelConfig;
import org.occurrent.subscription.blocking.durable.catchup.StreamCatchupSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * A resume at a position through a catch-up model over a {@link DurableSubscriptionModel} takes the catch-up model's
 * claim for the subscription, and then the durable model's lock for it. A pause or a cancel for the same subscription
 * from another thread waits for it, or it for them, in either order, and both calls return. Each scenario is bounded
 * with {@code assertTimeoutPreemptively}, so a deadlock fails the test instead of hanging the build.
 */
@DisplayName("CatchupOverDurableResumeAtPositionLockOrder")
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupOverDurableResumeAtPositionLockOrderTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint POSITION = new StringBasedCheckpoint("position");
    private static final Duration SCENARIO_TIMEOUT = Duration.ofSeconds(30);
    private static final long STILL_WAITING_MILLIS = 300;

    @Test
    void a_pause_waits_for_a_resume_at_a_position_in_the_wrapped_model_and_both_return() {
        assertTimeoutPreemptively(SCENARIO_TIMEOUT, () -> {
            // Given a paused subscription, and a resume at a position held inside the wrapped model
            Stack stack = Stack.withPausedSubscription();
            stack.wrapped.holdNext(Operation.RESUME_AT_POSITION);

            // When a pause arrives from another thread while the resume holds the claim and the durable lock
            Concurrently calls = stack.callWhileHeld(stack::resumeAtPosition, stack::pause);

            // Then the pause waited, and both returned once the resume was let go, the pause landing on the resumed subscription
            calls.firstReturned();
            calls.secondReturned();
            assertAll(
                    () -> assertThat(calls.secondWaitedForTheFirst()).as("pause waited").isTrue(),
                    () -> assertThat(stack.storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(POSITION),
                    () -> assertThat(stack.wrapped.isPaused(SUBSCRIPTION_ID)).as("paused").isTrue()
            );
        });
    }

    @Test
    void a_cancel_waits_for_a_resume_at_a_position_in_the_wrapped_model_and_both_return() {
        assertTimeoutPreemptively(SCENARIO_TIMEOUT, () -> {
            // Given a paused subscription, and a resume at a position held inside the wrapped model
            Stack stack = Stack.withPausedSubscription();
            stack.wrapped.holdNext(Operation.RESUME_AT_POSITION);

            // When a cancel arrives from another thread while the resume holds the claim and the durable lock
            Concurrently calls = stack.callWhileHeld(stack::resumeAtPosition, stack::cancel);

            // Then the cancel waited, and both returned once the resume was let go, the cancel landing on the resumed subscription
            calls.firstReturned();
            calls.secondReturned();
            assertAll(
                    () -> assertThat(calls.secondWaitedForTheFirst()).as("cancel waited").isTrue(),
                    () -> assertThat(stack.wrapped.isPaused(SUBSCRIPTION_ID)).as("paused").isFalse(),
                    () -> assertThat(stack.wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isFalse()
            );
        });
    }

    @Test
    void a_resume_at_a_position_waits_for_a_pause_in_the_wrapped_model_and_both_return() {
        assertTimeoutPreemptively(SCENARIO_TIMEOUT, () -> {
            // Given a running subscription, and a pause held inside the wrapped model
            Stack stack = Stack.withRunningSubscription();
            stack.wrapped.holdNext(Operation.PAUSE);

            // When a resume at a position arrives from another thread while the pause holds the claim
            Concurrently calls = stack.callWhileHeld(stack::pause, stack::resumeAtPosition);

            // Then the resume waited, and both returned once the pause was let go, the resume landing on the paused subscription
            calls.firstReturned();
            calls.secondReturned();
            assertAll(
                    () -> assertThat(calls.secondWaitedForTheFirst()).as("resume waited").isTrue(),
                    () -> assertThat(stack.storage.read(SUBSCRIPTION_ID)).as("stored").isEqualTo(POSITION),
                    () -> assertThat(stack.wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isTrue()
            );
        });
    }

    @Test
    void a_resume_at_a_position_waits_for_a_cancel_in_the_wrapped_model_and_throws_what_the_wrapped_model_throws_for_an_unknown_subscription() {
        assertTimeoutPreemptively(SCENARIO_TIMEOUT, () -> {
            // Given a running subscription, and a cancel held inside the wrapped model
            Stack stack = Stack.withRunningSubscription();
            stack.wrapped.holdNext(Operation.CANCEL);

            // When a resume at a position arrives from another thread while the cancel holds the claim and the durable lock
            Concurrently calls = stack.callWhileHeld(stack::cancel, stack::resumeAtPosition);

            // Then the resume waited, the cancel returned, and the resume found nothing to resume once it got its turn
            calls.firstReturned();
            Throwable resumeFailure = catchThrowable(calls::secondReturned);
            assertAll(
                    () -> assertThat(calls.secondWaitedForTheFirst()).as("resume waited").isTrue(),
                    () -> assertThat(resumeFailure).isInstanceOf(ExecutionException.class).cause().isInstanceOf(IllegalStateException.class).hasMessageContaining("is not paused"),
                    () -> assertThat(stack.wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isFalse()
            );
        });
    }

    private enum Operation {
        RESUME_AT_POSITION, PAUSE, CANCEL
    }

    // The catch-up model over the durable model over the wrapped model, as the Spring Boot starter builds them below the competing consumer
    private static final class Stack {
        final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        final HoldableModel wrapped = new HoldableModel();
        final StreamCatchupSubscriptionModel catchup;

        private Stack() {
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
            catchup = new StreamCatchupSubscriptionModel(durable, unused(EventStoreQueries.class, CatchupReads.class), new CatchupSubscriptionModelConfig(100));
        }

        static Stack withRunningSubscription() {
            Stack stack = new Stack();
            stack.catchup.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> {
            }).waitUntilStarted();
            return stack;
        }

        static Stack withPausedSubscription() {
            Stack stack = withRunningSubscription();
            stack.pause();
            return stack;
        }

        void resumeAtPosition() {
            RepositionableSubscriptions.findIn(catchup).orElseThrow().resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(POSITION));
        }

        void pause() {
            catchup.pauseSubscription(SUBSCRIPTION_ID);
        }

        void cancel() {
            catchup.cancelSubscription(SUBSCRIPTION_ID);
        }

        // Runs first on a thread until it is held inside the wrapped model, then second on another thread, and lets the first go once second has had time to get in the way
        Concurrently callWhileHeld(Runnable first, Runnable second) throws InterruptedException {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> firstCall = pool.submit(first);
                wrapped.awaitHeld();
                Future<?> secondCall = pool.submit(second);
                boolean secondWaited;
                try {
                    secondCall.get(STILL_WAITING_MILLIS, TimeUnit.MILLISECONDS);
                    secondWaited = false;
                } catch (TimeoutException stillWaiting) {
                    secondWaited = true;
                } catch (ExecutionException returnedWithFailure) {
                    secondWaited = false;
                }
                wrapped.release();
                return new Concurrently(pool, firstCall, secondCall, secondWaited);
            } catch (RuntimeException | Error e) {
                wrapped.release();
                pool.shutdownNow();
                throw e;
            }
        }
    }

    private record Concurrently(ExecutorService pool, Future<?> first, Future<?> second, boolean secondWaitedForTheFirst) {
        void firstReturned() throws Exception {
            first.get(10, TimeUnit.SECONDS);
        }

        void secondReturned() throws Exception {
            try {
                second.get(10, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    /**
     * Tracks running and paused subscriptions as a live model does, and holds the next call of one operation until released.
     */
    private static final class HoldableModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        private final Set<String> running = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private volatile @Nullable Operation holding = null;
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void holdNext(Operation operation) {
            holding = operation;
        }

        void awaitHeld() throws InterruptedException {
            assertThat(held.await(10, TimeUnit.SECONDS)).as("the wrapped model was reached").isTrue();
        }

        void release() {
            released.countDown();
        }

        private void holdIf(Operation operation) {
            if (holding == operation) {
                held.countDown();
                try {
                    assertThat(released.await(20, TimeUnit.SECONDS)).as("released").isTrue();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            holdIf(Operation.RESUME_AT_POSITION);
            if (!paused.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            running.add(subscriptionId);
            return subscription(subscriptionId);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException("a plain resume is not expected");
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            holdIf(Operation.PAUSE);
            if (!running.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not running");
            }
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            holdIf(Operation.CANCEL);
            running.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("global");
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        private static Subscription subscription(String subscriptionId) {
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

    // An implementation of the interface that fails every call
    @SuppressWarnings("unchecked")
    private static <T> T unused(Class<T> type, Class<?>... moreTypes) {
        Class<?>[] interfaces = new Class<?>[1 + moreTypes.length];
        interfaces[0] = type;
        System.arraycopy(moreTypes, 0, interfaces, 1, moreTypes.length);
        return (T) Proxy.newProxyInstance(type.getClassLoader(), interfaces, (proxy, method, args) -> {
            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "an unused " + type.getSimpleName();
                };
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }
}
