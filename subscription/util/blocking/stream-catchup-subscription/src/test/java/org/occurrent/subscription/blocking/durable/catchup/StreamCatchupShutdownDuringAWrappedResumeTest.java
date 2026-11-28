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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A resume of a subscription that the catch-up passes on to a durable model, which resumes it in the model it wraps,
 * runs in the catch-up's turn, under the model lock. When that resume blocks until the wrapped model is shut down, a
 * {@code shutdown()} made on another thread has to shut the wrapped model down without waiting for the lock, as it does
 * for any other call the lock holder waits for, or it waits for a lock that is never released. A call for another
 * subscription that waits for the lock meanwhile has to complete too.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupShutdownDuringAWrappedResumeTest {

    private static final String A = "subscription-a";
    private static final String B = "subscription-b";
    // The shutdown shuts the wrapped model down without the lock once the lock holder has waited 100 ms for one subscription, so it returns
    // well within a second. It throws after 10 seconds when it has to wait for the lock, so a wait that reaches 9 seconds is the lock waited for
    private static final Duration SHUTDOWN_BOUND = Duration.ofSeconds(9);

    private InMemorySubscriptionModel inMemory;
    private ResumeThatBlocksUntilShutdown live;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_a_catch_up_over_a_durable_model_over_a_model_whose_resume_blocks_until_it_is_shut_down_with_two_paused_subscriptions() {
        inMemory = new InMemorySubscriptionModel();
        live = new ResumeThatBlocksUntilShutdown(inMemory);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(live, storage);
        catchup = new StreamCatchupSubscriptionModel(durable, new InMemoryEventStore(inMemory),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        for (String id : new String[]{A, B}) {
            assertThat(catchup.subscribe(id, StartAt.now(), event -> {
            }).waitUntilStarted(Duration.ofSeconds(10))).as("whether " + id + " started").isTrue();
            catchup.pauseSubscription(id);
        }
    }

    @AfterEach
    void let_the_blocked_resume_go_and_shut_down_without_waiting_for_a_call_that_never_returns() {
        live.letTheBlockedResumeGo();
        shutDownOnADaemonThread(catchup::shutdown);
        shutDownOnADaemonThread(inMemory::shutdown);
    }

    @Test
    @Timeout(60)
    void a_shutdown_made_while_the_wrapped_model_resumes_a_subscription_returns_and_so_does_a_resume_of_another_subscription_waiting_for_the_lock() throws Exception {
        // Given a resume of A that is inside the wrapped model's resume, which blocks until the wrapped model is shut down
        live.blocksResumes();
        Call resumeOfA = Call.onAThread("resume-of-a", () -> catchup.resumeSubscription(A));
        assertThat(live.insideTheResume.await(15, TimeUnit.SECONDS)).as("whether the resume of A is inside the wrapped model's resume").isTrue();
        // And a resume of B that waits for the model lock the resume of A holds
        Call resumeOfB = Call.onAThread("resume-of-b", () -> catchup.resumeSubscription(B));
        await().atMost(15, TimeUnit.SECONDS).until(() -> resumeOfB.thread.getState() != Thread.State.RUNNABLE);
        assertThat(resumeOfB.outcome.isDone()).as("whether the resume of B returned while the resume of A holds the lock").isFalse();

        // When the model is shut down from a third thread
        Call shutdown = Call.onAThread("shutdown", catchup::shutdown);

        // Then it returns without waiting for the lock, which is the shutdown of the wrapped model ending the resume of A
        assertThat(shutdown.isDoneWithin(SHUTDOWN_BOUND)).as("whether the shutdown returned within " + SHUTDOWN_BOUND.toSeconds() + " seconds, while the resume of A is in the wrapped model's resume").isTrue();
        assertThat(shutdown.threw()).as("what the shutdown threw").isNull();
        assertThat(live.shutDown.getCount()).as("whether the wrapped model was shut down").isZero();
        // And the resume of A, which the wrapped model ended by throwing, and the resume of B, which waited for the lock, both complete. The test asks for
        // completion only, since a resume made on a model that is shutting down may return or throw, and neither leaves anything waiting
        assertThat(resumeOfA.isDoneWithin(SHUTDOWN_BOUND)).as("whether the resume of A completed").isTrue();
        assertThat(resumeOfB.isDoneWithin(SHUTDOWN_BOUND)).as("whether the resume of B completed, which it does by returning or by throwing once the lock is free").isTrue();
    }

    private static final class Call {
        final Thread thread;
        final CompletableFuture<@Nullable Throwable> outcome = new CompletableFuture<>();

        private Call(String name, Runnable call) {
            thread = new Thread(() -> {
                try {
                    call.run();
                    outcome.complete(null);
                } catch (Throwable e) {
                    outcome.complete(e);
                }
            }, name);
            thread.setDaemon(true);
        }

        static Call onAThread(String name, Runnable call) {
            Call made = new Call(name, call);
            made.thread.start();
            return made;
        }

        boolean isDoneWithin(Duration timeout) {
            try {
                outcome.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                return outcome.isDone();
            }
        }

        @Nullable Throwable threw() {
            return outcome.isDone() ? outcome.getNow(null) : null;
        }
    }

    private static void shutDownOnADaemonThread(Runnable shutdown) {
        Call.onAThread("shutdown-after-the-test", shutdown).isDoneWithin(Duration.ofSeconds(10));
    }

    /**
     * Resumes like a model that isn't part of Occurrent whose resume blocks, once told to, until the model is shut
     * down, and then throws, as a resume does that finds the model closed. Not repositionable, so a durable model
     * wrapping it resumes it with {@code resumeSubscription(id)}.
     */
    private static final class ResumeThatBlocksUntilShutdown implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        private volatile boolean blocksResumes;
        final CountDownLatch insideTheResume = new CountDownLatch(1);
        final CountDownLatch shutDown = new CountDownLatch(1);

        private ResumeThatBlocksUntilShutdown(InMemorySubscriptionModel delegate) {
            this.delegate = delegate;
        }

        void blocksResumes() {
            blocksResumes = true;
        }

        void letTheBlockedResumeGo() {
            shutDown.countDown();
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (blocksResumes) {
                insideTheResume.countDown();
                try {
                    shutDown.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("The model was shut down while " + subscriptionId + " was resumed");
            }
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            shutDown.countDown();
            delegate.shutdown();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return true;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }
    }
}
