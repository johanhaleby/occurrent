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
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An evaluation of the start position runs outside the subscription-id lock. When a cancel deletes the checkpoint an
 * evaluation has read before that evaluation stores this node's write version with it, the evaluation is refused, so
 * the cancelled subscription is not left with a checkpoint again. An evaluation that has no version to store is not
 * refused, and starts from the checkpoint it read.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelCancelBetweenReadAndStampTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Checkpoint STORED = new StringBasedCheckpoint("stored-checkpoint");

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void shutDown() {
        executor.shutdownNow();
    }

    @Test
    void a_later_evaluation_whose_read_a_cancel_overtakes_stores_nothing_and_is_refused() throws Exception {
        HoldingStorage storage = new HoldingStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        CapturingFeed feed = new CapturingFeed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        Future<?> evaluation = executor.submit(() -> {
            storage.holdReadsOf(Thread.currentThread());
            return feed.startAt.get(new SubscriptionModelContext(CapturingFeed.class));
        });
        assertThat(storage.readReturned.await(10, TimeUnit.SECONDS)).as("the evaluation read the checkpoint").isTrue();
        durable.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("test setup, the cancel deleted the checkpoint").isNull();
        storage.release.countDown();

        Throwable thrown = catchThrowable(() -> evaluation.get(10, TimeUnit.SECONDS));

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("stored checkpoint").isNull(),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(thrown).as("thrown by the evaluation").isInstanceOf(ExecutionException.class)
                        .cause().isInstanceOf(IllegalStateException.class)
        );
    }

    @Test
    void a_later_evaluation_without_a_version_whose_read_a_cancel_overtakes_starts_from_the_checkpoint_it_read_and_stores_nothing() throws Exception {
        HoldingStorage storage = new HoldingStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        CapturingFeed feed = new CapturingFeed();
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.empty());
        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        Future<StartAt> evaluation = executor.submit(() -> {
            storage.holdReadsOf(Thread.currentThread());
            return feed.startAt.get(new SubscriptionModelContext(CapturingFeed.class));
        });
        assertThat(storage.readReturned.await(10, TimeUnit.SECONDS)).as("the evaluation read the checkpoint").isTrue();
        durable.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("test setup, the cancel deleted the checkpoint").isNull();
        storage.release.countDown();

        AtomicReference<StartAt> returned = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> returned.set(evaluation.get(10, TimeUnit.SECONDS)));

        assertAll(
                () -> assertThat(thrown).as("thrown by the evaluation").isNull(),
                () -> assertThat(returned.get()).as("start position returned").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo(STORED.asString())),
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("stored checkpoint").isNull(),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty()
        );
    }

    @Test
    void an_evaluation_that_settles_on_another_thread_while_subscribe_is_under_way_stores_nothing_and_is_refused_when_a_cancel_overtakes_its_read() {
        HoldingStorage storage = new HoldingStorage();
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        EvaluatingOnAnotherThreadFeed feed = new EvaluatingOnAnotherThreadFeed(storage, executor);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage, id -> OptionalLong.of(2));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });
        assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("test setup, the subscribing thread settled and stored its version").hasValue(2L);
        durable.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("test setup, the cancel deleted the checkpoint").isNull();
        storage.release.countDown();

        Future<?> evaluation = Objects.requireNonNull(feed.evaluation.get(), "the wrapped model started no evaluation");
        Throwable thrown = catchThrowable(() -> evaluation.get(10, TimeUnit.SECONDS));

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("stored checkpoint").isNull(),
                () -> assertThat(storage.delegate.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                () -> assertThat(thrown).as("thrown by the evaluation").isInstanceOf(ExecutionException.class)
                        .cause().isInstanceOf(IllegalStateException.class)
        );
    }

    /**
     * Over {@link InMemoryCheckpointStorage}, holds a read of one thread right after the read returned, until released.
     */
    private static final class HoldingStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final CountDownLatch readReturned = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private volatile @Nullable Thread held;

        void holdReadsOf(Thread thread) {
            held = thread;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint read = delegate.read(subscriptionId);
            if (Thread.currentThread() == held) {
                held = null;
                readReturned.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return read;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }
    }

    /**
     * Keeps the start position it is given and evaluates nothing, so every evaluation is the test's own.
     */
    private static final class CapturingFeed extends NoOpFeed {
        volatile StartAt startAt;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this.startAt = startAt;
            return started(subscriptionId);
        }
    }

    /**
     * Evaluates the start position on a thread of its own during {@code subscribe}, without waiting for that
     * evaluation to finish, only for it to have read the stored checkpoint, which {@link HoldingStorage} then holds.
     */
    private static final class EvaluatingOnAnotherThreadFeed extends NoOpFeed {
        final AtomicReference<@Nullable Future<?>> evaluation = new AtomicReference<>();
        private final HoldingStorage storage;
        private final ExecutorService executor;

        EvaluatingOnAnotherThreadFeed(HoldingStorage storage, ExecutorService executor) {
            this.storage = storage;
            this.executor = executor;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            evaluation.set(executor.submit(() -> {
                storage.holdReadsOf(Thread.currentThread());
                return startAt.get(new SubscriptionModelContext(EvaluatingOnAnotherThreadFeed.class));
            }));
            try {
                if (!storage.readReturned.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("the evaluation never read the stored checkpoint");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return started(subscriptionId);
        }
    }

    private abstract static class NoOpFeed implements CheckpointAwareSubscriptionModel {
        Subscription started(String subscriptionId) {
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

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("global-checkpoint");
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
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
