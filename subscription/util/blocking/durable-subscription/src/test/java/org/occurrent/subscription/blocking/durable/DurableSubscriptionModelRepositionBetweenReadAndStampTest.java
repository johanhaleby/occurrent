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
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * An evaluation of the start position from the model default reads a stored checkpoint P and, with a write version
 * configured, stores this node's version with it. When a resume, a resume at a position or a restart after a lost
 * history stores a position R2 after that read and before that store, storing P again could replace R2 with an earlier
 * position, so the evaluation is refused and R2 stays stored. The storage here stores the checkpoint it is asked to
 * resolve whatever it holds, as a MongoDB storage does for an earlier operation time, so storing the version with P, when it isn't refused,
 * shows as P stored.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelRepositionBetweenReadAndStampTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Checkpoint STORED = new StringBasedCheckpoint("stored-checkpoint");
    private static final Checkpoint STORED_BY_THE_REPOSITION = new StringBasedCheckpoint("stored-by-the-reposition");
    private static final String REFUSAL = "was resumed, or restarted after its history was lost, after this evaluation read the checkpoint stored-checkpoint";

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final HoldingStorage storage = new HoldingStorage();

    @AfterEach
    void shutDown() {
        storage.release.countDown();
        executor.shutdownNow();
    }

    @Test
    void a_later_evaluation_is_refused_when_a_plain_resume_stored_a_position_between_its_read_and_its_stamp() throws Exception {
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        DurableSubscriptionModel durable = subscribedOverStoredCheckpoint(wrapped);
        Future<@Nullable StartAt> evaluation = evaluateAndHoldAfterTheRead(wrapped);

        anotherNodeStoresWhilePaused(durable);
        durable.resumeSubscription(SUBSCRIPTION_ID);
        storage.release.countDown();

        assertRefusedAndStillStored(evaluation);
    }

    @Test
    void a_later_evaluation_is_refused_when_a_resume_at_a_position_stored_it_between_its_read_and_its_stamp() throws Exception {
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        DurableSubscriptionModel durable = subscribedOverStoredCheckpoint(wrapped);
        Future<@Nullable StartAt> evaluation = evaluateAndHoldAfterTheRead(wrapped);

        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_REPOSITION));
        storage.release.countDown();

        assertRefusedAndStillStored(evaluation);
    }

    @Test
    void a_later_evaluation_is_refused_when_a_restart_after_a_lost_history_stored_a_position_between_its_read_and_its_stamp() throws Exception {
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        subscribedOverStoredCheckpoint(wrapped);
        Future<@Nullable StartAt> evaluation = evaluateAndHoldAfterTheRead(wrapped);

        wrapped.historyLossListeners.getFirst().restartingAfterHistoryLoss(SUBSCRIPTION_ID, STORED_BY_THE_REPOSITION, () -> true);
        storage.release.countDown();

        assertRefusedAndStillStored(evaluation);
    }

    @Test
    void an_evaluation_that_settles_on_another_thread_while_subscribe_is_under_way_is_refused_when_a_plain_resume_stored_a_position_between_its_read_and_its_stamp() throws Exception {
        EvaluatingOnAnotherThreadModel wrapped = new EvaluatingOnAnotherThreadModel();
        DurableSubscriptionModel durable = subscribedOverStoredCheckpoint(wrapped);
        Future<@Nullable StartAt> evaluation = Objects.requireNonNull(wrapped.evaluation.get(), "the wrapped model started no evaluation");

        anotherNodeStoresWhilePaused(durable);
        durable.resumeSubscription(SUBSCRIPTION_ID);
        storage.release.countDown();

        assertRefusedAndStillStored(evaluation);
    }

    @Test
    void an_evaluation_that_settles_on_another_thread_while_subscribe_is_under_way_is_refused_when_a_resume_at_a_position_stored_it_between_its_read_and_its_stamp() throws Exception {
        EvaluatingOnAnotherThreadModel wrapped = new EvaluatingOnAnotherThreadModel();
        DurableSubscriptionModel durable = subscribedOverStoredCheckpoint(wrapped);
        Future<@Nullable StartAt> evaluation = Objects.requireNonNull(wrapped.evaluation.get(), "the wrapped model started no evaluation");

        durable.pauseSubscription(SUBSCRIPTION_ID);
        durable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_REPOSITION));
        storage.release.countDown();

        assertRefusedAndStillStored(evaluation);
    }

    @Test
    void an_evaluation_that_settles_on_another_thread_while_subscribe_is_under_way_is_refused_when_a_restart_after_a_lost_history_stored_a_position_between_its_read_and_its_stamp() throws Exception {
        EvaluatingOnAnotherThreadModel wrapped = new EvaluatingOnAnotherThreadModel();
        subscribedOverStoredCheckpoint(wrapped);
        Future<@Nullable StartAt> evaluation = Objects.requireNonNull(wrapped.evaluation.get(), "the wrapped model started no evaluation");

        wrapped.historyLossListeners.getFirst().restartingAfterHistoryLoss(SUBSCRIPTION_ID, STORED_BY_THE_REPOSITION, () -> true);
        storage.release.countDown();

        assertRefusedAndStillStored(evaluation);
    }

    private DurableSubscriptionModel subscribedOverStoredCheckpoint(QuietPositionReportingModel wrapped) {
        storage.delegate.save(SUBSCRIPTION_ID, STORED);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage, id -> OptionalLong.of(2));
        durable.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("test setup, checkpoint stored by the subscribe").isEqualTo(STORED);
        return durable;
    }

    // Returns once the evaluation has read what is stored, and holds it there until the test releases it
    private Future<@Nullable StartAt> evaluateAndHoldAfterTheRead(QuietPositionReportingModel wrapped) {
        Future<@Nullable StartAt> evaluation = executor.submit(() -> {
            storage.holdReadsOf(Thread.currentThread());
            return wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        });
        assertThat(awaitQuietly(storage.readReturned)).as("the evaluation read the checkpoint").isTrue();
        return evaluation;
    }

    // Stored behind the model's back, as another node does while the subscription is paused, so the plain resume goes to it
    private void anotherNodeStoresWhilePaused(DurableSubscriptionModel durable) {
        durable.pauseSubscription(SUBSCRIPTION_ID);
        storage.delegate.delete(SUBSCRIPTION_ID);
        storage.delegate.save(SUBSCRIPTION_ID, STORED_BY_THE_REPOSITION);
    }

    private void assertRefusedAndStillStored(Future<?> evaluation) throws Exception {
        Throwable refusal = failureOf(evaluation);

        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("stored checkpoint").isNotNull()
                        .extracting(Checkpoint::asString).isEqualTo(STORED_BY_THE_REPOSITION.asString()),
                () -> assertThat(refusal).as("refusal of the evaluation").isInstanceOf(IllegalStateException.class).hasMessageContaining(REFUSAL)
        );
    }

    private static @Nullable Throwable failureOf(Future<?> evaluation) throws Exception {
        try {
            evaluation.get(10, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static boolean awaitQuietly(CountDownLatch latch) {
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Over {@link InMemoryCheckpointStorage}, holds the next read of one thread right after it returned, until released.
     * Resolving a checkpoint with a version stores it whatever is stored, as a MongoDB storage does for an earlier
     * operation time.
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
                awaitQuietly(release);
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
            return Optional.of(delegate.save(subscriptionId, candidate, CheckpointWriteCondition.notOlderThan(writeVersion)));
        }
    }

    /**
     * Evaluates the start position on a thread of its own during {@code subscribe}, without waiting for that evaluation
     * to finish, only for it to have read the stored checkpoint, which {@link HoldingStorage} then holds. Pauses and
     * resumes, so the test can reposition the subscription once {@code subscribe} has returned.
     */
    private final class EvaluatingOnAnotherThreadModel extends QuietPositionReportingModel implements RepositionableSubscriptions {
        final AtomicReference<@Nullable Future<@Nullable StartAt>> evaluation = new AtomicReference<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            Subscription subscription = super.subscribe(subscriptionId, filter, startAt, action);
            evaluation.set(executor.submit(() -> {
                storage.holdReadsOf(Thread.currentThread());
                return evaluateStartAt(subscriptionId);
            }));
            if (!awaitQuietly(storage.readReturned)) {
                throw new AssertionError("the evaluation never read the stored checkpoint");
            }
            return subscription;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return super.isRunning(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            paused.remove(subscriptionId);
            return resumed(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return resumeSubscription(subscriptionId);
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }

        private Subscription resumed(String subscriptionId) {
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
