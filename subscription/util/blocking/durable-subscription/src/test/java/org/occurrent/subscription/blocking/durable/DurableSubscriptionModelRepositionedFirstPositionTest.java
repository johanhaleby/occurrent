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

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * An evaluation of the start position of a subscription from the model default, which the wrapped model makes after
 * the subscribe returned, as a MongoDB model does when it restarts. It reads that nothing is stored, and before it
 * records the position the wrapped model answers as the first, a resume at a position or a restart after a lost history
 * stores where the subscription reads from next. Recording the first position then could replace that with an earlier
 * one, so the evaluation is refused and evaluating again starts from what is stored. The evaluation is held on a
 * thread of the test after its read, and the wrapped model is a fake the test reads from by hand.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelRepositionedFirstPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint FIRST_POSITION = new StringBasedCheckpoint("first-position");
    private static final Checkpoint STORED_BY_THE_RESUME = new StringBasedCheckpoint("stored-by-the-resume");
    private static final Checkpoint STORED_BY_THE_RESTART = new StringBasedCheckpoint("stored-by-the-restart");
    private static final String REFUSAL = "was resumed, or restarted after its history was lost, after this evaluation found no checkpoint stored";

    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final HoldsTheEvaluationAfterItsRead storage = new HoldsTheEvaluationAfterItsRead();
    private final ExecutorService evaluationThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheEvaluationAndStopItsThreads() {
        storage.evaluationMayContinue.countDown();
        evaluationThreads.shutdownNow();
    }

    @Test
    void an_evaluation_that_read_nothing_stored_is_refused_when_a_resume_at_a_position_stored_one_before_it_recorded_the_first_position() throws Exception {
        // Given
        DurableSubscriptionModel model = subscribedWithNothingStoredAndNoPositionToRecord();
        wrapped.globalCheckpoint = FIRST_POSITION;
        Future<@Nullable StartAt> evaluation = evaluateAndHoldAfterTheRead();
        model.pauseSubscription(SUBSCRIPTION_ID);

        // When
        model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_RESUME));
        storage.evaluationMayContinue.countDown();
        Throwable refusal = failureOf(evaluation);

        // Then
        StartAt evaluatedAgain = wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(STORED_BY_THE_RESUME),
                () -> assertThat(refusal).as("refusal of the evaluation").isInstanceOf(IllegalStateException.class).hasMessageContaining(REFUSAL),
                () -> assertThat(evaluatedAgain).as("start position evaluated again").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).isEqualTo(STORED_BY_THE_RESUME))
        );
    }

    @Test
    void an_evaluation_that_read_nothing_stored_is_refused_when_a_restart_after_a_lost_history_stored_a_position_before_it_recorded_the_first_position() throws Exception {
        // Given
        subscribedWithNothingStoredAndNoPositionToRecord();
        wrapped.globalCheckpoint = FIRST_POSITION;
        Future<@Nullable StartAt> evaluation = evaluateAndHoldAfterTheRead();

        // When
        wrapped.historyLossListeners.getFirst().restartingAfterHistoryLoss(SUBSCRIPTION_ID, STORED_BY_THE_RESTART, () -> true);
        storage.evaluationMayContinue.countDown();
        Throwable refusal = failureOf(evaluation);

        // Then
        StartAt evaluatedAgain = wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(STORED_BY_THE_RESTART),
                () -> assertThat(refusal).as("refusal of the evaluation").isInstanceOf(IllegalStateException.class).hasMessageContaining(REFUSAL),
                () -> assertThat(evaluatedAgain).as("start position evaluated again").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).isEqualTo(STORED_BY_THE_RESTART))
        );
    }

    @Test
    void an_evaluation_that_read_after_a_resume_at_a_position_found_nothing_stored_still_records_the_first_position() {
        // Given
        DurableSubscriptionModel model = subscribedWithNothingStoredAndNoPositionToRecord();
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_RESUME));
        storage.delete(SUBSCRIPTION_ID);
        wrapped.globalCheckpoint = FIRST_POSITION;

        // When
        StartAt evaluated = wrapped.evaluateStartAt(SUBSCRIPTION_ID);

        // Then
        assertAll(
                () -> assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(FIRST_POSITION),
                () -> assertThat(evaluated).as("start position evaluated").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).isEqualTo(FIRST_POSITION))
        );
    }

    private DurableSubscriptionModel subscribedWithNothingStoredAndNoPositionToRecord() {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).startWhenNoStartPositionCanBeRecorded(true));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        assertThat(storage.delegate.read(SUBSCRIPTION_ID)).as("checkpoint stored by the subscribe").isNull();
        return model;
    }

    // Returns once the evaluation has read what is stored, and holds it there until the test lets it go on
    private Future<@Nullable StartAt> evaluateAndHoldAfterTheRead() {
        Future<@Nullable StartAt> evaluation = evaluationThreads.submit(() -> {
            storage.holdTheNextReadOfThisThread();
            return wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        });
        assertThat(awaitQuietly(storage.evaluationRead)).as("evaluation read what is stored").isTrue();
        return evaluation;
    }

    private static @Nullable Throwable failureOf(Future<?> evaluation) throws Exception {
        try {
            evaluation.get(10, SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static boolean awaitQuietly(CountDownLatch latch) {
        try {
            return latch.await(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Holds the next read of one thread after it has found what is stored, until the test lets it go on. A lost
     * {@code ifAbsent()} write is settled by moving the position stored to the candidate, as a storage that overrides the
     * two-argument resolver can, so a first position recorded over a stored one shows as the checkpoint stored.
     */
    private static final class HoldsTheEvaluationAfterItsRead implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        final CountDownLatch evaluationRead = new CountDownLatch(1);
        final CountDownLatch evaluationMayContinue = new CountDownLatch(1);
        private volatile @Nullable Thread heldThread;

        void holdTheNextReadOfThisThread() {
            heldThread = Thread.currentThread();
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint checkpoint = delegate.read(subscriptionId);
            if (Thread.currentThread() == heldThread) {
                heldThread = null;
                evaluationRead.countDown();
                awaitQuietly(evaluationMayContinue);
            }
            return checkpoint;
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
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return Optional.of(delegate.save(subscriptionId, candidate));
        }
    }
}
