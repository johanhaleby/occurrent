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
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

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
 * A resume at a position while an evaluation of the start position of a subscription from the model default reads what
 * is stored. The storage doesn't evaluate write conditions and the model is allowed to write a first position over a
 * checkpoint, so a first position recorded after the resume's write replaces the position the resume stored. The write of
 * the resume is held before it lands, and the evaluation reads that nothing is stored and goes on to record the first
 * position while it is held, so the evaluation is refused, or starts from the position of the resume, whichever the
 * write does.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelResumeWriteFirstPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint FIRST_POSITION = new StringBasedCheckpoint("first-position");
    private static final Checkpoint STORED_BY_THE_RESUME = new StringBasedCheckpoint("stored-by-the-resume");
    private static final String REFUSAL = "was resumed, or restarted after its history was lost, after this evaluation found no checkpoint stored";

    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final HoldsTheWriteOfTheResume storage = new HoldsTheWriteOfTheResume();
    private final ExecutorService threads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheWriteAndStopTheThreads() {
        storage.writeMayProceed.countDown();
        threads.shutdownNow();
    }

    @Test
    void an_evaluation_that_reads_nothing_stored_while_the_write_of_a_resume_at_a_position_is_held_does_not_record_a_first_position_over_the_position_the_resume_stores() throws Exception {
        // Given
        DurableSubscriptionModel model = subscribedWithNothingStored();
        wrapped.globalCheckpoint = FIRST_POSITION;
        model.pauseSubscription(SUBSCRIPTION_ID);
        Future<Object> resume = threads.submit(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_RESUME)));
        assertThat(awaitQuietly(storage.writeHeld)).as("write of the resume held").isTrue();

        // When
        Future<@Nullable StartAt> evaluation = threads.submit(() -> {
            storage.markTheEvaluationThread();
            return wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        });
        assertThat(awaitQuietly(storage.evaluationRead)).as("evaluation read what is stored").isTrue();
        storage.writeMayProceed.countDown();
        Throwable resumeFailure = failureOf(resume);
        Outcome evaluated = outcomeOf(evaluation);

        // Then
        assertAll(
                () -> assertThat(resumeFailure).as("failure of the resume").isNull(),
                () -> assertRefusedOrStartedFromTheResume(evaluated),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(STORED_BY_THE_RESUME)
        );
    }

    @Test
    void an_evaluation_that_reads_nothing_stored_while_the_write_of_a_resume_at_a_position_is_held_is_refused_when_that_write_throws() throws Exception {
        // Given
        DurableSubscriptionModel model = subscribedWithNothingStored();
        wrapped.globalCheckpoint = FIRST_POSITION;
        storage.failTheHeldWrite = true;
        model.pauseSubscription(SUBSCRIPTION_ID);
        Future<Object> resume = threads.submit(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_RESUME)));
        assertThat(awaitQuietly(storage.writeHeld)).as("write of the resume held").isTrue();

        // When
        Future<@Nullable StartAt> evaluation = threads.submit(() -> {
            storage.markTheEvaluationThread();
            return wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        });
        assertThat(awaitQuietly(storage.evaluationRead)).as("evaluation read what is stored").isTrue();
        storage.writeMayProceed.countDown();
        Throwable resumeFailure = failureOf(resume);
        Outcome evaluated = outcomeOf(evaluation);

        // Then
        assertAll(
                () -> assertThat(resumeFailure).as("failure of the resume").isInstanceOf(IllegalStateException.class).hasMessage(HoldsTheWriteOfTheResume.FAILURE),
                () -> assertThat(evaluated.failure()).as("refusal of the evaluation").isInstanceOf(IllegalStateException.class).hasMessageContaining(REFUSAL),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isNull()
        );
    }

    private DurableSubscriptionModel subscribedWithNothingStored() {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(everyEvent()).startWhenNoStartPositionCanBeRecorded(true).startWhenFirstPositionCanBeOverwritten(true));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored by the subscribe").isNull();
        return model;
    }

    private static void assertRefusedOrStartedFromTheResume(Outcome evaluated) {
        if (evaluated.failure() != null) {
            assertThat(evaluated.failure()).as("refusal of the evaluation").isInstanceOf(IllegalStateException.class).hasMessageContaining(REFUSAL);
        } else {
            assertThat(evaluated.startAt()).as("start position evaluated").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                    startAt -> assertThat(startAt.checkpoint).isEqualTo(STORED_BY_THE_RESUME));
        }
    }

    private record Outcome(@Nullable StartAt startAt, @Nullable Throwable failure) {
    }

    private static Outcome outcomeOf(Future<@Nullable StartAt> evaluation) throws Exception {
        try {
            return new Outcome(evaluation.get(10, SECONDS), null);
        } catch (ExecutionException e) {
            return new Outcome(null, e.getCause());
        }
    }

    private static @Nullable Throwable failureOf(Future<?> future) throws Exception {
        try {
            future.get(10, SECONDS);
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
     * Holds the write of the position of the resume before it lands, until the test lets it go on, and then writes it, or
     * throws without writing it. Doesn't evaluate write conditions, so the first position is written unconditionally.
     * Tells the test when the evaluation has read what is stored.
     */
    private static final class HoldsTheWriteOfTheResume extends InMemoryCheckpointStorage {
        static final String FAILURE = "the write of the resume failed";

        final CountDownLatch writeHeld = new CountDownLatch(1);
        final CountDownLatch writeMayProceed = new CountDownLatch(1);
        final CountDownLatch evaluationRead = new CountDownLatch(1);
        volatile boolean failTheHeldWrite;
        private volatile @Nullable Thread evaluationThread;

        void markTheEvaluationThread() {
            evaluationThread = Thread.currentThread();
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint checkpoint = super.read(subscriptionId);
            if (Thread.currentThread() == evaluationThread) {
                evaluationRead.countDown();
            }
            return checkpoint;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (checkpoint.equals(STORED_BY_THE_RESUME)) {
                writeHeld.countDown();
                awaitQuietly(writeMayProceed);
                if (failTheHeldWrite) {
                    throw new IllegalStateException(FAILURE);
                }
            }
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return false;
        }
    }
}
