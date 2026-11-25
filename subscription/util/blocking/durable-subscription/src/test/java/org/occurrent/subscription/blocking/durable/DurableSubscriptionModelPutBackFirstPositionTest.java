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
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * An evaluation of the start position of a subscription from the model default, which the wrapped model makes after the
 * subscribe returned, while the put-back of a failed resume at a position is held before it lands. The evaluation reads the
 * position the resume stored, which the put-back is about to replace, so what it read is no longer what is stored once the
 * put-back has landed. It then doesn't make that position the start position of the subscription, so no quiet position is
 * saved over the checkpoint the put-back restored. The wrapped model fails the resume, and the evaluation is held on a
 * thread of the test after its read.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelPutBackFirstPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint PREVIOUS = new StringBasedCheckpoint("previous");
    private static final Checkpoint STORED_BY_THE_RESUME = new StringBasedCheckpoint("stored-by-the-resume");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final FailingResumeModel wrapped = new FailingResumeModel();
    private final HoldsThePutBack storage = new HoldsThePutBack();
    private final ExecutorService threads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseThePutBackAndStopTheThreads() {
        storage.putBackMayProceed.countDown();
        threads.shutdownNow();
    }

    @Test
    void an_evaluation_that_read_the_position_of_a_failed_resume_while_its_put_back_was_held_does_not_have_a_quiet_position_saved_over_the_checkpoint_the_put_back_stored() throws Exception {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage,
                new DurableSubscriptionModelConfig(everyEvent()).startWhenNoStartPositionCanBeRecorded(true).saveQuietPositionEvery(Duration.ofNanos(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        storage.saveUnheld(SUBSCRIPTION_ID, PREVIOUS);
        model.pauseSubscription(SUBSCRIPTION_ID);
        storage.holdThePutBack = true;
        Future<?> resume = threads.submit(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_RESUME)));
        assertThat(awaitQuietly(storage.putBackHeld)).as("put-back held").isTrue();
        Future<@Nullable StartAt> evaluation = threads.submit(() -> {
            storage.markTheEvaluationThread();
            return wrapped.evaluateStartAt(SUBSCRIPTION_ID);
        });
        assertThat(awaitQuietly(storage.evaluationRead)).as("evaluation read what is stored").isTrue();

        // When
        storage.putBackMayProceed.countDown();
        Throwable resumeFailure = failureOf(resume);
        StartAt evaluated = evaluation.get(10, SECONDS);
        Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
        if (saver != null) {
            saver.accept(QUIET);
        }

        // Then
        assertAll(
                () -> assertThat(resumeFailure).as("failure of the resume").isInstanceOf(IllegalStateException.class),
                () -> assertThat(evaluated).as("start position evaluated").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint).isEqualTo(STORED_BY_THE_RESUME)),
                () -> assertThat(saver).as("saver handed out for a quiet position").isNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(PREVIOUS)
        );
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
     * A wrapped model that can be paused, and whose resume at a position fails.
     */
    private static final class FailingResumeModel extends QuietPositionReportingModel implements RepositionableSubscriptions {
        private final Set<String> paused = ConcurrentHashMap.newKeySet();

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            throw new IllegalStateException("the resume failed");
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return true;
        }
    }

    /**
     * Holds the write that puts the checkpoint stored before the resume back, until the test lets it go on, and tells the
     * test when the evaluation has read what is stored.
     */
    private static final class HoldsThePutBack extends InMemoryCheckpointStorage {
        final CountDownLatch putBackHeld = new CountDownLatch(1);
        final CountDownLatch putBackMayProceed = new CountDownLatch(1);
        final CountDownLatch evaluationRead = new CountDownLatch(1);
        volatile boolean holdThePutBack;
        private volatile @Nullable Thread evaluationThread;

        void markTheEvaluationThread() {
            evaluationThread = Thread.currentThread();
        }

        void saveUnheld(String subscriptionId, Checkpoint checkpoint) {
            super.save(subscriptionId, checkpoint, CheckpointWriteCondition.any());
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
            if (holdThePutBack && checkpoint.equals(PREVIOUS)) {
                putBackHeld.countDown();
                awaitQuietly(putBackMayProceed);
            }
            return super.save(subscriptionId, checkpoint, condition);
        }
    }
}
