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

import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe at the model default that could record no start position, whose wrapped model evaluates the start
 * position later. An evaluation that reads a position and is overtaken by a plain resume must not hand out coverage
 * for a quiet position, since the resume stored the position of another node after that read.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelPlainResumeStaleFirstPositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint STORED_BEFORE_THE_EVALUATION_READ = new StringBasedCheckpoint("stored-before-the-evaluation-read");
    private static final Checkpoint STORED_BY_ANOTHER_NODE_AFTER_THE_READ = new GlobalCheckpoint(7);
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final RepositionableQuietPositionReportingModel wrapped = RepositionableQuietPositionReportingModel.continuingFromItsOwnPositionForAGlobalCheckpoint();
    private final StallingReadStorage storage = new StallingReadStorage();

    @Test
    void a_quiet_position_does_not_overwrite_the_checkpoint_a_plain_resume_recorded_while_an_evaluation_that_had_read_an_older_one_was_still_running() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = subscribedWithNoStartPositionRecorded();
        storage.save(SUBSCRIPTION_ID, STORED_BEFORE_THE_EVALUATION_READ);
        AtomicReference<StartAt> evaluated = new AtomicReference<>();
        Thread evaluation = new Thread(() -> evaluated.set(wrapped.evaluateStartAt(SUBSCRIPTION_ID)), "stalled-evaluation");
        storage.stallReadsOf(evaluation);
        evaluation.start();
        assertThat(storage.readReturned.await(5, SECONDS)).as("evaluation read the stored position").isTrue();

        // When
        model.pauseSubscription(SUBSCRIPTION_ID);
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE_AFTER_THE_READ);
        model.resumeSubscription(SUBSCRIPTION_ID);
        storage.release.countDown();
        evaluation.join(5_000);
        Consumer<Checkpoint> saver = saverWhenWanted();
        if (saver != null) {
            saver.accept(QUIET);
        }

        // Then
        assertAll(
                () -> assertThat(evaluation.isAlive()).as("evaluation still running").isFalse(),
                () -> assertThat(evaluated.get()).as("start position the evaluation returned").extracting(startAt -> ((StartAt.StartAtCheckpoint) startAt).checkpoint).isEqualTo(STORED_BEFORE_THE_EVALUATION_READ),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE_AFTER_THE_READ),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position, with nothing delivered since the resume").isEqualTo(STORED_BY_ANOTHER_NODE_AFTER_THE_READ)
        );
    }

    @Test
    void a_quiet_position_is_saved_over_the_position_an_evaluation_read_when_no_resume_overtook_it() throws InterruptedException {
        // Given
        DurableSubscriptionModel model = subscribedWithNoStartPositionRecorded();
        storage.save(SUBSCRIPTION_ID, STORED_BEFORE_THE_EVALUATION_READ);
        AtomicReference<StartAt> evaluated = new AtomicReference<>();
        Thread evaluation = new Thread(() -> evaluated.set(wrapped.evaluateStartAt(SUBSCRIPTION_ID)), "evaluation");
        storage.stallReadsOf(evaluation);
        evaluation.start();
        assertThat(storage.readReturned.await(5, SECONDS)).as("evaluation read the stored position").isTrue();

        // When
        storage.release.countDown();
        evaluation.join(5_000);
        Consumer<Checkpoint> saver = saverWhenWanted();
        if (saver != null) {
            saver.accept(QUIET);
        }

        // Then
        assertAll(
                () -> assertThat(model.isPaused(SUBSCRIPTION_ID)).as("subscription paused").isFalse(),
                () -> assertThat(evaluation.isAlive()).as("evaluation still running").isFalse(),
                () -> assertThat(evaluated.get()).as("start position the evaluation returned").extracting(startAt -> ((StartAt.StartAtCheckpoint) startAt).checkpoint).isEqualTo(STORED_BEFORE_THE_EVALUATION_READ),
                () -> assertThat(saver).as("quiet position wanted after the evaluation stored a position").isNotNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the quiet position").isEqualTo(QUIET)
        );
    }

    // No position can be recorded and nothing is stored, so the evaluations the wrapped model runs later find the position
    private DurableSubscriptionModel subscribedWithNoStartPositionRecorded() {
        DurableSubscriptionModelConfig config = new DurableSubscriptionModelConfig(__ -> false).startWhenNoStartPositionCanBeRecorded(true).saveQuietPositionEvery(Duration.ofMillis(1));
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, config);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
        });
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored by the subscribe").isNull();
        return model;
    }

    // Reads until a listener wants the quiet position, which the 1 ms interval allows after a moment. Null when none does
    private @Nullable Consumer<Checkpoint> saverWhenWanted() {
        try {
            return await().atMost(2, SECONDS).until(() -> wrapped.beforeReading(SUBSCRIPTION_ID), Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }

    // Reads from the real storage, then holds the one thread it is armed for until released, as a slow storage does
    private static final class StallingReadStorage extends InMemoryCheckpointStorage {
        final CountDownLatch readReturned = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<@Nullable Thread> stalled = new AtomicReference<>();

        void stallReadsOf(Thread thread) {
            stalled.set(thread);
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint read = super.read(subscriptionId);
            if (stalled.compareAndSet(Thread.currentThread(), null)) {
                readReturned.countDown();
                try {
                    if (!release.await(5, SECONDS)) {
                        throw new IllegalStateException("The stalled read was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return read;
        }
    }
}
