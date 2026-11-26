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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A plain {@link DurableSubscriptionModel#resumeSubscription(String)} of a paused subscription while a handler the pause
 * stopped waiting for is still running, whatever the wrapped model answers to
 * {@link org.occurrent.subscription.api.blocking.RepositionableSubscriptions#restartsAt(Checkpoint)}. The resume replaces
 * the run that handler belongs to, so the checkpoint of its event is never stored over the checkpoint the resume
 * went to. If it were, a crash would skip the replay that checkpoint, stored by another node, asked for.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelPlainResumeRunningHandlerTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint EVENT_BEFORE_THE_PAUSE = new StringBasedCheckpoint("event-before-the-pause");
    private static final Checkpoint EVENT_AFTER_THE_RESUME = new StringBasedCheckpoint("event-after-the-resume");
    private static final Checkpoint EVENT_OF_THE_RUNNING_HANDLER = new StringBasedCheckpoint("event-of-the-running-handler");
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("stored-by-another-node");
    private static final Checkpoint GLOBAL_STORED_BY_ANOTHER_NODE = new GlobalCheckpoint(7);
    private static final Checkpoint QUIET_BEFORE_E_IS_DELIVERED_AGAIN = new StringBasedCheckpoint("quiet-before-e-is-delivered-again");
    private static final Checkpoint QUIET_AFTER_E_IS_DELIVERED_AGAIN = new StringBasedCheckpoint("quiet-after-e-is-delivered-again");

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService handlerThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheHandlerAndStopItsThreads() {
        handlerMayReturn.countDown();
        handlerThreads.shutdownNow();
    }

    /**
     * The default of {@code restartsAt} is false, yet this model resumes at the checkpoint it is given. The resume
     * replaced the run of the running handler either way, so that run's save is refused. A quiet position still is not
     * stored until the event of that handler is delivered again, since the stored checkpoint isn't known to cover what was read before it.
     */
    @Test
    void a_plain_resume_of_a_wrapped_model_keeping_the_default_restartsAt_refuses_the_late_save_of_the_replaced_run_and_stores_again_once_the_event_is_delivered_again() throws Exception {
        // Given
        RepositionableQuietPositionReportingModel wrapped = RepositionableQuietPositionReportingModel.keepingTheDefaultRestartsAt();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        AtomicBoolean holdTheHandler = new AtomicBoolean();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
            if (holdTheHandler.get()) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        holdTheHandler.set(true);
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
        assertThat(awaitQuietly(handlerStarted)).as("handler started").isTrue();
        model.pauseSubscription(SUBSCRIPTION_ID);
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);
        holdTheHandler.set(false);
        Checkpoint storedAfterTheHandlerReturned = storage.read(SUBSCRIPTION_ID);
        Consumer<Checkpoint> saverBeforeEIsDeliveredAgain = saverWhenWanted(wrapped);
        if (saverBeforeEIsDeliveredAgain != null) {
            saverBeforeEIsDeliveredAgain.accept(QUIET_BEFORE_E_IS_DELIVERED_AGAIN);
        }
        Checkpoint storedAfterTheQuietPositionBeforeE = storage.read(SUBSCRIPTION_ID);
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER);
        Consumer<Checkpoint> saverAfterEIsDeliveredAgain = saverWhenWanted(wrapped);
        if (saverAfterEIsDeliveredAgain != null) {
            saverAfterEIsDeliveredAgain.accept(QUIET_AFTER_E_IS_DELIVERED_AGAIN);
        }

        // Then
        assertAll(
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE),
                () -> assertThat(storedAfterTheHandlerReturned).as("checkpoint stored after the handler the pause stopped waiting for returned").isEqualTo(STORED_BY_ANOTHER_NODE),
                () -> assertThat(storedAfterTheQuietPositionBeforeE).as("checkpoint stored after a quiet position before E is delivered again").isEqualTo(STORED_BY_ANOTHER_NODE),
                () -> assertThat(saverAfterEIsDeliveredAgain).as("quiet position wanted after E is delivered again").isNotNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position once E is delivered again").isEqualTo(QUIET_AFTER_E_IS_DELIVERED_AGAIN)
        );
    }

    /**
     * A resume at a {@code GlobalCheckpoint} goes on from the wrapped model's own position, so {@code restartsAt}
     * answers false. The run of the running handler is replaced all the same, so its save is refused, while a delivery
     * of the resumed run stores its own checkpoint.
     */
    @Test
    void a_plain_resume_of_a_wrapped_model_going_on_from_its_own_position_refuses_the_late_save_of_the_replaced_run_and_stores_the_checkpoint_of_a_later_delivery() throws Exception {
        // Given
        RepositionableQuietPositionReportingModel wrapped = RepositionableQuietPositionReportingModel.continuingFromItsOwnPositionForAGlobalCheckpoint();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        AtomicBoolean holdTheHandler = new AtomicBoolean();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
            if (holdTheHandler.get()) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        holdTheHandler.set(true);
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
        assertThat(awaitQuietly(handlerStarted)).as("handler started").isTrue();
        model.pauseSubscription(SUBSCRIPTION_ID);
        storage.save(SUBSCRIPTION_ID, GLOBAL_STORED_BY_ANOTHER_NODE);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);
        holdTheHandler.set(false);
        Checkpoint storedAfterTheHandlerReturned = storage.read(SUBSCRIPTION_ID);
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_AFTER_THE_RESUME);

        // Then
        assertAll(
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(GLOBAL_STORED_BY_ANOTHER_NODE),
                () -> assertThat(storedAfterTheHandlerReturned).as("checkpoint stored after the handler the pause stopped waiting for returned").isEqualTo(GLOBAL_STORED_BY_ANOTHER_NODE),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a delivery of the resumed run").isEqualTo(EVENT_AFTER_THE_RESUME)
        );
    }

    /**
     * The wrapped model reports the subscription as neither running nor paused when the resume checks, as a model does
     * that doesn't synchronize its two reads, and a pause on another thread comes before the wrapped model resumes, so it
     * restarts at the stored checkpoint. The handler read before the resume is the run that restart replaces, so its
     * checkpoint is not stored over the one the resume went to.
     */
    @Test
    void a_plain_resume_of_a_subscription_the_wrapped_model_reports_as_neither_running_nor_paused_refuses_the_late_save_of_a_handler_read_before_the_resume() throws Exception {
        // Given
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        RecordingStorage recordingStorage = new RecordingStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, recordingStorage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        AtomicBoolean holdTheHandler = new AtomicBoolean();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
            if (holdTheHandler.get()) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        holdTheHandler.set(true);
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
        assertThat(awaitQuietly(handlerStarted)).as("handler started").isTrue();
        recordingStorage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);
        recordingStorage.saved.clear();
        wrapped.reportsNothingRunning = true;
        assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused when the resume checks").isFalse();
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);

        // Then
        assertAll(
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE),
                () -> assertThat(recordingStorage.saved).as("checkpoints saved after the resume").isEmpty(),
                () -> assertThat(recordingStorage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the handler read before the resume returned").isEqualTo(STORED_BY_ANOTHER_NODE)
        );
    }

    /**
     * A resume of a subscription that is not paused does not claim that the stored checkpoint covers the deliveries read
     * before it, since a wrapped model that runs again by then throws instead of restarting there. So a quiet position is
     * not saved over a checkpoint another node stored, even though the wrapped model answers true to {@code restartsAt}.
     */
    @Test
    void a_plain_resume_of_a_subscription_the_wrapped_model_reports_as_neither_running_nor_paused_saves_no_quiet_position_over_the_checkpoint_stored_by_another_node() throws Exception {
        // Given
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);
        wrapped.reportsNothingRunning = true;
        assertThat(wrapped.isPaused(SUBSCRIPTION_ID)).as("subscription paused when the resume checks").isFalse();
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        Consumer<Checkpoint> saver = saverWhenWanted(wrapped);
        if (saver != null) {
            saver.accept(QUIET_BEFORE_E_IS_DELIVERED_AGAIN);
        }

        // Then
        assertAll(
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was asked to resume at").containsExactly(STORED_BY_ANOTHER_NODE),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after a quiet position read after the resume").isEqualTo(STORED_BY_ANOTHER_NODE)
        );
    }

    /**
     * The save of a delivery is in progress, holding the lock of its registration, when a plain resume of a subscription
     * the wrapped model reports as neither running nor paused and a pause on a third thread are made. The pause takes no
     * lock of the subscription, and the resume waits for the save, so all three finish once the save is released.
     */
    @Test
    void a_plain_resume_and_a_pause_made_while_a_save_is_in_progress_finish_once_the_save_is_released() {
        // Given
        RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
        wrapped.reportsNothingRunning = true;
        BlockingSaveStorage blockingStorage = new BlockingSaveStorage();
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, blockingStorage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        blockingStorage.blockSaves();
        CompletableFuture<Void> delivery = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
        assertThat(blockingStorage.awaitSaveStarted()).as("save started").isTrue();

        // When
        CompletableFuture<String> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(SUBSCRIPTION_ID).id(), handlerThreads);
        CompletableFuture<Void> pause = CompletableFuture.runAsync(() -> model.pauseSubscription(SUBSCRIPTION_ID), handlerThreads);
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> pause.get(), "pause while a save is in progress");
        blockingStorage.releaseSaves();

        // Then
        assertAll(
                () -> assertThat(assertTimeoutPreemptively(Duration.ofSeconds(10), () -> resume.get())).as("subscription the resume returned").isEqualTo(SUBSCRIPTION_ID),
                () -> assertTimeoutPreemptively(Duration.ofSeconds(10), () -> delivery.get()),
                () -> assertThat(blockingStorage.read(SUBSCRIPTION_ID)).as("checkpoint stored by the save that was in progress").isEqualTo(EVENT_OF_THE_RUNNING_HANDLER)
        );
    }

    private static final class RecordingStorage extends InMemoryCheckpointStorage {
        final List<Checkpoint> saved = new CopyOnWriteArrayList<>();

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saved.add(checkpoint);
            return super.save(subscriptionId, checkpoint, condition);
        }
    }

    private static final class BlockingSaveStorage extends InMemoryCheckpointStorage {
        private volatile boolean blocking;
        private final CountDownLatch saveStarted = new CountDownLatch(1);
        private final CountDownLatch saveMayReturn = new CountDownLatch(1);

        void blockSaves() {
            blocking = true;
        }

        void releaseSaves() {
            saveMayReturn.countDown();
        }

        boolean awaitSaveStarted() {
            return awaitQuietly(saveStarted);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (blocking) {
                saveStarted.countDown();
                awaitQuietly(saveMayReturn);
            }
            return super.save(subscriptionId, checkpoint, condition);
        }
    }

    // Reads until a listener wants the quiet position, which the 1 ms interval allows after a moment. Null when none does
    private static @Nullable Consumer<Checkpoint> saverWhenWanted(RepositionableQuietPositionReportingModel wrapped) {
        try {
            return await().atMost(2, SECONDS).until(() -> wrapped.beforeReading(SUBSCRIPTION_ID), Objects::nonNull);
        } catch (ConditionTimeoutException e) {
            return null;
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
}
