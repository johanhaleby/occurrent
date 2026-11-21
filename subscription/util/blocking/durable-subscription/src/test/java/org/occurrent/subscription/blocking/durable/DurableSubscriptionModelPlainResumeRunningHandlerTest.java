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
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
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
