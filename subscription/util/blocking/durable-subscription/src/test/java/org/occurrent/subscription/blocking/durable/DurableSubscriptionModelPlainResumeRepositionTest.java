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
import org.awaitility.core.ConditionTimeoutException;
import org.occurrent.subscription.Checkpoint;
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
 * A plain {@link DurableSubscriptionModel#resumeSubscription(String)} of a paused subscription that goes to the
 * checkpoint another node stored, since the subscription stored a checkpoint of its own after its start. That resume
 * counts as a reposition. What was read or handed out before it isn't stored over the checkpoint it resumes at, and what
 * is handed out after it is stored. The wrapped model is a fake the test reads from by hand.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelPlainResumeRepositionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint EVENT_BEFORE_THE_PAUSE = new StringBasedCheckpoint("event-before-the-pause");
    private static final Checkpoint STORED_BY_ANOTHER_NODE = new StringBasedCheckpoint("stored-by-another-node");
    private static final Checkpoint QUIET_BEFORE_THE_RESUME = new StringBasedCheckpoint("quiet-before-the-resume");
    private static final Checkpoint QUIET_AFTER_THE_RESUME = new StringBasedCheckpoint("quiet-after-the-resume");
    private static final Checkpoint EVENT_OF_THE_RUNNING_HANDLER = new StringBasedCheckpoint("event-of-the-running-handler");

    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService handlerThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheHandlerAndStopItsThreads() {
        handlerMayReturn.countDown();
        handlerThreads.shutdownNow();
    }

    @Test
    void a_quiet_position_handed_out_before_the_pause_is_not_stored_over_the_checkpoint_a_plain_resume_goes_to() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, withQuietPositions());
        subscribeAndStoreTheCheckpointOfAnEvent(model);
        Consumer<Checkpoint> saverHandedOutBeforeThePause = saverWhenWanted();
        assertThat(saverHandedOutBeforeThePause).as("quiet position wanted before the pause").isNotNull();
        model.pauseSubscription(SUBSCRIPTION_ID);
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        saverHandedOutBeforeThePause.accept(QUIET_BEFORE_THE_RESUME);

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(STORED_BY_ANOTHER_NODE),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").containsExactly(STORED_BY_ANOTHER_NODE)
        );
    }

    @Test
    void a_quiet_position_handed_out_before_the_pause_is_not_stored_over_the_checkpoint_of_the_last_event_when_a_plain_resume_goes_to_it() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, withQuietPositions());
        subscribeAndStoreTheCheckpointOfAnEvent(model);
        Consumer<Checkpoint> saverHandedOutBeforeThePause = saverWhenWanted();
        assertThat(saverHandedOutBeforeThePause).as("quiet position wanted before the pause").isNotNull();
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        saverHandedOutBeforeThePause.accept(QUIET_BEFORE_THE_RESUME);

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(EVENT_BEFORE_THE_PAUSE),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").containsExactly(EVENT_BEFORE_THE_PAUSE)
        );
    }

    @Test
    void a_quiet_position_handed_out_after_a_plain_resume_is_stored_once_the_handler_the_pause_stopped_waiting_for_has_returned() throws Exception {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, withQuietPositions());
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
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);

        // When
        Consumer<Checkpoint> saverHandedOutAfterTheResume = saverWhenWanted();
        if (saverHandedOutAfterTheResume != null) {
            saverHandedOutAfterTheResume.accept(QUIET_AFTER_THE_RESUME);
        }

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(QUIET_AFTER_THE_RESUME),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").containsExactly(STORED_BY_ANOTHER_NODE)
        );
    }

    @Test
    void the_checkpoint_of_an_event_whose_handler_the_pause_stopped_waiting_for_is_not_stored_over_the_checkpoint_a_plain_resume_goes_to() throws Exception {
        // Given
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
        storage.save(SUBSCRIPTION_ID, STORED_BY_ANOTHER_NODE);
        model.resumeSubscription(SUBSCRIPTION_ID);

        // When
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(STORED_BY_ANOTHER_NODE),
                () -> assertThat(wrapped.resumedAt).as("checkpoints the wrapped model was repositioned to").containsExactly(STORED_BY_ANOTHER_NODE)
        );
    }

    private static DurableSubscriptionModelConfig withQuietPositions() {
        return new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1));
    }

    // A subscribe at START that then stores the checkpoint of an event, so the subscription has stored one of its own
    private void subscribeAndStoreTheCheckpointOfAnEvent(DurableSubscriptionModel model) {
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_BEFORE_THE_PAUSE);
        assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored by the event").isEqualTo(EVENT_BEFORE_THE_PAUSE);
    }

    // Reads until a listener wants the quiet position, which the 1 ms interval allows after a moment. Null when none does
    private @Nullable Consumer<Checkpoint> saverWhenWanted() {
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
