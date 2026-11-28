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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A checkpoint that is not stored because a resume at a position came after its event was read is logged at debug level,
 * since nothing else shows that a delivered event stored nothing. A checkpoint that is stored is not logged that way.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelDroppedSaveLogTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final String DROPPED = "Did not store checkpoint";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint EVENT_OF_THE_RUNNING_HANDLER = new StringBasedCheckpoint("event-of-the-running-handler");
    private static final Checkpoint STORED_BY_THE_RESUME = new StringBasedCheckpoint("stored-by-the-resume");

    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(DurableSubscriptionModel.class);
    private @Nullable Level levelBefore;

    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService handlerThreads = Executors.newCachedThreadPool();

    @BeforeEach
    void captureTheDebugLog() {
        levelBefore = modelLog.getLevel();
        modelLog.setLevel(Level.DEBUG);
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void stopCapturingTheLogAndReleaseTheHandler() {
        modelLog.detachAppender(logged);
        modelLog.setLevel(levelBefore);
        handlerMayReturn.countDown();
        handlerThreads.shutdownNow();
    }

    @Test
    void a_checkpoint_not_stored_since_a_resume_at_a_position_came_after_its_event_was_read_is_logged_at_debug_level() throws Exception {
        // Given
        AtomicBoolean holdTheHandler = new AtomicBoolean(true);
        DurableSubscriptionModel model = subscribed(holdTheHandler);
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
        assertThat(awaitQuietly(handlerStarted)).as("handler started").isTrue();
        model.pauseSubscription(SUBSCRIPTION_ID);
        model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(STORED_BY_THE_RESUME));

        // When
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(STORED_BY_THE_RESUME),
                () -> assertThat(droppedSaves()).as("debug messages about a checkpoint not stored").singleElement().asString()
                        .startsWith(DROPPED + " " + EVENT_OF_THE_RUNNING_HANDLER.asString() + " of subscription " + SUBSCRIPTION_ID)
        );
    }

    @Test
    void a_checkpoint_that_is_stored_is_not_logged_as_not_stored() throws Exception {
        // Given
        AtomicBoolean holdTheHandler = new AtomicBoolean(false);
        subscribed(holdTheHandler);

        // When
        wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER);

        // Then
        assertAll(
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored").isEqualTo(EVENT_OF_THE_RUNNING_HANDLER),
                () -> assertThat(droppedSaves()).as("debug messages about a checkpoint not stored").isEmpty()
        );
    }

    private DurableSubscriptionModel subscribed(AtomicBoolean holdTheHandler) {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
            if (holdTheHandler.get()) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
        return model;
    }

    private List<String> droppedSaves() {
        return logged.list.stream()
                .filter(event -> event.getLevel().equals(Level.DEBUG))
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.startsWith(DROPPED))
                .toList();
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
