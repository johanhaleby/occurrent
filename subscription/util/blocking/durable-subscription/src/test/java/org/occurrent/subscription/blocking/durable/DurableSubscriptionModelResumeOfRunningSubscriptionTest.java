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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A {@link DurableSubscriptionModel#resumeSubscription(String)} or a resume at a position of a subscription the wrapped
 * model reports as running throws {@link SubscriptionAlreadyRunningException} before it reads or changes anything, so
 * the running subscription goes on saving as if it had not been called.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelResumeOfRunningSubscriptionTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint STORED = new StringBasedCheckpoint("stored");
    private static final Checkpoint EVENT_OF_THE_RUNNING_HANDLER = new StringBasedCheckpoint("event-of-the-running-handler");
    private static final Checkpoint RESUME_AT = new StringBasedCheckpoint("resume-at");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final CountingStorage storage = new CountingStorage();
    private final RepositionableQuietPositionReportingModel wrapped = new RepositionableQuietPositionReportingModel();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService handlerThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheHandlerAndStopItsThreads() {
        handlerMayReturn.countDown();
        handlerThreads.shutdownNow();
    }

    @Test
    void a_plain_resume_of_a_running_subscription_throws_and_reads_nothing_from_the_storage_and_resumes_nothing() {
        // Given
        DurableSubscriptionModel model = runningSubscriptionStoring();
        storage.reads.set(0);
        storage.writes.set(0);

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(storage.reads).as("reads of the storage").hasValue(0),
                () -> assertThat(storage.writes).as("writes to the storage").hasValue(0),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(0),
                () -> assertThat(wrapped.resumedAt).as("resumes of the wrapped model at a position").isEmpty()
        );
    }

    @Test
    void the_checkpoint_of_a_delivery_that_ends_after_a_refused_plain_resume_is_still_stored() throws Exception {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        AtomicBoolean holdTheHandler = new AtomicBoolean(true);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
            if (holdTheHandler.get()) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
        assertThat(awaitQuietly(handlerStarted)).as("handler started").isTrue();
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));

        // When
        handlerMayReturn.countDown();
        handler.get(10, SECONDS);

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the handler returned").isEqualTo(EVENT_OF_THE_RUNNING_HANDLER)
        );
    }

    @Test
    void a_quiet_position_is_still_saved_after_a_refused_plain_resume() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, STORED);
        Throwable thrown = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));

        // When
        Consumer<Checkpoint> saver = saverWhenWanted();
        if (saver != null) {
            saver.accept(QUIET);
        }

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(saver).as("quiet position wanted").isNotNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the quiet position").isEqualTo(QUIET)
        );
    }

    @Test
    void a_resume_at_a_position_of_a_running_subscription_throws_and_stores_nothing_and_resumes_nothing() {
        // Given
        DurableSubscriptionModel model = runningSubscriptionStoring();
        RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(model).orElseThrow();
        storage.reads.set(0);
        storage.writes.set(0);

        // When
        Throwable thrown = catchThrowable(() -> repositionable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(RESUME_AT)));

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(storage.writes).as("writes to the storage").hasValue(0),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("stored checkpoint").isEqualTo(STORED),
                () -> assertThat(wrapped.resumedAt).as("resumes of the wrapped model at a position").isEmpty(),
                () -> assertThat(wrapped.plainResumes).as("plain resumes of the wrapped model").hasValue(0)
        );
    }

    @Test
    void a_quiet_position_is_still_saved_after_a_refused_resume_at_a_position() {
        // Given
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, STORED);
        RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(model).orElseThrow();
        Throwable thrown = catchThrowable(() -> repositionable.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(RESUME_AT)));

        // When
        Consumer<Checkpoint> saver = saverWhenWanted();
        if (saver != null) {
            saver.accept(QUIET);
        }

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(SubscriptionAlreadyRunningException.class),
                () -> assertThat(saver).as("quiet position wanted").isNotNull(),
                () -> assertThat(storage.read(SUBSCRIPTION_ID)).as("checkpoint stored after the quiet position").isEqualTo(QUIET)
        );
    }

    private DurableSubscriptionModel runningSubscriptionStoring() {
        DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(START), cloudEvent -> {
        });
        wrapped.deliver(SUBSCRIPTION_ID, STORED);
        assertThat(wrapped.isRunning(SUBSCRIPTION_ID)).as("running").isTrue();
        return model;
    }

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

    private static final class CountingStorage extends InMemoryCheckpointStorage {
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();

        @Override
        public Checkpoint read(String subscriptionId) {
            reads.incrementAndGet();
            return super.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            writes.incrementAndGet();
            return super.save(subscriptionId, checkpoint, condition);
        }
    }
}
