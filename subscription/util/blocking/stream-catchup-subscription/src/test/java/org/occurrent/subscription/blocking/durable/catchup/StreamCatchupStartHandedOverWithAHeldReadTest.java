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

package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code start(true)} an action handed over resumes a paused subscription whose checkpoint read doesn't answer within
 * the ten seconds the start waits for its reads before its turn, nor within the ten seconds the lifecycle thread waits
 * for the resume to begin after that. The resume has entered its turn by then, so it isn't withdrawn, and runs once the
 * read answers.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupStartHandedOverWithAHeldReadTest {

    private static final String A = "a";
    private static final String X = "x";
    // More than the two waits of ten seconds a start and the lifecycle thread make for a read and for a resume to begin
    private static final long HOLD_THE_READ_MILLIS = 23_000;

    private final AtomicBoolean aStartedTheModel = new AtomicBoolean();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private StorageThatHoldsReadsOfX storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_with_a_paused_x_and_an_a_whose_action_starts_the_model() {
        inMemory = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        storage = new StorageThatHoldsReadsOfX();
        model = new StreamCatchupSubscriptionModel(new HookedSubscriptionModel(inMemory), eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
        model.subscribe(X, null, StartAt.dynamic(() -> null), event -> {
        });
        model.pauseSubscription(X);
        model.subscribe(A, null, StartAt.now(), event -> {
            if (aStartedTheModel.compareAndSet(false, true)) {
                model.start(true);
            }
        });
    }

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        storage.release.countDown();
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(120)
    void a_start_an_action_handed_over_resumes_a_paused_subscription_whose_read_answers_after_the_waits_for_the_read_and_for_the_resume_to_begin() throws Exception {
        // Given the checkpoint read of x is held back
        storage.holds = true;

        // When the action of a starts the model, so the start it handed over runs once the action has returned, and reads x
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(storage.entered.await(25, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();
        // And the read answers after both waits have expired
        Thread.sleep(HOLD_THE_READ_MILLIS);
        storage.release.countDown();

        // Then x runs, as the resume the start made takes effect once its read has answered
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (!model.isRunning(X) && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(model.isRunning(X)).as("whether x runs once its read answered").isTrue();
        assertThat(model.isPaused(X)).as("whether x is paused once its read answered").isFalse();
    }

    private static CloudEvent cloudEvent() {
        return CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    private static Executor daemonExecutor(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            thread.start();
        };
    }

    private static final class StorageThatHoldsReadsOfX extends InMemoryCheckpointStorage {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean holds;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (holds && X.equals(subscriptionId)) {
                entered.countDown();
                try {
                    release.await(80, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.read(subscriptionId);
        }
    }
}
