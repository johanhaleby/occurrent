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
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.internal.CatchupCheckpoints;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel.NOTHING_CHANGED;
import static org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel.START_TOOK_EFFECT;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A resume throws {@link IllegalStateException} saying it changed nothing when the claim of its subscription isn't free
 * within the wait. The {@code start(true)} that made the resume has taken effect all the same, in its turn or after it,
 * so it throws an {@link IllegalStateException} that says so, doesn't claim that nothing was changed, and has the
 * exception of the resume as its cause.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupStartWhoseResumeThrowsTest {

    private static final String X = "x";
    private static final long BOUND_SECONDS = 40;
    private static final Checkpoint LIVE_START = new StringBasedCheckpoint("token-1");
    // Replays from position 1, after the beginning
    private static final Checkpoint STORED = new StringBasedCheckpoint(CatchupCheckpoints.format(1, 1, 3, LIVE_START));

    private final AtomicBoolean xResumesFromTheBeginning = new AtomicBoolean();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private HookedSubscriptionModel wrapped;
    private StorageThatHoldsTheFirstReadOfX storage;
    private StreamCatchupSubscriptionModel model;
    private AbstractCatchupSubscriptionModel.Claim claimOfX;

    @BeforeEach
    void create_a_stopped_model_with_a_paused_x() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory);
        storage = new StorageThatHoldsTheFirstReadOfX();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
        model.subscribe(X, null, StartAt.dynamic(() -> xResumesFromTheBeginning.get() && wrapped.starts.get() == 0 ? StartAtTime.beginningOfTime() : null), event -> {
        });
        model.pauseSubscription(X);
        model.stop();
    }

    @AfterEach
    void release_the_claim_and_the_read_and_shut_down() throws Exception {
        storage.release.countDown();
        if (claimOfX != null) {
            claimOfX.close();
        }
        CompletableFuture.runAsync(() -> {
            model.shutdown();
            inMemory.shutdown();
        }, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
    }

    @Test
    void a_start_whose_resume_in_its_turn_throws_because_the_claim_is_not_free_throws_that_the_start_took_effect_with_the_exception_of_the_resume_as_cause() throws Exception {
        // Given x has a position stored behind the history, so the start resumes it itself, and another call holds the claim of x
        for (int i = 0; i < 3; i++) {
            eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        }
        xResumesFromTheBeginning.set(true);
        storage.save(X, STORED);
        claimOfX = model.claim(X);

        // When a start resumes x in its turn, and waits for the claim until the wait is over
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-under-test"));

        // Then the start throws that it took effect all the same, caused by the resume
        assertTheStartThrewThatItTookEffect(start);
        assertThat(wrapped.isRunning()).as("whether the live delegate runs, since the start took effect").isTrue();
    }

    @Test
    void a_start_whose_resume_after_its_turn_throws_because_the_claim_is_not_free_throws_that_the_start_took_effect_with_the_exception_of_the_resume_as_cause() throws Exception {
        // Given the read of x made before the turn of a start is held back, so the resume of x is left until after the turn
        storage.holds = true;
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-under-test"));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning()).as("whether the live delegate runs, which it does once the turn of the start has begun").isTrue());

        // When another call holds the claim of x, and the read answers
        claimOfX = model.claim(X);
        storage.release.countDown();

        // Then the start throws that it took effect all the same, caused by the resume, once the wait for the claim is over
        assertTheStartThrewThatItTookEffect(start);
    }

    private void assertTheStartThrewThatItTookEffect(CompletableFuture<Void> start) {
        assertThat(start).as("the start whose resume of x waited for a claim that was not free").failsWithin(BOUND_SECONDS, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .havingCause()
                .isInstanceOf(IllegalStateException.class)
                .withMessageEndingWith(START_TOOK_EFFECT)
                .satisfies(thrown -> assertThat(thrown.getMessage()).as("the message of what the start threw").doesNotEndWith(NOTHING_CHANGED))
                .withCauseInstanceOf(IllegalStateException.class);
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

    private static final class StorageThatHoldsTheFirstReadOfX extends InMemoryCheckpointStorage {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean holds;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (holds && X.equals(subscriptionId)) {
                holds = false;
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
