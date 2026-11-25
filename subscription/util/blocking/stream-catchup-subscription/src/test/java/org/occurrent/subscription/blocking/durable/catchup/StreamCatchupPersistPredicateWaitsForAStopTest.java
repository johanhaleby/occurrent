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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * The predicate that decides whether a replayed event's position is stored is code the caller supplied, and the replay
 * asks it before it takes the handover lock. A predicate that waits for a {@code stop()} on another thread therefore
 * lets that stop park the replay, instead of the replay holding the lock the stop waits for.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class StreamCatchupPersistPredicateWaitsForAStopTest {

    private static final String ID = "subscription";
    // Well under the 10 s a call waits for code the caller supplied, since this path needs no give-up
    private static final long A_BOUND_WELL_UNDER_THE_LIFECYCLE_WAIT_SECONDS = 5;

    private final InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel();
    private final CountDownLatch predicateReturned = new CountDownLatch(1);
    private final AtomicBoolean predicateWasAsked = new AtomicBoolean();
    private final AtomicBoolean stopCompletedWhileThePredicateWaited = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<Void>> stop = new AtomicReference<>();
    private StreamCatchupSubscriptionModel model;

    @AfterEach
    void shut_down() {
        if (model != null) {
            CompletableFuture.runAsync(() -> {
                model.shutdown();
                inMemory.shutdown();
            });
        }
    }

    @Test
    void a_persist_predicate_that_waits_for_a_stop_on_another_thread_returns_and_both_the_stop_and_a_shutdown_complete() throws Exception {
        // Given a replay of two events whose persist predicate stops the model on another thread and waits for that stop
        InMemoryEventStore eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        eventStore.write("stream", List.of(event("event1"), event("event2")));
        model = new StreamCatchupSubscriptionModel(new HookedSubscriptionModel(inMemory), eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseWhen(event -> {
                    if (predicateWasAsked.compareAndSet(false, true)) {
                        CompletableFuture<Void> stopped = CompletableFuture.runAsync(() -> model.stop());
                        stop.set(stopped);
                        try {
                            stopped.get(A_BOUND_WELL_UNDER_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
                            stopCompletedWhileThePredicateWaited.set(true);
                        } catch (Exception e) {
                            throw new IllegalStateException("the stop the predicate waits for did not complete", e);
                        } finally {
                            predicateReturned.countDown();
                        }
                    }
                    return true;
                })));

        // When the replay asks the predicate for the first event
        model.subscribe(ID, StartAtTime.beginningOfTime(), event -> {
        });

        // Then the predicate returns, which means the stop completed
        assertThat(predicateReturned.await(15, TimeUnit.SECONDS)).as("whether the persist predicate returned").isTrue();
        assertThat(stopCompletedWhileThePredicateWaited).as("whether the stop completed while the predicate waited for it").isTrue();
        assertThat(stop.get()).as("the stop the predicate waited for").succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(model.isRunning()).as("whether the model runs after that stop").isFalse();

        // And a shutdown completes within a bound well under the wait for code a caller supplied
        assertThat(CompletableFuture.runAsync(() -> model.shutdown())).as("shutdown() after the stop").succeedsWithin(A_BOUND_WELL_UNDER_THE_LIFECYCLE_WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").withTime(OffsetDateTime.now()).build();
    }
}
