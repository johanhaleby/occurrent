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
 * A {@code start(true)} an action handed over, whose read of a checkpoint doesn't answer, holds the lifecycle thread up
 * for the ten seconds it waits for that read and then its turn, not for a further ten seconds the lifecycle thread waits
 * for the resume the read left until after the turn. A {@code stop()} made from another thread just after the start
 * began waits behind it, so it returns well within the twenty seconds the two waits would have added up to.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupHandedStartWithAHungReadHoldsTheLifecycleThreadTest {

    private static final String A = "a";
    private static final String X = "x";
    // The wait for the read is ten seconds and the stop comes after the turn, so about ten. The two waits added up to twenty
    private static final long BOUND_SECONDS = 15;

    private final AtomicBoolean aStartedTheModel = new AtomicBoolean();
    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private StorageThatHoldsReadsOfX storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_with_a_paused_x_and_an_a_whose_action_hands_a_start_over() {
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
    void a_stop_made_from_another_thread_just_after_a_handed_start_whose_read_hangs_began_returns_after_the_wait_for_the_read_and_not_after_a_second_wait_for_the_resume() throws Exception {
        // Given the checkpoint read of x is held back, and the action of a hands a start over that reads it
        storage.holds = true;
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();

        // When a stop is made from another thread
        CompletableFuture<Void> stop = CompletableFuture.runAsync(model::stop, daemonExecutor("stop-under-test"));

        // Then it returns well within the two waits, while the read is still held back
        assertThat(stop).as("the stop made just after the handed start whose read hangs began").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
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
