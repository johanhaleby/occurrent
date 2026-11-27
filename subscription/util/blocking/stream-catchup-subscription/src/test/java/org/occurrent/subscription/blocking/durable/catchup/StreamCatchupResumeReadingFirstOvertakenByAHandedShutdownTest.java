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
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code resumeSubscription(..)} that reads the checkpoint of its subscription before it takes the model lock can have
 * a shutdown handed over, and run, while that read waits. The handed shutdown counts as one that came after the resume was
 * made, however many have returned by the time the read answers, so the resume changes nothing and doesn't resume the
 * subscription in a live delegate that is shut down.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupResumeReadingFirstOvertakenByAHandedShutdownTest {

    private static final String X = "x";
    private static final String Z = "z";
    private static final long BOUND_SECONDS = 40;

    private InMemorySubscriptionModel inMemory;
    private InMemoryEventStore eventStore;
    private HookedSubscriptionModel wrapped;
    private StorageThatHoldsTheReadOfX storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_with_a_paused_x() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory).withoutStreamPosition();
        storage = new StorageThatHoldsTheReadOfX();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
        model.subscribe(X, null, StartAt.now(), event -> {
        });
        model.pauseSubscription(X);
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
    void a_resume_whose_read_of_the_checkpoint_waits_while_a_handed_shutdown_runs_and_returns_changes_nothing_once_the_read_answers() throws Exception {
        // Given the checkpoint read of a resume of x is held back
        storage.holds = true;
        CompletableFuture<Subscription> resume = CompletableFuture.supplyAsync(() -> model.resumeSubscription(X), daemonExecutor("resume-of-x"));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the resume of x read the storage").isTrue();

        // And a listener under the model lock waits for a shutdown, which gives up and hands its shutdown over
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.listenForCatchup(Z, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                CompletableFuture<Void> shutdown = CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-under-test"));
                try {
                    shutdown.get(BOUND_SECONDS, TimeUnit.SECONDS);
                } catch (Exception expectedToGiveUpWaiting) {
                    // The handed shutdown runs once this listener returns
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        model.subscribePaused(Z, null, StartAtTime.beginningOfTime(), event -> {
        });
        CompletableFuture<Subscription> resumeOfZ = CompletableFuture.supplyAsync(() -> model.resumeSubscription(Z), daemonExecutor("resume-of-z"));

        // When the handed shutdown has shut the live delegate down and returned, and then the read of x answers
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(wrapped.shutdowns.get()).as("the shutdowns the live delegate got while the read of x was held back").isEqualTo(1));
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(Thread.getAllStackTraces().keySet())
                .as("the handed shutdown threads still alive")
                .noneMatch(thread -> thread.getName().equals("occurrent-catchup-handed-shutdown") && thread.isAlive()));
        storage.release.countDown();

        // Then the resume of x returns without resuming x in the live delegate that is shut down
        assertThat(resume).as("the resume of x once its read answered").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
        assertThat(model.isRunning(X)).as("whether x runs after the resume that read before the handed shutdown returned").isFalse();
        resumeOfZ.cancel(true);
    }

    @Test
    void a_start_whose_read_of_the_checkpoint_waits_while_a_handed_shutdown_runs_and_returns_changes_nothing_once_the_read_answers() throws Exception {
        // Given a stopped model, and a listener of z that holds the model lock until it is told to return
        model.stop();
        CountDownLatch listenerHoldsTheLock = new CountDownLatch(1);
        CountDownLatch listenerMayReturn = new CountDownLatch(1);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        model.listenForCatchup(Z, new CatchupListener() {
            @Override
            public void catchupStarted(Object episode) {
                listenerHoldsTheLock.countDown();
                try {
                    listenerMayReturn.await(80, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void historyRead(Object episode) {
            }
        });
        model.subscribePaused(Z, null, StartAtTime.beginningOfTime(), event -> {
        });
        CompletableFuture<Subscription> resumeOfZ = CompletableFuture.supplyAsync(() -> model.resumeSubscription(Z), daemonExecutor("resume-of-z"));
        assertThat(listenerHoldsTheLock.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the listener of z holds the model lock").isTrue();
        int startsBefore = wrapped.starts.get();

        // And a start whose checkpoint read of x is held back
        storage.holds = true;
        CompletableFuture<Void> start = CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-under-test"));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();

        // And a shutdown that gives up behind the listener and hands its shutdown over
        CompletableFuture<Void> shutdown = CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-under-test"));
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(shutdown).as("the shutdown that waited behind the listener").isCompletedExceptionally());

        // When the listener returns, the handed shutdown shuts the live delegate down and returns, and then the read of x answers
        listenerMayReturn.countDown();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(wrapped.shutdowns.get()).as("the shutdowns the live delegate got while the read of x was held back").isEqualTo(1));
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(Thread.getAllStackTraces().keySet())
                .as("the handed shutdown threads still alive")
                .noneMatch(thread -> thread.getName().equals("occurrent-catchup-handed-shutdown") && thread.isAlive()));
        storage.release.countDown();

        // Then the start has ended and started nothing in the live delegate that is shut down
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(start).as("the start once its read answered").isDone());
        assertThat(wrapped.starts.get()).as("the starts the live delegate got after the handed shutdown returned").isEqualTo(startsBefore);
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
        resumeOfZ.cancel(true);
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

    private static final class StorageThatHoldsTheReadOfX extends InMemoryCheckpointStorage {
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
