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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code start(true)} whose read of the checkpoint stored for a subscription doesn't answer within its wait resumes
 * that subscription once the read answers, after its turn. A {@code stopReplay()} made in between leaves the live
 * delegate running and the model stopped for replays, as the start followed by that {@code stopReplay()} would have, so
 * the live delegate resumes the subscription and the model isn't started again. A {@code stop()} made in between stops
 * the live delegate too, so the subscription stays paused.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupStartResumesAfterTheTurnTest {

    private static final String X = "x";
    private static final int BOUND_SECONDS = 25;

    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private StorageThatHoldsReadsOfX storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_over_a_wrapped_model_whose_storage_can_hold_back_a_read() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        storage = new StorageThatHoldsReadsOfX();
        model = new StreamCatchupSubscriptionModel(wrapped, new InMemoryEventStore(inMemory).withoutStreamPosition(),
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
    }

    @AfterEach
    void release_the_read_and_shut_down() throws Exception {
        storage.release.countDown();
        CompletableFuture.runAsync(model::shutdown, daemonExecutor("shutdown-of-the-model")).get(30, TimeUnit.SECONDS);
        inMemory.shutdown();
    }

    @Test
    @Timeout(90)
    void a_stopReplay_made_after_the_turn_of_a_start_does_not_keep_the_start_from_resuming_a_paused_subscription_whose_read_answered_late_in_the_live_delegate() throws Exception {
        // Given a paused subscription in a stopped model, and a storage that holds back the read of it made before the turn
        givenPausedInAStoppedModel();

        // When the model is started, and its wait for the read expires, which is when the live delegate runs again
        CompletableFuture<Void> start = startOffTheTestThread();
        thenTheTurnHasBegunWhileTheReadWaits();
        // And a stopReplay() is made after the turn
        CompletableFuture<Void> stopReplay = CompletableFuture.runAsync(model::stopReplay, daemonExecutor("stop-replay"));
        assertThat(stopReplay).as("the stopReplay made after the turn").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        // And the read answers
        storage.release.countDown();

        // Then the live delegate resumes the subscription, as the start followed by the stopReplay would have
        assertThat(start).as("the start once its read answered").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(model.isRunning(X)).as("whether x runs once its read answered, after the stopReplay").isTrue();
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").contains(X);
    }

    @Test
    @Timeout(90)
    void a_start_that_resumes_a_subscription_after_a_stopReplay_since_its_turn_does_not_start_the_model_again() throws Exception {
        // Given a paused subscription in a stopped model, and a storage that holds back the read of it made before the turn
        givenPausedInAStoppedModel();
        CompletableFuture<Void> start = startOffTheTestThread();
        thenTheTurnHasBegunWhileTheReadWaits();
        int startsOfTheLiveDelegate = wrapped.starts.get();

        // When a stopReplay() is made after the turn, and the read answers
        assertThat(CompletableFuture.runAsync(model::stopReplay, daemonExecutor("stop-replay"))).as("the stopReplay made after the turn").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        storage.release.countDown();
        assertThat(start).as("the start once its read answered").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(model.isRunning(X)).as("whether x runs once its read answered").isTrue();

        // Then the model is still stopped for replays, and the live delegate wasn't started again
        assertThat(model.stopped).as("whether the model counts as stopped for replays, as it does after a stopReplay()").isTrue();
        assertThat(wrapped.starts.get()).as("the starts of the live delegate after the turn").isEqualTo(startsOfTheLiveDelegate);
    }

    @Test
    @Timeout(90)
    void a_stop_made_after_the_turn_of_a_start_keeps_the_subscription_whose_read_answered_late_paused() throws Exception {
        // Given a paused subscription in a stopped model, and a storage that holds back the read of it made before the turn
        givenPausedInAStoppedModel();
        CompletableFuture<Void> start = startOffTheTestThread();
        thenTheTurnHasBegunWhileTheReadWaits();

        // When a stop() is made after the turn, and the read answers
        assertThat(CompletableFuture.runAsync(model::stop, daemonExecutor("stop"))).as("the stop made after the turn").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        storage.release.countDown();

        // Then the start returns and the subscription stays paused, as the start followed by that stop would have left it
        assertThat(start).as("the start once its read answered").succeedsWithin(BOUND_SECONDS, TimeUnit.SECONDS);
        assertThat(model.isPaused(X)).as("whether x is paused after the stop that came after the start").isTrue();
        assertThat(wrapped.resumed).as("the subscriptions the live delegate resumed").doesNotContain(X);
    }

    private void givenPausedInAStoppedModel() {
        model.subscribe(X, null, StartAt.dynamic(() -> null), event -> {
        });
        model.pauseSubscription(X);
        assertThat(model.isPaused(X)).as("whether x is paused before the start").isTrue();
        model.stop();
        storage.holds = true;
    }

    private CompletableFuture<Void> startOffTheTestThread() {
        return CompletableFuture.runAsync(() -> model.start(true), daemonExecutor("start-of-the-model"));
    }

    private void thenTheTurnHasBegunWhileTheReadWaits() throws InterruptedException {
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(wrapped.isRunning()).as("whether the live delegate runs, which it does once the turn of the start has begun").isTrue());
        assertThat(model.isRunning(X)).as("whether x runs before its read answered").isFalse();
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
