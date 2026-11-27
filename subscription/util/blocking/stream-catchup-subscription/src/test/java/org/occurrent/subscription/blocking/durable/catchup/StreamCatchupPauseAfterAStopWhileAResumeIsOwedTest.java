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
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code pauseSubscription(x)} after a {@code stop()} that came after a {@code start(true)} takes effect as it does in
 * the serial order of those calls, where the start resumed x and the stop stopped the model, so the pause of x throws.
 * It does also while the start's resume of x is owed because its checkpoint read has not answered.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class StreamCatchupPauseAfterAStopWhileAResumeIsOwedTest {

    private static final String A = "a";
    private static final String X = "x";
    private static final long BOUND_SECONDS = 25;

    private final AtomicBoolean aStartedTheModel = new AtomicBoolean();
    private InMemorySubscriptionModel inMemory;
    private HookedSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private StorageThatHoldsTheFirstReadOfX storage;
    private StreamCatchupSubscriptionModel model;

    @BeforeEach
    void create_a_model_with_an_a_whose_action_hands_a_start_over() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new HookedSubscriptionModel(inMemory);
        eventStore = new InMemoryEventStore(inMemory);
        storage = new StorageThatHoldsTheFirstReadOfX();
        model = new StreamCatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
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
    void a_pause_of_a_subscription_a_start_resumed_in_its_turn_throws_when_the_model_was_stopped_after_the_start() {
        // Given x is paused, and a start(true) resumed it in its turn
        givenAPausedX();
        model.start(true);
        assertThat(model.isPaused(X)).as("whether x is paused once the start resumed it").isFalse();

        // When the model is stopped
        model.stop();

        // Then the pause of x throws, as the pause of a subscription of a stopped model does
        assertThatThrownBy(() -> model.pauseSubscription(X)).as("the pause of x after the stop that came after the start").isInstanceOf(SubscriptionNotRunningException.class);
    }

    @Test
    void a_pause_of_a_subscription_whose_resume_a_start_still_owes_throws_when_the_model_was_stopped_after_the_start() throws Exception {
        // Given x is paused, and the read of a start(true) an action handed over is held past its turn, so the resume of x is owed
        givenAPausedX();
        storage.holds.set(true);
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent()));
        assertThat(storage.entered.await(BOUND_SECONDS, TimeUnit.SECONDS)).as("whether the start read the storage for x").isTrue();
        await().atMost(BOUND_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> assertThat(theResumeOwedFor(X)).as("whether the start owes x the resume it left until after its turn").isTrue());

        // When the model is stopped while the read is still held
        CompletableFuture.runAsync(model::stop, daemonExecutor("stop-of-the-model")).get(BOUND_SECONDS, TimeUnit.SECONDS);

        // Then the pause of x throws, as it does when the start has resumed x before the stop
        assertThatThrownBy(() -> model.pauseSubscription(X)).as("the pause of x after the stop that came after the start, while the resume of x is owed").isInstanceOf(SubscriptionNotRunningException.class);
    }

    private void givenAPausedX() {
        model.subscribe(X, null, StartAt.dynamic(() -> null), event -> {
        });
        model.pauseSubscription(X);
    }

    private boolean theResumeOwedFor(String subscriptionId) throws Exception {
        Field owedResumes = AbstractCatchupSubscriptionModel.class.getDeclaredField("owedResumes");
        owedResumes.setAccessible(true);
        return ((Map<?, ?>) owedResumes.get(model)).containsKey(subscriptionId);
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
        private final AtomicBoolean holds = new AtomicBoolean();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (X.equals(subscriptionId) && holds.compareAndSet(true, false)) {
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
