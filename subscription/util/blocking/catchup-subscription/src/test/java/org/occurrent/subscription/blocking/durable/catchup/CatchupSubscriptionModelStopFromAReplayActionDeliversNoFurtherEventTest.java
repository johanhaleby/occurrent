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
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A {@code stop()} of a {@link CatchupSubscriptionModel} called from inside the action of a replay returns at once and
 * runs on the lifecycle thread once the action has returned. No further event of that replay reaches the action from
 * the moment the action made the call, also while the lifecycle thread waits for the model lock, and a
 * {@code start(true)} made once the stop has run delivers the rest.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupSubscriptionModelStopFromAReplayActionDeliversNoFurtherEventTest {

    private static final Duration NO_FURTHER_EVENT_WITHIN = Duration.ofMillis(500);
    private static final OffsetDateTime HISTORY_BEGINS = OffsetDateTime.parse("2026-01-01T00:00:00Z");

    private InMemorySubscriptionModel inMemory;
    private StartGatedSubscriptionModel wrapped;
    private InMemoryEventStore eventStore;
    private CatchupSubscriptionModel model;
    private final CountDownLatch releaseTheStart = new CountDownLatch(1);
    private @Nullable CompletableFuture<Void> start;

    @BeforeEach
    void create_a_model_that_stores_the_position_after_each_replayed_event_over_a_wrapped_model_whose_start_can_be_held() {
        inMemory = new InMemorySubscriptionModel();
        wrapped = new StartGatedSubscriptionModel(inMemory, releaseTheStart);
        eventStore = new InMemoryEventStore(inMemory);
        model = new CatchupSubscriptionModel(wrapped, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(new InMemoryCheckpointStorage()).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
    }

    @AfterEach
    void release_everything_and_shut_down() {
        try {
            releaseTheStart.countDown();
            if (start != null) {
                assertThat(start).as("the start from another thread").succeedsWithin(Duration.ofSeconds(15));
            }
        } finally {
            model.shutdown();
            inMemory.shutdown();
        }
    }

    @Test
    @Timeout(30)
    void no_further_event_of_a_replay_reaches_the_action_once_it_called_a_stop_that_waits_for_the_model_lock_and_a_start_that_runs_the_replay_again_delivers_the_rest_once() throws InterruptedException {
        // Given
        write("1");
        write("2");
        write("3");
        List<String> received = new CopyOnWriteArrayList<>();
        AtomicBoolean first = new AtomicBoolean(true);
        CountDownLatch inTheFirstAction = new CountDownLatch(1);
        CountDownLatch makeTheCall = new CountDownLatch(1);
        CountDownLatch called = new CountDownLatch(1);
        CountDownLatch aFurtherDelivery = new CountDownLatch(1);
        model.subscribe("subscription", StartAtTime.beginningOfTime(), event -> {
            received.add(event.getId());
            if (!first.compareAndSet(true, false)) {
                aFurtherDelivery.countDown();
                return;
            }
            inTheFirstAction.countDown();
            awaitLatch(makeTheCall);
            model.stop();
            called.countDown();
        });
        assertThat(inTheFirstAction.await(5, TimeUnit.SECONDS)).as("the replay is in the action of its first event").isTrue();
        wrapped.holdStarts = true;
        start = CompletableFuture.runAsync(() -> model.start(false), CatchupSubscriptionModelStopFromAReplayActionDeliversNoFurtherEventTest::onADaemonThread);
        assertThat(wrapped.startEntered.await(5, TimeUnit.SECONDS)).as("a start from another thread holds the model lock inside the wrapped model's start").isTrue();

        // When
        makeTheCall.countDown();

        // Then
        assertThat(called.await(5, TimeUnit.SECONDS)).as("the stop() from the action returned while the model lock was held").isTrue();
        assertThat(aFurtherDelivery.await(NO_FURTHER_EVENT_WITHIN.toMillis(), TimeUnit.MILLISECONDS))
                .as("whether a further event of the replay reached the action within %d ms of the stop() it called, while the stop waited for the model lock", NO_FURTHER_EVENT_WITHIN.toMillis())
                .isFalse();
        releaseTheStart.countDown();
        assertThat(start).as("the start from another thread").succeedsWithin(Duration.ofSeconds(10));
        await().atMost(Duration.ofSeconds(10)).until(() -> !model.isRunning());
        assertThat(received).as("the events the action saw once the stop() it called had run").containsExactly("1");

        wrapped.holdStarts = false;
        model.start(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(received).as("the events the action saw once a start(true) ran the replay again").containsExactly("1", "2", "3"));
    }

    private void write(String id) {
        eventStore.write(UUID.randomUUID().toString(), List.of(cloudEvent(id)));
    }

    private static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(HISTORY_BEGINS.plusSeconds(Long.parseLong(id))).build();
    }

    private static void onADaemonThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "start-from-another-thread");
        thread.setDaemon(true);
        thread.start();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    // Wraps the in-memory model so a test can hold its start until it releases it
    private static final class StartGatedSubscriptionModel implements CheckpointAwareSubscriptionModel {
        private final InMemorySubscriptionModel delegate;
        private final CountDownLatch released;
        final CountDownLatch startEntered = new CountDownLatch(1);
        volatile boolean holdStarts;

        private StartGatedSubscriptionModel(InMemorySubscriptionModel delegate, CountDownLatch released) {
            this.delegate = delegate;
            this.released = released;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, liveStart(startAt), action);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribePaused(subscriptionId, filter, liveStart(startAt), action);
        }

        // The in-memory model starts at now or at its default only
        private static StartAt liveStart(StartAt startAt) {
            StartAt resolved = startAt.get(new StartAt.SubscriptionModelContext(InMemorySubscriptionModel.class));
            return resolved != null && resolved.isDefault() ? StartAt.subscriptionModelDefault() : StartAt.now();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint("in-memory-global-position");
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (holdStarts) {
                startEntered.countDown();
                awaitLatch(released);
            }
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
