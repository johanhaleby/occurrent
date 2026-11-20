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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A resume at a position on a {@link DurableSubscriptionModel} whose wrapped model returned from the pause while a
 * handler was still running, so that handler returns after the resume has stored its position. The wrapped model is a
 * fake that lets the pause return at once, and the handler is held on a latch until the test releases it.
 */
@DisplayName("DurableSubscriptionModelResumeAtPositionRunningHandler")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelResumeAtPositionRunningHandlerTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint EVENT_OF_THE_RUNNING_HANDLER = new StringBasedCheckpoint("event-of-the-running-handler");
    private static final Checkpoint LATER_THAN_THE_EVENT = new StringBasedCheckpoint("later-than-the-event");
    private static final Checkpoint EARLIER_THAN_THE_EVENT = new StringBasedCheckpoint("earlier-than-the-event");
    private static final Checkpoint EVENT_AFTER_THE_RESUME = new StringBasedCheckpoint("event-after-the-resume");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final PausableModel wrapped = new PausableModel();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService handlerThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheHandlerAndStopItsThreads() {
        handlerMayReturn.countDown();
        handlerThreads.shutdownNow();
    }

    @Nested
    @DisplayName("when a handler is still running after the pause returned")
    class When_a_handler_is_still_running_after_the_pause_returned {

        @Test
        void a_resume_at_a_later_position_leaves_that_position_stored_once_the_handler_has_returned() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(LATER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(LATER_THAN_THE_EVENT);
        }

        @Test
        void a_resume_at_an_earlier_position_does_not_skip_the_replay_the_caller_asked_for_after_a_restart() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }

        @Test
        void an_event_delivered_after_the_resume_still_stores_its_checkpoint() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            wrapped.deliver(SUBSCRIPTION_ID, EVENT_AFTER_THE_RESUME);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EVENT_AFTER_THE_RESUME);
        }

        @Test
        void a_resume_at_a_position_the_wrapped_model_cannot_restart_at_stores_nothing_so_the_running_handler_still_stores_its_checkpoint() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            wrapped.cannotRestartAt.add(EARLIER_THAN_THE_EVENT);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EVENT_OF_THE_RUNNING_HANDLER);
        }
    }

    @Nested
    @DisplayName("when a quiet position was read before the resume")
    class When_a_quiet_position_was_read_before_the_resume {

        @Test
        void the_quiet_position_saved_after_the_resume_is_not_stored_over_the_position_of_the_resume() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            Consumer<Checkpoint> saverReadBeforeTheResume = aSaverOnceTheIntervalHasPassed();
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            saverReadBeforeTheResume.accept(QUIET);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }
    }

    private Consumer<Checkpoint> aSaverOnceTheIntervalHasPassed() {
        AtomicReference<Consumer<Checkpoint>> saver = new AtomicReference<>();
        await().atMost(5, SECONDS).until(() -> {
            saver.set(wrapped.beforeReading(SUBSCRIPTION_ID));
            return saver.get() != null;
        });
        return saver.get();
    }

    private void subscribeFromTheStoredStart(DurableSubscriptionModel model) {
        storage.save(SUBSCRIPTION_ID, START);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            if (positionOf(cloudEvent).equals(EVENT_OF_THE_RUNNING_HANDLER)) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
    }

    private CompletableFuture<Void> deliverAHandlerThatStaysRunning(Checkpoint position) {
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, position), handlerThreads);
        assertThat(awaitLatch(handlerStarted)).as("handler started").isTrue();
        return handler;
    }

    private void handlerReturns(CompletableFuture<Void> handler) {
        handlerMayReturn.countDown();
        try {
            handler.get(5, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError("The handler did not return", e);
        }
    }

    private static Checkpoint positionOf(CloudEvent cloudEvent) {
        return ((CheckpointAwareCloudEvent) cloudEvent).getCheckpoint();
    }

    private static boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(5, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        awaitLatch(latch);
    }

    /**
     * A wrapped model that returns from a pause at once whatever its handlers are doing, restarts at any position
     * unless told otherwise, and lets the test read and deliver by hand.
     */
    private static final class PausableModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions, QuietPositionReportingSubscriptions {
        final Set<Checkpoint> cannotRestartAt = ConcurrentHashMap.newKeySet();
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private final List<QuietPositionListener> listeners = new CopyOnWriteArrayList<>();

        void deliver(String subscriptionId, Checkpoint position) {
            CloudEvent cloudEvent = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("type").build();
            actions.get(subscriptionId).accept(new CheckpointAwareCloudEvent(cloudEvent, position));
        }

        @Nullable Consumer<Checkpoint> beforeReading(String subscriptionId) {
            return listeners.isEmpty() ? null : listeners.getFirst().beforeReading(subscriptionId);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            if (!paused.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException("a plain resume is not expected");
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return !cannotRestartAt.contains(checkpoint);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return actions.containsKey(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }

        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public void shutdown() {
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }
}
