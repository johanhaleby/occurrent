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

package org.occurrent.subscription.reactor.durable;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelCancelCompletionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final StringBasedCheckpoint REACHED_BEFORE_THE_CANCEL = new StringBasedCheckpoint("reached-before-the-cancel");
    private static final String WHERE_THE_FEED_IS_NOW = "where-the-feed-is-now";
    private static final StringBasedCheckpoint REACHED_BY_THE_CANCELLED_SUBSCRIPTION = new StringBasedCheckpoint("reached-by-the-cancelled-subscription");

    @Test
    void completes_only_once_the_stored_position_is_deleted() throws Exception {
        // Given
        CountDownLatch deleteEntered = new CountDownLatch(1);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        PositionStorage storage = new PositionStorage();
        storage.beforeDelete = Mono.<Void>fromRunnable(() -> {
            deleteEntered.countDown();
            awaitLatch(releaseDelete);
        }).subscribeOn(Schedulers.boundedElastic());
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW), storage);
        runningFromAStoredPosition(model, storage);

        try {
            // When
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            assertThat(deleteEntered.await(5, TimeUnit.SECONDS)).as("the delete has reached the storage").isTrue();

            // Then
            assertThat(cancelled.isDone()).as("cancel reported as complete while the storage is still deleting the position").isFalse();

            releaseDelete.countDown();
            cancelled.get(5, TimeUnit.SECONDS);
            assertThat(hasPosition(storage)).as("position in storage at the moment the cancel reported completion").isFalse();
        } finally {
            releaseDelete.countDown();
        }
    }

    @Test
    void a_subscribe_after_the_cancel_completed_starts_from_its_own_start_at() {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.beforeDelete = Mono.delay(Duration.ofMillis(300)).then();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);

        // When
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
        subscribe(model);

        // Then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.startedAt).hasSize(2));
        assertThat(feed.startedAt.get(1)).as("start position of the subscription made after the cancel completed").hasToString(WHERE_THE_FEED_IS_NOW);
    }

    /**
     * The caller ignores what the cancel returns and subscribes the same id straight away, while the delete is still
     * on its way to the storage.
     */
    @Test
    void a_subscribe_that_did_not_wait_for_the_cancel_starts_from_its_own_start_at() {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.beforeDelete = Mono.delay(Duration.ofMillis(300)).then();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);

        // When
        model.cancelSubscription(SUBSCRIPTION_ID);
        subscribe(model);

        // Then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.startedAt).hasSize(2));
        assertThat(feed.startedAt.get(1)).as("start position of the subscription made without waiting for the cancel").hasToString(WHERE_THE_FEED_IS_NOW);
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT)).as("position in storage once the new subscription recorded its own").isEqualTo(new StringBasedCheckpoint(WHERE_THE_FEED_IS_NOW));
    }

    @Test
    void completes_only_once_the_cancel_of_the_wrapped_model_has_completed() {
        // Given
        Sinks.Empty<Void> wrappedCancel = Sinks.empty();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        wrapped.cancelled = wrappedCancel.asMono();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        subscribe(model);

        // When
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();

        // Then
        assertThat(cancelled.isDone()).as("cancel reported as complete while the wrapped model is still cancelling").isFalse();
        wrappedCancel.tryEmitEmpty();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(cancelled).isCompleted());
    }

    /**
     * The process ends after the call and before the delete reached the storage, so the position survives the crash.
     * A restarted process knows nothing of the subscription, and cancelling the same id there is how the caller
     * finishes the cancel.
     */
    @Test
    void a_cancel_the_process_did_not_live_to_complete_is_finished_by_cancelling_again_after_a_restart() throws Exception {
        // Given
        InMemoryCheckpointStorage durable = new InMemoryCheckpointStorage();
        CountDownLatch deleteEntered = new CountDownLatch(1);
        PositionStorage dyingProcessStorage = new PositionStorage(durable);
        dyingProcessStorage.beforeDelete = Mono.<Void>fromRunnable(deleteEntered::countDown).then(Mono.never());
        ReactorDurableSubscriptionModel dyingProcess = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW), dyingProcessStorage);
        runningFromAStoredPosition(dyingProcess, durable);

        // When
        CompletableFuture<Void> cancelledBeforeTheCrash = dyingProcess.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        assertThat(deleteEntered.await(5, TimeUnit.SECONDS)).as("the delete was started before the process ended").isTrue();

        // Then
        assertThat(cancelledBeforeTheCrash.isDone()).as("cancel reported as complete by a process that ended before its delete reached the storage").isFalse();
        assertThat(hasPosition(durable)).as("position in the durable storage left by the crash").isTrue();

        // When
        RecordingSubscriptionModel restartedFeed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel restartedProcess = new ReactorDurableSubscriptionModel(restartedFeed, durable);
        restartedProcess.cancelSubscription(SUBSCRIPTION_ID).toFuture().get(5, TimeUnit.SECONDS);
        subscribe(restartedProcess);

        // Then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(restartedFeed.startedAt).hasSize(1));
        assertThat(restartedFeed.startedAt.get(0)).as("start position of the subscription made after the restarted process completed the cancel").hasToString(WHERE_THE_FEED_IS_NOW);
    }

    /**
     * The storage has the save of the position the subscription reached, and applies it only after the cancel was
     * called, the way a save sent just before the cancel can reach a store after a delete sent just after it.
     */
    @Test
    void completes_only_once_a_position_save_the_cancelled_subscription_had_started_has_ended() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        CountDownLatch saveEntered = new CountDownLatch(1);
        CountDownLatch releaseSave = new CountDownLatch(1);
        storage.heldSave = REACHED_BY_THE_CANCELLED_SUBSCRIPTION;
        storage.heldSaveEntered = saveEntered;
        storage.releaseHeldSave = releaseSave;
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        feed.events = Flux.just(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).concatWith(Flux.never());
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        subscribe(model);
        assertThat(saveEntered.await(5, TimeUnit.SECONDS)).as("the save of the position the subscription reached has reached the storage").isTrue();

        try {
            // When
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();

            // Then
            assertThat(cancelled.isDone()).as("cancel reported as complete while a position save of the cancelled subscription was still in flight").isFalse();
            releaseSave.countDown();
            cancelled.get(5, TimeUnit.SECONDS);
            assertThat(hasPosition(storage)).as("position in storage once the cancel reported completion").isFalse();
        } finally {
            releaseSave.countDown();
        }
    }

    /**
     * A wrapped model can still be running an event through the action when the cancel reaches it, and the position
     * save behind that action then starts after the cancel has completed.
     */
    @Test
    void a_position_save_the_cancelled_subscription_starts_after_the_cancel_never_reaches_the_storage() {
        // Given
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        subscribe(model);
        Function<CloudEvent, Mono<Void>> actionOfTheCancelledSubscription = wrapped.actions.get(0);

        // When
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
        actionOfTheCancelledSubscription.apply(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).block(TIMEOUT);

        // Then
        assertThat(hasPosition(storage)).as("position in storage after the cancelled subscription handled an event following the completed cancel").isFalse();
    }

    private static void runningFromAStoredPosition(ReactorDurableSubscriptionModel model, CheckpointStorage storage) {
        storage.save(SUBSCRIPTION_ID, REACHED_BEFORE_THE_CANCEL).block(TIMEOUT);
        subscribe(model);
    }

    private static void subscribe(ReactorDurableSubscriptionModel model) {
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
    }

    private static CloudEvent eventAt(Checkpoint checkpoint) {
        CloudEvent event = CloudEventBuilder.v1().withId("1").withSource(URI.create("urn:test")).withType("Something").build();
        return new CheckpointAwareCloudEvent(event, checkpoint);
    }

    private static boolean hasPosition(CheckpointStorage storage) {
        return Boolean.TRUE.equals(storage.read(SUBSCRIPTION_ID).hasElement().block(TIMEOUT));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The latch was never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static class PositionStorage implements CheckpointStorage {
        private final CheckpointStorage backing;
        private volatile Mono<Void> beforeDelete = Mono.empty();
        // A save of this position waits for releaseHeldSave and is then written whether or not anyone still waits for it
        private volatile @Nullable Checkpoint heldSave;
        private volatile CountDownLatch heldSaveEntered = new CountDownLatch(0);
        private volatile CountDownLatch releaseHeldSave = new CountDownLatch(0);

        private PositionStorage() {
            this(new InMemoryCheckpointStorage());
        }

        private PositionStorage(CheckpointStorage backing) {
            this.backing = backing;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return backing.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            Checkpoint held = heldSave;
            if (held != null && held.asString().equals(checkpoint.asString())) {
                return Mono.fromCallable(() -> {
                    heldSaveEntered.countDown();
                    awaitLatch(releaseHeldSave);
                    return backing.save(subscriptionId, checkpoint, writeCondition).block(TIMEOUT);
                }).subscribeOn(Schedulers.boundedElastic());
            }
            return backing.save(subscriptionId, checkpoint, writeCondition);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return backing.writeVersion(subscriptionId);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return backing.evaluatesWriteConditions();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> beforeDelete.then(Mono.defer(() -> backing.delete(subscriptionId))));
        }
    }
}
