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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.ResumeStartPositions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelCancelCompletionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String PAUSE_HOOK = "pause-for-" + ReactorDurableSubscriptionModelCancelCompletionTest.class.getSimpleName();
    private static final String SUBSCRIPTION_ID = "sub";
    private static final StringBasedCheckpoint REACHED_BEFORE_THE_CANCEL = new StringBasedCheckpoint("reached-before-the-cancel");
    private static final String WHERE_THE_FEED_IS_NOW = "where-the-feed-is-now";
    private static final StringBasedCheckpoint REACHED_BY_THE_CANCELLED_SUBSCRIPTION = new StringBasedCheckpoint("reached-by-the-cancelled-subscription");
    private static final StringBasedCheckpoint HANDLED_BY_THE_NEW_SUBSCRIPTION = new StringBasedCheckpoint("handled-by-the-new-subscription");
    private static final StringBasedCheckpoint REBUILT_UP_TO_3 = new StringBasedCheckpoint("rebuilt-up-to-3");
    private static final StringBasedCheckpoint BEGINNING = new StringBasedCheckpoint("beginning");
    private static final StringBasedCheckpoint SLOW_TO_CANCEL = new StringBasedCheckpoint("slow-to-cancel");
    // How long the storage holds a save or a delete before the test lets it through
    private static final StringBasedCheckpoint DELIVERED_AFTER_THE_SHUTDOWN = new StringBasedCheckpoint("delivered-after-the-shutdown");
    private static final String WHERE_THE_FEED_WAS_AT_REGISTRATION = "where-the-feed-was-at-registration";
    private static final StringBasedCheckpoint WHERE_THE_FEED_IS_ONCE_THE_SUBSCRIBE_RETURNED = new StringBasedCheckpoint("where-the-feed-is-once-the-subscribe-returned");
    private static final Duration HELD_BY_THE_STORAGE = Duration.ofMillis(500);

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

    /**
     * The subscribe is still reading where to start when a cancel of the same id runs, with no subscription under that
     * id yet, and the subscription it makes goes on to handle an event after the cancel completed.
     */
    @Test
    void a_subscription_still_reading_its_start_position_during_a_cancel_stores_its_own_positions() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        storage.holdNextRead = true;
        CompletableFuture<Void> subscribed = CompletableFuture.runAsync(() -> subscribe(model));
        assertThat(storage.heldReadEntered.await(5, TimeUnit.SECONDS)).as("the subscribe is reading its start position").isTrue();

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            storage.releaseHeldRead.countDown();
            subscribed.get(5, TimeUnit.SECONDS);
            wrapped.actions.get(0).apply(eventAt(HANDLED_BY_THE_NEW_SUBSCRIPTION)).block(TIMEOUT);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT)).as("position stored for the new subscription after it handled an event").isEqualTo(HANDLED_BY_THE_NEW_SUBSCRIPTION.asString());
        } finally {
            storage.releaseHeldRead.countDown();
        }
    }

    /**
     * The subscribe reads the position of the subscription being cancelled before the delete reaches the storage, and
     * the read answers after the cancel completed.
     */
    @Test
    void a_subscribe_whose_start_position_read_overlaps_a_cancel_starts_from_its_own_start_at_and_stores_it() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        storage.holdNextRead = true;
        CompletableFuture<Void> subscribed = CompletableFuture.runAsync(() -> subscribe(model));
        assertThat(storage.heldReadEntered.await(5, TimeUnit.SECONDS)).as("the second subscribe is reading its start position").isTrue();

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            storage.releaseHeldRead.countDown();
            subscribed.get(5, TimeUnit.SECONDS);

            // Then
            assertThat(wrapped.startedAt.get(1)).as("start position of the subscription whose read overlapped the cancel").hasToString(WHERE_THE_FEED_IS_NOW);
            assertThat(storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT)).as("position stored for the subscription whose read overlapped the cancel").isEqualTo(WHERE_THE_FEED_IS_NOW);
        } finally {
            storage.releaseHeldRead.countDown();
        }
    }

    /**
     * This model drives the feed itself here. The action of the cancelled subscription has ended, and the save behind
     * it is about to start, when the cancel is called. The save goes on after the cancel completed.
     */
    @Test
    void a_position_save_the_cancelled_subscription_would_start_after_the_cancel_never_reaches_the_storage_when_this_model_drives_the_feed() throws Exception {
        // Given
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        feed.events = Flux.just(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).concatWith(Flux.never());
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        AtomicReference<@Nullable Thread> actionThread = new AtomicReference<>();
        CountDownLatch saveAboutToStart = new CountDownLatch(1);
        CountDownLatch releaseSave = new CountDownLatch(1);
        // The first operator the save assembles is where it goes on to check whether it may still write
        Hooks.onEachOperator(PAUSE_HOOK, publisher -> {
            if (Thread.currentThread() == actionThread.get() && saveAboutToStart.getCount() > 0 && publisher.getClass().getSimpleName().equals("MonoDefer")) {
                saveAboutToStart.countDown();
                awaitUninterruptibly(releaseSave);
            }
            return publisher;
        });

        try {
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.<Void>fromRunnable(() -> actionThread.set(Thread.currentThread()))
                    .subscribeOn(Schedulers.boundedElastic()));
            assertThat(saveAboutToStart.await(5, TimeUnit.SECONDS)).as("the action has ended and the save behind it is about to start").isTrue();

            // When
            model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            releaseSave.countDown();
            await().atMost(TIMEOUT).until(() -> isIdle(requireThread(actionThread)));

            // Then
            assertThat(hasPosition(storage)).as("position in storage after the save behind the action of the cancelled subscription went on").isFalse();
        } finally {
            releaseSave.countDown();
            Hooks.resetOnEachOperator(PAUSE_HOOK);
        }
    }

    /**
     * The cancel has taken the subscription out of this model and not yet told reads of the id to wait for its delete
     * when the subscribe of the same id arrives.
     */
    @Test
    void a_subscribe_arriving_while_the_cancel_is_still_under_way_starts_from_its_own_start_at() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);

        // When
        @Nullable Throwable subscribeFailure = subscribeWhileTheCancelIsHeld(model, publisher -> publisher.getClass().getSimpleName().equals("MonoWhen"));

        // Then
        assertThat(subscribeFailure).as("failure of the subscribe that arrived during the cancel").isNull();
        assertThat(wrapped.startedAt.get(1)).as("start position of the subscribe that arrived during the cancel").hasToString(WHERE_THE_FEED_IS_NOW);
        assertThat(storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT)).as("position stored for the subscribe that arrived during the cancel").isEqualTo(WHERE_THE_FEED_IS_NOW);
    }

    /**
     * The subscribe finds the delete of the cancel before the cancel has started it, and the storage deletes and saves
     * on the thread that asks it to.
     */
    @Test
    void a_subscribe_that_finds_a_delete_the_cancel_has_not_started_yet_waits_for_it_and_records_its_own_start_position() throws Exception {
        // Given
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);

        // When
        @Nullable Throwable subscribeFailure = subscribeWhileTheCancelIsHeld(model, __ -> readsOfTheIdWaitForADelete(model));

        // Then
        assertThat(subscribeFailure).as("failure of the subscribe that found the delete").isNull();
        assertThat(wrapped.startedAt.get(0)).as("start position of the subscribe that found the delete").hasToString(WHERE_THE_FEED_IS_NOW);
        assertThat(storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT)).as("position stored for the subscribe that found the delete").isEqualTo(WHERE_THE_FEED_IS_NOW);
    }

    /**
     * The storage applies the save of the position the cancelled subscription reached only well after the cancel was
     * called, and applies it whether or not anything still waits for it, the way a save already sent to a store does.
     * The rebuild of the same id is subscribed without waiting for the cancel.
     */
    @Test
    void a_position_save_of_the_cancelled_subscription_that_the_storage_is_slow_to_answer_never_overwrites_the_rebuilds_position() throws Exception {
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
        runningFromAStoredPosition(model, storage);
        assertThat(saveEntered.await(5, TimeUnit.SECONDS)).as("the save of the position the subscription reached has reached the storage").isTrue();

        try {
            // When
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            feed.events = Flux.just(eventAt(REBUILT_UP_TO_3)).concatWith(Flux.never());
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty());
            waitFor(HELD_BY_THE_STORAGE);
            boolean cancelCompletedWhileTheSaveWasHeld = cancelled.isDone();
            releaseSave.countDown();
            assertThat(storage.heldSaveApplied.await(5, TimeUnit.SECONDS)).as("the storage applied the held save of the cancelled subscription").isTrue();

            // Then
            // The rebuild handled the latest event of the id, and nothing the cancelled subscription reached may outlive
            // the cancel, so a restart has to resume from the rebuild's position
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storedPosition(storage)).as("position stored once the storage applied the save of the cancelled subscription and the rebuild handled an event").isEqualTo(REBUILT_UP_TO_3.asString()));
            assertThat(cancelCompletedWhileTheSaveWasHeld).as("cancel reported as complete while a position save of the cancelled subscription was held").isFalse();
            cancelled.get(5, TimeUnit.SECONDS);
        } finally {
            releaseSave.countDown();
        }
    }

    /**
     * The storage applies the delete only well after the cancel was called, and applies it whether or not anything still
     * waits for it. The next subscription of the id is made without waiting for the cancel.
     */
    @Test
    void a_delete_the_storage_is_slow_to_answer_never_removes_the_position_of_the_next_subscription_of_the_id() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            // When
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            feed.events = Flux.just(eventAt(HANDLED_BY_THE_NEW_SUBSCRIPTION)).concatWith(Flux.never());
            subscribe(model);
            waitFor(HELD_BY_THE_STORAGE);
            boolean cancelCompletedWhileTheDeleteWasHeld = cancelled.isDone();
            releaseDelete.countDown();
            storage.heldDeleteApplied.get(5, TimeUnit.SECONDS);

            // Then
            // The new subscription handled an event after the cancel, so a restart has to resume from there
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storedPosition(storage)).as("position stored once the storage applied the delete of the cancel").isEqualTo(HANDLED_BY_THE_NEW_SUBSCRIPTION.asString()));
            // Nothing was stored once the delete had ended, so the default start is where the feed is
            assertThat(feed.startedAt.get(1)).as("start position of the subscription made without waiting for the cancel").hasToString(WHERE_THE_FEED_IS_NOW);
            assertThat(cancelCompletedWhileTheDeleteWasHeld).as("cancel reported as complete while its delete was held").isFalse();
            cancelled.get(5, TimeUnit.SECONDS);
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * The rebuild is subscribed without waiting for the cancel, with a start position that reads the stored position
     * itself and replays from the beginning when none is stored, the way the Spring Boot starter's BEGINNING start with
     * the default resume behaviour does.
     */
    @Test
    void a_rebuild_that_did_not_wait_for_the_cancel_and_reads_the_stored_position_itself_replays_from_the_beginning() {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.beforeDelete = Mono.delay(Duration.ofMillis(300)).then();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);

        // When
        model.cancelSubscription(SUBSCRIPTION_ID);
        model.subscribe(SUBSCRIPTION_ID, null, ResumeStartPositions.replayThenResume(SUBSCRIPTION_ID, storage, StartAt.checkpoint(BEGINNING)), __ -> Mono.empty());

        // Then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.startedAt).hasSize(2));
        // The cancel deleted the only stored position, and with none stored this start position replays from the beginning
        assertThat(feed.startedAt.get(1)).as("start position of the rebuild subscribed right after the cancel").hasToString(BEGINNING.asString());
    }

    /**
     * As above, for a DCB subscription.
     */
    @Test
    void a_dcb_rebuild_that_did_not_wait_for_the_cancel_and_reads_the_stored_position_itself_replays_from_the_beginning() {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.beforeDelete = Mono.delay(Duration.ofMillis(300)).then();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);

        // When
        model.cancelSubscription(SUBSCRIPTION_ID);
        model.subscribe(SUBSCRIPTION_ID, null, ResumeStartPositions.replayThenResumeDcb(SUBSCRIPTION_ID, storage, DcbStartAt.beginning()).toStartAt(), __ -> Mono.empty());

        // Then
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.startedAt).hasSize(2));
        // The cancel deleted the only stored position, and with none stored this start position replays from the beginning
        assertThat(feed.startedAt.get(1)).as("start position of the DCB rebuild subscribed right after the cancel").hasToString(DcbStartAt.beginning().toStartAt().toString());
    }

    /**
     * As above, on a wrapped model that manages named subscriptions of its own.
     */
    @Test
    void a_rebuild_on_a_named_model_that_did_not_wait_for_the_cancel_and_reads_the_stored_position_itself_replays_from_the_beginning() {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.beforeDelete = Mono.delay(Duration.ofMillis(300)).then();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);

        // When
        model.cancelSubscription(SUBSCRIPTION_ID);
        model.subscribe(SUBSCRIPTION_ID, null, ResumeStartPositions.replayThenResume(SUBSCRIPTION_ID, storage, StartAt.checkpoint(BEGINNING)), __ -> Mono.empty())
                .waitUntilStarted().block(TIMEOUT);

        // Then
        // The cancel deleted the only stored position, and with none stored this start position replays from the beginning
        assertThat(wrapped.startedAt.get(1)).as("start position of the rebuild subscribed right after the cancel").hasToString(BEGINNING.asString());
    }

    /**
     * A thread that must not block, a WebFlux request thread for example, subscribes the same id while the storage
     * still holds the delete of the cancel, with a start position that reads the stored position itself.
     */
    @Test
    void a_subscribe_on_a_non_blocking_thread_while_the_delete_runs_returns_and_starts_once_the_delete_has_ended() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID);
            CompletableFuture<Subscription> subscribed = Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, replayThenResume(storage), __ -> Mono.empty()))
                    .subscribeOn(Schedulers.parallel())
                    .toFuture();

            // Then
            assertThat(catchThrowable(() -> subscribed.get(5, TimeUnit.SECONDS))).as("failure of the subscribe made on a non-blocking thread while the delete runs").isNull();
            releaseDelete.countDown();
            subscribed.get().waitUntilStarted().block(TIMEOUT);
            assertThat(wrapped.startedAt.get(1)).as("start position of the subscription made on a non-blocking thread while the delete ran").hasToString(BEGINNING.asString());
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * The wrapped model is slow to take the subscribe of one id when the caller cancels another.
     */
    @Test
    void a_cancel_of_one_id_does_not_wait_for_the_wrapped_model_to_take_the_subscribe_of_another() throws Exception {
        // Given
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        wrapped.beforeSubscribe = __ -> {
            subscribeEntered.countDown();
            awaitLatch(releaseSubscribe);
        };
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe("a", null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty()));
        assertThat(subscribeEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is taking the subscribe of a").isTrue();

        try {
            // When
            CompletableFuture<Void> cancelled = CompletableFuture.runAsync(() -> model.cancelSubscription("b"));

            // Then
            assertThat(catchThrowable(() -> cancelled.get(5, TimeUnit.SECONDS))).as("failure of the cancel of b while the wrapped model takes the subscribe of a").isNull();
        } finally {
            releaseSubscribe.countDown();
            subscribed.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * The wrapped model is taking the subscribe of an id when a cancel of the same id runs. The subscribe has read where
     * to start by then, so the cancel comes after it, and the subscription it makes is the one the cancel stops.
     */
    @Test
    void a_cancel_of_an_id_the_wrapped_model_is_taking_a_subscribe_of_waits_for_it_and_then_cancels_that_subscription() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.save(SUBSCRIPTION_ID, REACHED_BEFORE_THE_CANCEL).block(TIMEOUT);
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        wrapped.beforeSubscribe = __ -> {
            subscribeEntered.countDown();
            awaitLatch(releaseSubscribe);
        };
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
        assertThat(subscribeEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is taking the subscribe").isTrue();
        AtomicReference<@Nullable Mono<Void>> cancelled = new AtomicReference<>();
        Thread canceller = new Thread(() -> cancelled.set(model.cancelSubscription(SUBSCRIPTION_ID)));

        try {
            // When
            canceller.start();
            // Either returned already or waiting for the wrapped model to finish taking the subscribe
            await().atMost(TIMEOUT).until(() -> !canceller.isAlive() || isWaiting(canceller));

            // Then
            assertThat(wrapped.calls).as("calls the wrapped model received while it took the subscribe").containsExactly("subscribe sub began");
            releaseSubscribe.countDown();
            subscribed.get(5, TimeUnit.SECONDS).waitUntilStarted().block(TIMEOUT);
            canceller.join(TIMEOUT.toMillis());
            Mono<Void> cancel = cancelled.get();
            assertThat(cancel).as("what the cancel answered").isNotNull();
            requireNonNull(cancel).block(TIMEOUT);
            assertThat(wrapped.calls).as("calls the wrapped model received once the cancel completed")
                    .containsExactly("subscribe sub began", "subscribe sub returned", "cancel sub");
            assertThat(wrapped.startedAt).as("start positions the wrapped model was handed").extracting(Object::toString).containsExactly(REACHED_BEFORE_THE_CANCEL.asString());
            // What the wrapped model was handed, run the way it would deliver an event
            wrapped.actions.get(0).apply(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).block(TIMEOUT);
            assertThat(storedPosition(storage)).as("position stored once the cancel completed").isNull();
        } finally {
            releaseSubscribe.countDown();
        }
    }

    /**
     * A subscribe of the same id with a start position that reads the stored position itself waits for the delete of
     * the cancel, and the model shuts down while the storage still holds that delete.
     */
    @Test
    void a_shutdown_ends_a_subscribe_still_waiting_for_the_delete_of_its_id() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            AtomicReference<@Nullable Thread> subscriber = new AtomicReference<>();
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> {
                subscriber.set(Thread.currentThread());
                return model.subscribe(SUBSCRIPTION_ID, null, replayThenResume(storage), __ -> Mono.empty());
            });
            // Either returned already or waiting for the delete on the thread that subscribes
            await().atMost(TIMEOUT).until(() -> subscribed.isDone() || isWaiting(subscriber.get()));

            // When
            model.shutdown();

            // Then
            Throwable outcome = catchThrowable(() -> subscribed.thenCompose(subscription -> subscription.waitUntilStarted().toFuture()).get(5, TimeUnit.SECONDS));
            assertThat(outcome).as("how the subscribe waiting for the delete ended once the model shut down").hasRootCauseInstanceOf(SubscriptionModelShutdownException.class);
            releaseDelete.countDown();
            storage.heldDeleteApplied.get(5, TimeUnit.SECONDS);
            // Nothing signals a start that never happens, so this looks once the storage has had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(wrapped.subscribedIds).as("subscribes the wrapped model received").hasSize(1);
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * As above with the subscription-model default start position, which this model reads on the thread that
     * subscribes before it hands the subscription to the wrapped model.
     */
    @Test
    void a_shutdown_ends_a_subscribe_with_the_model_default_start_position_still_waiting_for_the_delete_of_its_id() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            AtomicReference<@Nullable Thread> subscriber = new AtomicReference<>();
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> {
                subscriber.set(Thread.currentThread());
                return model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
            });
            await().atMost(TIMEOUT).until(() -> isWaiting(subscriber.get()));

            // When
            model.shutdown();

            // Then
            Throwable outcome = catchThrowable(() -> subscribed.get(5, TimeUnit.SECONDS));
            assertThat(outcome).as("how the subscribe waiting for the delete on its own thread ended once the model shut down").hasRootCauseInstanceOf(SubscriptionModelShutdownException.class);
            releaseDelete.countDown();
            storage.heldDeleteApplied.get(5, TimeUnit.SECONDS);
            // Nothing signals a start that never happens, so this looks once the storage has had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(wrapped.subscribedIds).as("subscribes the wrapped model received").hasSize(1);
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * As the first of the two above, when this model drives the feed itself.
     */
    @Test
    void a_shutdown_ends_a_subscribe_still_waiting_for_the_delete_of_its_id_when_this_model_drives_the_feed() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;
        AtomicBoolean startPositionResolved = new AtomicBoolean();

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(__ -> {
                startPositionResolved.set(true);
                return StartAt.checkpoint(BEGINNING);
            }), __ -> Mono.empty());

            // When
            model.shutdown();

            // Then
            assertThat(catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT))).as("how waiting for the start of the subscribe that waited for the delete ended once the model shut down")
                    .isInstanceOf(SubscriptionModelShutdownException.class);
            releaseDelete.countDown();
            storage.heldDeleteApplied.get(5, TimeUnit.SECONDS);
            // Nothing signals a start that never happens, so this looks once the storage has had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(startPositionResolved).as("start position of the subscribe that waited for the delete resolved after the shutdown").isFalse();
            assertThat(feed.startedAt).as("subscriptions the feed started").hasSize(1);
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * The storage is slow to answer the read of where one subscription starts, and answers it on the thread that
     * subscribes, while the caller subscribes, pauses, resumes and cancels another id.
     */
    @Test
    void a_slow_start_position_read_of_one_id_does_not_hold_up_the_calls_for_another_when_this_model_drives_the_feed() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW), storage);
        CountDownLatch releaseRead = new CountDownLatch(1);
        storage.releaseReadOfA = releaseRead;
        CompletableFuture<Subscription> subscribedA = CompletableFuture.supplyAsync(() -> model.subscribe("a", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
        assertThat(storage.readOfAEntered.await(5, TimeUnit.SECONDS)).as("the storage is reading where a starts").isTrue();

        try {
            // When
            CompletableFuture<Void> callsForB = CompletableFuture.runAsync(() -> subscribePauseResumeAndCancel(model, "b"));

            // Then
            assertThat(catchThrowable(() -> callsForB.get(5, TimeUnit.SECONDS))).as("failure of the calls for b while the storage reads where a starts").isNull();
        } finally {
            releaseRead.countDown();
            subscribedA.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * The dynamic start position of one subscription is slow to answer while the caller subscribes, pauses, resumes
     * and cancels another id, and subscribes the first id again.
     */
    @Test
    void a_dynamic_start_position_of_one_id_slow_to_answer_does_not_hold_up_the_calls_for_another_when_this_model_drives_the_feed() throws Exception {
        // Given
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW), new InMemoryCheckpointStorage());
        CountDownLatch functionEntered = new CountDownLatch(1);
        CountDownLatch releaseFunction = new CountDownLatch(1);
        CompletableFuture<Subscription> subscribedA = CompletableFuture.supplyAsync(() -> model.subscribe("a", null, StartAt.dynamic(() -> {
            functionEntered.countDown();
            awaitLatch(releaseFunction);
            return StartAt.checkpoint(BEGINNING);
        }), __ -> Mono.empty()));
        assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("the start position of a is being resolved").isTrue();
        AtomicBoolean duplicateResolved = new AtomicBoolean();

        try {
            // When
            CompletableFuture<Void> callsForB = CompletableFuture.runAsync(() -> subscribePauseResumeAndCancel(model, "b"));
            CompletableFuture<Subscription> duplicate = CompletableFuture.supplyAsync(() -> model.subscribe("a", null, StartAt.dynamic(() -> {
                duplicateResolved.set(true);
                return StartAt.checkpoint(BEGINNING);
            }), __ -> Mono.empty()));

            // Then
            assertThat(catchThrowable(() -> callsForB.get(5, TimeUnit.SECONDS))).as("failure of the calls for b while the start position of a is resolved").isNull();
            assertThat(catchThrowable(() -> duplicate.get(5, TimeUnit.SECONDS))).as("failure of the second subscribe of a").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            assertThat(duplicateResolved).as("start position of the second subscribe of a resolved").isFalse();
        } finally {
            releaseFunction.countDown();
            subscribedA.get(5, TimeUnit.SECONDS).waitUntilStarted().block(TIMEOUT);
        }
    }

    /**
     * The wrapped model is slow to cancel the feed of one subscription that the caller pauses, while the caller
     * subscribes, pauses, resumes and cancels another id.
     */
    @Test
    void a_pause_of_one_id_the_wrapped_model_is_slow_to_cancel_does_not_hold_up_the_calls_for_another_when_this_model_drives_the_feed() throws Exception {
        // Given
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        CountDownLatch cancelEntered = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        AtomicBoolean firstCancel = new AtomicBoolean(true);
        feed.events = Flux.<CloudEvent>never().doOnCancel(() -> {
            if (firstCancel.getAndSet(false)) {
                cancelEntered.countDown();
                awaitLatch(releaseCancel);
            }
        });
        model.subscribe("a", null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        CompletableFuture<Void> pausedA = CompletableFuture.runAsync(() -> model.pauseSubscription("a"));
        assertThat(cancelEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is cancelling the feed of a").isTrue();

        try {
            // When
            CompletableFuture<Void> callsForB = CompletableFuture.runAsync(() -> subscribePauseResumeAndCancel(model, "b"));

            // Then
            assertThat(catchThrowable(() -> callsForB.get(5, TimeUnit.SECONDS))).as("failure of the calls for b while the wrapped model cancels the feed of a").isNull();
        } finally {
            releaseCancel.countDown();
            pausedA.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A subscription registered while this model is stopped has not started when the model shuts down, and one
     * registered while it ran had.
     */
    @Test
    void a_shutdown_ends_the_wait_for_a_subscription_that_has_not_started_when_this_model_drives_the_feed() {
        // Given
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW), new InMemoryCheckpointStorage());
        Subscription started = model.subscribe("started", null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty());
        started.waitUntilStarted().block(TIMEOUT);
        model.stop();
        Subscription notStarted = model.subscribe("not-started", null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty());

        // When
        model.shutdown();

        // Then
        assertThat(catchThrowable(() -> notStarted.waitUntilStarted().block(TIMEOUT))).as("how waiting for the start of the subscription the model shut down before it started ended")
                .isInstanceOf(SubscriptionModelShutdownException.class);
        assertThat(catchThrowable(() -> started.waitUntilStarted().block(TIMEOUT))).as("failure of waiting for the start of the subscription that started before the shutdown").isNull();
    }

    /**
     * The caller cancels the id without waiting, subscribes it again with a dynamic start position, which has to wait
     * for the delete and so returns right away, and then cancels the id once more and waits for that cancel.
     */
    @Test
    void a_cancel_ends_a_subscribe_that_has_returned_and_waits_for_the_delete_of_an_earlier_cancel() {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        storage.beforeDelete = Mono.delay(HELD_BY_THE_STORAGE).then();
        model.cancelSubscription(SUBSCRIPTION_ID);
        AtomicBoolean startPositionResolved = new AtomicBoolean();
        Subscription waitingForTheDelete = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
            startPositionResolved.set(true);
            return StartAt.checkpoint(BEGINNING);
        }), __ -> Mono.empty());

        // When
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);

        // Then
        int subscribesWhenTheCancelCompleted = wrapped.subscribedIds.size();
        // Nothing signals a hand-over that never happens, so this looks once the storage has had as long again
        waitFor(HELD_BY_THE_STORAGE);
        assertThat(wrapped.subscribedIds).as("subscribes the wrapped model received after the cancel completed").hasSize(subscribesWhenTheCancelCompleted);
        assertThat(startPositionResolved).as("start position of the cancelled subscribe resolved").isFalse();
        assertThat(catchThrowable(() -> waitingForTheDelete.waitUntilStarted().block(TIMEOUT))).as("how waiting for the start of the cancelled subscribe ended")
                .isInstanceOf(CancellationException.class);
        // What the wrapped model was handed last, run the way it would deliver an event
        wrapped.actions.get(wrapped.actions.size() - 1).apply(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).block(TIMEOUT);
        assertThat(storedPosition(storage)).as("position stored once the cancel completed").isNull();
    }

    /**
     * The caller cancels a subscription and, without waiting, subscribes the id again with a dynamic start position,
     * which has to wait for the delete and so returns right away. The caller then cancels or pauses that subscribe
     * before the delete has ended.
     */
    // A shutdown is covered by a_shutdown_ends_a_subscribe_still_waiting_for_the_delete_of_its_id_when_this_model_drives_the_feed
    @ParameterizedTest
    @EnumSource(names = {"CANCEL", "PAUSE"})
    void a_cancel_or_a_pause_ends_the_wait_for_a_subscribe_that_waits_for_a_delete_when_this_model_drives_the_feed(EndedBy endedBy) {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;
        AtomicBoolean startPositionResolved = new AtomicBoolean();

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            Subscription waitingForTheDelete = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                startPositionResolved.set(true);
                return StartAt.checkpoint(BEGINNING);
            }), __ -> Mono.empty());

            // When
            endedBy.end(model, SUBSCRIPTION_ID);

            // Then
            assertThat(catchThrowable(() -> waitingForTheDelete.waitUntilStarted().block(TIMEOUT))).as("how waiting for the start of the subscribe ended")
                    .isInstanceOf(CancellationException.class);
            releaseDelete.countDown();
            // Nothing signals a start that never happens, so this looks once the storage has had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(startPositionResolved).as("start position of the subscribe resolved").isFalse();
            assertThat(feed.startedAt).as("subscriptions the feed started").hasSize(1);
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * start(true) has reserved every subscription it starts and is held in the dynamic start position of the first
     * one while the caller cancels or pauses another, or shuts the model down.
     */
    @ParameterizedTest
    @EnumSource
    void a_subscription_that_start_has_reserved_and_not_started_yet_never_resolves_its_start_position_once_ended(EndedBy endedBy) throws Exception {
        // Given
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        CountDownLatch functionEntered = new CountDownLatch(1);
        CountDownLatch releaseFunction = new CountDownLatch(1);
        AtomicBoolean startPositionOfAResolved = new AtomicBoolean();
        model.stop();
        // Started first, since start(true) goes through the ids in the order the map holds them and "0" comes before
        // "a" there
        model.subscribe("0", null, StartAt.dynamic(() -> {
            functionEntered.countDown();
            awaitLatch(releaseFunction);
            return StartAt.checkpoint(BEGINNING);
        }), __ -> Mono.empty());
        model.subscribe("a", null, StartAt.dynamic(() -> {
            startPositionOfAResolved.set(true);
            return StartAt.checkpoint(REACHED_BEFORE_THE_CANCEL);
        }), __ -> Mono.empty());
        CompletableFuture<Void> started = CompletableFuture.runAsync(() -> model.start(true));
        assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("start(true) resolves the start position of 0").isTrue();

        try {
            // When
            endedBy.end(model, "a");
            releaseFunction.countDown();
            started.get(5, TimeUnit.SECONDS);

            // Then
            assertThat(startPositionOfAResolved).as("start position of a resolved after it was ended").isFalse();
            assertThat(feed.startedAt).as("start positions the feed was subscribed from").noneMatch(startAt -> startAt.toString().equals(REACHED_BEFORE_THE_CANCEL.asString()));
        } finally {
            releaseFunction.countDown();
        }
    }

    /**
     * stop() is held cancelling the feed of one subscription, a feed slow to cancel, before it disposes a second one
     * whose generation it has retired. start(true) is held in the dynamic start position of a third before it
     * disposes the second one's old generation. The caller cancels the second one in between and an event reaches its
     * old generation afterwards.
     */
    @Test
    void a_generation_that_stop_retired_and_start_replaced_writes_no_position_after_a_cancel_completed() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        Sinks.Many<CloudEvent> feedOfA = Sinks.many().multicast().directBestEffort();
        CountDownLatch slowCancelEntered = new CountDownLatch(1);
        CountDownLatch releaseSlowCancel = new CountDownLatch(1);
        AtomicBoolean cancelSlowly = new AtomicBoolean();
        CheckpointAwareSubscriptionModel feed = new CheckpointAwareSubscriptionModel() {
            @Override
            public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
                if (startAt.toString().equals(SLOW_TO_CANCEL.asString())) {
                    return Flux.<CloudEvent>never().doOnCancel(() -> {
                        if (cancelSlowly.get()) {
                            slowCancelEntered.countDown();
                            awaitLatch(releaseSlowCancel);
                        }
                    });
                }
                return startAt.toString().equals(BEGINNING.asString()) ? feedOfA.asFlux() : Flux.never();
            }

            @Override
            public Mono<Checkpoint> globalCheckpoint() {
                return Mono.just(new StringBasedCheckpoint(WHERE_THE_FEED_IS_NOW));
            }
        };
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        AtomicBoolean holdTheFunction = new AtomicBoolean();
        CountDownLatch functionEntered = new CountDownLatch(1);
        CountDownLatch releaseFunction = new CountDownLatch(1);
        model.stop();
        // Started first by start(true), and before "@" and "a" in the order stop() goes through, since the map holds
        // "0" and "@" ahead of "a"
        model.subscribe("0", null, StartAt.dynamic(() -> {
            if (holdTheFunction.get()) {
                functionEntered.countDown();
                awaitLatch(releaseFunction);
            }
            return StartAt.checkpoint(REACHED_BEFORE_THE_CANCEL);
        }), __ -> Mono.empty());
        model.start(false);
        model.subscribe("@", null, StartAt.checkpoint(SLOW_TO_CANCEL), __ -> Mono.empty());
        model.subscribe("a", null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        cancelSlowly.set(true);
        holdTheFunction.set(true);
        CompletableFuture<Void> stopped = CompletableFuture.runAsync(model::stop);
        assertThat(slowCancelEntered.await(5, TimeUnit.SECONDS)).as("stop() cancels the feed of @").isTrue();
        CompletableFuture<Void> started = CompletableFuture.runAsync(() -> model.start(true));
        assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("start(true) resolves the start position of 0").isTrue();

        try {
            // When
            model.cancelSubscription("a").block(TIMEOUT);
            feedOfA.tryEmitNext(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION));

            // Then
            assertThat(storage.read("a").map(Checkpoint::asString).block(TIMEOUT)).as("position of a stored after its cancel completed").isNull();
        } finally {
            releaseSlowCancel.countDown();
            releaseFunction.countDown();
            stopped.get(5, TimeUnit.SECONDS);
            started.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Two of three subscriptions registered while the model was stopped have a dynamic start position that throws.
     */
    @Test
    void start_keeps_each_subscription_whose_start_position_throws_paused_and_starts_the_rest() {
        // Given
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        IllegalStateException firstFailure = new IllegalStateException("x cannot answer");
        IllegalStateException secondFailure = new IllegalStateException("z cannot answer");
        model.subscribe("x", null, StartAt.dynamic(() -> {
            throw firstFailure;
        }), __ -> Mono.empty());
        model.subscribe("y", null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty());
        model.subscribe("z", null, StartAt.dynamic(() -> {
            throw secondFailure;
        }), __ -> Mono.empty());

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));

        // Then
        assertThat(model.isRunning("y")).as("y running").isTrue();
        assertThat(model.isPaused("x")).as("x paused").isTrue();
        assertThat(model.isPaused("z")).as("z paused").isTrue();
        assertThat(feed.startedAt).as("start positions the feed was subscribed from").map(StartAt::toString).containsExactly(BEGINNING.asString());
        assertThat(thrown).as("what start(true) threw").isIn(firstFailure, secondFailure);
        assertThat(thrown.getSuppressed()).as("what start(true) suppressed").containsExactly(thrown == firstFailure ? secondFailure : firstFailure);
    }

    /**
     * The wrapped model is running an event through the action it was handed when the model shuts down, and the
     * action ends only after that.
     */
    @Test
    void a_position_save_that_a_subscription_handed_to_the_wrapped_model_would_start_after_a_shutdown_never_reaches_the_storage() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        CountDownLatch actionEntered = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(BEGINNING), __ -> Mono.fromRunnable(() -> {
            actionEntered.countDown();
            awaitLatch(releaseAction);
        }));
        CompletableFuture<Void> delivered = CompletableFuture.runAsync(() -> wrapped.actions.get(0).apply(eventAt(DELIVERED_AFTER_THE_SHUTDOWN)).block(TIMEOUT));
        assertThat(actionEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model runs an event through the action").isTrue();

        try {
            // When
            model.shutdown();
            releaseAction.countDown();
            delivered.get(5, TimeUnit.SECONDS);

            // Then
            assertThat(storedPosition(storage)).as("position stored once the action that the shutdown overtook ended").isNull();
        } finally {
            releaseAction.countDown();
        }
    }

    /**
     * A subscribe that waited for the delete of an earlier cancel is being handed to the wrapped model when the caller
     * cancels the id again or shuts the model down.
     */
    @ParameterizedTest
    @EnumSource(names = {"CANCEL", "SHUTDOWN"})
    void a_cancel_or_a_shutdown_reaches_the_wrapped_model_only_once_it_has_taken_a_subscribe_that_waited_for_a_delete(EndedBy endedBy) throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        storage.beforeDelete = Mono.delay(HELD_BY_THE_STORAGE).then();
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        wrapped.beforeSubscribe = __ -> {
            subscribeEntered.countDown();
            awaitLatch(releaseSubscribe);
        };
        model.cancelSubscription(SUBSCRIPTION_ID);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> StartAt.checkpoint(BEGINNING)), __ -> Mono.empty());
        assertThat(subscribeEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is taking the subscribe that waited for the delete").isTrue();
        Thread ending = new Thread(() -> endedBy.end(model, SUBSCRIPTION_ID));

        try {
            // When
            ending.start();
            // Either returned already or waiting for the wrapped model to finish taking the subscribe
            await().atMost(TIMEOUT).until(() -> !ending.isAlive() || isWaiting(ending));

            // Then
            assertThat(wrapped.calls).as("calls the wrapped model received while it took the subscribe")
                    .containsExactly("subscribe sub began", "subscribe sub returned", "cancel sub", "subscribe sub began");
            releaseSubscribe.countDown();
            ending.join(TIMEOUT.toMillis());
            assertThat(ending.isAlive()).as("still ending the subscription once the wrapped model took the subscribe").isFalse();
            assertThat(wrapped.calls).as("calls the wrapped model received once the subscription was ended")
                    .containsExactly("subscribe sub began", "subscribe sub returned", "cancel sub", "subscribe sub began", "subscribe sub returned",
                            endedBy == EndedBy.CANCEL ? "cancel sub" : "shutdown");
            // What the wrapped model was handed last, run the way it would deliver an event
            wrapped.actions.get(wrapped.actions.size() - 1).apply(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).block(TIMEOUT);
            assertThat(storedPosition(storage)).as("position stored once the subscription was ended").isNull();
        } finally {
            releaseSubscribe.countDown();
        }
    }

    /**
     * A subscribe that waited for the delete of an earlier cancel has resolved its start position and is held at the
     * last check before it is handed to the wrapped model when the model shuts down.
     */
    @Test
    void a_subscribe_that_waited_for_a_delete_is_never_handed_to_the_wrapped_model_once_a_shutdown_has_begun() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        storage.beforeDelete = Mono.delay(HELD_BY_THE_STORAGE).then();
        CountDownLatch functionEntered = new CountDownLatch(1);
        CountDownLatch releaseFunction = new CountDownLatch(1);
        AtomicReference<@Nullable Thread> handingOver = new AtomicReference<>();
        model.cancelSubscription(SUBSCRIPTION_ID);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
            handingOver.set(Thread.currentThread());
            functionEntered.countDown();
            awaitUninterruptibly(releaseFunction);
            return StartAt.checkpoint(BEGINNING);
        }), __ -> Mono.empty());
        assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("the subscribe that waited for the delete resolves its start position").isTrue();
        // The lock every check of whether the subscribe was ended takes, so holding it holds the subscribe at that check
        Object positionLock = field(model, "positionLock");
        Thread shuttingDown = new Thread(model::shutdown);

        try {
            // When
            synchronized (positionLock) {
                releaseFunction.countDown();
                await().atMost(TIMEOUT).until(() -> requireThread(handingOver).getState() == Thread.State.BLOCKED);
                shuttingDown.start();
                // The flag is set before the shutdown takes any lock, so a shutdown held on one has begun
                await().atMost(TIMEOUT).until(() -> shuttingDown.getState() == Thread.State.BLOCKED || isWaiting(shuttingDown));
            }
            shuttingDown.join(TIMEOUT.toMillis());

            // Then
            // Nothing signals a hand-over that never happens, so this looks once the storage has had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(wrapped.calls).as("calls the wrapped model received").containsExactly("subscribe sub began", "subscribe sub returned", "cancel sub", "shutdown");
        } finally {
            releaseFunction.countDown();
        }
    }

    /**
     * A subscribe that waited for the delete of an earlier cancel is resolving its start position when the model shuts
     * down, and the function answers only after that.
     */
    @Test
    void a_shutdown_while_a_subscribe_that_waited_for_a_delete_resolves_its_start_position_drops_no_error() throws Exception {
        // Given
        List<Throwable> dropped = new CopyOnWriteArrayList<>();
        Hooks.onErrorDropped(dropped::add);
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        storage.beforeDelete = Mono.delay(HELD_BY_THE_STORAGE).then();
        CountDownLatch functionEntered = new CountDownLatch(1);
        CountDownLatch releaseFunction = new CountDownLatch(1);
        CountDownLatch functionAnswered = new CountDownLatch(1);

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            Subscription waitingForTheDelete = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                functionEntered.countDown();
                awaitUninterruptibly(releaseFunction);
                functionAnswered.countDown();
                return StartAt.checkpoint(BEGINNING);
            }), __ -> Mono.empty());
            assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("the subscribe that waited for the delete resolves its start position").isTrue();

            // When
            model.shutdown();
            releaseFunction.countDown();

            // Then
            assertThat(catchThrowable(() -> waitingForTheDelete.waitUntilStarted().block(TIMEOUT))).as("how waiting for the start of the subscribe ended")
                    .isInstanceOf(SubscriptionModelShutdownException.class);
            assertThat(functionAnswered.await(5, TimeUnit.SECONDS)).as("the function answered").isTrue();
            // Nothing signals what the subscribe does once its function answered, so this looks once the storage has
            // had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(dropped).as("errors Reactor dropped because nothing was left to receive them").isEmpty();
            assertThat(wrapped.subscribedIds).as("subscribes the wrapped model received").hasSize(1);
        } finally {
            releaseFunction.countDown();
            Hooks.resetOnErrorDropped();
        }
    }

    /**
     * A subscribe on a stopped model has taken the id and is held before it returns, while start(true) takes the
     * subscription over and starts it. The storage still holds the delete of an earlier cancel, which the subscription
     * that start(true) starts waits for before it reads the stored position. The feed moves on once the subscribe has
     * returned.
     */
    @Test
    void a_subscription_that_start_takes_over_before_its_subscribe_returned_starts_from_where_the_feed_was_at_registration_when_this_model_drives_the_feed() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_WAS_AT_REGISTRATION);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;
        model.cancelSubscription(SUBSCRIPTION_ID);
        model.stop();
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        Thread subscriber = new Thread(() -> subscribe(model));
        // The first operator the subscribe assembles once it has taken the id and released the monitor
        Hooks.onEachOperator(PAUSE_HOOK, publisher -> {
            if (Thread.currentThread() == subscriber && !Thread.holdsLock(model) && held.getCount() > 0) {
                held.countDown();
                awaitUninterruptibly(releaseSubscribe);
            }
            return publisher;
        });

        try {
            subscriber.start();
            assertThat(held.await(5, TimeUnit.SECONDS)).as("the subscribe is held once it has taken the id").isTrue();

            // When
            model.start(true);
            releaseSubscribe.countDown();
            subscriber.join(TIMEOUT.toMillis());
            assertThat(subscriber.isAlive()).as("subscribe still running once released").isFalse();
            feed.globalCheckpoint = WHERE_THE_FEED_IS_ONCE_THE_SUBSCRIBE_RETURNED;
            releaseDelete.countDown();

            // Then
            await().atMost(TIMEOUT).until(() -> feed.startedAt.size() == 2);
            assertThat(feed.startedAt).as("start positions the feed was subscribed from").map(StartAt::toString)
                    .containsExactly(REACHED_BEFORE_THE_CANCEL.asString(), WHERE_THE_FEED_WAS_AT_REGISTRATION);
        } finally {
            releaseSubscribe.countDown();
            releaseDelete.countDown();
            Hooks.resetOnEachOperator(PAUSE_HOOK);
        }
    }

    /**
     * A subscribe that waited for the delete of an earlier cancel is resolving its dynamic start position when the
     * caller cancels the id again, and the function answers, or throws, only once that cancel has returned. This model
     * does not wait for a function it did not write, so the function runs to its end, and what it answers starts
     * nothing.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_dynamic_start_position_resolving_when_a_cancel_returns_starts_nothing_and_drops_no_error_when_this_model_drives_the_feed(boolean functionThrows) throws Exception {
        // Given
        List<Throwable> dropped = new CopyOnWriteArrayList<>();
        Hooks.onErrorDropped(dropped::add);
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        storage.beforeDelete = Mono.delay(HELD_BY_THE_STORAGE).then();
        CountDownLatch functionEntered = new CountDownLatch(1);
        CountDownLatch releaseFunction = new CountDownLatch(1);
        CountDownLatch functionEnded = new CountDownLatch(1);

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            Subscription resolving = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                functionEntered.countDown();
                awaitUninterruptibly(releaseFunction);
                functionEnded.countDown();
                if (functionThrows) {
                    throw new IllegalStateException("Answered once the cancel had returned");
                }
                return StartAt.checkpoint(BEGINNING);
            }), __ -> Mono.empty());
            assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("the subscribe that waited for the delete resolves its start position").isTrue();

            // When
            Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
            releaseFunction.countDown();
            cancelled.block(TIMEOUT);

            // Then
            assertThat(functionEnded.await(5, TimeUnit.SECONDS)).as("the function ran to its end after the cancel returned").isTrue();
            assertThat(catchThrowable(() -> resolving.waitUntilStarted().block(TIMEOUT))).as("how waiting for the start of the cancelled subscribe ended")
                    .isInstanceOf(CancellationException.class);
            // Nothing signals what the subscribe does once its function answered, so this looks once the storage has
            // had as long again
            waitFor(HELD_BY_THE_STORAGE);
            assertThat(feed.startedAt).as("subscriptions the feed started").hasSize(1);
            assertThat(storedPosition(storage)).as("position stored once the cancel completed").isNull();
            assertThat(dropped).as("errors Reactor dropped because nothing was left to receive them").isEmpty();
        } finally {
            releaseFunction.countDown();
            Hooks.resetOnErrorDropped();
        }
    }

    private enum EndedBy {
        CANCEL {
            @Override
            void end(ReactorDurableSubscriptionModel model, String subscriptionId) {
                model.cancelSubscription(subscriptionId);
            }
        },
        PAUSE {
            @Override
            void end(ReactorDurableSubscriptionModel model, String subscriptionId) {
                model.pauseSubscription(subscriptionId);
            }
        },
        SHUTDOWN {
            @Override
            void end(ReactorDurableSubscriptionModel model, String subscriptionId) {
                model.shutdown();
            }
        };

        abstract void end(ReactorDurableSubscriptionModel model, String subscriptionId);
    }

    private static void subscribePauseResumeAndCancel(ReactorDurableSubscriptionModel model, String subscriptionId) {
        model.subscribe(subscriptionId, null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty());
        model.pauseSubscription(subscriptionId);
        model.resumeSubscription(subscriptionId);
        model.subscriptionIds();
        model.cancelSubscription(subscriptionId).block(TIMEOUT);
    }

    private static StartAt replayThenResume(CheckpointStorage storage) {
        return ResumeStartPositions.replayThenResume(SUBSCRIPTION_ID, storage, StartAt.checkpoint(BEGINNING));
    }

    private static boolean isWaiting(@Nullable Thread thread) {
        return thread != null && (thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING);
    }

    // Holds the thread that cancels at the first operator it assembles once pauseHere answers true, and subscribes the
    // same id on another thread while it is held. Answers what that subscribe threw.
    private static @Nullable Throwable subscribeWhileTheCancelIsHeld(ReactorDurableSubscriptionModel model, Predicate<Object> pauseHere) throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> cancelCalled = new CompletableFuture<>();
        AtomicReference<@Nullable Throwable> subscribeFailure = new AtomicReference<>();
        Thread canceller = new Thread(() -> {
            try {
                model.cancelSubscription(SUBSCRIPTION_ID);
                cancelCalled.complete(null);
            } catch (Throwable throwable) {
                cancelCalled.completeExceptionally(throwable);
            }
        });
        Thread subscriber = new Thread(() -> {
            try {
                subscribe(model);
            } catch (Throwable throwable) {
                subscribeFailure.set(throwable);
            }
        });
        Hooks.onEachOperator(PAUSE_HOOK, publisher -> {
            if (Thread.currentThread() == canceller && held.getCount() > 0 && pauseHere.test(publisher)) {
                held.countDown();
                awaitUninterruptibly(release);
            }
            return publisher;
        });
        try {
            canceller.start();
            assertThat(held.await(5, TimeUnit.SECONDS)).as("the cancel is held where the test pauses it").isTrue();
            subscriber.start();
            // A subscribe that waits waits for the cancel, which goes on only once it is released
            await().atMost(TIMEOUT).until(() -> !subscriber.isAlive() || subscriber.getState() != Thread.State.RUNNABLE);
            release.countDown();
            cancelCalled.get(5, TimeUnit.SECONDS);
            subscriber.join(TIMEOUT.toMillis());
            assertThat(subscriber.isAlive()).as("subscribe still running once the cancel was released").isFalse();
            return subscribeFailure.get();
        } finally {
            release.countDown();
            Hooks.resetOnEachOperator(PAUSE_HOOK);
        }
    }

    // Asked on the thread that cancels. True once reads and writes of the id wait for a delete and that thread no
    // longer holds the lock they take to find it.
    private static boolean readsOfTheIdWaitForADelete(ReactorDurableSubscriptionModel model) {
        Object positionLock = field(model, "positionLock");
        if (Thread.holdsLock(positionLock)) {
            return false;
        }
        synchronized (positionLock) {
            return ((Map<?, ?>) field(model, "positionDeletes")).containsKey(SUBSCRIPTION_ID);
        }
    }

    private static Object field(ReactorDurableSubscriptionModel model, String name) {
        try {
            Field field = ReactorDurableSubscriptionModel.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(model);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    // A scheduler thread that has gone back to waiting for its next task
    private static boolean isIdle(Thread thread) {
        return (thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING)
               && Arrays.stream(thread.getStackTrace()).noneMatch(frame -> frame.getClassName().startsWith("org.occurrent"));
    }

    private static Thread requireThread(AtomicReference<@Nullable Thread> thread) {
        Thread value = thread.get();
        if (value == null) {
            throw new IllegalStateException("No thread recorded");
        }
        return value;
    }

    // The cancel disposes the subscription, which can interrupt the thread held here
    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try {
            while (latch.getCount() > 0 && System.nanoTime() < deadline) {
                try {
                    latch.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
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

    private static @Nullable String storedPosition(CheckpointStorage storage) {
        return storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT);
    }

    private static void waitFor(Duration duration) {
        await().pollDelay(duration).atMost(duration.plusSeconds(5)).until(() -> true);
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
        private final CountDownLatch heldSaveApplied = new CountDownLatch(1);
        // Set, a delete waits for it and is then applied whether or not anyone still waits for it
        private volatile @Nullable CountDownLatch releaseHeldDelete;
        private final CompletableFuture<Void> heldDeleteApplied = new CompletableFuture<>();
        // The next read answers what the storage held when it arrived, and only once releaseHeldRead is released
        private volatile boolean holdNextRead = false;
        private final CountDownLatch heldReadEntered = new CountDownLatch(1);
        private final CountDownLatch releaseHeldRead = new CountDownLatch(1);
        // Set, a read of the id a waits for it on the thread that reads, before the read is even handed back
        private volatile @Nullable CountDownLatch releaseReadOfA;
        private final CountDownLatch readOfAEntered = new CountDownLatch(1);

        private PositionStorage() {
            this(new InMemoryCheckpointStorage());
        }

        private PositionStorage(CheckpointStorage backing) {
            this.backing = backing;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            CountDownLatch releaseRead = releaseReadOfA;
            if (releaseRead != null && subscriptionId.equals("a")) {
                readOfAEntered.countDown();
                awaitLatch(releaseRead);
            }
            if (!holdNextRead) {
                return backing.read(subscriptionId);
            }
            holdNextRead = false;
            // What the storage holds when the read arrives, handed back only once the test releases it
            return backing.read(subscriptionId).map(Optional::of).defaultIfEmpty(Optional.empty())
                    .map(stored -> {
                        heldReadEntered.countDown();
                        awaitLatch(releaseHeldRead);
                        return stored;
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(Mono::justOrEmpty);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            Checkpoint held = heldSave;
            if (held != null && held.asString().equals(checkpoint.asString())) {
                CompletableFuture<Checkpoint> saved = CompletableFuture.supplyAsync(() -> {
                    heldSaveEntered.countDown();
                    awaitLatch(releaseHeldSave);
                    Checkpoint written = backing.save(subscriptionId, checkpoint, writeCondition).block(TIMEOUT);
                    heldSaveApplied.countDown();
                    return written;
                });
                return Mono.fromFuture(saved, true);
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
            CountDownLatch release = releaseHeldDelete;
            if (release != null) {
                CompletableFuture<Void> deleted = CompletableFuture.runAsync(() -> {
                    awaitLatch(release);
                    backing.delete(subscriptionId).block(TIMEOUT);
                    heldDeleteApplied.complete(null);
                });
                return Mono.fromFuture(deleted, true);
            }
            return Mono.defer(() -> beforeDelete.then(Mono.defer(() -> backing.delete(subscriptionId))));
        }
    }
}
