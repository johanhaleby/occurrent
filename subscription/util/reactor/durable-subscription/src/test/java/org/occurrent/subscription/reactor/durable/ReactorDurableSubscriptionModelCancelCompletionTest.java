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
import org.junit.jupiter.params.provider.CsvSource;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

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
    private static final StringBasedCheckpoint WHERE_THE_FEED_IS_ONCE_THE_ID_IS_TAKEN = new StringBasedCheckpoint("where-the-feed-is-once-the-id-is-taken");
    private static final Duration HELD_BY_THE_STORAGE = Duration.ofMillis(500);
    private static final String DELETE_FAILED = "The storage cannot delete right now";

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
     * on its way to the storage. The subscription starts at once from the subscription-model default, which reads the
     * checkpoint the storage still holds, so it resumes from where the cancelled subscription was, as it would without
     * the cancel. Waiting for what the cancel returns is what gives a clean start, as the test above shows.
     */
    @Test
    void a_subscribe_that_did_not_wait_for_the_cancel_resumes_from_the_checkpoint_the_storage_still_holds() {
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
        assertThat(feed.startedAt.get(1)).as("start position of the subscription made without waiting for the cancel").hasToString(REACHED_BEFORE_THE_CANCEL.asString());
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
     * The wrapped model answers its cancel with a Mono that does nothing until it is subscribed to, as a Mono built
     * with {@code Mono.fromRunnable(..)} does. The caller ignores what the cancel returns, so the model subscribes to
     * that Mono itself, once, however often the caller then subscribes to what it got back.
     */
    @Test
    void cancels_in_the_wrapped_model_whose_cancel_takes_effect_only_once_subscribed_whether_or_not_the_caller_subscribes() {
        // Given
        AtomicInteger cancelsTakenEffect = new AtomicInteger();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        wrapped.cancelled = Mono.fromRunnable(cancelsTakenEffect::incrementAndGet);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        subscribe(model);

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);

        // Then
        assertThat(cancelsTakenEffect).as("cancels that took effect in the wrapped model before anything subscribed to the returned Mono").hasValue(1);
        cancelled.block(TIMEOUT);
        cancelled.block(TIMEOUT);
        assertThat(cancelsTakenEffect).as("cancels that took effect in the wrapped model once the caller subscribed twice").hasValue(1);
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
     * id yet. The cancel began after the subscribe did, so it ends that subscribe as it ends one that has started.
     */
    @Test
    void a_subscribe_still_reading_its_start_position_when_a_cancel_of_the_id_runs_is_ended_by_that_cancel() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        storage.holdNextRead = true;
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
        assertThat(storage.heldReadEntered.await(5, TimeUnit.SECONDS)).as("the subscribe is reading its start position").isTrue();

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            storage.releaseHeldRead.countDown();
            Subscription subscription = subscribed.get(5, TimeUnit.SECONDS);

            // Then
            assertThat(catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT))).as("outcome of the subscribe the cancel overtook").isInstanceOf(CancellationException.class);
            assertThat(wrapped.subscribedIds).as("subscriptions handed to the wrapped model").isEmpty();
            assertThat(hasPosition(storage)).as("position stored after the cancel completed").isFalse();
        } finally {
            storage.releaseHeldRead.countDown();
        }
    }

    /**
     * The subscribe reads the position of the subscription being cancelled before the delete reaches the storage, and
     * the read answers after the cancel completed. The subscribe is ended, so that position is never where a
     * subscription of the id starts.
     */
    @Test
    void a_subscribe_whose_start_position_read_overlaps_a_cancel_is_ended_by_it_and_stores_nothing() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        storage.holdNextRead = true;
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
        assertThat(storage.heldReadEntered.await(5, TimeUnit.SECONDS)).as("the second subscribe is reading its start position").isTrue();

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            storage.releaseHeldRead.countDown();
            Subscription subscription = subscribed.get(5, TimeUnit.SECONDS);

            // Then
            assertThat(catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT))).as("outcome of the subscribe whose read overlapped the cancel").isInstanceOf(CancellationException.class);
            assertThat(wrapped.startedAt).as("start positions the wrapped model was handed").extracting(Object::toString).containsExactly(REACHED_BEFORE_THE_CANCEL.asString());
            assertThat(hasPosition(storage)).as("position stored after the cancel completed").isFalse();
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
     * The cancel has taken the subscription out of this model, and the storage has not deleted the checkpoint yet, when
     * the subscribe of the same id arrives. The subscribe reads that checkpoint and the subscription resumes from it,
     * as it would without the cancel.
     */
    @Test
    void a_subscribe_arriving_while_the_cancel_is_still_under_way_resumes_from_the_checkpoint_the_storage_still_holds() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);

        // When
        @Nullable Throwable subscribeFailure = subscribeWhileTheCancelIsHeld(model, publisher -> publisher.getClass().getSimpleName().equals("MonoWhen"));

        // Then
        assertThat(subscribeFailure).as("failure of the subscribe that arrived during the cancel").isNull();
        assertThat(wrapped.startedAt.get(1)).as("start position of the subscribe that arrived during the cancel").hasToString(REACHED_BEFORE_THE_CANCEL.asString());
    }

    /**
     * The subscribe finds the delete of the cancel before the cancel has started it, and the storage deletes and saves
     * on the thread that asks it to. Nothing is stored for the id, so the subscription starts from where the feed is,
     * and records that start position once the delete has ended.
     */
    @Test
    void a_subscribe_that_finds_a_delete_the_cancel_has_not_started_yet_records_its_own_start_position_once_it_has_ended() throws Exception {
        // Given
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);

        // When
        @Nullable Throwable subscribeFailure = subscribeWhileTheCancelIsHeld(model, __ -> writesOfTheIdWaitForADelete(model));

        // Then
        assertThat(subscribeFailure).as("failure of the subscribe that found the delete").isNull();
        assertThat(wrapped.startedAt.get(0)).as("start position of the subscribe that found the delete").hasToString(WHERE_THE_FEED_IS_NOW);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT)).as("position stored for the subscribe that found the delete").isEqualTo(WHERE_THE_FEED_IS_NOW));
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
            // The storage still held the checkpoint of the cancelled subscription at the subscribe, so the default
            // start resumes from it
            assertThat(feed.startedAt.get(1)).as("start position of the subscription made without waiting for the cancel").hasToString(REACHED_BEFORE_THE_CANCEL.asString());
            assertThat(cancelCompletedWhileTheDeleteWasHeld).as("cancel reported as complete while its delete was held").isFalse();
            cancelled.get(5, TimeUnit.SECONDS);
        } finally {
            releaseDelete.countDown();
        }
    }

    /**
     * The rebuild is subscribed without waiting for the cancel, with a start position that reads the stored position
     * itself and replays from the beginning when none is stored, the way the Spring Boot starter's BEGINNING start with
     * the default resume behaviour does. Its function runs at the subscribe, before the storage has deleted the
     * checkpoint of the cancelled subscription, so the rebuild resumes from that checkpoint instead of replaying, as it
     * would without the cancel.
     */
    @Test
    void a_rebuild_that_did_not_wait_for_the_cancel_and_reads_the_stored_position_itself_resumes_from_the_checkpoint_the_storage_still_holds() {
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
        // The storage still held the checkpoint of the cancelled subscription when the start position read it
        assertThat(feed.startedAt.get(1)).as("start position of the rebuild subscribed right after the cancel").hasToString(REACHED_BEFORE_THE_CANCEL.asString());
    }

    /**
     * As above, for a DCB subscription.
     */
    @Test
    void a_dcb_rebuild_that_did_not_wait_for_the_cancel_and_reads_the_stored_position_itself_resumes_from_the_checkpoint_the_storage_still_holds() {
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
        // The storage still held the checkpoint of the cancelled subscription when the start position read it
        assertThat(feed.startedAt.get(1)).as("start position of the DCB rebuild subscribed right after the cancel").hasToString(REACHED_BEFORE_THE_CANCEL.asString());
    }

    /**
     * As above, on a wrapped model that manages named subscriptions of its own.
     */
    @Test
    void a_rebuild_on_a_named_model_that_did_not_wait_for_the_cancel_and_reads_the_stored_position_itself_resumes_from_the_checkpoint_the_storage_still_holds() {
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
        // The storage still held the checkpoint of the cancelled subscription when the start position read it
        assertThat(wrapped.startedAt.get(1)).as("start position of the rebuild subscribed right after the cancel").hasToString(REACHED_BEFORE_THE_CANCEL.asString());
    }

    /**
     * The storage fails the delete of the cancel a few times before it succeeds, as during a short outage. After a
     * failed delete the checkpoint of the cancelled subscription is still stored, so the model tries the delete again
     * until it succeeds, and the cancel completes then. A subscribe of the id made meanwhile does not wait for the
     * delete. Its start position reads the checkpoint the storage still holds, so it resumes from there, a rebuild
     * whose start position reads the stored position itself included, as it would without the cancel.
     */
    @ParameterizedTest
    @CsvSource({"false, rebuild", "false, default", "true, rebuild", "true, default"})
    void a_subscribe_made_while_the_delete_of_a_cancel_is_tried_again_resumes_from_the_checkpoint_the_storage_still_holds(boolean handsOver, String startPosition) {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        RecordingSubscriptionModel feed = handsOver ? wrapped.feed : new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        List<StartAt> startedAt = handsOver ? wrapped.startedAt : feed.startedAt;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(handsOver ? wrapped : feed, storage);
        runningFromAStoredPosition(model, storage);
        storage.deleteFailures.set(3);

        try {
            // When
            Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
            model.subscribe(SUBSCRIPTION_ID, null, startPosition.equals("rebuild") ? replayThenResume(storage) : StartAt.subscriptionModelDefault(), __ -> Mono.empty());
            Throwable cancelEnded = catchThrowable(() -> cancelled.block(TIMEOUT));

            // Then
            await().atMost(TIMEOUT).until(() -> startedAt.size() == 2);
            assertThat(resolved(startedAt.get(1))).as("start position of the subscription made right after the cancel")
                    .hasToString(REACHED_BEFORE_THE_CANCEL.asString());
            assertThat(cancelEnded).as("how the cancel ended").isNull();
            assertThat(storage.deleteAttempts).as("deletes the storage was asked for").hasValue(4);
        } finally {
            model.shutdown();
        }
    }

    /**
     * The storage fails every delete, as during an outage that outlasts the process. The model keeps trying until it
     * is shut down, so the cancel ends neither way before then, and once the shutdown stopped the tries it fails with
     * what the storage answered last.
     */
    @Test
    void a_shutdown_stops_the_tries_of_a_delete_that_keeps_failing_and_the_cancel_then_fails_with_the_last_error() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        storage.deleteFailures.set(Integer.MAX_VALUE);
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 3 || cancelled.isDone());
        boolean cancelEndedBeforeTheShutdown = cancelled.isDone();

        // When
        model.shutdown();

        // Then
        assertThat(cancelEndedBeforeTheShutdown).as("whether the cancel ended while the delete was being tried again").isFalse();
        assertThat(catchThrowable(() -> cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))).as("how the cancel ended once the model shut down")
                .hasRootCauseMessage(DELETE_FAILED);
    }

    /**
     * The storage fails every delete, and the model is shut down while it waits to try again. The shutdown ends that
     * wait at once, so the storage is asked for no further delete and the cancel fails with what the storage answered
     * last. A second cancel of the id, whose delete is queued behind the first, makes no try either and fails with the
     * same error.
     */
    @Test
    void a_shutdown_while_a_failed_delete_waits_to_be_tried_again_asks_the_storage_for_no_further_delete() {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        runningFromAStoredPosition(model, storage);
        storage.deleteFailures.set(Integer.MAX_VALUE);
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        // After four failed tries the next one is due 400 milliseconds or more after the last
        await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(5)).until(() -> storage.deleteAttempts.get() >= 4);
        CompletableFuture<Void> cancelledAgain = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        int deletesAskedForBeforeTheShutdown = storage.deleteAttempts.get();

        // When
        model.shutdown();

        // Then
        assertThat(catchThrowable(() -> cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))).as("how the cancel ended once the model shut down")
                .hasRootCauseMessage(DELETE_FAILED);
        assertThat(catchThrowable(() -> cancelledAgain.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))).as("how the cancel queued behind it ended once the model shut down")
                .hasRootCauseMessage(DELETE_FAILED);
        assertThat(storage.deleteAttempts).as("deletes the storage was asked for, once both cancels had ended").hasValue(deletesAskedForBeforeTheShutdown);
    }

    /**
     * A rebuild with a start position that reads the stored position itself is subscribed while the storage still holds
     * the delete of a cancel of the id. Its function runs where it would without the delete, which is at the subscribe
     * when it is handed to a wrapped model that is stopped, and at the start of this model when the subscribe came
     * while this model was stopped. Neither waits for the delete, and the function reads the checkpoint of the
     * cancelled subscription, which the storage still holds, so the rebuild resumes from it.
     */
    @ParameterizedTest
    @CsvSource({"true, stopped", "false, started-once-subscribed"})
    void a_rebuild_subscribed_while_the_delete_of_a_cancel_runs_resumes_from_the_checkpoint_the_storage_still_holds_without_waiting_for_the_delete(boolean handsOver, String where) {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        RecordingSubscriptionModel feed = handsOver ? wrapped.feed : new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        List<StartAt> startedAt = handsOver ? wrapped.startedAt : feed.startedAt;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(handsOver ? wrapped : feed, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID);
            if (where.equals("stopped")) {
                wrapped.running = false;
            } else {
                model.stop();
            }
            model.subscribe(SUBSCRIPTION_ID, null, replayThenResume(storage), __ -> Mono.empty());
            if (where.equals("started-once-subscribed")) {
                model.start(true);
            }

            // Then
            await().atMost(TIMEOUT).until(() -> startedAt.size() == 2);
            assertThat(storage.heldDeleteApplied).as("whether the storage had applied the delete when the rebuild started").isNotDone();
            assertThat(resolved(startedAt.get(1))).as("start position of the rebuild subscribed while the delete ran")
                    .hasToString(REACHED_BEFORE_THE_CANCEL.asString());
        } finally {
            releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * As above, subscribed on a thread where Reactor does not allow blocking, a WebFlux request thread for example.
     * The function runs at the subscribe and reads the stored position with a blocking call, which Reactor refuses on
     * such a thread, so the subscribe throws, as it does without the delete.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_rebuild_subscribed_on_a_thread_that_may_not_block_while_the_delete_of_a_cancel_runs_is_refused_there(boolean handsOver) {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        RecordingSubscriptionModel feed = handsOver ? wrapped.feed : new RecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        List<StartAt> startedAt = handsOver ? wrapped.startedAt : feed.startedAt;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(handsOver ? wrapped : feed, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            // When
            model.cancelSubscription(SUBSCRIPTION_ID);
            Throwable thrown = catchThrowable(() -> Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, replayThenResume(storage), __ -> Mono.empty()))
                    .subscribeOn(Schedulers.parallel())
                    .block(TIMEOUT));

            // Then
            assertThat(thrown).as("how the subscribe on a thread that may not block ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("is blocking");
            assertThat(startedAt).as("subscriptions started").hasSize(1);
        } finally {
            releaseDelete.countDown();
            model.shutdown();
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
     * The wrapped model is taking the subscribe of an id when a cancel of the same id runs, so the cancel can reach the
     * wrapped model before the subscription the subscribe makes there. The cancel began after the subscribe did, so it
     * ends that subscribe, and the subscription that model made is cancelled there. Starting it again after the delete
     * would read the feed position later than the subscribe returned and skip what was written in between.
     */
    @Test
    void a_subscribe_the_wrapped_model_takes_while_a_cancel_of_the_same_id_runs_is_ended_by_that_cancel() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        storage.save(SUBSCRIPTION_ID, REACHED_BEFORE_THE_CANCEL).block(TIMEOUT);
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        wrapped.beforeSubscribe = __ -> {
            if (subscribeEntered.getCount() > 0) {
                subscribeEntered.countDown();
                awaitLatch(releaseSubscribe);
            }
        };
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), event -> Mono.fromRunnable(() -> handled.add(event))));
        assertThat(subscribeEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is taking the subscribe").isTrue();

        try {
            // When
            CompletableFuture<Mono<Void>> cancelling = CompletableFuture.supplyAsync(() -> model.cancelSubscription(SUBSCRIPTION_ID));

            // Then
            assertThat(catchThrowable(() -> cancelling.get(5, TimeUnit.SECONDS))).as("failure of the cancel call while the wrapped model takes the subscribe").isNull();
            CompletableFuture<Void> cancelCompleted = cancelling.get().toFuture();
            await().atMost(TIMEOUT).until(() -> !hasPosition(storage));
            assertThat(cancelCompleted).as("cancel completed while the wrapped model still takes the subscribe").isNotDone();
            releaseSubscribe.countDown();
            cancelCompleted.get(5, TimeUnit.SECONDS);
            Subscription subscription = subscribed.get(5, TimeUnit.SECONDS);
            assertThat(catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT))).as("outcome of the subscribe the cancel overtook").isInstanceOf(CancellationException.class);
            assertThat(wrapped.calls).as("calls the wrapped model received, the cancel the subscribe made itself included")
                    .containsExactly("subscribe sub began", "cancel sub", "subscribe sub returned", "cancel sub");
            wrapped.actions.get(0).apply(eventAt(HANDLED_BY_THE_NEW_SUBSCRIPTION)).block(TIMEOUT);
            assertThat(handled).as("events the caller's action handled after the cancel completed").isEmpty();
            assertThat(hasPosition(storage)).as("position stored after the cancel completed").isFalse();
        } finally {
            releaseSubscribe.countDown();
        }
    }

    /**
     * The cancel reaches the wrapped model before the subscription the subscribe makes there, so that cancel finds
     * nothing, and the cancel the subscribe sends once that model has taken it fails. The subscription can then still be
     * in that model, so the cancel cannot report it gone and the shutdown logs it. The caller's action does not run for
     * what that model delivers to it after that.
     */
    @ParameterizedTest
    @EnumSource(value = EndedBy.class, names = {"CANCEL", "SHUTDOWN"})
    void a_cancel_or_a_shutdown_whose_cancel_in_the_wrapped_model_fails_after_that_model_took_the_subscribe_reports_it_and_runs_no_action(EndedBy endedBy) throws Exception {
        // Given
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, new InMemoryCheckpointStorage());
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        wrapped.beforeSubscribe = __ -> {
            subscribeEntered.countDown();
            awaitLatch(releaseSubscribe);
        };
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(BEGINNING), event -> Mono.fromRunnable(() -> handled.add(event))));
        assertThat(subscribeEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is taking the subscribe").isTrue();
        IllegalStateException cancelFailure = new IllegalStateException("the wrapped model could not cancel the subscription");

        try {
            // When
            final Mono<Void> cancelled;
            if (endedBy == EndedBy.CANCEL) {
                cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
                wrapped.cancelled = Mono.error(cancelFailure);
            } else {
                wrapped.cancelled = Mono.error(cancelFailure);
                model.shutdown();
                cancelled = Mono.empty();
            }
            releaseSubscribe.countDown();
            subscribed.handle((subscription, throwable) -> null).get(5, TimeUnit.SECONDS);

            // Then
            await().atMost(TIMEOUT).until(() -> wrapped.calls.contains("subscribe sub returned") && wrapped.cancelledIds.size() == (endedBy == EndedBy.CANCEL ? 2 : 1));
            if (endedBy == EndedBy.CANCEL) {
                assertThat(catchThrowable(() -> cancelled.block(TIMEOUT))).as("outcome of the cancel whose cancel in the wrapped model failed").isSameAs(cancelFailure);
            }
            wrapped.actions.get(0).apply(eventAt(HANDLED_BY_THE_NEW_SUBSCRIPTION)).block(TIMEOUT);
            assertThat(handled).as("events the caller's action handled for the subscription that model kept").isEmpty();
        } finally {
            releaseSubscribe.countDown();
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
     * The caller cancels the id without waiting, subscribes it again, and then cancels the id once more before the
     * first delete has ended and waits for that cancel. The second subscription starts at the subscribe, and the second
     * cancel ends it in the wrapped model, so an event that model still delivers to it afterwards stores no position.
     */
    @Test
    void a_cancel_ends_a_subscription_made_while_the_delete_of_an_earlier_cancel_runs_and_leaves_no_position_of_it_stored() {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        runningFromAStoredPosition(model, storage);
        CountDownLatch releaseDelete = new CountDownLatch(1);
        storage.releaseHeldDelete = releaseDelete;

        try {
            model.cancelSubscription(SUBSCRIPTION_ID);
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty());

            // When
            Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
            releaseDelete.countDown();
            cancelled.block(TIMEOUT);

            // Then
            assertThat(wrapped.calls).as("calls the wrapped model received")
                    .containsExactly("subscribe sub began", "subscribe sub returned", "cancel sub", "subscribe sub began", "subscribe sub returned", "cancel sub");
            // What the wrapped model was handed last, run the way it would deliver an event
            wrapped.actions.get(wrapped.actions.size() - 1).apply(eventAt(REACHED_BY_THE_CANCELLED_SUBSCRIPTION)).block(TIMEOUT);
            assertThat(storedPosition(storage)).as("position stored once the cancel completed").isNull();
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
     * A subscribe on a stopped model has taken the id and is held before it returns. The feed moves on, and start(true)
     * takes the subscription over and starts it while the subscribe is still held.
     */
    @Test
    void a_subscription_that_start_takes_over_before_its_subscribe_returned_starts_from_where_the_feed_was_at_registration_when_this_model_drives_the_feed() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_WAS_AT_REGISTRATION);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.stop();
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        Thread subscriber = new Thread(() -> subscribe(model));
        holdOnceItHasTakenTheId(model, subscriber, held, releaseSubscribe);

        try {
            subscriber.start();
            assertThat(held.await(5, TimeUnit.SECONDS)).as("the subscribe is held once it has taken the id").isTrue();

            // When
            feed.globalCheckpoint = WHERE_THE_FEED_IS_ONCE_THE_ID_IS_TAKEN;
            model.start(true);
            releaseSubscribe.countDown();
            subscriber.join(TIMEOUT.toMillis());

            // Then
            await().atMost(TIMEOUT).until(() -> !feed.startedAt.isEmpty());
            assertThat(feed.startedAt).as("start positions the feed was subscribed from").map(StartAt::toString)
                    .containsExactly(WHERE_THE_FEED_WAS_AT_REGISTRATION);
        } finally {
            releaseSubscribe.countDown();
            Hooks.resetOnEachOperator(PAUSE_HOOK);
        }
    }

    /**
     * A subscribe on a running model has taken the id and is held before it starts the subscription when the model is
     * stopped, or the subscription paused. The feed moves on before the model is started again, or the subscription
     * resumed.
     */
    @ParameterizedTest
    @EnumSource
    void a_subscription_set_aside_before_its_subscribe_started_it_starts_from_where_the_feed_was_at_registration_when_this_model_drives_the_feed(SetAsideBy setAsideBy) throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        RecordingSubscriptionModel feed = new RecordingSubscriptionModel(WHERE_THE_FEED_WAS_AT_REGISTRATION);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        Thread subscriber = new Thread(() -> subscribe(model));
        holdOnceItHasTakenTheId(model, subscriber, held, releaseSubscribe);

        try {
            subscriber.start();
            assertThat(held.await(5, TimeUnit.SECONDS)).as("the subscribe is held once it has taken the id").isTrue();

            // When
            setAsideBy.setAside(model);
            releaseSubscribe.countDown();
            subscriber.join(TIMEOUT.toMillis());
            assertThat(feed.startedAt).as("start positions the feed was subscribed from before the subscription was taken up again").isEmpty();
            feed.globalCheckpoint = WHERE_THE_FEED_IS_ONCE_THE_ID_IS_TAKEN;
            setAsideBy.takeUpAgain(model);

            // Then
            await().atMost(TIMEOUT).until(() -> !feed.startedAt.isEmpty());
            assertThat(feed.startedAt).as("start positions the feed was subscribed from").map(StartAt::toString)
                    .containsExactly(WHERE_THE_FEED_WAS_AT_REGISTRATION);
        } finally {
            releaseSubscribe.countDown();
            Hooks.resetOnEachOperator(PAUSE_HOOK);
        }
    }

    /**
     * The wrapped model takes the subscribe of an id and does not return from it when the model is shut down.
     */
    @Test
    void a_shutdown_returns_and_shuts_the_wrapped_model_down_while_that_model_takes_a_subscribe_that_does_not_return() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        CountDownLatch subscribeEntered = new CountDownLatch(1);
        CountDownLatch releaseSubscribe = new CountDownLatch(1);
        wrapped.beforeSubscribe = __ -> {
            subscribeEntered.countDown();
            awaitLatch(releaseSubscribe);
        };
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(BEGINNING), __ -> Mono.empty()));
        assertThat(subscribeEntered.await(5, TimeUnit.SECONDS)).as("the wrapped model is taking the subscribe").isTrue();
        Thread shuttingDown = new Thread(model::shutdown);
        shuttingDown.setDaemon(true);

        try {
            // When
            shuttingDown.start();
            shuttingDown.join(TIMEOUT.toMillis());

            // Then
            assertThat(shuttingDown.isAlive()).as("still shutting down while the wrapped model takes the subscribe").isFalse();
            assertThat(wrapped.calls).as("calls the wrapped model received while it took the subscribe").containsExactly("subscribe sub began", "shutdown");
            releaseSubscribe.countDown();
            assertThat(catchThrowable(() -> subscribed.get(5, TimeUnit.SECONDS))).as("how the subscribe the shutdown overtook ended")
                    .hasCauseInstanceOf(SubscriptionModelShutdownException.class);
            assertThat(wrapped.calls).as("calls the wrapped model received once it had taken the subscribe")
                    .containsExactly("subscribe sub began", "shutdown", "subscribe sub returned", "cancel sub");
            // What the wrapped model was handed, run the way it would deliver an event
            wrapped.actions.get(0).apply(eventAt(DELIVERED_AFTER_THE_SHUTDOWN)).block(TIMEOUT);
            assertThat(storedPosition(storage)).as("position stored once the subscription the shutdown overtook handled an event").isNull();
        } finally {
            releaseSubscribe.countDown();
        }
    }

    /**
     * Two subscribes hand their ids to a wrapped model that calls the dynamic start position it is handed while it
     * takes the subscribe, as the catch-up models do when this model passes one through, and the start position of
     * each cancels the other id there.
     */
    @Test
    void subscribes_whose_start_positions_cancel_each_others_id_while_the_wrapped_model_takes_them_both_return() throws Exception {
        // Given
        PositionStorage storage = new PositionStorage();
        NamedRecordingSubscriptionModel wrapped = new NamedRecordingSubscriptionModel(WHERE_THE_FEED_IS_NOW);
        wrapped.resolvesDynamicStartPositions = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(wrapped, storage);
        CountDownLatch bothBeingTaken = new CountDownLatch(2);
        Thread a = new Thread(() -> model.subscribe("a", null, cancelsWhileTaken(model, "b", bothBeingTaken), __ -> Mono.empty()));
        Thread b = new Thread(() -> model.subscribe("b", null, cancelsWhileTaken(model, "a", bothBeingTaken), __ -> Mono.empty()));
        a.setDaemon(true);
        b.setDaemon(true);

        // When
        a.start();
        b.start();
        a.join(TIMEOUT.toMillis());
        b.join(TIMEOUT.toMillis());

        // Then
        assertThat(List.of(a.isAlive(), b.isAlive())).as("subscribes of a and b still running").containsExactly(false, false);
        assertThat(wrapped.cancelledIds).as("ids the wrapped model was asked to cancel").contains("a", "b");
    }

    /**
     * A subscribe made while the delete of an earlier cancel runs is resolving its dynamic start position on the thread
     * that subscribes when the caller cancels the id again, and the function answers, or throws, only once that cancel
     * has returned. This model does not wait for a function it did not write, so the function runs to its end, and
     * what it answers starts nothing. What it throws reaches the caller of the subscribe, as it does without the
     * cancel.
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
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                functionEntered.countDown();
                awaitUninterruptibly(releaseFunction);
                functionEnded.countDown();
                if (functionThrows) {
                    throw new IllegalStateException("Answered once the cancel had returned");
                }
                return StartAt.checkpoint(BEGINNING);
            }), __ -> Mono.empty()));
            assertThat(functionEntered.await(5, TimeUnit.SECONDS)).as("the subscribe resolves its start position").isTrue();

            // When
            Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
            releaseFunction.countDown();
            cancelled.block(TIMEOUT);

            // Then
            assertThat(functionEnded.await(5, TimeUnit.SECONDS)).as("the function ran to its end after the cancel returned").isTrue();
            Throwable outcome = catchThrowable(() -> subscribed.get(5, TimeUnit.SECONDS).waitUntilStarted().block(TIMEOUT));
            if (functionThrows) {
                assertThat(outcome).as("how the cancelled subscribe ended").hasRootCauseMessage("Answered once the cancel had returned");
            } else {
                assertThat(outcome).as("how waiting for the start of the cancelled subscribe ended").isInstanceOf(CancellationException.class);
            }
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

    private enum SetAsideBy {
        STOP {
            @Override
            void setAside(ReactorDurableSubscriptionModel model) {
                model.stop();
            }

            @Override
            void takeUpAgain(ReactorDurableSubscriptionModel model) {
                model.start(true);
            }
        },
        PAUSE {
            @Override
            void setAside(ReactorDurableSubscriptionModel model) {
                model.pauseSubscription(SUBSCRIPTION_ID);
            }

            @Override
            void takeUpAgain(ReactorDurableSubscriptionModel model) {
                model.resumeSubscription(SUBSCRIPTION_ID);
            }
        };

        abstract void setAside(ReactorDurableSubscriptionModel model);

        abstract void takeUpAgain(ReactorDurableSubscriptionModel model);
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

    // What a dynamic start position handed to the wrapped model answers when that model resolves it
    private static @Nullable StartAt resolved(StartAt startAt) {
        StartAt resolved = startAt;
        while (resolved != null && resolved.isDynamic()) {
            resolved = resolved.get(new StartAt.SubscriptionModelContext(ReactorDurableSubscriptionModelCancelCompletionTest.class));
        }
        return resolved;
    }

    private static StartAt replayThenResume(CheckpointStorage storage) {
        return ResumeStartPositions.replayThenResume(SUBSCRIPTION_ID, storage, StartAt.checkpoint(BEGINNING));
    }

    // Holds the subscribe on that thread at the first operator it assembles once it has taken the id and released the
    // monitor, which is before it starts the subscription or returns
    private static void holdOnceItHasTakenTheId(ReactorDurableSubscriptionModel model, Thread subscriber, CountDownLatch held, CountDownLatch release) {
        Hooks.onEachOperator(PAUSE_HOOK, publisher -> {
            if (Thread.currentThread() == subscriber && held.getCount() > 0 && !Thread.holdsLock(model)
                && (model.isRunning(SUBSCRIPTION_ID) || model.isPaused(SUBSCRIPTION_ID))) {
                held.countDown();
                awaitUninterruptibly(release);
            }
            return publisher;
        });
    }

    // Answers nothing when this model asks, so this model hands the function to the wrapped model. Cancels the other id
    // when the wrapped model asks while it takes the subscribe, once the subscribes of both ids are being taken.
    private static StartAt cancelsWhileTaken(ReactorDurableSubscriptionModel model, String otherId, CountDownLatch bothBeingTaken) {
        AtomicInteger asked = new AtomicInteger();
        return StartAt.dynamic(() -> {
            int call = asked.incrementAndGet();
            if (call == 1) {
                return null;
            }
            if (call == 2) {
                bothBeingTaken.countDown();
                awaitLatch(bothBeingTaken);
                model.cancelSubscription(otherId);
            }
            return StartAt.now();
        });
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

    // Asked on the thread that cancels. True once position writes of the id wait for a delete and that thread no
    // longer holds the lock they take to find it.
    private static boolean writesOfTheIdWaitForADelete(ReactorDurableSubscriptionModel model) {
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
        // How many deletes fail before one is let through, and how many the storage was asked for
        private final AtomicInteger deleteFailures = new AtomicInteger();
        private final AtomicInteger deleteAttempts = new AtomicInteger();
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
            deleteAttempts.incrementAndGet();
            if (deleteFailures.getAndUpdate(failures -> Math.max(0, failures - 1)) > 0) {
                return Mono.error(new IllegalStateException(DELETE_FAILED));
            }
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
