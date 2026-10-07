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

import ch.qos.logback.classic.Level;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.LongStream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscribe or a resume that takes over the delete a cancel of the same id started starts where a subscribe with no
 * delete running would, and skips no event written after it returned.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelDeleteTakenOverTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SUBSCRIPTION_ID = "sub";
    // A position after every event a test writes, so no event stores it
    private static final String QUIET_POSITION = "1000";
    private static final String SAVE_FAILED = "The storage cannot save right now";
    private static final String READ_FAILED = "The storage cannot read right now";
    private static final String DELETE_FAILED = "The storage lost the answer to the delete";
    private static final String START_FAILED = "The wrapped model could not start the subscription";

    /**
     * The subscription from the model default starts where it would with no delete running, and the subscribe returns
     * while the delete or the position write it took over is still held. It starts from the checkpoint the cancelled
     * subscription stored or was writing, or from where the feed was at the subscribe when there is none, so it skips
     * no event written after the subscribe.
     * <ul>
     *     <li>nothing-stored: storage holds nothing when the delete tries, and the try is held</li>
     *     <li>deleted-before: an earlier cancel deleted the checkpoint, and the try of a second cancel is held. On a
     *     storage that evaluates a condition on a delete, that try reads nothing and deletes nothing, so nothing is
     *     held</li>
     *     <li>write-in-flight-fails: the first position write of the cancelled subscription is held and then fails, so
     *     the delete waits for it, and an event is written after the cancel</li>
     *     <li>write-in-flight-arrives: the same, except that the write reaches the store</li>
     * </ul>
     */
    @ParameterizedTest
    @CsvSource({
            "false, nothing-stored, false", "false, nothing-stored, true",
            "false, deleted-before, false", "false, deleted-before, true",
            "true, deleted-before, false", "true, deleted-before, true",
            "true, write-in-flight-fails, false", "true, write-in-flight-fails, true",
            "false, write-in-flight-arrives, false", "false, write-in-flight-arrives, true",
            "true, write-in-flight-arrives, false", "true, write-in-flight-arrives, true"})
    void a_subscribe_that_takes_over_a_delete_starts_where_it_would_with_no_delete_running(boolean conditionalDeletes, String held, boolean mayBlock) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, held, release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            long startsAfter = heldCall.startsAfter() < 0 ? feed.present.get() : heldCall.startsAfter();

            // When
            CompletableFuture<Subscription> subscribed = mayBlock
                    ? CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller)
                    : CompletableFuture.completedFuture(Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)))
                    .subscribeOn(Schedulers.parallel()).block(TIMEOUT));
            Throwable returnedWhileHeld = catchThrowable(() -> subscribed.get(2, TimeUnit.SECONDS));
            List<Long> writtenWhileHeld = List.of(feed.write(), feed.write());
            release.countDown();
            long writtenAfter = feed.write();

            // Then
            subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            untilDelivered(delivered, writtenAfter);
            List<Long> expected = positionsAfter(startsAfter, writtenAfter);
            assertThat(expected).containsAll(writtenWhileHeld);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(returnedWhileHeld).as("the subscribe returning while the storage call it took over is held").isNull();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(expected);
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A resume that comes once a delete removed the checkpoint and before the write back of the subscribe that took it
     * over reached the store takes the delete over too. A write back that fails is tried again, with a warning, so the
     * resume starts from the checkpoint written back either way and skips no event.
     */
    @ParameterizedTest
    @ValueSource(strings = {"arrives", "fails once"})
    void a_resume_while_a_write_back_is_held_takes_the_delete_over(String writeBack) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch releaseDelete = new CountDownLatch(1);
        CountDownLatch releaseWriteBack = new CountDownLatch(1);
        LoggedByTheModel logged = new LoggedByTheModel();
        try {
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
            long stored = 0;
            for (int i = 0; i < 5; i++) {
                stored = feed.write();
            }
            String lastStored = String.valueOf(stored);
            await().atMost(TIMEOUT).until(() -> lastStored.equals(storage.stored()));
            storage.deleteGate = releaseDelete;
            model.cancelSubscription(SUBSCRIPTION_ID);
            assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();
            onParallel(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)));
            long writtenBeforeTheWriteBack = feed.write();
            storage.ifAbsentGate = releaseWriteBack;
            storage.ifAbsentFails = writeBack.equals("fails once");
            storage.deleteGate = null;
            releaseDelete.countDown();
            assertThat(storage.ifAbsentEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("write back held").isTrue();
            assertThat(storage.stored()).as("checkpoint stored while the write back is held").isEqualTo("-");
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            Subscription resumed = onParallel(() -> model.resumeSubscription(SUBSCRIPTION_ID));
            long writtenWhileHeld = feed.write();
            releaseWriteBack.countDown();
            long writtenAfter = feed.write();

            // Then
            Throwable refused = catchThrowable(() -> resumed.waitUntilStarted(TIMEOUT).block());
            untilDelivered(delivered, writtenAfter);
            assertThat(delivered).as("events delivered after the cancel").containsExactly(writtenBeforeTheWriteBack, writtenWhileHeld, writtenAfter);
            assertThat(refused).as("why the resume did not start").isNull();
            assertThat(logged.at(Level.WARN)).as("warnings about the write back")
                    .filteredOn(message -> message.startsWith("Could not write back the stored checkpoint of subscription " + SUBSCRIPTION_ID))
                    .hasSize(writeBack.equals("fails once") ? 1 : 0);
        } finally {
            logged.close();
            releaseDelete.countDown();
            releaseWriteBack.countDown();
            model.shutdown();
        }
    }

    /**
     * A try of the delete a cancel started removes the checkpoint, and the subscribe comes once the try has ended and
     * before the delete is retired, with no try under way. The delete still holds the checkpoint the try read, so the
     * subscribe takes it over, writes that checkpoint back and resumes from it, and skips no event written after the
     * cancel.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_after_a_try_removed_the_checkpoint_and_before_the_delete_is_retired_resumes_from_it(boolean conditionalDeletes) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService canceller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch beforeRetired = new CountDownLatch(1);
        CountDownLatch retire = new CountDownLatch(1);
        AtomicInteger heldAt = new AtomicInteger();
        model.runBeforeADeleteIsRetired(() -> {
            if (heldAt.getAndIncrement() == 0) {
                beforeRetired.countDown();
                awaitLatch(retire);
            }
        });
        try {
            long stored = storedBeforeTheCancel(model, storage, feed);
            // On a thread of its own, since the delete runs on the thread that cancels and is held there
            CompletableFuture<Void> cancelled = CompletableFuture.runAsync(() -> model.cancelSubscription(SUBSCRIPTION_ID), canceller);
            assertThat(beforeRetired.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held before it is retired").isTrue();
            String storedOnceTheTryEnded = storage.stored();
            feed.write();
            feed.write();

            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered));
            retire.countDown();
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            assertThat(storedOnceTheTryEnded).as("checkpoint stored once the try ended").isEqualTo("-");
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(stored, writtenAfter));
            cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            retire.countDown();
            canceller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A try of the delete a cancel started removes the checkpoint and then fails, as when the answer of the storage is
     * lost on the way back, and the subscribe comes while the delete waits to try again. The checkpoint that try read is
     * kept, also once a later try reads nothing, so the subscribe takes the delete over, writes that checkpoint back and
     * resumes from it, and skips no event written after the cancel.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_while_the_delete_waits_to_try_again_after_a_try_that_removed_the_checkpoint_failed_resumes_from_it(boolean conditionalDeletes) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        List<Long> delivered = new CopyOnWriteArrayList<>();
        try {
            long stored = storedBeforeTheCancel(model, storage, feed);
            storage.nextDeleteAppliesThenFails = true;
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            assertThat(storage.appliedDeleteFailed.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete applied and then failed").isTrue();
            String storedOnceTheTryFailed = storage.stored();
            feed.write();
            feed.write();

            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered));
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            assertThat(storedOnceTheTryFailed).as("checkpoint stored once the try failed").isEqualTo("-");
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(stored, writtenAfter));
            cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            model.shutdown();
        }
    }

    /**
     * A second cancel of the id comes while the try of the delete the first cancel started is held, so the delete of
     * the second runs once the first has ended. The first removes the checkpoint and the first cancel completes, and the
     * second starts on the thread that ended the first, where its read of storage is held. A subscribe that comes then
     * finds the first delete retired and the second one with nothing read, so it starts as with nothing stored, from
     * where the feed is, and does not resume from the checkpoint the completed cancel removed.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_after_a_cancel_completed_does_not_resume_from_the_checkpoint_its_delete_removed_while_a_later_delete_of_the_id_reads(boolean conditionalDeletes) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch releaseFirstDelete = new CountDownLatch(1);
        CountDownLatch releaseSecondRead = new CountDownLatch(1);
        try {
            storedBeforeTheCancel(model, storage, feed);
            storage.deleteGate = releaseFirstDelete;
            CompletableFuture<Void> firstCancel = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("first delete held").isTrue();
            storage.deleteGate = null;
            CompletableFuture<Void> secondCancel = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            // The first try read storage before it was held, so the next read is that of the second delete
            storage.nextReadHeldOnItsThread = releaseSecondRead;
            releaseFirstDelete.countDown();
            assertThat(storage.readHeldOnItsThread.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("read of the second delete held").isTrue();
            firstCancel.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            String storedOnceTheFirstCancelCompleted = storage.stored();
            feed.write();
            feed.write();

            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered));
            releaseSecondRead.countDown();
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            assertThat(storedOnceTheFirstCancelCompleted).as("checkpoint stored once the first cancel completed").isEqualTo("-");
            assertThat(delivered).as("events delivered to the subscription").containsExactly(writtenAfter);
            secondCancel.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            releaseFirstDelete.countDown();
            releaseSecondRead.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe from a start position of its own, with a persist predicate that declines every event, takes over the
     * delete a cancel of the id started, whether this model drives the feed or hands the subscription to a wrapped model
     * that manages named subscriptions. It writes back the checkpoint that delete read, so the store holds it once the
     * cancel completes.
     */
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void a_subscribe_from_a_start_position_of_its_own_whose_predicate_stores_nothing_writes_back_the_checkpoint_a_delete_it_takes_over_read(boolean conditionalDeletes, boolean handedToTheWrappedModel) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = handedToTheWrappedModel ? new NamedFeed() : new Feed();
        AtomicBoolean storesPositions = new AtomicBoolean(true);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage, new ReactorDurableSubscriptionModelConfig(__ -> storesPositions.get()));
        CountDownLatch release = new CountDownLatch(1);
        try {
            long stored = storedBeforeTheCancel(model, storage, feed);
            storesPositions.set(false);
            storage.deleteGate = release;
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();

            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), action(new CopyOnWriteArrayList<>()));
            release.countDown();
            cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            feed.write();

            // Then
            assertThat(untilStored(storage, stored)).as("checkpoint stored once the cancel completed").isEqualTo(String.valueOf(stored));
        } finally {
            release.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe handed to a wrapped model that manages named subscriptions takes over the delete or the position
     * write a cancel of the id left running. The start position is read at the call, from storage, from the position
     * write in flight, or from where the wrapped model is when there is neither, so the subscribe returns while that
     * storage call is held and the subscription starts where it would with no delete running. Recording that position
     * waits for the delete, and an event delivered meanwhile waits for it too.
     * <ul>
     *     <li>nothing-stored: storage holds nothing when the delete tries, and the try is held</li>
     *     <li>write-in-flight-fails: the first position write of the cancelled subscription is held and then fails, so
     *     the delete waits for it, and an event is written after the cancel</li>
     *     <li>write-in-flight-arrives: the same, except that the write reaches the store</li>
     *     <li>stored: storage holds the checkpoint of the cancelled subscription, and the try that deletes it is held</li>
     * </ul>
     */
    @ParameterizedTest
    @CsvSource({"false, nothing-stored", "false, write-in-flight-fails", "true, write-in-flight-fails",
            "false, write-in-flight-arrives", "true, write-in-flight-arrives", "false, stored", "true, stored"})
    void a_subscribe_handed_to_a_wrapped_model_that_takes_over_a_delete_starts_where_it_would_with_no_delete_running(boolean conditionalDeletes, String held) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, held, release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            long startsAfter = heldCall.startsAfter() < 0 ? feed.present.get() : heldCall.startsAfter();

            // When
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller);
            Throwable returnedWhileHeld = catchThrowable(() -> subscribed.get(2, TimeUnit.SECONDS));
            List<Long> writtenWhileHeld = List.of(feed.write(), feed.write());
            release.countDown();
            long writtenAfter = feed.write();

            // Then
            Throwable thrown = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted(TIMEOUT).block());
            untilDelivered(delivered, writtenAfter);
            List<Long> expected = positionsAfter(startsAfter, writtenAfter);
            assertThat(expected).containsAll(writtenWhileHeld);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(returnedWhileHeld).as("the subscribe returning while the storage call it took over is held").isNull();
                softly.assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(expected);
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe handed to a wrapped model takes over a delete that a cancel of the id left running, and recording the
     * position it read at the call fails once that delete ended. The subscription then ends as one refused for that
     * reason would. Waiting for its start fails with the failure, it is cancelled in the wrapped model, and its action
     * gets no event.
     */
    @Test
    void a_subscribe_handed_to_a_wrapped_model_whose_start_position_cannot_be_recorded_once_the_delete_it_took_over_ended_ends() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "nothing-stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            storage.ifAbsentGate = new CountDownLatch(0);
            storage.ifAbsentFails = true;

            // When
            Subscription subscription = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            long writtenWhileHeld = feed.write();
            release.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block());
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            assertThat(thrown).as("how waiting for the start of the subscription ended").hasStackTraceContaining(SAVE_FAILED);
            assertThat(delivered).as("events delivered to the subscription, of %s and %s", writtenWhileHeld, writtenAfter).isEmpty();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.subscriptions).as("subscriptions in the wrapped model").isEmpty());
            assertThat(model.isRunning(SUBSCRIPTION_ID)).as("the subscription running").isFalse();
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe from the model default takes over a delete whose try is held, and its start position then cannot be
     * read, so it never starts. No subscription needs the checkpoint any more, so the delete goes ahead and removes it,
     * and the cancel completes once it has. A later subscribe from the model default starts from where the feed is.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_that_takes_over_a_delete_and_cannot_read_its_start_position_lets_the_delete_remove_the_checkpoint(boolean conditionalDeletes) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Mono<Void> cancelled = cancelWithCheckpointStored(model, storage, feed, release);
            storage.readsFail = true;
            Subscription failed = subscribeOn(caller, model, new CopyOnWriteArrayList<>());
            Throwable notStarted = catchThrowable(() -> failed.waitUntilStarted(TIMEOUT).block());
            storage.readsFail = false;

            // When
            release.countDown();
            cancelled.block(TIMEOUT);
            String storedOnceTheCancelEnded = storage.stored();
            Later later = subscribeAgain(model, feed, caller);

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(notStarted).as("why the subscribe that took the delete over did not start").hasStackTraceContaining(READ_FAILED);
                softly.assertThat(storedOnceTheCancelEnded).as("checkpoint stored once the cancel completed").isEqualTo("-");
                softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                        .containsExactly(later.writtenAfter());
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe takes over a delete whose try is held, and a pause comes before it started. Its resume then cannot
     * read the start position, so the subscription never starts and is dropped. The delete goes ahead and removes the
     * checkpoint, and a later subscribe from the model default starts from where the feed is.
     */
    @Test
    void a_resume_that_cannot_read_its_start_position_after_a_pause_before_the_subscription_started_lets_the_delete_remove_the_checkpoint() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Mono<Void> cancelled = cancelWithCheckpointStored(model, storage, feed, release);
            // Waits for the write back after the held try before it opens the feed
            Subscription takingOver = subscribeOn(caller, model, new CopyOnWriteArrayList<>());
            model.pauseSubscription(SUBSCRIPTION_ID);
            Throwable pausedBeforeItStarted = catchThrowable(() -> takingOver.waitUntilStarted(TIMEOUT).block());
            storage.readsFail = true;
            Subscription resumed = CompletableFuture.supplyAsync(() -> model.resumeSubscription(SUBSCRIPTION_ID), caller).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Throwable notStarted = catchThrowable(() -> resumed.waitUntilStarted(TIMEOUT).block());
            storage.readsFail = false;

            // When
            release.countDown();
            cancelled.block(TIMEOUT);
            String storedOnceTheCancelEnded = storage.stored();
            Later later = subscribeAgain(model, feed, caller);

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(pausedBeforeItStarted).as("how the subscribe that took the delete over ended").isInstanceOf(java.util.concurrent.CancellationException.class);
                softly.assertThat(notStarted).as("why the resume did not start").hasStackTraceContaining(READ_FAILED);
                softly.assertThat(storedOnceTheCancelEnded).as("checkpoint stored once the cancel completed").isEqualTo("-");
                softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                        .containsExactly(later.writtenAfter());
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe takes over a delete whose try is held and a second cancel ends it before it started. A subscribe
     * after that takes over both deletes and cannot read its start position, so no subscription needs the checkpoint.
     * Both deletes go ahead and remove it. That holds on the path that drives the feed, and on the one that hands the
     * subscription to a wrapped model, where the second cancel ends the first subscribe while it reads the store, or
     * once it is registered and waits for the delete to record its start position.
     */
    @ParameterizedTest
    @ValueSource(strings = {"drives-the-feed", "reads-the-store", "handed-over"})
    void a_subscribe_that_cannot_read_its_start_position_after_a_cancel_ended_one_that_took_the_delete_over_lets_the_delete_remove_the_checkpoint(String endedWhile) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        boolean readsTheStore = endedWhile.equals("reads-the-store");
        boolean handedOver = endedWhile.equals("handed-over");
        Feed feed = readsTheStore || handedOver ? new NamedFeed() : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        try {
            cancelWithCheckpointStored(model, storage, feed, release);
            if (readsTheStore) {
                storage.readGate = releaseRead;
            }
            CompletableFuture<Subscription> takingOver = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())), caller);
            if (readsTheStore) {
                assertThat(storage.readEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("read of the store held").isTrue();
            } else {
                takingOver.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            Mono<Void> cancelledAgain = model.cancelSubscription(SUBSCRIPTION_ID);
            // One handed over waits for the delete it took over before it tells how it ended
            Throwable cancelledBeforeItStarted = handedOver
                    ? new java.util.concurrent.CancellationException()
                    : catchThrowable(() -> takingOver.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted(TIMEOUT).block());
            releaseRead.countDown();
            storage.readsFail = true;
            Throwable notStarted = catchThrowable(() -> subscribeOn(caller, model, new CopyOnWriteArrayList<>()).waitUntilStarted(TIMEOUT).block());
            storage.readsFail = false;

            // When
            release.countDown();
            cancelledAgain.block(TIMEOUT);
            String storedOnceTheCancelEnded = storage.stored();
            Later later = subscribeAgain(model, feed, caller);

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(cancelledBeforeItStarted).as("how the subscribe that took the delete over ended").isInstanceOf(java.util.concurrent.CancellationException.class);
                softly.assertThat(notStarted).as("why the subscribe after the second cancel did not start").hasStackTraceContaining(READ_FAILED);
                softly.assertThat(storedOnceTheCancelEnded).as("checkpoint stored once the second cancel completed").isEqualTo("-");
                softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                        .containsExactly(later.writtenAfter());
            });
        } finally {
            release.countDown();
            releaseRead.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe handed to a wrapped model takes over a delete whose try is held, and is handed where the wrapped model
     * is. Another node stores an earlier position right before the subscribe records its own, and the storage settles the
     * race by position in favour of that one. The subscription is then subscribed again in the wrapped model from the
     * earlier position, so it delivers the event between the two, and every event after it once.
     */
    @Test
    void a_subscribe_handed_to_a_wrapped_model_whose_first_position_loses_to_an_earlier_one_starts_again_from_that_one() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "nothing-stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            long storedElsewhere = feed.write();
            long handedOver = feed.write();
            storage.storedElsewhereBeforeIfAbsent = new StringBasedCheckpoint(String.valueOf(storedElsewhere));
            storage.resolvesRaceByPosition = true;

            // When
            Subscription subscription = subscribeOn(caller, model, delivered);
            List<Long> writtenWhileHeld = List.of(feed.write(), feed.write());
            untilHandedOver(feed);
            release.countDown();
            long writtenAfter = feed.write();

            // Then
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block());
            untilDelivered(delivered, writtenAfter);
            String stored = untilStored(storage, writtenAfter);
            assertThat(writtenWhileHeld).containsExactly(handedOver + 1, handedOver + 2);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
                softly.assertThat(stored).as("checkpoint stored").isEqualTo(String.valueOf(writtenAfter));
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * The first run of a subscription that is started again from an earlier position reports quiet positions that come
     * after events its action skipped, so none of them may be saved.
     */
    @Test
    void a_quiet_position_reported_before_the_subscription_is_started_again_from_an_earlier_position_is_not_saved() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage, new ReactorDurableSubscriptionModelConfig(1).saveQuietPositionEvery(Duration.ofNanos(1)));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "nothing-stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            long storedElsewhere = feed.write();
            feed.write();
            storage.storedElsewhereBeforeIfAbsent = new StringBasedCheckpoint(String.valueOf(storedElsewhere));
            storage.resolvesRaceByPosition = true;
            Subscription subscription = subscribeOn(caller, model, delivered);
            feed.write();
            feed.write();
            Function<Checkpoint, Mono<Void>> saveQuietPosition = feed.quietPositionSaverFor(SUBSCRIPTION_ID);
            assertThat(saveQuietPosition).as("save offered to the first run of the subscription").isNotNull();

            // When
            CompletableFuture<String> storedOnceTheSaveEnded = saveQuietPosition.apply(new StringBasedCheckpoint(QUIET_POSITION)).then(Mono.fromSupplier(storage::stored)).toFuture();
            release.countDown();
            long writtenAfter = feed.write();

            // Then
            String storedWhenTheSaveEnded = storedOnceTheSaveEnded.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block());
            untilDelivered(delivered, writtenAfter);
            String stored = untilStored(storage, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(storedWhenTheSaveEnded).as("checkpoint stored once the save of the quiet position ended").isNotEqualTo(QUIET_POSITION);
                softly.assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
                softly.assertThat(stored).as("checkpoint stored").isEqualTo(String.valueOf(writtenAfter));
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe handed to a wrapped model whose first position loses to an earlier one is paused while it is started
     * again from that one. It stays paused, delivers nothing until it is resumed, and then delivers every event after
     * the earlier position once.
     * <ul>
     *     <li>before-the-start-again: paused right after the subscribe, while the delete it took over is still held</li>
     *     <li>before-the-cancel: paused while the wrapped model cancels the first subscription, before it is gone there</li>
     *     <li>between-the-cancel-and-the-subscribe: paused once the wrapped model no longer has the subscription, before
     *     it has it again</li>
     *     <li>after-the-subscribe: paused once the wrapped model has the subscription again, before its subscribe
     *     returned</li>
     * </ul>
     */
    @ParameterizedTest
    @ValueSource(strings = {"before-the-start-again", "before-the-cancel", "between-the-cancel-and-the-subscribe", "after-the-subscribe"})
    void a_pause_while_a_subscription_handed_to_a_wrapped_model_is_started_again_keeps_it_paused_until_it_is_resumed(String pausedWhen) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            long storedElsewhere = loseTheFirstPosition(model, storage, feed, release);
            @Nullable CountDownLatch startingAgain = switch (pausedWhen) {
                case "before-the-cancel" -> feed.holdCancel(2, true);
                case "between-the-cancel-and-the-subscribe" -> feed.holdCancel(2, false);
                case "after-the-subscribe" -> feed.holdSubscribe(3);
                default -> null;
            };

            // When
            subscribeOn(caller, model, delivered);
            final Throwable pauseFailed;
            if (startingAgain == null) {
                pauseFailed = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));
                untilHandedOver(feed);
                release.countDown();
            } else {
                untilHandedOver(feed);
                release.countDown();
                assertThat(startingAgain.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("start again held").isTrue();
                pauseFailed = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));
                feed.letGo();
            }
            await().atMost(TIMEOUT).until(() -> feed.subscribes.get() >= 3);
            long writtenWhilePaused = feed.write();
            List<Long> deliveredWhilePaused = deliveredWithin(delivered, writtenWhilePaused);
            boolean pausedAnswer = model.isPaused(SUBSCRIPTION_ID);
            boolean runningAnswer = model.isRunning(SUBSCRIPTION_ID);
            Throwable resumeFailed = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(pauseFailed).as("how the pause ended").isNull();
                softly.assertThat(deliveredWhilePaused).as("events delivered while paused").isEmpty();
                softly.assertThat(pausedAnswer).as("isPaused while paused").isTrue();
                softly.assertThat(runningAnswer).as("isRunning while paused").isFalse();
                softly.assertThat(resumeFailed).as("how the resume ended").isNull();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
            });
        } finally {
            release.countDown();
            feed.letGo();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A model that hands subscriptions to a wrapped model is stopped once the wrapped model no longer has a subscription
     * it starts again from an earlier first position, and before it has it again. The subscription answers that it is
     * paused, delivers nothing while the model is stopped, and delivers every event after the earlier position once the
     * model starts again.
     */
    @Test
    void a_stop_while_a_subscription_handed_to_a_wrapped_model_is_started_again_keeps_it_paused_until_the_model_starts() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            long storedElsewhere = loseTheFirstPosition(model, storage, feed, release);
            CountDownLatch startingAgain = feed.holdCancel(2, false);

            // When
            subscribeOn(caller, model, delivered);
            untilHandedOver(feed);
            release.countDown();
            assertThat(startingAgain.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("start again held").isTrue();
            model.stop();
            boolean pausedWhileStartedAgain = model.isPaused(SUBSCRIPTION_ID);
            boolean runningWhileStartedAgain = model.isRunning(SUBSCRIPTION_ID);
            feed.letGo();
            await().atMost(TIMEOUT).until(() -> feed.subscribes.get() >= 3);
            long writtenWhileStopped = feed.write();
            List<Long> deliveredWhileStopped = deliveredWithin(delivered, writtenWhileStopped);
            boolean pausedOnceStartedAgain = model.isPaused(SUBSCRIPTION_ID);
            model.start(true);
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(pausedWhileStartedAgain).as("isPaused while the wrapped model does not have the subscription").isTrue();
                softly.assertThat(runningWhileStartedAgain).as("isRunning while the wrapped model does not have the subscription").isFalse();
                softly.assertThat(deliveredWhileStopped).as("events delivered while stopped").isEmpty();
                softly.assertThat(pausedOnceStartedAgain).as("isPaused once the wrapped model has the subscription again").isTrue();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
            });
        } finally {
            release.countDown();
            feed.letGo();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscription handed to a wrapped model is paused once the wrapped model no longer has it, while it is started
     * again from an earlier first position, and then cancelled before the wrapped model has it again. The cancel
     * completes, the subscription delivers nothing, and a later subscribe of the id from the model default starts from
     * where the feed is.
     */
    @Test
    void a_cancel_after_a_pause_while_a_subscription_handed_to_a_wrapped_model_is_started_again_ends_it() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            loseTheFirstPosition(model, storage, feed, release);
            CountDownLatch startingAgain = feed.holdCancel(2, false);

            // When
            subscribeOn(caller, model, delivered);
            untilHandedOver(feed);
            release.countDown();
            assertThat(startingAgain.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("start again held").isTrue();
            Throwable pauseFailed = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));
            Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
            feed.letGo();
            Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
            long writtenAfterCancel = feed.write();
            List<Long> deliveredAfterCancel = deliveredWithin(delivered, writtenAfterCancel);
            boolean pausedAnswer = model.isPaused(SUBSCRIPTION_ID);
            boolean runningAnswer = model.isRunning(SUBSCRIPTION_ID);
            Later later = subscribeAgain(model, feed, caller);

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(pauseFailed).as("how the pause ended").isNull();
                softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
                softly.assertThat(deliveredAfterCancel).as("events delivered to the cancelled subscription").isEmpty();
                softly.assertThat(pausedAnswer).as("isPaused once cancelled").isFalse();
                softly.assertThat(runningAnswer).as("isRunning once cancelled").isFalse();
                softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                        .containsExactly(later.writtenAfter());
            });
        } finally {
            release.countDown();
            feed.letGo();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe of the id comes once the wrapped model no longer has a subscription handed to it that it starts again
     * from an earlier first position, and before it has it again. It is refused as a duplicate, as the wrapped model
     * refuses it while it has the subscription, and the subscription started again delivers every event after the
     * earlier position.
     */
    @Test
    void a_subscribe_of_the_id_while_a_subscription_handed_to_a_wrapped_model_is_started_again_is_refused_as_a_duplicate() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        ExecutorService otherCaller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            long storedElsewhere = loseTheFirstPosition(model, storage, feed, release);
            CountDownLatch startingAgain = feed.holdCancel(2, false);
            subscribeOn(caller, model, delivered);
            untilHandedOver(feed);
            release.countDown();
            assertThat(startingAgain.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("start again held").isTrue();

            // When
            Throwable refused = catchThrowable(() -> subscribeOn(otherCaller, model, new CopyOnWriteArrayList<>()));
            feed.letGo();
            await().atMost(TIMEOUT).until(() -> feed.subscribes.get() >= 3);
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(refused).as("how the second subscribe ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
                softly.assertThat(delivered).as("events delivered to the subscription started again").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
            });
        } finally {
            release.countDown();
            feed.letGo();
            caller.shutdownNow();
            otherCaller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe of the id whose start position would wait for the checkpoint a cancel deleted to be written back is
     * refused as a duplicate at the call while the model still records a subscription of the id that it handed to a
     * wrapped model. That includes a subscription whose start the wrapped model failed and of which it kept nothing, as
     * ReactorMongoSubscriptionModel does after an error it can't recover from. The start position is never asked.
     */
    @Test
    void a_subscribe_that_would_wait_is_refused_while_the_model_records_a_subscription_of_the_id_whose_start_failed_in_the_wrapped_model() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();
            feed.startFails = true;
            Subscription handedOver = model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), action(new CopyOnWriteArrayList<>()));
            Throwable startFailed = catchThrowable(() -> handedOver.waitUntilStarted(TIMEOUT).block());
            feed.startFails = false;
            AtomicInteger startPositionAsked = new AtomicInteger();

            // When
            Throwable refused = catchThrowable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
                startPositionAsked.incrementAndGet();
                return StartAt.now();
            }), action(new CopyOnWriteArrayList<>())));

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(startFailed).as("how waiting for the start of the subscription handed over ended").hasMessage(START_FAILED);
                softly.assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("whether the wrapped model has a subscription of the id").isFalse();
                softly.assertThat(refused).as("how the subscribe that would wait ended").isInstanceOf(DuplicateSubscriptionIdException.class);
                softly.assertThat(startPositionAsked).as("times the start position was asked").hasValue(0);
            });
        } finally {
            release.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscription handed to a wrapped model is paused right as the model, having started it again there from an
     * earlier first position, has found no state kept meanwhile left to put in place. It delivers nothing until it is
     * resumed, and then delivers every event after the earlier position, the one under way at the pause again.
     */
    @Test
    void a_pause_as_a_subscription_handed_to_a_wrapped_model_has_been_started_again_pauses_it_there() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            long storedElsewhere = loseTheFirstPosition(model, storage, feed, release);
            Called pause = callOnceNothingIsLeftToPutInPlace(model, () -> model.pauseSubscription(SUBSCRIPTION_ID));
            subscribeOn(caller, model, delivered);

            // When
            untilHandedOver(feed);
            release.countDown();
            Throwable pauseFailed = pause.ended();
            long writtenWhilePaused = feed.write();
            List<Long> deliveredWhilePaused = deliveredWithin(delivered, writtenWhilePaused);
            boolean pausedAnswer = model.isPaused(SUBSCRIPTION_ID);
            Throwable resumeFailed = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(pauseFailed).as("how the pause ended").isNull();
                softly.assertThat(deliveredWhilePaused).as("events delivered while paused").doesNotContain(writtenWhilePaused);
                softly.assertThat(pausedAnswer).as("isPaused while paused").isTrue();
                softly.assertThat(resumeFailed).as("how the resume ended").isNull();
                softly.assertThat(delivered.stream().distinct().toList()).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscription handed to a wrapped model is paused before it is started again there from an earlier first
     * position, and resumed right as the model has put that pause in place there and found nothing else left to put
     * in place. It delivers every event after the earlier position once, and what the resume returned starts.
     */
    @Test
    void a_resume_as_a_paused_subscription_handed_to_a_wrapped_model_has_been_started_again_resumes_it_there() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            long storedElsewhere = loseTheFirstPosition(model, storage, feed, release);
            AtomicReference<@Nullable Subscription> resumed = new AtomicReference<>();
            Called resume = callOnceNothingIsLeftToPutInPlace(model, () -> resumed.set(model.resumeSubscription(SUBSCRIPTION_ID)));
            subscribeOn(caller, model, delivered);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            untilHandedOver(feed);
            release.countDown();
            Throwable resumeFailed = resume.ended();
            long writtenAfter = feed.write();

            // Then
            untilDelivered(delivered, writtenAfter);
            @Nullable Subscription resumedSubscription = resumed.get();
            Throwable startFailed = catchThrowable(() -> requireNonNull(resumedSubscription).waitUntilStarted(TIMEOUT).block());
            boolean pausedAnswer = model.isPaused(SUBSCRIPTION_ID);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(resumeFailed).as("how the resume ended").isNull();
                softly.assertThat(startFailed).as("how waiting for what the resume returned ended").isNull();
                softly.assertThat(pausedAnswer).as("isPaused once resumed").isFalse();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(storedElsewhere, writtenAfter));
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A cancel of the id and a subscribe of it right after come while the wrapped model takes the subscribe that starts
     * a subscription handed to it again from an earlier first position. The later subscription stays in the wrapped
     * model and delivers what is written after it, and the one started again ends.
     */
    @Test
    void a_cancel_and_a_subscribe_of_the_id_while_a_subscription_handed_to_a_wrapped_model_is_subscribed_there_again_leave_the_later_one_there() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        ExecutorService otherCaller = Executors.newSingleThreadExecutor();
        List<Long> later = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        try {
            loseTheFirstPosition(model, storage, feed, release);
            CountDownLatch subscribingAgain = feed.holdSubscribe(3);
            Subscription first = subscribeOn(caller, model, new CopyOnWriteArrayList<>());
            untilHandedOver(feed);
            release.countDown();
            assertThat(subscribingAgain.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("subscribe that starts it again held").isTrue();

            // When
            Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
            Subscription laterSubscription = subscribeOn(otherCaller, model, later);
            feed.letGo();
            Throwable firstEnded = catchThrowable(() -> first.waitUntilStarted(TIMEOUT).block());
            // Completes once the start again has ended, including a cancel it sends the wrapped model
            Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
            laterSubscription.waitUntilStarted(TIMEOUT).block();
            long writtenAfter = feed.write();

            // Then
            untilDelivered(later, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(firstEnded).as("how waiting for the start of the first subscription ended").isInstanceOf(CancellationException.class);
                softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
                softly.assertThat(later).as("events delivered to the later subscription").contains(writtenAfter);
                softly.assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the wrapped model").isTrue();
            });
        } finally {
            release.countDown();
            feed.letGo();
            caller.shutdownNow();
            otherCaller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * Against a wrapped model that lets a subscribe replace the subscription of the id it has, a subscribe of the id is
     * refused while the first position of an earlier one is being recorded. Once that fails, a subscribe of the id
     * starts in the wrapped model and delivers what is written after it.
     */
    @Test
    void a_subscribe_handed_to_a_wrapped_model_whose_first_position_fails_refuses_a_later_one_of_the_id_until_it_has_ended() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        PausableFeed feed = new PausableFeed(true);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        ExecutorService otherCaller = Executors.newSingleThreadExecutor();
        List<Long> later = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch releaseFirstPosition = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "nothing-stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            storage.ifAbsentGate = releaseFirstPosition;
            storage.ifAbsentFails = true;
            Subscription first = subscribeOn(caller, model, new CopyOnWriteArrayList<>());
            release.countDown();
            assertThat(storage.ifAbsentEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("first position held").isTrue();
            Throwable refusedWhileHeld = catchThrowable(() -> subscribeOn(otherCaller, model, later));

            // When
            releaseFirstPosition.countDown();
            Throwable firstEnded = catchThrowable(() -> first.waitUntilStarted(TIMEOUT).block());
            // Time for the end of the first subscription to cancel the id in the wrapped model, if it does
            Thread.sleep(300);
            subscribeOn(otherCaller, model, later).waitUntilStarted(TIMEOUT).block();
            long writtenAfter = feed.write();

            // Then
            untilDelivered(later, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(refusedWhileHeld).as("how the subscribe made while the first position was held ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
                softly.assertThat(firstEnded).as("how waiting for the start of the first subscription ended").hasStackTraceContaining(SAVE_FAILED);
                softly.assertThat(later).as("events delivered to the later subscription").contains(writtenAfter);
                softly.assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the wrapped model").isTrue();
            });
        } finally {
            release.countDown();
            releaseFirstPosition.countDown();
            feed.letGo();
            caller.shutdownNow();
            otherCaller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe handed to a wrapped model takes over a delete whose try is held. Another node stores a position right
     * before the subscribe records its own, and the storage cannot order the two, so recording it is refused and the
     * subscription never starts. The refused write wrote nothing, so the delete goes ahead and removes the position the
     * other node stored, and a later subscribe from the model default starts from where the feed is.
     */
    @Test
    void a_subscribe_handed_to_a_wrapped_model_whose_first_position_is_refused_lets_the_delete_remove_the_checkpoint() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "nothing-stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            // Past every event this test writes
            storage.storedElsewhereBeforeIfAbsent = new StringBasedCheckpoint("1000");

            // When
            Subscription refused = subscribeOn(caller, model, new CopyOnWriteArrayList<>());
            release.countDown();
            Throwable notStarted = catchThrowable(() -> refused.waitUntilStarted(TIMEOUT).block());
            String stored = untilNothingStored(storage);
            Later later = subscribeAgain(model, feed, caller);

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(notStarted).as("why the subscribe that took the delete over did not start").hasStackTraceContaining("StartPositionAlreadyPinnedException");
                softly.assertThat(stored).as("checkpoint stored once the subscribe was refused").isEqualTo("-");
                softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                        .containsExactly(later.writtenAfter());
            });
        } finally {
            release.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscribe handed to a wrapped model takes over a delete that writes the checkpoint back at once, and returns
     * while the write of its start position is held. It is handed the checkpoint it read and delivers what is written
     * after it.
     */
    @Test
    void a_subscribe_handed_to_a_wrapped_model_that_takes_over_a_delete_written_back_at_once_returns_while_its_start_position_is_written() throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(true);
        NamedFeed feed = new NamedFeed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch releaseWrites = new CountDownLatch(1);
        try {
            Held heldCall = cancelWhileHeld(model, storage, feed, "stored", release);
            assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
            storage.notOlderThanGate = releaseWrites;

            // When
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller);
            Throwable returnedWhileHeld = catchThrowable(() -> subscribed.get(2, TimeUnit.SECONDS));
            long writtenWhileHeld = feed.write();
            storage.notOlderThanGate = null;
            releaseWrites.countDown();
            release.countDown();
            long writtenAfter = feed.write();

            // Then
            Throwable thrown = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted(TIMEOUT).block());
            untilDelivered(delivered, writtenAfter);
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(returnedWhileHeld).as("the subscribe returning while the write of its start position is held").isNull();
                softly.assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
                softly.assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(positionsAfter(heldCall.startsAfter(), writtenAfter));
            });
            assertThat(writtenWhileHeld).isEqualTo(heldCall.startsAfter() + 1);
        } finally {
            release.countDown();
            releaseWrites.countDown();
            caller.shutdownNow();
            model.shutdown();
        }
    }

    /**
     * A subscription from the model default is registered on a stopped model while storage holds 2, and the feed is at
     * 4. The start asks the storage to settle which of the two is the first position, and the storage holds that
     * answer while the caller cancels the subscription and something outside this model removes what is stored. A
     * subscribe of the id then takes the delete of the cancel over. The storage answers that it cannot compare the two
     * and wrote nothing, so 4 was never stored, and the subscribe that finds nothing stored starts from where the feed
     * was at its call, as a new subscription.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_that_takes_over_a_delete_waiting_for_a_settled_first_position_does_not_start_from_the_position_it_never_wrote(boolean conditionalDeletes) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(conditionalDeletes);
        Feed feed = new Feed();
        feed.write();
        feed.write();
        storage.storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("2"), CheckpointWriteCondition.any()).block(TIMEOUT);
        feed.write();
        feed.write();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        CountDownLatch releaseResolver = new CountDownLatch(1);
        storage.resolverGate = releaseResolver;
        List<Long> delivered = new CopyOnWriteArrayList<>();

        try {
            model.stop();
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>()));
            model.start(true);
            assertThat(storage.resolverEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the start asks the storage to settle the first position").isTrue();
            feed.write();
            model.cancelSubscription(SUBSCRIPTION_ID);
            storage.storage.delete(SUBSCRIPTION_ID).block(TIMEOUT);

            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered));
            long writtenAfterTheSubscribe = feed.write();
            releaseResolver.countDown();
            long writtenOnceSettled = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(writtenOnceSettled));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(writtenAfterTheSubscribe, writtenOnceSettled);
        } finally {
            releaseResolver.countDown();
            model.shutdown();
        }
    }

    // What a subscribe of the id from the model default delivered, of an event written before it and one written after
    private record Later(List<Long> delivered, long writtenBefore, long writtenAfter) {
    }

    private static Later subscribeAgain(ReactorDurableSubscriptionModel model, Feed feed, ExecutorService caller) throws Exception {
        long writtenBefore = feed.write();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        subscribeOn(caller, model, delivered).waitUntilStarted(TIMEOUT).block();
        long writtenAfter = feed.write();
        untilDelivered(delivered, writtenAfter);
        return new Later(delivered, writtenBefore, writtenAfter);
    }

    private static Subscription subscribeOn(ExecutorService caller, ReactorDurableSubscriptionModel model, List<Long> delivered) throws Exception {
        return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    // Subscribes the id from the model default, waits until the checkpoint of three events is stored, then cancels it
    // while the try of the delete is held until release opens. Answers the cancel.
    private static Mono<Void> cancelWithCheckpointStored(ReactorDurableSubscriptionModel model, GatedStorage storage, Feed feed, CountDownLatch release) throws InterruptedException {
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
        long written = 0;
        for (int i = 0; i < 3; i++) {
            written = feed.write();
        }
        String lastWritten = String.valueOf(written);
        await().atMost(TIMEOUT).until(() -> lastWritten.equals(storage.stored()));
        storage.deleteGate = release;
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();
        return cancelled;
    }

    // Cancels a subscription of the id while the try of the delete is held until release opens, writes two events, and
    // sets the storage so that another node stores the first of them right before a subscribe from the model default
    // records the second as its first position, and the race goes to the earlier one. Answers the position of the first.
    private static long loseTheFirstPosition(ReactorDurableSubscriptionModel model, GatedStorage storage, Feed feed, CountDownLatch release) throws InterruptedException {
        Held heldCall = cancelWhileHeld(model, storage, feed, "nothing-stored", release);
        assertThat(heldCall.entered().await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("storage call held").isTrue();
        long storedElsewhere = feed.write();
        feed.write();
        storage.storedElsewhereBeforeIfAbsent = new StringBasedCheckpoint(String.valueOf(storedElsewhere));
        storage.resolvesRaceByPosition = true;
        return storedElsewhere;
    }

    // Waits a second for the event to arrive, and answers what was delivered by then either way
    private static List<Long> deliveredWithin(List<Long> delivered, long position) {
        try {
            await().atMost(Duration.ofSeconds(1)).until(() -> delivered.contains(position));
        } catch (ConditionTimeoutException notDelivered) {
            // What was delivered instead is asserted next
        }
        return List.copyOf(delivered);
    }

    // The storage call a cancel left held, and the position a subscribe from the model default starts after then, or -1
    // for where the feed is at that subscribe
    private record Held(CountDownLatch entered, long startsAfter) {
    }

    // Starts a subscription of the id, then cancels it while the storage call named by held is held until release opens
    private static Held cancelWhileHeld(ReactorDurableSubscriptionModel model, GatedStorage storage, Feed feed, String held, CountDownLatch release) throws InterruptedException {
        switch (held) {
            case "nothing-stored" -> {
                model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
                storage.deleteGate = release;
                model.cancelSubscription(SUBSCRIPTION_ID);
                return new Held(storage.deleteEntered, -1);
            }
            case "deleted-before" -> {
                model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
                long written = feed.write();
                await().atMost(TIMEOUT).until(() -> String.valueOf(written).equals(storage.stored()));
                model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
                storage.deleteGate = release;
                model.cancelSubscription(SUBSCRIPTION_ID);
                // A conditional try that reads nothing makes no delete, so nothing is held then
                return new Held(storage.conditionalDeletes ? new CountDownLatch(0) : storage.deleteEntered, -1);
            }
            case "stored" -> {
                model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
                long written = feed.write();
                await().atMost(TIMEOUT).until(() -> String.valueOf(written).equals(storage.stored()));
                storage.deleteGate = release;
                model.cancelSubscription(SUBSCRIPTION_ID);
                return new Held(storage.deleteEntered, written);
            }
            default -> {
                model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
                storage.heldSave = release;
                storage.heldSaveFails = held.equals("write-in-flight-fails");
                long inFlight = feed.write();
                assertThat(storage.saveEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("position write held").isTrue();
                model.cancelSubscription(SUBSCRIPTION_ID);
                feed.write();
                return new Held(storage.saveEntered, inFlight);
            }
        }
    }

    // A call the model makes on the thread that puts the state kept for a subscription started again in place in the
    // wrapped model, once that thread has found nothing left to put there
    private record Called(CountDownLatch made, AtomicReference<@Nullable Throwable> failure) {
        // Answers how the call ended, once it has
        private @Nullable Throwable ended() throws InterruptedException {
            assertThat(made.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("call made once nothing was left to put in place").isTrue();
            return failure.get();
        }
    }

    // The subscribe returns before it reads storage, so a test lets the delete go only once it is handed over
    private static void untilHandedOver(SubscriptionModel feed) {
        await().atMost(TIMEOUT).until(() -> feed.isRunning(SUBSCRIPTION_ID) || feed.isPaused(SUBSCRIPTION_ID));
    }

    private static Called callOnceNothingIsLeftToPutInPlace(ReactorDurableSubscriptionModel model, Runnable call) {
        Called called = new Called(new CountDownLatch(1), new AtomicReference<>());
        model.runOnceNothingIsLeftToPutInPlace(() -> {
            if (called.made().getCount() > 0) {
                called.failure().set(catchThrowable(call::run));
                called.made().countDown();
            }
        });
        return called;
    }

    // Starts a subscription of the id from the model default and has it store the checkpoint of three events, and
    // answers the position of the last
    private static long storedBeforeTheCancel(ReactorDurableSubscriptionModel model, GatedStorage storage, Feed feed) {
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
        feed.write();
        feed.write();
        long stored = feed.write();
        await().atMost(TIMEOUT).until(() -> String.valueOf(stored).equals(storage.stored()));
        return stored;
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("latch released").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static List<Long> positionsAfter(long start, long last) {
        return LongStream.rangeClosed(start + 1, last).boxed().toList();
    }

    // Waits a while for the event to arrive and returns either way, since the assertion that follows says what arrived
    private static void untilDelivered(List<Long> delivered, long position) {
        try {
            await().atMost(Duration.ofSeconds(3)).until(() -> delivered.contains(position));
        } catch (ConditionTimeoutException notDelivered) {
            // What was delivered instead is asserted next
        }
    }

    // Waits a while for the checkpoint to be stored and answers what is stored either way, since the assertion that
    // follows says what it is
    private static String untilStored(GatedStorage storage, long position) {
        try {
            await().atMost(Duration.ofSeconds(3)).until(() -> String.valueOf(position).equals(storage.stored()));
        } catch (ConditionTimeoutException notStored) {
            // What is stored instead is asserted next
        }
        return storage.stored();
    }

    // Waits a while for nothing to be stored and answers what is stored either way
    private static String untilNothingStored(GatedStorage storage) {
        try {
            await().atMost(Duration.ofSeconds(3)).until(() -> "-".equals(storage.stored()));
        } catch (ConditionTimeoutException stillStored) {
            // What is stored instead is asserted next
        }
        return storage.stored();
    }

    private static Subscription onParallel(java.util.concurrent.Callable<Subscription> call) {
        return Mono.fromCallable(call).subscribeOn(Schedulers.parallel()).block(TIMEOUT);
    }

    private static Function<CloudEvent, Mono<Void>> action(List<Long> delivered) {
        return cloudEvent -> Mono.fromRunnable(() -> delivered.add(Long.parseLong(cloudEvent.getId())));
    }

    // Holds a delete, the next write back, the next position write, the next read or every conditional write at a
    // version, while the latch it was given is closed. It can also fail every read, store a checkpoint of another node
    // right before the next write on the condition that nothing is stored, settle a first-position race by position,
    // apply the next delete and then fail it, hold the next read on the thread that subscribes to it, and hold a
    // first-position race that then settles nothing.
    private static final class GatedStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        private final boolean conditionalDeletes;
        private final CountDownLatch deleteEntered = new CountDownLatch(1);
        private final CountDownLatch ifAbsentEntered = new CountDownLatch(1);
        private final CountDownLatch saveEntered = new CountDownLatch(1);
        private final CountDownLatch readEntered = new CountDownLatch(1);
        private volatile @Nullable CountDownLatch deleteGate;
        private volatile @Nullable CountDownLatch ifAbsentGate;
        private volatile boolean ifAbsentFails;
        private volatile @Nullable CountDownLatch heldSave;
        private volatile boolean heldSaveFails = true;
        private volatile @Nullable CountDownLatch readGate;
        private volatile boolean readsFail;
        private volatile @Nullable CountDownLatch notOlderThanGate;
        private volatile @Nullable Checkpoint storedElsewhereBeforeIfAbsent;
        private volatile boolean resolvesRaceByPosition;
        // Set, a first-position race is held until it opens, and then answers that it cannot compare and wrote nothing
        private volatile @Nullable CountDownLatch resolverGate;
        private final CountDownLatch resolverEntered = new CountDownLatch(1);
        // Set, the next delete removes the checkpoint and then fails, as one whose answer is lost on the way back
        private volatile boolean nextDeleteAppliesThenFails;
        private final CountDownLatch appliedDeleteFailed = new CountDownLatch(1);
        // Set, the next read waits for it on the thread that subscribes to the read, which nothing else then runs on
        private volatile @Nullable CountDownLatch nextReadHeldOnItsThread;
        private final CountDownLatch readHeldOnItsThread = new CountDownLatch(1);

        private GatedStorage(boolean conditionalDeletes) {
            this.conditionalDeletes = conditionalDeletes;
        }

        private String stored() {
            Checkpoint checkpoint = storage.read(SUBSCRIPTION_ID).block();
            return checkpoint == null ? "-" : checkpoint.asString();
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                @Nullable CountDownLatch onItsThread = nextReadHeldOnItsThread;
                if (onItsThread != null) {
                    nextReadHeldOnItsThread = null;
                    readHeldOnItsThread.countDown();
                    awaitOrFail(onItsThread);
                }
                if (readsFail) {
                    return Mono.error(new IllegalStateException(READ_FAILED));
                }
                @Nullable CountDownLatch gate = readGate;
                if (gate != null) {
                    readGate = null;
                    readEntered.countDown();
                    return held(gate).then(Mono.defer(() -> storage.read(subscriptionId)));
                }
                return storage.read(subscriptionId);
            });
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            @Nullable CountDownLatch gate = resolverGate;
            if (gate != null) {
                resolverEntered.countDown();
                return held(gate).then(Mono.empty());
            }
            if (!resolvesRaceByPosition) {
                return Mono.empty();
            }
            return storage.read(subscriptionId)
                    .filter(stored -> Long.parseLong(stored.asString()) <= Long.parseLong(candidate.asString()))
                    .switchIfEmpty(Mono.defer(() -> storage.save(subscriptionId, candidate, CheckpointWriteCondition.any())));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                @Nullable Checkpoint elsewhere = storedElsewhereBeforeIfAbsent;
                if (elsewhere != null && condition instanceof CheckpointWriteCondition.IfAbsent) {
                    storedElsewhereBeforeIfAbsent = null;
                    return storage.save(subscriptionId, elsewhere, CheckpointWriteCondition.any())
                            .then(Mono.defer(() -> storage.save(subscriptionId, checkpoint, condition)));
                }
                @Nullable CountDownLatch versionGate = notOlderThanGate;
                if (versionGate != null && condition instanceof CheckpointWriteCondition.NotOlderThan) {
                    return held(versionGate).then(Mono.defer(() -> storage.save(subscriptionId, checkpoint, condition)));
                }
                @Nullable CountDownLatch saveGate = heldSave;
                if (saveGate != null) {
                    heldSave = null;
                    saveEntered.countDown();
                    return held(saveGate).then(heldSaveFails
                            ? Mono.error(new IllegalStateException(SAVE_FAILED))
                            : Mono.defer(() -> storage.save(subscriptionId, checkpoint, condition)));
                }
                @Nullable CountDownLatch gate = ifAbsentGate;
                if (gate != null && condition instanceof CheckpointWriteCondition.IfAbsent) {
                    ifAbsentGate = null;
                    ifAbsentEntered.countDown();
                    return held(gate).then(ifAbsentFails
                            ? Mono.error(new IllegalStateException(SAVE_FAILED))
                            : Mono.defer(() -> storage.save(subscriptionId, checkpoint, condition)));
                }
                return storage.save(subscriptionId, checkpoint, condition);
            });
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return storage.evaluatesWriteConditions();
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return storage.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return heldDelete(() -> storage.delete(subscriptionId));
        }

        @Override
        public Mono<Void> delete(String subscriptionId, CheckpointWriteCondition condition) {
            return conditionalDeletes ? heldDelete(() -> storage.delete(subscriptionId, condition)) : CheckpointStorage.super.delete(subscriptionId, condition);
        }

        @Override
        public boolean evaluatesDeleteConditions() {
            return conditionalDeletes;
        }

        private Mono<Void> heldDelete(java.util.function.Supplier<Mono<Void>> delete) {
            return Mono.defer(() -> {
                if (nextDeleteAppliesThenFails) {
                    nextDeleteAppliesThenFails = false;
                    return delete.get()
                            .then(Mono.<Void>error(new IllegalStateException(DELETE_FAILED)))
                            .doOnError(__ -> appliedDeleteFailed.countDown());
                }
                @Nullable CountDownLatch gate = deleteGate;
                if (gate == null) {
                    return delete.get();
                }
                deleteEntered.countDown();
                return held(gate).then(Mono.defer(delete));
            });
        }

        private static Mono<Boolean> held(CountDownLatch gate) {
            return Mono.fromCallable(() -> gate.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).subscribeOn(Schedulers.boundedElastic());
        }

        private static void awaitOrFail(CountDownLatch gate) {
            try {
                if (!gate.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Held read was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    // Delivers every event written after where a subscription starts, and answers where it is at once
    private static class Feed implements CheckpointAwareSubscriptionModel {
        final AtomicLong present = new AtomicLong();
        private final Sinks.Many<CloudEvent> written = Sinks.many().replay().all();

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.defer(() -> {
                long start = startOf(startAt, present.get());
                return written.asFlux().filter(event -> Long.parseLong(event.getId()) > start);
            });
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.fromSupplier(() -> new StringBasedCheckpoint(String.valueOf(present.get())));
        }

        synchronized long write() {
            long position = present.incrementAndGet();
            CloudEvent event = CloudEventBuilder.v1().withId(String.valueOf(position)).withSource(URI.create("urn:test")).withType("Something").build();
            written.tryEmitNext(new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(String.valueOf(position))));
            return position;
        }

        private static long startOf(StartAt startAt, long present) {
            @Nullable StartAt resolved = startAt;
            while (resolved != null && resolved.isDynamic()) {
                resolved = resolved.get(new SubscriptionModelContext(ReactorDurableSubscriptionModelDeleteTakenOverTest.class));
            }
            if (resolved == null || resolved.isNow() || resolved.isDefault()) {
                return present;
            }
            return Long.parseLong(resolved.toString());
        }
    }

    // A feed for subscriptions it manages by name, each reading the events written after where it starts on a thread
    // of its own, so a write does not wait for an action. While startFails is set, a subscribe keeps nothing of the
    // id and fails its start with START_FAILED, as ReactorMongoSubscriptionModel does after an error it can't
    // recover from.
    private static final class NamedFeed extends Feed implements SubscriptionModel, QuietPositionReportingSubscriptions {
        private final Map<String, Disposable> subscriptions = new ConcurrentHashMap<>();
        private final List<QuietPositionListener> quietPositionListeners = new CopyOnWriteArrayList<>();
        private volatile boolean startFails;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            if (startFails) {
                return new Subscription() {
                    @Override
                    public String id() {
                        return subscriptionId;
                    }

                    @Override
                    public Mono<Void> waitUntilStarted() {
                        return Mono.error(new IllegalStateException(START_FAILED));
                    }
                };
            }
            Disposable reading = subscribe(filter, startAt).publishOn(Schedulers.boundedElastic()).concatMap(action).subscribe();
            if (subscriptions.putIfAbsent(subscriptionId, reading) != null) {
                reading.dispose();
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }

        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
            quietPositionListeners.add(listener);
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
            quietPositionListeners.remove(listener);
        }

        // The function that saves a quiet position, as offered for the next read of the id, or null when none is
        private @Nullable Function<Checkpoint, Mono<Void>> quietPositionSaverFor(String subscriptionId) {
            return quietPositionListeners.getFirst().beforeReading(subscriptionId).block(TIMEOUT);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            @Nullable Disposable reading = subscriptions.remove(subscriptionId);
            if (reading != null) {
                reading.dispose();
            }
            return Mono.empty();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public void stop() {
        }

        @Override
        public void shutdown() {
            subscriptions.values().forEach(Disposable::dispose);
            subscriptions.clear();
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }
    }

    // A feed for subscriptions it manages by name that it pauses, resumes, stops and starts. A subscription moves past an
    // event only once its action for it has ended, and a pause ends the action under way, so a resume delivers that
    // event again. A subscribe while stopped keeps the subscription paused. It refuses a duplicate subscribe, unless
    // it was made to let a subscribe replace the subscription of the id. holdSubscribe and holdCancel hold the call of
    // that number, counted from the first, until letGo.
    private static final class PausableFeed extends Feed implements SubscriptionModel {
        private final boolean replaces;
        private final Map<String, Reading> readings = new ConcurrentHashMap<>();
        private final AtomicInteger subscribes = new AtomicInteger();
        private final AtomicInteger cancels = new AtomicInteger();
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch letGo = new CountDownLatch(1);
        private volatile int heldSubscribe = -1;
        private volatile int heldCancel = -1;
        private volatile boolean cancelHeldBeforeItRemoves;
        private boolean stopped;

        private PausableFeed(boolean replaces) {
            this.replaces = replaces;
        }

        // Held once the subscription is in this model, before it reads anything
        private CountDownLatch holdSubscribe(int number) {
            heldSubscribe = number;
            return held;
        }

        private CountDownLatch holdCancel(int number, boolean beforeItRemoves) {
            cancelHeldBeforeItRemoves = beforeItRemoves;
            heldCancel = number;
            return held;
        }

        private void letGo() {
            letGo.countDown();
        }

        private void holdHere() {
            held.countDown();
            try {
                letGo.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            int number = subscribes.incrementAndGet();
            Reading reading = new Reading(action, Feed.startOf(startAt, present.get()));
            synchronized (this) {
                if (!replaces && readings.containsKey(subscriptionId)) {
                    throw new DuplicateSubscriptionIdException(subscriptionId);
                }
                @Nullable Reading replaced = readings.put(subscriptionId, reading);
                if (replaced != null) {
                    replaced.stopReading();
                }
                reading.paused = stopped;
            }
            if (number == heldSubscribe) {
                holdHere();
            }
            synchronized (this) {
                if (readings.get(subscriptionId) == reading && !reading.paused) {
                    reading.read();
                }
            }
            return started(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            Reading reading = known(subscriptionId);
            if (reading.paused) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is already paused");
            }
            reading.paused = true;
            reading.stopReading();
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            Reading reading = known(subscriptionId);
            if (!reading.paused) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is already running");
            }
            reading.paused = false;
            reading.read();
            return started(subscriptionId);
        }

        private Reading known(String subscriptionId) {
            @Nullable Reading reading = readings.get(subscriptionId);
            if (reading == null) {
                throw new IllegalStateException("No subscription " + subscriptionId);
            }
            return reading;
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            int number = cancels.incrementAndGet();
            if (number == heldCancel && cancelHeldBeforeItRemoves) {
                holdHere();
            }
            synchronized (this) {
                @Nullable Reading reading = readings.remove(subscriptionId);
                if (reading != null) {
                    reading.stopReading();
                }
            }
            if (number == heldCancel && !cancelHeldBeforeItRemoves) {
                holdHere();
            }
            return Mono.empty();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            stopped = false;
            if (resumeSubscriptionsAutomatically) {
                readings.values().stream().filter(reading -> reading.paused).forEach(reading -> {
                    reading.paused = false;
                    reading.read();
                });
            }
        }

        @Override
        public synchronized void stop() {
            stopped = true;
            readings.values().stream().filter(reading -> !reading.paused).forEach(reading -> {
                reading.paused = true;
                reading.stopReading();
            });
        }

        @Override
        public synchronized void shutdown() {
            readings.values().forEach(Reading::stopReading);
            readings.clear();
        }

        @Override
        public synchronized boolean isRunning() {
            return !stopped;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            @Nullable Reading reading = readings.get(subscriptionId);
            return reading != null && !reading.paused;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            @Nullable Reading reading = readings.get(subscriptionId);
            return reading != null && reading.paused;
        }

        private static Subscription started(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }

        private final class Reading {
            private final Function<CloudEvent, Mono<Void>> action;
            private volatile long handled;
            private volatile boolean paused;
            private volatile @Nullable Disposable reading;

            private Reading(Function<CloudEvent, Mono<Void>> action, long startsAfter) {
                this.action = action;
                this.handled = startsAfter;
            }

            private void read() {
                reading = PausableFeed.this.subscribe(null, StartAt.checkpoint(new StringBasedCheckpoint(String.valueOf(handled))))
                        .publishOn(Schedulers.boundedElastic())
                        .concatMap(event -> action.apply(event).then(Mono.fromRunnable(() -> handled = Long.parseLong(event.getId()))))
                        .subscribe();
            }

            private void stopReading() {
                @Nullable Disposable current = reading;
                if (current != null) {
                    current.dispose();
                }
            }
        }
    }
}
