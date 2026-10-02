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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.LongStream;

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
    private static final String SAVE_FAILED = "The storage cannot save right now";
    private static final String READ_FAILED = "The storage cannot read right now";

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
     * over reached the store takes the delete over too. It starts from the checkpoint written back, or is refused with
     * the failure of that write, and either way skips no event.
     */
    @ParameterizedTest
    @ValueSource(strings = {"arrives", "fails"})
    void a_resume_while_a_write_back_is_held_takes_the_delete_over(String writeBack) throws Exception {
        // Given
        GatedStorage storage = new GatedStorage(false);
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        List<Long> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch releaseDelete = new CountDownLatch(1);
        CountDownLatch releaseWriteBack = new CountDownLatch(1);
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
            storage.ifAbsentFails = writeBack.equals("fails");
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
            if (writeBack.equals("arrives")) {
                untilDelivered(delivered, writtenAfter);
                assertThat(delivered).as("events delivered after the cancel").containsExactly(writtenBeforeTheWriteBack, writtenWhileHeld, writtenAfter);
                assertThat(refused).as("why the resume did not start").isNull();
            } else {
                if (refused == null) {
                    untilDelivered(delivered, writtenAfter);
                }
                assertThat(delivered).as("events delivered after the cancel").isEmpty();
                assertThat(refused).as("why the resume did not start").hasStackTraceContaining(SAVE_FAILED);
            }
        } finally {
            releaseDelete.countDown();
            releaseWriteBack.countDown();
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
    // right before the next write on the condition that nothing is stored, and settle a first-position race by
    // position.
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
    // of its own, so a write does not wait for an action
    private static final class NamedFeed extends Feed implements SubscriptionModel {
        private final Map<String, Disposable> subscriptions = new ConcurrentHashMap<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
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
}
