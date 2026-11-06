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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscription whose dynamic start position answers {@code StartAt.now()}, handed by the durable model to a wrapped
 * model that manages named subscriptions. It starts from where the feed was at the subscribe call, which is read
 * before the function runs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelDynamicNowHandOverTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration RETURNS_AT_ONCE = Duration.ofSeconds(5);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final String READ_FAILED = "The wrapped model cannot tell where its feed is right now";
    private static final String REFUSED = "The wrapped model does not support this filter";

    private final NamedFeed feed = new NamedFeed();
    private final ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
    private final CompletableFuture<Void> release = new CompletableFuture<>();
    private final CompletableFuture<Void> readCancelled = new CompletableFuture<>();
    private final ExecutorService caller = Executors.newSingleThreadExecutor();
    private final LoggedByTheModel logged = new LoggedByTheModel();

    @AfterEach
    void shutdown() {
        release.complete(null);
        logged.close();
        caller.shutdownNow();
        model.shutdown();
    }

    @Test
    void an_event_written_while_the_function_runs_is_delivered() {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        AtomicLong writtenWhileTheFunctionRan = new AtomicLong();
        StartAt startAt = StartAt.dynamic(() -> {
            writtenWhileTheFunctionRan.set(feed.write());
            return StartAt.now();
        });

        // When
        model.subscribe(SUBSCRIPTION_ID, null, startAt, action(delivered)).waitUntilStarted().block(TIMEOUT);
        long writtenAfter = feed.write();

        // Then
        await().atMost(TIMEOUT).until(() -> delivered.contains(writtenAfter));
        assertThat(delivered).as("events delivered").containsExactly(writtenWhileTheFunctionRan.get(), writtenAfter);
    }

    @Test
    void a_read_that_answers_nothing_fails_the_start_and_is_logged_as_an_error() {
        // Given
        feed.presentRead = atTheCall -> Mono.empty();

        // When
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, dynamicNow(), cloudEvent -> Mono.empty());

        // Then
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the subscription did not start").isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("The StartAt.dynamic(..) function of subscription " + SUBSCRIPTION_ID + " answered StartAt.now(), and the wrapped subscription model "
                                        + NamedFeed.class.getName() + " answered nothing when asked where its feed was when subscribe(..) was called.");
        assertThat(logged.at(Level.ERROR)).as("errors logged").anySatisfy(message -> assertThat(message).startsWith("Subscription " + SUBSCRIPTION_ID + " could not be started"));
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the failed start").isFalse();
    }

    @Test
    void the_id_can_be_subscribed_again_without_a_cancel_after_a_read_that_answers_nothing() {
        // Given
        feed.presentRead = atTheCall -> Mono.empty();
        Subscription failed = model.subscribe(SUBSCRIPTION_ID, null, dynamicNow(), cloudEvent -> Mono.empty());
        Throwable notStarted = catchThrowable(() -> failed.waitUntilStarted().block(TIMEOUT));
        feed.presentRead = Mono::just;
        List<Long> delivered = new CopyOnWriteArrayList<>();

        // When
        Throwable subscribedAgain = catchThrowable(() -> model.subscribe(SUBSCRIPTION_ID, null, dynamicNow(), action(delivered)).waitUntilStarted().block(TIMEOUT));
        long writtenAfter = feed.write();

        // Then
        assertThat(notStarted).as("why the first subscribe did not start").isInstanceOf(IllegalStateException.class);
        assertThat(subscribedAgain).as("how the second subscribe of the id ended").isNull();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered to the second subscribe").contains(writtenAfter));
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").containsExactly(SUBSCRIPTION_ID);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model").isTrue();
    }

    @Test
    void a_read_that_answers_nothing_hands_the_subscription_over_at_now_when_configured_to_start_anyway() {
        // Given
        ReactorDurableSubscriptionModel startsAnyway = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage(),
                new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        List<Long> delivered = new CopyOnWriteArrayList<>();
        feed.presentRead = atTheCall -> Mono.empty();
        StartAt startAt = StartAt.dynamic(() -> {
            feed.write();
            return StartAt.now();
        });

        try {
            // When
            Throwable notStarted = catchThrowable(() -> startsAnyway.subscribe(SUBSCRIPTION_ID, null, startAt, action(delivered)).waitUntilStarted().block(TIMEOUT));
            long writtenAfter = feed.write();

            // Then
            assertThat(notStarted).as("how waiting for the start ended").isNull();
            // startWhenNoStartPositionCanBeRecorded accepts losing what is written before the wrapped model opens its
            // feed, so the event written while the function ran is skipped
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered once the subscription started").contains(writtenAfter));
            assertThat(delivered).as("events delivered").containsExactly(writtenAfter);
            assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model").isTrue();
        } finally {
            startsAnyway.shutdown();
        }
    }

    @Test
    void a_read_that_fails_is_read_again_and_the_subscription_starts_from_the_call() {
        // Given
        AtomicInteger reads = new AtomicInteger();
        // The first two reads fail, so the read that answers comes after a delay
        feed.presentRead = atTheCall -> Mono.defer(() -> reads.getAndIncrement() < 2 ? Mono.error(new IllegalStateException(READ_FAILED)) : Mono.just(atTheCall));
        List<Long> delivered = new CopyOnWriteArrayList<>();
        AtomicLong writtenWhileTheFunctionRan = new AtomicLong();
        StartAt startAt = StartAt.dynamic(() -> {
            writtenWhileTheFunctionRan.set(feed.write());
            return StartAt.now();
        });

        // When
        Throwable notStarted = catchThrowable(() -> model.subscribe(SUBSCRIPTION_ID, null, startAt, action(delivered)).waitUntilStarted().block(TIMEOUT));
        long writtenAfter = feed.write();

        // Then
        assertThat(notStarted).as("how waiting for the start ended").isNull();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered once the subscription started").contains(writtenAfter));
        assertThat(delivered).as("events delivered").containsExactly(writtenWhileTheFunctionRan.get(), writtenAfter);
        assertThat(failedReadWarnings()).as("warnings logged for a failed read").containsExactly(failedRead(1), failedRead(2));
        assertThat(logged.at(Level.ERROR)).as("errors logged").isEmpty();
    }

    @Test
    void a_refusal_of_the_wrapped_model_fails_the_start_instead_of_the_call_and_is_logged_as_an_error() {
        // Given
        feed.refusal = new IllegalArgumentException(REFUSED);

        // When
        CompletableFuture<Subscription> subscribed = subscribeFromADynamicNow();

        // Then
        assertThat(subscribed).as("the subscribe the wrapped model refuses").succeedsWithin(RETURNS_AT_ONCE);
        Throwable notStarted = catchThrowable(() -> subscribed.join().waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the subscription did not start").isInstanceOf(IllegalArgumentException.class).hasMessage(REFUSED);
        assertThat(logged.at(Level.ERROR)).as("errors logged").anySatisfy(message -> assertThat(message).startsWith("Subscription " + SUBSCRIPTION_ID + " could not be started"));
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the refusal").isFalse();
        assertThat(model.subscriptionIds()).as("subscription ids after the refusal").doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void a_pause_before_the_hand_over_leaves_the_subscription_paused_in_the_wrapped_model_until_it_is_resumed() {
        // Given
        subscribedWhileTheReadIsHeld();

        // When
        model.pauseSubscription(SUBSCRIPTION_ID);
        boolean pausedWhileHeld = model.isPaused(SUBSCRIPTION_ID);
        release.complete(null);

        // Then
        assertThat(pausedWhileHeld).as("paused while the read is held").isTrue();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.isPaused(SUBSCRIPTION_ID)).as("paused in the wrapped model once handed over").isTrue());
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once handed over").isFalse();

        model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted().block(TIMEOUT);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once resumed").isTrue();
    }

    @Test
    void a_cancel_before_the_hand_over_leaves_nothing_running_and_the_id_can_be_subscribed_again() {
        // Given
        Subscription subscription = subscribedWhileTheReadIsHeld();

        // When
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
        release.complete(null);

        // Then
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the cancelled subscription did not start").isInstanceOf(CancellationException.class);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the cancel").isFalse();

        model.subscribe(SUBSCRIPTION_ID, null, dynamicNow(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took once subscribed again").containsExactly(SUBSCRIPTION_ID);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once subscribed again").isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_cancel_while_the_read_is_held_fails_the_start_at_once_and_cancels_the_read(boolean readAgainAfterFailures) {
        // Given
        Subscription subscription = readAgainAfterFailures ? subscribedWhileTheReadAgainAfterFailuresIsHeld() : subscribedWhileTheReadIsHeld();

        // When
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);

        // Then
        assertThat(subscription.waitUntilStarted().toFuture()).as("waiting for the start of the cancelled subscription")
                .failsWithin(RETURNS_AT_ONCE).withThrowableThat().isInstanceOf(CancellationException.class);
        assertThat(readCancelled).as("the read held when the cancel came, cancelled").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(release).as("the release of the held read").isNotDone();
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
    }

    @Test
    void a_shutdown_while_the_read_again_after_failures_is_held_cancels_the_read() {
        // Given
        Subscription subscription = subscribedWhileTheReadAgainAfterFailuresIsHeld();

        // When
        model.shutdown();

        // Then
        assertThat(subscription.waitUntilStarted().toFuture()).as("waiting for the start of the subscription once shut down")
                .failsWithin(RETURNS_AT_ONCE).withThrowableOfType(ExecutionException.class).withCauseInstanceOf(SubscriptionModelShutdownException.class);
        assertThat(readCancelled).as("the read held when the shutdown came, cancelled").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"cancel", "shutdown"})
    void a_cancel_or_a_shutdown_while_a_failed_read_waits_to_be_read_again_fails_the_start_and_reads_no_more(String end) {
        // Given
        HeldDelays delays = new HeldDelays();
        Schedulers.Snapshot schedulers = Schedulers.setFactoryWithSnapshot(delays);

        try {
            AtomicInteger reads = readsThatFailTwice();
            CompletableFuture<Subscription> subscribed = subscribeFromADynamicNow();
            await().atMost(TIMEOUT).until(() -> delays.waiting() == 1);

            // When
            if (end.equals("cancel")) {
                model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            } else {
                model.shutdown();
            }
            int waitingAfterTheEnd = delays.waiting();
            delays.letGo();

            // Then
            Class<? extends Throwable> why = end.equals("cancel") ? CancellationException.class : SubscriptionModelShutdownException.class;
            Throwable notStarted = catchThrowable(() -> subscribed.join().waitUntilStarted().block(TIMEOUT));
            assertThat(notStarted).as("why the subscription did not start after the " + end).isInstanceOf(why);
            assertThat(waitingAfterTheEnd).as("retries of the read waiting after the " + end).isZero();
            assertThat(reads).as("reads of where the feed was, once the retries waiting were let go").hasValue(2);
            assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        } finally {
            Schedulers.resetFrom(schedulers);
        }
    }

    @Test
    void a_pause_and_a_resume_while_a_failed_read_waits_to_be_read_again_leave_the_subscription_running_from_the_call_once_handed_over() {
        // Given
        HeldDelays delays = new HeldDelays();
        Schedulers.Snapshot schedulers = Schedulers.setFactoryWithSnapshot(delays);
        List<Long> delivered = new CopyOnWriteArrayList<>();

        try {
            AtomicInteger reads = readsThatFailTwice();
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, dynamicNow(), action(delivered));
            await().atMost(TIMEOUT).until(() -> delays.waiting() == 1);
            long writtenWhileTheReadFailed = feed.write();

            // When
            model.pauseSubscription(SUBSCRIPTION_ID);
            boolean pausedWhileTheReadWaits = model.isPaused(SUBSCRIPTION_ID);
            Subscription resumed = model.resumeSubscription(SUBSCRIPTION_ID);
            delays.letGo();

            // Then
            Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            Throwable notResumed = catchThrowable(() -> resumed.waitUntilStarted().block(TIMEOUT));
            long writtenAfter = feed.write();
            assertThat(pausedWhileTheReadWaits).as("paused while the read waits to be read again").isTrue();
            assertThat(notStarted).as("how waiting for the start ended").isNull();
            assertThat(notResumed).as("how waiting for the resume ended").isNull();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered once handed over").contains(writtenAfter));
            assertThat(delivered).as("events delivered").containsExactly(writtenWhileTheReadFailed, writtenAfter);
            assertThat(reads).as("reads of where the feed was").hasValue(3);
            assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model").isTrue();
            assertThat(logged.at(Level.ERROR)).as("errors logged").isEmpty();
        } finally {
            Schedulers.resetFrom(schedulers);
        }
    }

    @Test
    void a_read_that_answers_nothing_hands_a_subscribe_that_waits_for_the_checkpoint_a_cancel_deleted_over_at_now_when_configured_to_start_anyway() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        ReactorDurableSubscriptionModel waits = new ReactorDurableSubscriptionModel(feed, storage,
                new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        List<Long> delivered = new CopyOnWriteArrayList<>();
        feed.presentRead = atTheCall -> Mono.empty();

        try {
            Subscription subscription = subscribedWhileACancelDeletes(waits, storage, StartAt.dynamic(() -> {
                feed.write();
                return StartAt.now();
            }), action(delivered));

            // When
            storage.releaseDelete.complete(null);
            Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            long writtenAfter = feed.write();

            // Then
            assertThat(notStarted).as("how waiting for the start ended").isNull();
            // startWhenNoStartPositionCanBeRecorded accepts losing what is written before the wrapped model opens its
            // feed, so the event written while the function ran is skipped
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered once the subscription started").contains(writtenAfter));
            assertThat(delivered).as("events delivered").containsExactly(writtenAfter);
            assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model").isTrue();
        } finally {
            storage.releaseDelete.complete(null);
            waits.shutdown();
        }
    }

    @Test
    void a_read_that_answers_nothing_fails_the_start_of_a_subscribe_that_waits_for_the_checkpoint_a_cancel_deleted_to_be_written_back() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        ReactorDurableSubscriptionModel waits = new ReactorDurableSubscriptionModel(feed, storage);
        AtomicBoolean asked = new AtomicBoolean();
        feed.presentRead = atTheCall -> Mono.empty();

        try {
            Subscription subscription = subscribedWhileACancelDeletes(waits, storage, StartAt.dynamic(() -> {
                asked.set(true);
                return StartAt.now();
            }), cloudEvent -> Mono.empty());
            boolean askedBeforeTheDeleteEnded = asked.get();

            // When
            storage.releaseDelete.complete(null);
            Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));

            // Then
            assertThat(askedBeforeTheDeleteEnded).as("the function asked before the delete ended").isFalse();
            assertThat(notStarted).as("why the subscription did not start").isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith("The StartAt.dynamic(..) function of subscription " + SUBSCRIPTION_ID + " answered StartAt.now(), and the wrapped subscription model "
                                            + NamedFeed.class.getName() + " answered nothing when asked where its feed was when subscribe(..) was called.");
            assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        } finally {
            storage.releaseDelete.complete(null);
            waits.shutdown();
        }
    }

    @Test
    void a_function_that_answers_another_dynamic_start_position_that_answers_now_starts_from_the_call_when_the_subscribe_waits_for_the_checkpoint_a_cancel_deleted() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        ReactorDurableSubscriptionModel waits = new ReactorDurableSubscriptionModel(feed, storage);
        AtomicBoolean asked = new AtomicBoolean();
        AtomicLong writtenWhileTheFunctionRan = new AtomicLong();
        List<Long> delivered = new CopyOnWriteArrayList<>();

        try {
            Subscription subscription = subscribedWhileACancelDeletes(waits, storage, StartAt.dynamic(() -> {
                asked.set(true);
                writtenWhileTheFunctionRan.set(feed.write());
                return dynamicNow();
            }), action(delivered));
            boolean askedBeforeTheDeleteEnded = asked.get();

            // When
            storage.releaseDelete.complete(null);
            Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            long writtenAfter = feed.write();

            // Then
            assertThat(askedBeforeTheDeleteEnded).as("the function asked before the delete ended").isFalse();
            assertThat(notStarted).as("how waiting for the start ended").isNull();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered once the subscription started").contains(writtenAfter));
            assertThat(delivered).as("events delivered").containsExactly(writtenWhileTheFunctionRan.get(), writtenAfter);
        } finally {
            storage.releaseDelete.complete(null);
            waits.shutdown();
        }
    }

    private List<String> failedReadWarnings() {
        return logged.at(Level.WARN).stream().filter(message -> message.startsWith("Could not read where the feed")).toList();
    }

    private static String failedRead(int attempt) {
        return "Could not read where the feed was when subscription " + SUBSCRIPTION_ID + " asked to start from the present, on attempt " + attempt
               + ". It is read again before the subscription opens its feed.";
    }

    private static StartAt dynamicNow() {
        return StartAt.dynamic(StartAt::now);
    }

    private CompletableFuture<Subscription> subscribeFromADynamicNow() {
        return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, dynamicNow(), cloudEvent -> Mono.empty()), caller);
    }

    private static Function<CloudEvent, Mono<Void>> action(List<Long> delivered) {
        return cloudEvent -> Mono.fromRunnable(() -> delivered.add(Long.parseLong(cloudEvent.getId())));
    }

    // The first two reads of where the feed was fail, so the read waits to be read again, and the third answers
    private AtomicInteger readsThatFailTwice() {
        AtomicInteger reads = new AtomicInteger();
        feed.presentRead = atTheCall -> Mono.defer(() -> reads.incrementAndGet() <= 2 ? Mono.error(new IllegalStateException(READ_FAILED)) : Mono.just(atTheCall));
        return reads;
    }

    // Holds the read of where the feed was until release completes. Only the read made after this is held.
    private Subscription subscribedWhileTheReadIsHeld() {
        feed.presentRead = this::heldUntilReleased;
        CompletableFuture<Subscription> subscribed = subscribeFromADynamicNow();
        assertThat(subscribed).as("the subscribe while the read is held").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took while the read is held").isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running while the read is held").isTrue();
        return subscribed.join();
    }

    // As subscribedWhileTheReadIsHeld, with the first two reads failing and the read made after a delay held
    private Subscription subscribedWhileTheReadAgainAfterFailuresIsHeld() {
        AtomicInteger reads = new AtomicInteger();
        CompletableFuture<Void> heldAfterTheFailures = new CompletableFuture<>();
        feed.presentRead = atTheCall -> Mono.defer(() -> reads.getAndIncrement() < 2
                ? Mono.error(new IllegalStateException(READ_FAILED))
                : heldUntilReleased(atTheCall).doOnSubscribe(__ -> heldAfterTheFailures.complete(null)));
        CompletableFuture<Subscription> subscribed = subscribeFromADynamicNow();
        assertThat(subscribed).as("the subscribe while the read is held").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(heldAfterTheFailures).as("the read made after the failed ones, held").succeedsWithin(TIMEOUT);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took while the read is held").isEmpty();
        return subscribed.join();
    }

    // Answers atTheCall once release completes, and completes readCancelled when cancelled before that
    private Mono<Checkpoint> heldUntilReleased(Checkpoint atTheCall) {
        return Mono.fromFuture(release, true).thenReturn(atTheCall).doOnCancel(() -> readCancelled.complete(null));
    }

    // Subscribes while a cancel of the id deletes a stored checkpoint, so the subscribe waits for that checkpoint to be
    // written back before it asks startAt
    private static Subscription subscribedWhileACancelDeletes(ReactorDurableSubscriptionModel model, HeldDeleteStorage storage, StartAt startAt,
                                                              Function<CloudEvent, Mono<Void>> action) {
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("0")).block(TIMEOUT);
        model.cancelSubscription(SUBSCRIPTION_ID).subscribe();
        assertThat(storage.deleteEntered).as("the delete of the cancel").succeedsWithin(TIMEOUT);
        return model.subscribe(SUBSCRIPTION_ID, null, startAt, action);
    }

    // Holds each delete until releaseDelete completes, and evaluates no delete condition
    private static final class HeldDeleteStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage stored = new InMemoryCheckpointStorage();
        private final CompletableFuture<Void> deleteEntered = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseDelete = new CompletableFuture<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return stored.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return stored.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return stored.evaluatesWriteConditions();
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return stored.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> {
                deleteEntered.complete(null);
                return Mono.fromFuture(releaseDelete, true);
            }).then(stored.delete(subscriptionId));
        }
    }

    // Manages named subscriptions by id and refuses a duplicate id. Each reads the events written after
    // where it starts and moves past an event once its action for it has ended, and a pause stops the reading until a
    // resume. globalCheckpointAsOfNow() hands where the feed is at the call to presentRead, which answers it by
    // default.
    private static final class NamedFeed implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private final AtomicLong present = new AtomicLong();
        private final Sinks.Many<CloudEvent> written = Sinks.many().replay().all();
        private final Map<String, Reading> readings = new ConcurrentHashMap<>();
        private final List<String> subscribedIds = new CopyOnWriteArrayList<>();
        private volatile Function<Checkpoint, Mono<Checkpoint>> presentRead = Mono::just;
        private volatile @Nullable RuntimeException refusal;

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

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return presentRead.apply(new StringBasedCheckpoint(String.valueOf(present.get())));
        }

        synchronized long write() {
            long position = present.incrementAndGet();
            CloudEvent event = CloudEventBuilder.v1().withId(String.valueOf(position)).withSource(URI.create("urn:test")).withType("Something").build();
            written.tryEmitNext(new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(String.valueOf(position))));
            return position;
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            @Nullable RuntimeException refused = refusal;
            if (refused != null) {
                throw refused;
            } else if (readings.containsKey(subscriptionId)) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            Reading reading = new Reading(action, startOf(startAt, present.get()));
            readings.put(subscriptionId, reading);
            subscribedIds.add(subscriptionId);
            reading.read();
            return started(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            @Nullable Reading reading = readings.get(subscriptionId);
            if (reading != null && !reading.paused) {
                reading.paused = true;
                reading.stopReading();
            }
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            @Nullable Reading reading = readings.get(subscriptionId);
            if (reading != null && reading.paused) {
                reading.paused = false;
                reading.read();
            }
            return started(subscriptionId);
        }

        @Override
        public synchronized Mono<Void> cancelSubscription(String subscriptionId) {
            @Nullable Reading reading = readings.remove(subscriptionId);
            if (reading != null) {
                reading.stopReading();
            }
            return Mono.empty();
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

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public void stop() {
        }

        @Override
        public synchronized void shutdown() {
            readings.values().forEach(Reading::stopReading);
            readings.clear();
        }

        private static long startOf(StartAt startAt, long present) {
            @Nullable StartAt resolved = startAt;
            while (resolved != null && resolved.isDynamic()) {
                resolved = resolved.get(new SubscriptionModelContext(ReactorDurableSubscriptionModelDynamicNowHandOverTest.class));
            }
            if (resolved == null || resolved.isNow() || resolved.isDefault()) {
                return present;
            }
            return Long.parseLong(resolved.toString());
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
                reading = NamedFeed.this.subscribe(null, StartAt.checkpoint(new StringBasedCheckpoint(String.valueOf(handled))))
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
