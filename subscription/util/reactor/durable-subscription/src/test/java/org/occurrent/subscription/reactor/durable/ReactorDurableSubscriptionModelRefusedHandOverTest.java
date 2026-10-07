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
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscribe from the subscription-model default that the wrapped model refuses at the hand-over stores nothing for
 * the id, and an event the wrapped model delivers waits for the first position to be stored.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelRefusedHandOverTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final String NOTHING_STORED = "-";

    private final ProbeStorage storage = new ProbeStorage();
    private final ExecutorService caller = Executors.newCachedThreadPool();
    private final LoggedByTheModel logged = new LoggedByTheModel();
    private TwoStepFeed feed = new TwoStepFeed();
    private ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);

    @AfterEach
    void shutdown() {
        storage.releaseRead.complete(null);
        storage.releaseSave.complete(null);
        logged.close();
        caller.shutdownNow();
        model.shutdown();
    }

    static Stream<Arguments> callsThatMoveTheSubscriptionInTwoSteps() {
        return Stream.of(
                Arguments.of(Named.<Consumer<ReactorDurableSubscriptionModelRefusedHandOverTest>>of("a stop of the durable model", test -> test.model.stop())),
                Arguments.of(Named.<Consumer<ReactorDurableSubscriptionModelRefusedHandOverTest>>of("a start of the durable model that resumes", test -> {
                    test.feed.moving = __ -> {
                    };
                    test.model.stop();
                    test.feed.moving = test.feed.armed;
                    test.model.start(true);
                })),
                Arguments.of(Named.<Consumer<ReactorDurableSubscriptionModelRefusedHandOverTest>>of("a pause the wrapped model makes itself", test -> test.feed.pauseSubscription(SUBSCRIPTION_ID))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("callsThatMoveTheSubscriptionInTwoSteps")
    void a_duplicate_subscribe_the_wrapped_model_refuses_only_at_the_hand_over_stores_nothing(Consumer<ReactorDurableSubscriptionModelRefusedHandOverTest> moveTheSubscription) {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        feed.present.set(5);
        AtomicReference<Object> duplicate = new AtomicReference<>();
        feed.armed = __ -> duplicate.set(subscribeFromTheModelDefaultOnAnotherThread());
        feed.moving = feed.armed;

        // When
        moveTheSubscription.accept(this);

        // Then
        assertThat(duplicate.get()).as("the duplicate subscribe made while the wrapped model moved the subscription").isNotNull();
        Throwable refused = duplicate.get() instanceof Subscription subscription
                ? catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT))
                : (Throwable) duplicate.get();
        assertThat(refused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(storage.stored()).as("the checkpoint stored for the id after the refused duplicate").isEqualTo(NOTHING_STORED);
        assertThat(feed.subscribed).as("subscriptions the wrapped model took").containsExactly(SUBSCRIPTION_ID);
    }

    @Test
    void a_duplicate_subscribe_is_refused_at_the_call_while_a_wrapped_model_that_lists_its_ids_moves_the_subscription() {
        // Given
        feed = new IntrospectableTwoStepFeed();
        model = new ReactorDurableSubscriptionModel(feed, storage);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        feed.present.set(5);
        AtomicReference<CompletableFuture<Subscription>> duplicate = new AtomicReference<>();
        // Waited for only briefly, since the wrapped model lists its ids only once the stop has released its lock
        feed.moving = __ -> {
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()), caller);
            duplicate.set(subscribed);
            catchThrowable(() -> subscribed.get(500, TimeUnit.MILLISECONDS));
        };

        // When
        model.stop();

        // Then
        assertThat(duplicate.get()).as("the duplicate subscribe made during the stop").failsWithin(TIMEOUT)
                .withThrowableOfType(java.util.concurrent.ExecutionException.class).withCauseInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(storage.stored()).as("the checkpoint stored for the id after the refused duplicate").isEqualTo(NOTHING_STORED);
    }

    @Test
    void a_subscribe_whose_filter_the_wrapped_model_refuses_stores_nothing() {
        // Given
        feed.present.set(4);
        feed.refusal = new IllegalArgumentException("unsupported filter");

        // When
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());

        // Then
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the subscription did not start").isInstanceOf(IllegalArgumentException.class).hasMessage("unsupported filter");
        assertThat(storage.stored()).as("the checkpoint stored for the id after the refused filter").isEqualTo(NOTHING_STORED);
        assertThat(storage.writes.get()).as("writes to storage").isZero();
    }

    @Test
    void an_event_the_wrapped_model_delivers_reaches_the_action_only_once_the_first_position_is_stored() throws Exception {
        // Given
        feed.present.set(7);
        storage.holdNextSave();
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.fromRunnable(() -> delivered.add(cloudEvent.getId())));
        assertThat(storage.heldSaveEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the first position write is held").isTrue();
        assertThat(feed.subscribed).as("subscriptions the wrapped model took before the first position is stored").containsExactly(SUBSCRIPTION_ID);

        // When
        CompletableFuture<Void> handled = feed.deliver(SUBSCRIPTION_ID, 8).toFuture();

        // Then
        assertThat(catchThrowable(() -> handled.get(300, TimeUnit.MILLISECONDS))).as("the delivery while the first position write is held").isNotNull();
        assertThat(delivered).as("events the action got while the first position write is held").isEmpty();
        storage.releaseSave.complete(null);
        assertThat(handled).as("the delivery once the first position is stored").succeedsWithin(TIMEOUT);
        assertThat(delivered).as("events the action got once the first position is stored").containsExactly("8");
    }

    @Test
    void a_cancel_while_the_first_position_is_written_leaves_nothing_running_or_stored() throws Exception {
        // Given
        feed.present.set(7);
        storage.holdNextSave();
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());
        assertThat(storage.heldSaveEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the first position write is held").isTrue();

        // When
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        storage.releaseSave.complete(null);

        // Then
        assertThat(cancelled).as("the cancel").succeedsWithin(TIMEOUT);
        // The wrapped model had taken the subscribe before the cancel came, so the start may have completed
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("what ended the wait for the start").satisfiesAnyOf(
                failure -> assertThat(failure).isNull(),
                failure -> assertThat(failure).isInstanceOf(CancellationException.class));
        assertThat(logged.at(Level.ERROR)).as("errors logged").isEmpty();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.stored()).as("the checkpoint stored for the id after the cancel").isEqualTo(NOTHING_STORED));
        assertThat(feed.running).as("subscriptions running in the wrapped model").isEmpty();
        assertThat(feed.paused).as("subscriptions paused in the wrapped model").isEmpty();
    }

    @Test
    void a_shutdown_while_the_start_position_is_read_ends_the_start_and_hands_nothing_over() throws Exception {
        // Given
        storage.holdNextRead();
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());
        assertThat(storage.heldReadEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the start position read is held").isTrue();
        CompletableFuture<Void> started = subscription.waitUntilStarted().toFuture();

        // When
        model.shutdown();

        // Then
        Throwable notStarted = catchThrowable(() -> started.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertThat(notStarted).as("why the subscription did not start").hasCauseInstanceOf(SubscriptionModelShutdownException.class);
        storage.releaseRead.complete(null);
        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(feed.subscribed).as("subscriptions the wrapped model took").isEmpty();
            assertThat(storage.writes.get()).as("writes to storage").isZero();
        });
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the shutdown").isFalse();
        assertThat(logged.at(Level.ERROR)).as("errors logged").isEmpty();
    }

    @Test
    void a_read_of_the_start_position_that_does_not_answer_is_logged_at_warn_every_10_seconds_until_a_shutdown_ends_it() throws Exception {
        // Given
        storage.holdNextRead();
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());

        // When
        await().atMost(Duration.ofSeconds(15)).until(() -> !stillWaiting().isEmpty());
        model.shutdown();
        int warnedBeforeTheShutdown = stillWaiting().size();

        // Then
        assertThat(stillWaiting()).as("warnings while the read did not answer").first().asString()
                .startsWith("Subscription " + SUBSCRIPTION_ID + " is still waiting for its start position to be read, after 10 seconds.");
        assertThat(catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT))).as("why the subscription did not start").isInstanceOf(SubscriptionModelShutdownException.class);
        Thread.sleep(10_500);
        assertThat(stillWaiting()).as("warnings in the 10.5 seconds after the shutdown").hasSize(warnedBeforeTheShutdown);
        assertThat(waitingForACallToTheWrappedModel()).as("warnings of a wait for a call to the wrapped model, with no such call made").isEmpty();
    }

    @Test
    void a_subscribe_that_waits_for_one_call_to_the_wrapped_model_after_another_is_logged_at_warn_counted_from_the_first_wait() throws Exception {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        CompletableFuture<Subscription> duplicate = subscribeFromTheModelDefaultWhileTheWrappedModelPausesTheSubscription();
        feed.resumeSubscription(SUBSCRIPTION_ID);
        AtomicInteger callsLeft = new AtomicInteger(4);
        model.runOnceNoWrappedCallIsInFlight(() -> {
            if (callsLeft.getAndDecrement() > 0) {
                sendACallTheWrappedModelTakes(Duration.ofSeconds(3));
            }
        });

        // When
        storage.releaseRead.complete(null);
        Subscription subscription = duplicate.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Throwable duplicateRefused = catchThrowable(() -> subscription.waitUntilStarted().block(Duration.ofSeconds(30)));

        // Then
        assertThat(duplicateRefused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(callsLeft.get()).as("calls left to send once the duplicate was refused").isNegative();
        assertThat(waitingForACallToTheWrappedModel()).as("warnings while 4 calls of 3 seconds each were sent to the wrapped model one after another").first().asString()
                .startsWith("Subscription " + SUBSCRIPTION_ID + " is still waiting for a call this model made to the wrapped model")
                .contains(", 10 seconds after its hand-over to that model started.");
    }

    // The hand-over checks for a call in flight before it reads the start position and again before it calls the
    // wrapped model, and here it waits 7 seconds at each check
    @Test
    void a_subscribe_that_waits_for_a_call_to_the_wrapped_model_before_and_after_its_start_position_is_read_is_logged_at_warn_counted_from_the_start_of_the_hand_over() throws Exception {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        storage.holdNextRead();
        storage.releaseRead.complete(null);
        CountDownLatch pausedByTheWrappedModel = new CountDownLatch(1);
        AtomicBoolean readBeforeTheSecondCall = new AtomicBoolean();
        AtomicInteger noCallInFlight = new AtomicInteger();
        model.runOnceNoWrappedCallIsInFlight(() -> {
            int found = noCallInFlight.incrementAndGet();
            try {
                if (found == 1 && pausedByTheWrappedModel.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    sendACallTheWrappedModelTakes(Duration.ofSeconds(7));
                } else if (found == 3) {
                    readBeforeTheSecondCall.set(storage.heldReadEntered.getCount() == 0);
                    sendACallTheWrappedModelTakes(Duration.ofSeconds(7));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        AtomicReference<CompletableFuture<Subscription>> duplicate = new AtomicReference<>();
        feed.moving = __ -> {
            duplicate.set(CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()), caller));
            duplicate.get().join();
        };
        feed.pauseSubscription(SUBSCRIPTION_ID);
        feed.moving = __ -> {
        };

        // When
        pausedByTheWrappedModel.countDown();
        Subscription subscription = duplicate.get().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Throwable duplicateRefused = catchThrowable(() -> subscription.waitUntilStarted().block(Duration.ofSeconds(30)));

        // Then
        assertThat(duplicateRefused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(noCallInFlight.get()).as("times the hand-over found no call in flight").isEqualTo(4);
        assertThat(readBeforeTheSecondCall).as("the start position read before the second call was sent").isTrue();
        assertThat(waitingForACallToTheWrappedModel()).as("warnings while the hand-over waited 7 seconds for a call before the start position was read and 7 seconds for one after").singleElement().asString()
                .startsWith("Subscription " + SUBSCRIPTION_ID + " is still waiting for a call this model made to the wrapped model")
                .contains(", 10 seconds after its hand-over to that model started.");
    }

    // The thread that ends a call to the wrapped model goes on once the hand-over is on its way to a thread of the
    // model's own. Here that thread waits until the hand-over has found the next call and started to wait again.
    @Test
    void a_subscribe_that_waits_again_before_the_thread_that_ended_the_call_before_has_gone_on_is_logged_at_warn() throws Exception {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        CompletableFuture<Subscription> duplicate = subscribeFromTheModelDefaultWhileTheWrappedModelPausesTheSubscription();
        feed.resumeSubscription(SUBSCRIPTION_ID);
        AtomicReference<@Nullable Thread> endsTheFirstCall = new AtomicReference<>();
        AtomicBoolean heldBack = new AtomicBoolean();
        Schedulers.onScheduleHook(SUBSCRIPTION_ID, scheduled -> {
            if (Thread.currentThread() != endsTheFirstCall.get() || !heldBack.compareAndSet(false, true)) {
                return scheduled;
            }
            Thread elsewhere = new Thread(scheduled);
            elsewhere.start();
            try {
                elsewhere.join(TIMEOUT.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return () -> {
            };
        });
        AtomicInteger noCallInFlight = new AtomicInteger();
        model.runOnceNoWrappedCallIsInFlight(() -> {
            int found = noCallInFlight.incrementAndGet();
            if (found == 1) {
                endsTheFirstCall.set(sendACallTheWrappedModelTakes(Duration.ofSeconds(2)));
            } else if (found == 2) {
                sendACallTheWrappedModelTakes(Duration.ofSeconds(12));
            }
        });

        // When
        Throwable duplicateRefused;
        try {
            storage.releaseRead.complete(null);
            Subscription subscription = duplicate.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            duplicateRefused = catchThrowable(() -> subscription.waitUntilStarted().block(Duration.ofSeconds(30)));
        } finally {
            Schedulers.resetOnScheduleHook(SUBSCRIPTION_ID);
        }

        // Then
        assertThat(duplicateRefused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(heldBack).as("the thread that ended the first call held back until the hand-over waited again").isTrue();
        assertThat(waitingForACallToTheWrappedModel()).as("warnings while the hand-over waited 12 seconds for the second call").first().asString()
                .startsWith("Subscription " + SUBSCRIPTION_ID + " is still waiting for a call this model made to the wrapped model")
                .contains(", 10 seconds after its hand-over to that model started.");
    }

    @Test
    void a_resume_kept_for_a_duplicate_refused_at_the_hand_over_reaches_the_subscription_the_wrapped_model_holds() throws Exception {
        // Given
        feed.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty());
        CompletableFuture<Subscription> duplicate = subscribeFromTheModelDefaultWhileTheWrappedModelPausesTheSubscription();

        // When
        Subscription resumed = model.resumeSubscription(SUBSCRIPTION_ID);
        Throwable duplicateRefused = handOver(duplicate);

        // Then
        assertThat(duplicateRefused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("the subscription the wrapped model holds running after the resume").isTrue();
        assertThat(catchThrowable(() -> resumed.waitUntilStarted().block(TIMEOUT))).as("how waiting for what the resume returned ended").isNull();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).as("isPaused once the duplicate is refused").isFalse();
    }

    @Test
    void a_pause_kept_for_a_duplicate_refused_at_the_hand_over_reaches_the_subscription_the_wrapped_model_holds() throws Exception {
        // Given
        feed.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty());
        CompletableFuture<Subscription> duplicate = subscribeFromTheModelDefaultWhileTheWrappedModelPausesTheSubscription();
        feed.resumeSubscription(SUBSCRIPTION_ID);

        // When
        model.pauseSubscription(SUBSCRIPTION_ID);
        Throwable duplicateRefused = handOver(duplicate);

        // Then
        assertThat(duplicateRefused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(feed.isPaused(SUBSCRIPTION_ID)).as("the subscription the wrapped model holds paused after the pause").isTrue();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).as("isPaused once the duplicate is refused").isTrue();
    }

    // A stop asks the duplicate's state as well as reaching the wrapped model, and the resume after the start passes to
    // the wrapped model for the subscription this model registered
    @Test
    void a_stop_kept_for_a_duplicate_refused_at_the_hand_over_does_not_undo_a_resume_made_after_it() throws Exception {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        CompletableFuture<Subscription> duplicate = subscribeFromTheModelDefaultWhileTheWrappedModelPausesTheSubscription();
        feed.resumeSubscription(SUBSCRIPTION_ID);

        // When
        model.stop();
        model.start(false);
        model.resumeSubscription(SUBSCRIPTION_ID);
        Throwable duplicateRefused = handOver(duplicate);

        // Then
        assertThat(duplicateRefused).as("why the duplicate did not start").isInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("the registered subscription running after the resume").isTrue();
    }

    // The wrapped model pauses the subscription itself, and between its two steps, where it answers neither running nor
    // paused for the id, a subscribe of the id from the model default passes the check at the call. Its read of the
    // start position is held, so it waits to be handed over.
    private CompletableFuture<Subscription> subscribeFromTheModelDefaultWhileTheWrappedModelPausesTheSubscription() throws Exception {
        storage.holdNextRead();
        AtomicReference<CompletableFuture<Subscription>> duplicate = new AtomicReference<>();
        feed.moving = __ -> {
            duplicate.set(CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()), caller));
            try {
                storage.heldReadEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        feed.pauseSubscription(SUBSCRIPTION_ID);
        feed.moving = __ -> {
        };
        assertThat(storage.heldReadEntered.getCount()).as("reads of the duplicate's start position held").isZero();
        assertThat(duplicate.get().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the duplicate returned by the call").isNotNull();
        return duplicate.get();
    }

    // Lets the read of the start position go on, and answers what the hand-over failed the duplicate with
    private Throwable handOver(CompletableFuture<Subscription> duplicate) throws Exception {
        storage.releaseRead.complete(null);
        Subscription subscription = duplicate.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
    }

    // A pause, or a resume of a paused subscription, made on another thread, that the wrapped model takes for as long as
    // given. Returns, once the wrapped model has the call, the thread that makes it, which also ends it.
    private Thread sendACallTheWrappedModelTakes(Duration taking) {
        CountDownLatch taken = new CountDownLatch(1);
        AtomicReference<Thread> making = new AtomicReference<>();
        feed.moving = __ -> {
            making.set(Thread.currentThread());
            taken.countDown();
            try {
                Thread.sleep(taking.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        boolean pause = feed.isRunning(SUBSCRIPTION_ID);
        caller.execute(() -> {
            if (pause) {
                model.pauseSubscription(SUBSCRIPTION_ID);
            } else {
                model.resumeSubscription(SUBSCRIPTION_ID);
            }
        });
        try {
            assertThat(taken.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the wrapped model has the call").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return making.get();
    }

    private List<String> waitingForACallToTheWrappedModel() {
        return logged.at(Level.WARN).stream().filter(message -> message.contains("for a call this model made to the wrapped model")).collect(Collectors.toList());
    }

    private List<String> stillWaiting() {
        return logged.at(Level.WARN).stream().filter(message -> message.contains("is still waiting for its start position")).collect(Collectors.toList());
    }

    // Made on another thread, as the wrapped model holds its lock while it moves the subscription
    private Object subscribeFromTheModelDefaultOnAnotherThread() {
        try {
            return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()), caller)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return e.getCause() == null ? e : e.getCause();
        }
    }

    private static final class ProbeStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        private final AtomicInteger writes = new AtomicInteger();
        private final CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseSave = new CompletableFuture<>();
        private final CountDownLatch heldReadEntered = new CountDownLatch(1);
        private final CountDownLatch heldSaveEntered = new CountDownLatch(1);
        private final AtomicBoolean holdsNextRead = new AtomicBoolean();
        private final AtomicBoolean holdsNextSave = new AtomicBoolean();

        void holdNextRead() {
            holdsNextRead.set(true);
        }

        void holdNextSave() {
            holdsNextSave.set(true);
        }

        String stored() {
            @Nullable Checkpoint checkpoint = storage.read(SUBSCRIPTION_ID).block();
            return checkpoint == null ? NOTHING_STORED : checkpoint.asString();
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                if (holdsNextRead.compareAndSet(true, false)) {
                    heldReadEntered.countDown();
                    return Mono.fromFuture(releaseRead, true).then(storage.read(subscriptionId));
                }
                return storage.read(subscriptionId);
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
            return Mono.defer(() -> {
                writes.incrementAndGet();
                return held().then(storage.save(subscriptionId, checkpoint));
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                writes.incrementAndGet();
                return held().then(storage.save(subscriptionId, checkpoint, condition));
            });
        }

        private Mono<Void> held() {
            if (holdsNextSave.compareAndSet(true, false)) {
                heldSaveEntered.countDown();
                return Mono.fromFuture(releaseSave, true);
            }
            return Mono.empty();
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
        public boolean evaluatesDeleteConditions() {
            return storage.evaluatesDeleteConditions();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> {
                writes.incrementAndGet();
                return storage.delete(subscriptionId);
            });
        }

        @Override
        public Mono<Void> delete(String subscriptionId, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                writes.incrementAndGet();
                return storage.delete(subscriptionId, condition);
            });
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return storage.resolveFirstCheckpointRace(subscriptionId, candidate);
        }
    }

    // Moves a subscription between running and paused in two steps under its own lock, and answers isRunning and
    // isPaused without that lock, as ReactorMongoSubscriptionModel does. moving runs between the two steps.
    private static class TwoStepFeed implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        final AtomicLong present = new AtomicLong();
        final Set<String> running = ConcurrentHashMap.newKeySet();
        final Set<String> paused = ConcurrentHashMap.newKeySet();
        final List<String> subscribed = new CopyOnWriteArrayList<>();
        private final Map<String, Function<CloudEvent, Mono<Void>>> actions = new ConcurrentHashMap<>();
        volatile Consumer<String> moving = __ -> {
        };
        volatile Consumer<String> armed = __ -> {
        };
        volatile @Nullable RuntimeException refusal;
        private volatile boolean isRunning = true;

        Mono<Void> deliver(String subscriptionId, long position) {
            CloudEvent event = CloudEventBuilder.v1().withId(String.valueOf(position)).withSource(URI.create("urn:test")).withType("Something").build();
            return actions.get(subscriptionId).apply(new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(String.valueOf(position))));
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.never();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.fromSupplier(() -> new StringBasedCheckpoint(String.valueOf(present.get())));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            RuntimeException refused = refusal;
            if (refused != null) {
                throw refused;
            }
            synchronized (this) {
                if (running.contains(subscriptionId) || paused.contains(subscriptionId)) {
                    throw new DuplicateSubscriptionIdException(subscriptionId);
                }
                subscribed.add(subscriptionId);
                actions.put(subscriptionId, action);
                (isRunning ? running : paused).add(subscriptionId);
            }
            return started(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            if (running.remove(subscriptionId)) {
                moving.accept(subscriptionId);
                paused.add(subscriptionId);
            }
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            if (paused.remove(subscriptionId)) {
                moving.accept(subscriptionId);
                running.add(subscriptionId);
            }
            return started(subscriptionId);
        }

        @Override
        public synchronized Mono<Void> cancelSubscription(String subscriptionId) {
            running.remove(subscriptionId);
            paused.remove(subscriptionId);
            actions.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public synchronized void stop() {
            isRunning = false;
            List.copyOf(running).forEach(this::pauseSubscription);
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            isRunning = true;
            if (resumeSubscriptionsAutomatically) {
                List.copyOf(paused).forEach(this::resumeSubscription);
            }
        }

        @Override
        public synchronized void shutdown() {
            running.clear();
            paused.clear();
        }

        @Override
        public boolean isRunning() {
            return isRunning;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
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
    }

    // Lists its ids under the lock it moves a subscription under, as ReactorMongoSubscriptionModel does
    private static final class IntrospectableTwoStepFeed extends TwoStepFeed implements IntrospectableSubscriptions {
        @Override
        public synchronized Set<String> subscriptionIds() {
            return Stream.concat(running.stream(), paused.stream()).collect(Collectors.toUnmodifiableSet());
        }
    }
}
