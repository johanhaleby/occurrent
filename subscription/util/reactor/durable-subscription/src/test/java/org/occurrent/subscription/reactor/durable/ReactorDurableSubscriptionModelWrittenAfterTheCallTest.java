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
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * An event written after {@code subscribe(..)} or {@code start(..)} returned reaches a subscription that starts from the
 * subscription-model default or from {@link StartAt#now()}, also when the subscribe comes while the delete of an earlier
 * cancel of the same id runs.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelWrittenAfterTheCallTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final String POSITION_READ_FAILED = "The position of the feed cannot be read right now";
    private static final String DELETE_FAILED = "The storage cannot delete right now";
    private static final String SAVE_FAILED = "The storage cannot save right now";

    /**
     * The model is stopped when the subscribe comes, and a cancel of the same id is still deleting its position. The
     * dynamic start position runs where it would without the delete. StartAt.now() starts from where the feed is at the
     * start, so what is written after the start is delivered. The model default starts from where the feed was at the
     * subscribe, so what is written between the subscribe and the start is delivered too.
     */
    @ParameterizedTest
    @CsvSource({"false, now", "false, default", "true, now", "true, default"})
    void an_event_written_after_start_returned_reaches_a_subscription_made_on_a_stopped_model_while_a_delete_runs(boolean handsOver, String answer) throws Exception {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(false) : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        if (!handsOver) {
            model.stop();
        }
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> answer(answer)), deliveredTo(delivered));
            long writtenBeforeTheStart = feed.write();
            // On another thread, so the release below comes even should the start block
            CompletableFuture<Void> started = CompletableFuture.runAsync(() -> model.start(true));
            storage.releaseDelete.countDown();
            started.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            List<Long> writtenAfterTheStart = List.of(feed.write(), feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheStart));
            if (answer.equals("default")) {
                assertThat(delivered).as("events delivered to the subscription from the model default").contains(String.valueOf(writtenBeforeTheStart));
            } else {
                assertThat(delivered).as("events delivered to the subscription from StartAt.now()").doesNotContain(String.valueOf(writtenBeforeTheStart));
            }
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The wrapped model answers where its feed is only some time after it is asked, as a model that asks a database
     * does, and the subscribe comes while the delete of an earlier cancel of the same id runs. The dynamic start
     * position runs at the subscribe, as it does without the delete, so what is written right after the subscribe
     * returned is delivered.
     */
    @ParameterizedTest
    @CsvSource({"false, now", "false, default", "true, now", "true, default"})
    void an_event_written_right_after_the_subscribe_returned_reaches_a_subscription_made_while_a_delete_runs_when_the_wrapped_model_answers_late(boolean handsOver, String answer) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        feed.answerDelay = Duration.ofMillis(300);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> answer(answer)), deliveredTo(delivered));
            List<Long> written = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            subscription.waitUntilStarted().block(TIMEOUT);
            written.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(written.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(written));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe on a thread where Reactor does not allow blocking comes while the delete of an earlier cancel of the
     * same id runs, with a dynamic start position that answers StartAt.now(), and the wrapped model answers where its
     * feed is only some time after it is asked. The subscription starts from where the feed was at the subscribe,
     * however late the read answers and whenever the subscription opens its feed, so what is written right after the
     * subscribe returned, while the delete still runs, is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_dynamic_start_position_answering_now_subscribed_on_a_thread_that_may_not_block_while_a_delete_runs_delivers_what_is_written_after_the_subscribe_returned(boolean handsOver) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        feed.answerDelay = Duration.ofMillis(300);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            Subscription subscription = requireNonNull(Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered)))
                    .subscribeOn(Schedulers.parallel())
                    .block(TIMEOUT));
            List<Long> written = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            written.add(feed.write());

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(written.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(written));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe on a thread where Reactor does not allow blocking comes while the delete of an earlier cancel of the
     * same id runs, with a dynamic start position that answers the model default, and is handed to a wrapped model that
     * manages named subscriptions. The function runs at the subscribe, and the model default is read there by
     * blocking, which Reactor refuses on such a thread. So the subscribe throws, as it does without the delete, and
     * nothing reaches the wrapped model.
     */
    @Test
    void a_dynamic_start_position_answering_the_model_default_is_refused_on_a_thread_that_may_not_block_while_a_delete_runs_when_handed_over() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        NamedFeed feed = new NamedFeed(true);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);

        try {
            // When
            Throwable thrown = catchThrowable(() -> Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::subscriptionModelDefault), __ -> Mono.empty()))
                    .subscribeOn(Schedulers.parallel())
                    .block(TIMEOUT));

            // Then
            assertThat(thrown).as("how the subscribe on a thread that may not block ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("are blocking");
            assertThat(feed.subscriptions).as("subscriptions handed to the wrapped model").isEmpty();
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The subscribe comes while the delete of an earlier cancel of the same id runs, with a dynamic start position that
     * answers StartAt.now(), and the read of where the feed is fails or answers nothing. A read that failed is read
     * again until it answers, and one that answers nothing has the subscription open its feed at StartAt.now(). Either
     * way the subscription starts, as it does without the delete, and what is written after the subscribe returned is
     * delivered.
     */
    @ParameterizedTest
    @CsvSource({"false, fails", "false, empty", "true, fails", "true, empty"})
    void a_dynamic_start_position_answering_now_starts_while_a_delete_runs_when_the_read_of_where_the_feed_is_fails_or_answers_nothing(boolean handsOver, String read) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            feed.readFails = read.equals("fails");
            feed.answersNothing = read.equals("empty");
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered));
            feed.readFails = false;
            feed.answersNothing = false;
            List<Long> written = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            written.add(feed.write());

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(written.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(written));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The model drives the feed itself and is stopped when a subscribe from the model default comes, and the wrapped
     * model answers where its feed is only some time after it is asked. The model default means where the feed was
     * when the subscription was registered, and the read answers with that moment however late it answers, so what is
     * written right after the subscribe returned is delivered once the model is started.
     */
    @Test
    void an_event_written_right_after_a_subscribe_on_a_stopped_model_returned_reaches_a_subscription_from_the_model_default_when_the_model_it_drives_answers_late() {
        // Given
        Feed feed = new Feed();
        feed.answerDelay = Duration.ofMillis(300);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered));
            long writtenRightAfterTheReturn = feed.write();
            model.start(true);
            long writtenAfterTheStart = feed.write();

            // Then
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered to the subscription")
                    .containsExactly(String.valueOf(writtenRightAfterTheReturn), String.valueOf(writtenAfterTheStart)));
        } finally {
            model.shutdown();
        }
    }

    /**
     * The storage fails every delete of an earlier cancel of the same id, as during an outage, and still holds the
     * checkpoint of the cancelled subscription. A subscribe from the model default takes the delete over, reads that
     * checkpoint and resumes from it, as it does without the delete, and returns without waiting for the delete. The
     * model makes no further try of the delete, so once the storage deletes again, the stored position is that of the
     * last event.
     */
    @Test
    void a_subscribe_from_the_model_default_resumes_from_the_checkpoint_the_storage_still_holds_while_a_delete_keeps_failing() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = new Feed();
        long stored = feed.write();
        long writtenAfterTheCheckpoint = feed.write();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint(String.valueOf(stored))).block(TIMEOUT);
        storage.deleteFails = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            // On another thread that may block, so a subscribe that waits for the delete fails the test instead of
            // hanging it
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)));
            Throwable subscribeThrew = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            assertThat(subscribeThrew).as("how the subscribe ended while the delete kept failing").isNull();
            long writtenRightAfterTheReturn = feed.write();
            Throwable thrown = catchThrowable(() -> subscribed.join().waitUntilStarted().block(TIMEOUT));
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheCheckpoint)));
            storage.deleteFails = false;
            storage.releaseDelete.countDown();

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended while the delete kept failing").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenRightAfterTheReturn)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(String.valueOf(writtenAfterTheCheckpoint), String.valueOf(writtenRightAfterTheReturn));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT)).as("position stored once the delete succeeded")
                    .isEqualTo(new StringBasedCheckpoint(String.valueOf(writtenRightAfterTheReturn))));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The wrapped model answers where its feed is only some time after it is asked, and this model drives the feed
     * itself. A subscribe from the model default records where the feed was when it was registered, and the read
     * answers with that moment however late it answers, so what is written right after the subscribe returned is
     * delivered rather than skipped by a position that answered after it.
     */
    @Test
    void an_event_written_right_after_the_subscribe_returned_reaches_a_subscription_from_the_model_default_when_the_model_it_drives_answers_late() {
        // Given
        Feed feed = new Feed();
        feed.answerDelay = Duration.ofMillis(300);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered));
            List<Long> writtenAfterTheReturn = new ArrayList<>(List.of(feed.write(), feed.write()));
            subscription.waitUntilStarted().block(TIMEOUT);
            writtenAfterTheReturn.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheReturn.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheReturn));
        } finally {
            model.shutdown();
        }
    }

    /**
     * A subscription from StartAt.now() that this model drives begins where the feed was when subscribe(..) was called,
     * however late it starts. The model is stopped at the subscribe, so what is written between the subscribe and the
     * start is delivered once the model is started.
     */
    @Test
    void a_subscription_from_now_registered_on_a_stopped_model_delivers_what_is_written_before_the_model_is_started() {
        // Given
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), deliveredTo(delivered));
            long writtenBeforeTheStart = feed.write();
            model.start(true);
            // The handle a subscribe on a stopped model returned reports a refusal and not the start
            await().atMost(TIMEOUT).until(() -> feed.started() == 1);
            long writtenAfterTheStart = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(ids(List.of(writtenBeforeTheStart, writtenAfterTheStart)).toArray(String[]::new));
        } finally {
            model.shutdown();
        }
    }

    /**
     * A subscription from StartAt.now() that this model drives is paused before its start position is stored, while
     * the read of where the feed was at the subscribe has not answered. The resume begins from that same moment, so
     * what is written while it was paused is delivered.
     */
    @Test
    void a_subscription_from_now_paused_before_it_started_delivers_what_is_written_while_it_was_paused() {
        // Given
        Feed feed = new Feed();
        feed.readHeld = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), deliveredTo(delivered));
            model.pauseSubscription(SUBSCRIPTION_ID);
            long writtenWhilePaused = feed.write();
            Subscription resumed = model.resumeSubscription(SUBSCRIPTION_ID);
            feed.releaseReads();
            resumed.waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheResume = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheResume)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(ids(List.of(writtenWhilePaused, writtenAfterTheResume)).toArray(String[]::new));
        } finally {
            model.shutdown();
        }
    }

    /**
     * The wrapped model answers nothing for where its feed was at the subscribe, which it documents as an unresolvable
     * problem. The subscription from StartAt.now() then opens its feed at StartAt.now() when it starts, as it did before
     * the model read where the feed was at the subscribe, so what is written between the subscribe and the start is not
     * delivered.
     */
    @Test
    void a_subscription_from_now_whose_read_of_where_the_feed_was_answers_nothing_opens_its_feed_at_the_start() {
        // Given
        Feed feed = new Feed();
        feed.answersNothing = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), deliveredTo(delivered));
            feed.write();
            model.start(true);
            // The handle a subscribe on a stopped model returned reports a refusal and not the start
            await().atMost(TIMEOUT).until(() -> feed.started() == 1);
            long writtenAfterTheStart = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(String.valueOf(writtenAfterTheStart));
        } finally {
            model.shutdown();
        }
    }

    /**
     * The read of where the feed was at the subscribe fails for a subscription from StartAt.now(). It is read again,
     * with a warning for each failure, until it answers, and the subscription starts from where the feed was at the
     * subscribe, so what is written while the read keeps failing is delivered.
     */
    @Test
    void a_subscription_from_now_whose_read_of_where_the_feed_was_fails_reads_it_again_and_delivers_what_is_written_meanwhile() {
        // Given
        Feed feed = new Feed();
        feed.readFails = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        List<String> delivered = new CopyOnWriteArrayList<>();

        try (LoggedByTheModel logged = new LoggedByTheModel()) {
            // When
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), deliveredTo(delivered));
            long writtenWhileTheReadFails = feed.write();
            await().atMost(TIMEOUT).until(() -> logged.at(Level.WARN).stream().anyMatch(message -> message.contains("on attempt 1")));
            feed.readFails = false;
            subscription.waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheStart = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(ids(List.of(writtenWhileTheReadFails, writtenAfterTheStart)).toArray(String[]::new));
            assertThat(logged.at(Level.WARN)).as("warnings logged").anyMatch(message -> message.startsWith("Could not read where the feed was when subscription " + SUBSCRIPTION_ID + " asked to start from the present"));
        } finally {
            model.shutdown();
        }
    }

    /**
     * A subscription registered while this model was stopped, from a dynamic start position that answers the model
     * default, is refused when the model is started if the position read at registration failed. The handle the
     * subscribe returned is the only one the caller holds, so the refusal ends its wait there rather than leaving it
     * waiting on an id the refusal removed.
     */
    @Test
    void a_refusal_when_the_model_is_started_ends_the_wait_of_the_subscribe_that_registered_the_subscription_while_it_was_stopped() {
        // Given
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        feed.readFails = true;
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::subscriptionModelDefault), __ -> Mono.empty());
        feed.readFails = false;

        try {
            // When
            model.start(true);
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(POSITION_READ_FAILED);
            assertThat(model.isRunning(SUBSCRIPTION_ID)).as("whether the subscription is running").isFalse();
        } finally {
            model.shutdown();
        }
    }

    /**
     * A start of the model waits for no read of where the feed is, so a read that answers only much later does not hold
     * up the start, also while the delete of an earlier cancel of the same id runs. A dynamic start position answering
     * StartAt.now() begins where the feed was when the start asked it, so what is written after the start returned and
     * before the read answered is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_start_of_the_model_returns_and_delivers_what_is_written_after_it_when_the_read_of_where_the_feed_answers_late_behind_a_delete(boolean handsOver) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(false) : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        if (!handsOver) {
            model.stop();
        }
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered));
        feed.readHeld = true;

        try {
            // When
            CompletableFuture<Void> started = CompletableFuture.runAsync(() -> model.start(true));
            Throwable startEnded = catchThrowable(() -> started.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            List<Long> writtenAfterTheStart = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            writtenAfterTheStart.add(feed.write());
            feed.releaseReads();

            // Then
            assertThat(startEnded).as("how the start ended while the read of where the feed was held").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(writtenAfterTheStart));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe or a resume on a thread that may block, with a dynamic start position answering StartAt.now(), comes
     * while the delete of an earlier cancel of the same id runs, and a read of where the feed is answers only once the
     * delete has ended. The call returns without waiting for the read or for the delete, as it does without the
     * delete, and the subscription begins where the feed was at the call, so what is written after the call returned
     * is delivered.
     */
    @ParameterizedTest
    @CsvSource({"false, subscribe", "false, resume", "true, subscribe"})
    void a_call_with_a_dynamic_start_position_answering_now_returns_without_waiting_for_a_delete_or_for_a_read_of_where_the_feed_is(boolean handsOver, String call) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        feed.readHeld = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        boolean resumes = call.equals("resume");
        if (resumes) {
            model.stop();
        }
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();
        Function<CloudEvent, Mono<Void>> action = deliveredTo(delivered);
        if (resumes) {
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), action);
        }

        try {
            // When
            CompletableFuture<Subscription> called = CompletableFuture.supplyAsync(() -> resumes
                    ? model.resumeSubscription(SUBSCRIPTION_ID)
                    : model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), action));
            // Shorter than the delete is held, so a call that waits for the delete fails here
            Throwable callEnded = catchThrowable(() -> called.get(2, TimeUnit.SECONDS));
            List<Long> writtenAfterTheCall = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            writtenAfterTheCall.add(feed.write());
            feed.releaseReads();

            // Then
            assertThat(callEnded).as("how the call ended while the delete ran").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheCall.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(writtenAfterTheCall));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The wrapped model is stopped when the subscribe comes, while the delete of an earlier cancel of the same id runs,
     * and answers where its feed is only some time after it is asked. The function runs at the subscribe and hands
     * StartAt.now() to the wrapped model, which applies it when it is started, so a start from a thread where Reactor
     * does not allow blocking has no read to wait for, and what is written after the start returned is delivered.
     */
    @Test
    void an_event_written_after_a_start_on_a_thread_that_may_not_block_returned_reaches_a_subscription_made_while_a_delete_runs() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        NamedFeed feed = new NamedFeed(false);
        feed.answerDelay = Duration.ofMillis(300);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered));
            Mono.fromRunnable(() -> model.start(true)).subscribeOn(Schedulers.parallel()).block(TIMEOUT);
            List<Long> writtenAfterTheStart = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            writtenAfterTheStart.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(writtenAfterTheStart));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * When this model drives the feed itself and is stopped, a dynamic start position runs at the start of the model,
     * as it does without the delete of an earlier cancel of the same id, also when the model is started on a thread
     * where Reactor does not allow blocking. One that answers StartAt.now() needs no read of where the feed is, so the
     * start waits on nothing, and what is written after the start returned is delivered.
     */
    @Test
    void a_dynamic_start_position_answering_now_starts_at_a_start_on_a_thread_that_may_not_block_of_the_model_it_drives_while_a_delete_runs() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.stop();
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered));

        try {
            // When
            Mono.fromRunnable(() -> model.start(true)).subscribeOn(Schedulers.parallel()).block(TIMEOUT);
            List<Long> writtenAfterTheStart = new ArrayList<>(List.of(feed.write(), feed.write()));
            storage.releaseDelete.countDown();
            // Not waitUntilStarted(), which a registration made while the model was stopped keeps waiting on once the
            // start has taken it over
            await().atMost(TIMEOUT).until(() -> feed.started() == 1 || !model.isRunning(SUBSCRIPTION_ID));
            writtenAfterTheStart.add(feed.write());

            // Then
            assertThat(model.isRunning(SUBSCRIPTION_ID)).as("whether the subscription is running").isTrue();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheStart));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The storage evaluates no condition on a delete, so the position write after an event waits for the try of the
     * delete of an earlier cancel under way when the subscribe came. A pause that comes while that write waits resumes
     * after the event, since its action already ran. Resuming from where the subscription started would skip what was
     * written while it was paused, because StartAt.now() means the present at the resume again.
     */
    @Test
    void a_resume_after_a_pause_while_a_position_write_waits_for_a_delete_starts_after_the_last_event_whose_action_ran() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long deliveredBeforeThePause = feed.write();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(deliveredBeforeThePause)));
            model.pauseSubscription(SUBSCRIPTION_ID);
            long writtenWhilePaused = feed.write();
            model.resumeSubscription(SUBSCRIPTION_ID);
            long writtenAfterTheResume = feed.write();
            storage.releaseDelete.countDown();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheResume)));
            assertThat(delivered).as("events delivered to the subscription")
                    .containsExactly(String.valueOf(deliveredBeforeThePause), String.valueOf(writtenWhilePaused), String.valueOf(writtenAfterTheResume));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * With startWhenNoStartPositionCanBeRecorded, a subscription from the model default whose read of where the feed
     * is answers nothing starts from the present, recording nothing. Nothing is stored for the id, so the subscribe
     * opens the feed at the call while the delete of an earlier cancel of it runs, as it does without the delete, and
     * returns without waiting for that delete. What is written while the delete runs is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscription_from_the_model_default_that_may_start_without_a_recorded_position_opens_the_feed_at_the_call_while_a_delete_runs(boolean mayBlock) throws InterruptedException {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = new Feed();
        feed.answersNothing = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage,
                new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            assertThat(storage.deleteHeld.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage holds a delete").isTrue();
            // When
            CompletableFuture<Subscription> subscribed = Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)))
                    .subscribeOn(mayBlock ? Schedulers.boundedElastic() : Schedulers.parallel())
                    .toFuture();
            Throwable returnedWhileTheDeleteRan = catchThrowable(() -> subscribed.get(2, TimeUnit.SECONDS));
            List<Long> written = new ArrayList<>(List.of(feed.write(), feed.write()));
            storage.releaseDelete.countDown();
            written.add(feed.write());

            // Then
            assertThat(returnedWhileTheDeleteRan).as("how waiting for the subscribe to return while the delete ran ended").isNull();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(written)));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A start of the model starts every subscription registered while it was stopped without waiting for any of their
     * reads of where the feed is, so one read that never answers neither holds up the start nor the subscriptions
     * after it. The subscribe whose read hangs returns without waiting for it, its subscription does not start, and a
     * shutdown ends the wait for that start.
     */
    @Test
    void a_start_of_the_model_returns_and_starts_the_other_subscriptions_when_one_read_of_where_the_feed_is_never_answers() throws Exception {
        // Given
        Feed feed = new Feed();
        feed.firstReadHangs = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        CompletableFuture<Void> hung = CompletableFuture.supplyAsync(() -> model.subscribe("hung", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()))
                .orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).join().waitUntilStarted().toFuture();
        await().atMost(TIMEOUT).until(() -> model.isPaused("hung"));
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe("answered", null, StartAt.subscriptionModelDefault(), deliveredTo(delivered));
        long writtenBeforeTheStart = feed.write();

        try {
            // When
            CompletableFuture<Void> started = CompletableFuture.runAsync(() -> model.start(true));
            Throwable startEnded = catchThrowable(() -> started.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            long writtenAfterTheStart = feed.write();

            // Then
            assertThat(startEnded).as("how the start ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart)));
            assertThat(delivered).as("events delivered to the subscription whose read answered")
                    .containsExactly(String.valueOf(writtenBeforeTheStart), String.valueOf(writtenAfterTheStart));
            assertThat(hung).as("start of the subscription whose read hangs").isNotDone();
        } finally {
            model.shutdown();
        }
        assertThat(catchThrowable(() -> hung.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).getCause())
                .as("how the subscription whose read hangs ended once the model was shut down").isInstanceOf(SubscriptionModelShutdownException.class);
    }

    /**
     * A checkpoint stored for the subscription is where it starts, so a subscribe from the model default asks the
     * wrapped model where its feed is only when storage holds nothing. One that never answers then neither holds up
     * the subscribe nor what the subscription delivers.
     */
    @Test
    void a_subscribe_from_the_model_default_with_a_stored_checkpoint_does_not_ask_where_the_feed_is() {
        // Given
        Feed feed = new Feed();
        long stored = feed.write();
        long writtenBeforeTheSubscribe = feed.write();
        feed.readHangs = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint(String.valueOf(stored))).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() ->
                    model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)));
            Throwable subscribeEnded = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            long writtenAfterTheSubscribe = feed.write();

            // Then
            assertThat(subscribeEnded).as("how the subscribe ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheSubscribe)));
            assertThat(delivered).as("events delivered to the subscription")
                    .containsExactly(String.valueOf(writtenBeforeTheSubscribe), String.valueOf(writtenAfterTheSubscribe));
            assertThat(feed.reads).as("reads of where the feed is").hasValue(0);
        } finally {
            model.shutdown();
        }
    }

    /**
     * A subscribe from the model default handed to a wrapped model that manages named subscriptions waits for its read
     * of where the feed is before it returns, and one where this model drives the feed itself returns at once and
     * keeps its subscription waiting for that read. A cancel of the id ends either wait, along with the subscription,
     * so a read that never answers does not hold the thread that subscribed once nobody can use the subscription.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_cancel_ends_a_subscribe_that_waits_for_a_read_of_where_the_feed_is(boolean handsOver) {
        // Given
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        feed.readHangs = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());

        try {
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() ->
                    model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
            await().atMost(TIMEOUT).until(() -> feed.reads.get() == 1);

            // When
            model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
            Throwable subscribeEnded = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

            // Then
            assertThat(subscribeEnded).as("how the subscribe ended").isNull();
            assertThat(catchThrowable(() -> subscribed.join().waitUntilStarted().block(TIMEOUT)))
                    .as("how waiting for the start of the cancelled subscription ended").isInstanceOf(java.util.concurrent.CancellationException.class);
        } finally {
            model.shutdown();
        }
    }

    /**
     * The wrapped model is stopped at the subscribe, the read of where its feed is fails or answers nothing, and the
     * delete of an earlier cancel of the same id runs. A dynamic start position answering StartAt.now() needs no such
     * read, so the subscription starts when the wrapped model is started, as it does without the delete.
     */
    @ParameterizedTest
    @ValueSource(strings = {"fails", "empty"})
    void a_dynamic_start_position_answering_now_starts_with_a_stopped_wrapped_model_while_a_delete_runs_when_the_read_of_where_the_feed_is_fails_or_answers_nothing(String read) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        NamedFeed feed = new NamedFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        feed.readFails = read.equals("fails");
        feed.answersNothing = read.equals("empty");
        Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), __ -> Mono.empty());
        feed.readFails = false;
        feed.answersNothing = false;

        try {
            // When
            Throwable startEnded = catchThrowable(() -> model.start(true));
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));

            // Then
            assertThat(startEnded).as("how the start ended").isNull();
            assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
            assertThat(feed.started()).as("subscriptions that began reading from the feed").isEqualTo(1);
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe from the model default on a stopped model that this model drives returns at once, and its read of
     * where the feed is never answers. start(true) starts the subscription, which waits for that read, and a pause of
     * the id, or a stop of the model, then ends the wait. Waiting for the start of what the subscribe returned ends with
     * CancellationException, since the subscription never started.
     */
    @ParameterizedTest
    @ValueSource(strings = {"pause", "stop"})
    void a_pause_or_a_stop_after_a_start_ends_the_wait_for_the_start_of_a_subscription_on_a_stopped_model_whose_read_of_where_the_feed_is_never_answers(String endedBy) {
        // Given
        Feed feed = new Feed();
        feed.readHangs = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
        await().atMost(TIMEOUT).until(() -> feed.reads.get() == 1 && model.isPaused(SUBSCRIPTION_ID));
        model.start(true);

        try {
            // When
            if (endedBy.equals("pause")) {
                model.pauseSubscription(SUBSCRIPTION_ID);
            } else {
                model.stop();
            }
            Throwable subscribeEnded = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

            // Then
            assertThat(subscribeEnded).as("how the subscribe ended").isNull();
            assertThat(catchThrowable(() -> subscribed.join().waitUntilStarted().block(TIMEOUT)))
                    .as("how waiting for the start of the subscription ended").isInstanceOf(CancellationException.class);
        } finally {
            model.shutdown();
        }
    }

    /**
     * The storage fails every delete of an earlier cancel of the same id, as during an outage, and still holds the
     * checkpoint of the cancelled subscription. A subscribe of the id takes the delete over, so the model makes no
     * further try and the cancel completes. The subscription resumes from that checkpoint and stores the position of
     * each event it handles as it would with no delete running, so the stored position is that of the last event while
     * the storage still fails every delete. A wrapped model that manages named subscriptions runs the action of what it
     * replays within its own subscribe and waits for it, and that subscribe returns too.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_that_takes_over_a_delete_that_keeps_failing_stores_the_position_of_each_event_and_the_cancel_completes(boolean handsOver) throws Exception {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        storage.deleteFails = true;
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        long stored = feed.write();
        List<Long> replayed = List.of(feed.write(), feed.write());
        storage.save(SUBSCRIPTION_ID, checkpoint(stored)).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            // On another thread that may block, so a subscribe that waits for the delete fails the test instead of
            // hanging it
            CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)));
            Throwable subscribeEnded = catchThrowable(() -> subscribed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            // Asserted here, since the wrapped model is still taking a subscribe that did not return, and writing to it
            // would wait for that
            assertThat(subscribeEnded).as("how the subscribe ended while the delete kept failing").isNull();
            List<Long> writtenAfterTheReturn = List.of(feed.write(), feed.write());
            Throwable cancelEnded = catchThrowable(() -> cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            int deletesAskedForOnceTheCancelEnded = storage.deleteAttempts.get();
            // Longer than the wait before the next try after a few failures
            await().pollDelay(Duration.ofMillis(500)).atMost(TIMEOUT).until(() -> true);

            // Then
            List<Long> everyEvent = new ArrayList<>(replayed);
            everyEvent.addAll(writtenAfterTheReturn);
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("events delivered while the delete kept failing")
                    .containsExactlyElementsOf(ids(everyEvent)));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT)).as("position stored while the delete kept failing")
                    .isEqualTo(checkpoint(writtenAfterTheReturn.getLast())));
            assertThat(cancelEnded).as("how the cancel ended once the subscribe took its delete over").isNull();
            assertThat(storage.deleteAttempts).as("deletes the storage was asked for, once the cancel had ended").hasValue(deletesAskedForOnceTheCancelEnded);
        } finally {
            model.shutdown();
        }
    }

    /**
     * The storage fails the delete of an earlier cancel of the same id, and still holds the checkpoint of the cancelled
     * subscription, when a subscribe of the id takes the delete over. The subscription handles two events, and is then
     * paused or the model stopped, and resumed or not, before the storage deletes again. The model then ends, an event
     * is written while it is down, and a new model on the same storage subscribes the id from the model default. It
     * resumes from the last event the first model handled, so the event written while it was down is delivered.
     */
    @ParameterizedTest
    @CsvSource({"default, pause, false", "now, stop, false", "default, stop, true", "now, pause, true"})
    void a_subscription_that_took_over_a_delete_and_was_paused_or_stopped_skips_no_event_once_the_model_is_rebuilt(String startAt, String endedBy, boolean resumedFirst) throws Exception {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        storage.releaseDelete.countDown();
        storage.deleteFails = true;
        Feed feed = new Feed();
        storage.save(SUBSCRIPTION_ID, checkpoint(feed.write())).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
        ReactorDurableSubscriptionModel rebuilt = new ReactorDurableSubscriptionModel(feed, storage);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, answer(startAt), deliveredTo(delivered)))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted().block(TIMEOUT);
            List<Long> handled = List.of(feed.write(), feed.write());
            await().atMost(TIMEOUT).until(() -> delivered.containsAll(ids(handled)));

            // When
            if (endedBy.equals("pause")) {
                model.pauseSubscription(SUBSCRIPTION_ID);
            } else {
                model.stop();
            }
            if (resumedFirst && endedBy.equals("pause")) {
                model.resumeSubscription(SUBSCRIPTION_ID);
            } else if (resumedFirst) {
                model.start(true);
            }
            storage.deleteFails = false;
            cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            model.shutdown();
            long writtenWhileDown = feed.write();
            rebuilt.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheRebuild = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheRebuild)));
            assertThat(delivered).as("events delivered before and after the rebuild").contains(String.valueOf(writtenWhileDown));
        } finally {
            model.shutdown();
            rebuilt.shutdown();
        }
    }

    /**
     * The storage evaluates a condition on a delete, holds the delete of an earlier cancel of the same id, and still
     * holds the checkpoint of the cancelled subscription. A subscribe of the id from the model default resumes from
     * that checkpoint and handles two events while the storage holds the delete. The storage then applies the delete,
     * and the process ends right after it, so nothing the model asks of the storage afterwards reaches it. An event is
     * written while the process is down, and a new model on the same storage subscribes the id from the model default.
     * It resumes from the last event the first model handled, so nothing written after that event is skipped.
     */
    @Test
    void a_rebuild_after_the_process_ended_right_after_the_storage_applied_a_delete_that_a_subscribe_took_over_skips_no_event() throws Exception {
        // Given
        InMemoryCheckpointStorage store = new InMemoryCheckpointStorage();
        HeldDeleteStorage storage = new HeldDeleteStorage(store, true);
        storage.processEndsOnceDeleted = true;
        Feed feed = new Feed();
        long stored = feed.write();
        long writtenAfterTheCheckpoint = feed.write();
        store.save(SUBSCRIPTION_ID, checkpoint(stored)).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        ReactorDurableSubscriptionModel rebuilt = new ReactorDurableSubscriptionModel(feed, new HeldDeleteStorage(store, true));
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted().block(TIMEOUT);
            List<Long> handled = List.of(feed.write(), feed.write());
            await().atMost(TIMEOUT).until(() -> delivered.containsAll(ids(handled)));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(store.read(SUBSCRIPTION_ID).block(TIMEOUT)).as("position stored while the storage held the delete")
                    .isEqualTo(checkpoint(handled.getLast())));

            // When
            storage.releaseDelete.countDown();
            assertThat(storage.deleteApplied.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage applied the delete").isTrue();
            model.shutdown();
            long writtenWhileDown = feed.write();
            rebuilt.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheRebuild = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheRebuild)));
            assertThat(delivered).as("events delivered before and after the rebuild")
                    .containsExactly(String.valueOf(writtenAfterTheCheckpoint), String.valueOf(handled.get(0)), String.valueOf(handled.get(1)),
                            String.valueOf(writtenWhileDown), String.valueOf(writtenAfterTheRebuild));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
            rebuilt.shutdown();
        }
    }

    /**
     * The storage evaluates a condition on a delete and holds the delete of an earlier cancel of the same id while it
     * still holds the checkpoint of the cancelled subscription. A subscribe of the id from StartAt.now() or from a
     * position of its own writes that checkpoint back, and the write back has not reached the store when the process
     * ends. The storage applies the delete, and the subscription handles an event afterwards. The process then ends, an
     * event is written while it is down, and a new model on the same storage subscribes the id from the model default.
     * It resumes from the event the first model handled, as it would had no delete been running, so the event written
     * while the process was down is delivered.
     */
    @ParameterizedTest
    @ValueSource(strings = {"now", "own position"})
    void a_rebuild_after_the_process_ended_before_a_write_back_reached_the_store_resumes_from_the_last_event_handled(String startAt) throws Exception {
        // Given
        InMemoryCheckpointStorage store = new InMemoryCheckpointStorage();
        HeldDeleteStorage storage = new HeldDeleteStorage(store, true);
        storage.firstVersionedSaveNeverArrives = true;
        Feed feed = new Feed();
        long stored = feed.write();
        store.save(SUBSCRIPTION_ID, checkpoint(stored)).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
        ReactorDurableSubscriptionModel rebuilt = new ReactorDurableSubscriptionModel(feed, new HeldDeleteStorage(store, true));
        StartAt from = startAt.equals("now") ? StartAt.now() : StartAt.checkpoint(checkpoint(stored));
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, from, deliveredTo(delivered)))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted().block(TIMEOUT);
            storage.releaseDelete.countDown();
            assertThat(storage.deleteApplied.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage applied the delete").isTrue();
            long handled = feed.write();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(handled)));
            // Long enough for the position of the event to be stored, where nothing holds it back
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);

            // When
            storage.processEnded = true;
            model.shutdown();
            long writtenWhileDown = feed.write();
            rebuilt.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheRebuild = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheRebuild)));
            assertThat(delivered).as("events delivered before and after the rebuild")
                    .containsExactly(String.valueOf(handled), String.valueOf(writtenWhileDown), String.valueOf(writtenAfterTheRebuild));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
            rebuilt.shutdown();
        }
    }

    /**
     * The storage evaluates a condition on a delete and holds the delete of an earlier cancel of the same id while it
     * still holds the checkpoint of the cancelled subscription. A subscribe of the id from the model default writes
     * that checkpoint back, and the write back never reaches the store, as one still on its way there when the process
     * ends. The storage applies the delete before or after the subscription reads storage. The process then ends, an
     * event is written while it is down, and a new model on the same storage subscribes the id from the model default.
     * It resumes from the checkpoint of the cancelled subscription, as it would had no delete been running, so the
     * event written while the process was down is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void a_rebuild_after_the_process_ended_before_a_write_back_reached_the_store_resumes_a_subscription_from_the_model_default(boolean deleteAppliedBeforeTheRead) throws Exception {
        // Given
        InMemoryCheckpointStorage store = new InMemoryCheckpointStorage();
        HeldDeleteStorage storage = new HeldDeleteStorage(store, true);
        storage.firstVersionedSaveNeverArrives = true;
        Feed feed = new Feed();
        store.save(SUBSCRIPTION_ID, checkpoint(feed.write())).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
        ReactorDurableSubscriptionModel rebuilt = new ReactorDurableSubscriptionModel(feed, new HeldDeleteStorage(store, true));
        CountDownLatch releaseRead = new CountDownLatch(1);
        if (deleteAppliedBeforeTheRead) {
            storage.releaseNextRead = releaseRead;
        }
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // On another thread, so a subscribe that waits for the write back does not hang the test
            CompletableFuture.runAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)));
            if (deleteAppliedBeforeTheRead) {
                // Not asserted, since a subscribe that waits for the write back before it reads never gets there
                storage.nextReadEntered.await(1, TimeUnit.SECONDS);
            } else {
                // Long enough for the subscription to read storage, where nothing holds it back
                await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
            }
            storage.releaseDelete.countDown();
            assertThat(storage.deleteApplied.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage applied the delete").isTrue();
            releaseRead.countDown();
            // Long enough for the subscription to start, where nothing holds it back
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);

            // When
            storage.processEnded = true;
            model.shutdown();
            long writtenWhileDown = feed.write();
            rebuilt.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheRebuild = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheRebuild)));
            assertThat(delivered).as("events delivered before and after the rebuild")
                    .containsExactly(String.valueOf(writtenWhileDown), String.valueOf(writtenAfterTheRebuild));
        } finally {
            releaseRead.countDown();
            storage.releaseDelete.countDown();
            model.shutdown();
            rebuilt.shutdown();
        }
    }

    /**
     * The storage evaluates a condition on a delete, and keeps the earlier of a position it is offered and the one it
     * holds, as the MongoDB storages do. A cancel of the id starts a delete, and a subscription of the id from the
     * model default is registered while the model is stopped, where the feed is then. Another node of the id stores a
     * later checkpoint, the delete holds it, and starting the model writes it back. The registration starts from where
     * the feed was when it was registered, and handles no event before the process ends. The write back reaches the
     * store once the subscription started, or never, as one still on its way there when the process ends. A new model
     * on the same storage then resumes the id from where the feed was at the registration, so every event written
     * after it is delivered.
     */
    @ParameterizedTest
    @ValueSource(strings = {"once the subscription started", "never"})
    void a_rebuild_resumes_a_registration_made_while_the_model_was_stopped_from_where_the_feed_was_then_whenever_the_write_back_arrives(String writeBackArrives) throws Exception {
        // Given
        InMemoryCheckpointStorage store = new InMemoryCheckpointStorage();
        HeldDeleteStorage storage = new HeldDeleteStorage(store, true);
        storage.resolvesByOrder = true;
        CountDownLatch releaseWriteBack = new CountDownLatch(1);
        if (writeBackArrives.equals("never")) {
            storage.firstVersionedSaveNeverArrives = true;
        } else {
            storage.releaseFirstVersionedSave = releaseWriteBack;
        }
        storage.deleteFails = true;
        Feed feed = new Feed();
        store.save(SUBSCRIPTION_ID, checkpoint(feed.write())).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        HeldDeleteStorage rebuiltStorage = new HeldDeleteStorage(store, true);
        rebuiltStorage.resolvesByOrder = true;
        ReactorDurableSubscriptionModel rebuilt = new ReactorDurableSubscriptionModel(feed, rebuiltStorage);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            model.stop();
            model.cancelSubscription(SUBSCRIPTION_ID);
            await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
            // Handles nothing before the process ends
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.never());
            List<Long> afterTheRegistration = List.of(feed.write(), feed.write());
            store.save(SUBSCRIPTION_ID, checkpoint(afterTheRegistration.get(1))).block(TIMEOUT);
            storage.deleteFails = false;
            assertThat(storage.deleteHeld.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage holds a delete").isTrue();
            model.start(true);
            // Long enough for the subscription to start, where nothing holds it back
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
            storage.releaseDelete.countDown();
            assertThat(storage.deleteApplied.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage applied the delete").isTrue();
            releaseWriteBack.countDown();
            // Long enough for the write back to reach the store and for the subscription to start, where nothing
            // holds either back
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);

            // When
            storage.processEnded = true;
            model.shutdown();
            long writtenWhileDown = feed.write();
            rebuilt.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheRebuild = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheRebuild)));
            assertThat(delivered).as("events delivered after the rebuild")
                    .containsExactly(String.valueOf(afterTheRegistration.get(0)), String.valueOf(afterTheRegistration.get(1)),
                            String.valueOf(writtenWhileDown), String.valueOf(writtenAfterTheRebuild));
        } finally {
            releaseWriteBack.countDown();
            storage.releaseDelete.countDown();
            model.shutdown();
            rebuilt.shutdown();
        }
    }

    /**
     * The storage evaluates a condition on a delete and holds the delete of an earlier cancel of the same id while it
     * still holds the checkpoint of the cancelled subscription. A subscribe of the id from an earlier position of its
     * own writes that checkpoint back, and the storage holds the write back. The storage applies the delete, and the
     * subscription is paused and resumed before the write back reaches the store. The resumed subscription handles an
     * event and stores its position, and then the write back reaches the store. The process ends before the
     * subscription handles the next event, and a new model on the same storage subscribes the id from the model
     * default. It resumes from the event the resumed subscription handled, so no event after it is skipped.
     */
    @Test
    void a_subscription_resumed_before_a_write_back_reached_the_store_keeps_the_position_it_stored() throws Exception {
        // Given
        InMemoryCheckpointStorage store = new InMemoryCheckpointStorage();
        HeldDeleteStorage storage = new HeldDeleteStorage(store, true);
        CountDownLatch releaseWriteBack = new CountDownLatch(1);
        storage.releaseFirstVersionedSave = releaseWriteBack;
        Feed feed = new Feed();
        long ownPosition = feed.write();
        long handledOnceResumed = feed.write();
        long neverHandled = feed.write();
        store.save(SUBSCRIPTION_ID, checkpoint(neverHandled)).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        ReactorDurableSubscriptionModel rebuilt = new ReactorDurableSubscriptionModel(feed, new HeldDeleteStorage(store, true));
        AtomicBoolean firstDelivery = new AtomicBoolean(true);
        List<String> delivered = new CopyOnWriteArrayList<>();
        // The first delivery never ends, and neither does any of an event after the one handled once resumed
        Function<CloudEvent, Mono<Void>> action = event -> firstDelivery.getAndSet(false) || !event.getId().equals(String.valueOf(handledOnceResumed))
                ? Mono.never()
                : Mono.fromRunnable(() -> delivered.add(event.getId()));

        try {
            CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
            await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(checkpoint(ownPosition)), action).waitUntilStarted().block(TIMEOUT);
            storage.releaseDelete.countDown();
            assertThat(storage.deleteApplied.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the storage applied the delete").isTrue();
            // Long enough for the delete to end its try, where nothing holds it back
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted().block(TIMEOUT);
            await().atMost(TIMEOUT).until(() -> String.valueOf(handledOnceResumed).equals(store.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(TIMEOUT)));
            releaseWriteBack.countDown();
            cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // When
            storage.processEnded = true;
            model.shutdown();
            long writtenWhileDown = feed.write();
            rebuilt.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered)).waitUntilStarted().block(TIMEOUT);
            long writtenAfterTheRebuild = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheRebuild)));
            assertThat(delivered).as("events delivered before and after the rebuild")
                    .containsExactly(String.valueOf(handledOnceResumed), String.valueOf(neverHandled), String.valueOf(writtenWhileDown),
                            String.valueOf(writtenAfterTheRebuild));
        } finally {
            releaseWriteBack.countDown();
            storage.releaseDelete.countDown();
            model.shutdown();
            rebuilt.shutdown();
        }
    }

    /**
     * A subscribe of the id comes on a thread where Reactor does not allow blocking while the delete of an earlier
     * cancel of the same id runs, and the storage still holds the checkpoint of the cancelled subscription. Reactor
     * refuses the subscribe, by refusing the read of the model default where the subscription is handed to a wrapped
     * model that manages named subscriptions, and by refusing the read a dynamic start position makes itself where this
     * model drives the subscription and the storage evaluates a condition on a delete, since nothing then waits to be
     * written back and the function is asked on the subscribing thread. After the refused subscribe no subscription of
     * the id exists, so the delete goes ahead and removes the checkpoint before the cancel completes, as it does with no
     * subscribe. Where this model drives the subscription on a storage without that condition, the checkpoint waits to
     * be written back and the function is asked on another thread once it is, so nothing refuses the subscribe.
     */
    @ParameterizedTest
    @CsvSource({"false, true", "true, false", "true, true"})
    void a_subscribe_that_reactor_refuses_while_a_delete_runs_leaves_the_delete_to_remove_the_checkpoint(boolean handsOver, boolean conditionalDeletes) throws Exception {
        // Given
        InMemoryCheckpointStorage store = new InMemoryCheckpointStorage();
        HeldDeleteStorage storage = new HeldDeleteStorage(store, conditionalDeletes);
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        store.save(SUBSCRIPTION_ID, checkpoint(feed.write())).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();
        await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
        StartAt startAt = handsOver
                ? StartAt.subscriptionModelDefault()
                : StartAt.dynamic(() -> store.read(SUBSCRIPTION_ID).blockOptional().isPresent() ? StartAt.subscriptionModelDefault() : StartAt.now());

        try {
            // When
            Throwable thrown = catchThrowable(() -> Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, startAt, __ -> Mono.empty()))
                    .subscribeOn(Schedulers.parallel())
                    .block(TIMEOUT));
            storage.releaseDelete.countDown();
            cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Then
            assertThat(thrown).as("how the subscribe on a thread that may not block ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("blocking");
            assertThat(store.read(SUBSCRIPTION_ID).map(Checkpoint::asString).blockOptional(TIMEOUT)).as("checkpoint stored once the cancel completed").isEmpty();
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscription from the model default finds nothing stored, so it records where the feed is as its first
     * position. That write fails once, as during a short outage, or is refused because another node stored a position
     * for the id right before it. A subscribe that took over the delete of an earlier cancel of the id, which the
     * storage fails, ends the same way as one with no delete running, and so does the delivery of an event written
     * afterwards and what is stored then.
     */
    @ParameterizedTest
    @CsvSource({"false, fails", "false, refused", "true, fails", "true, refused"})
    void a_first_position_that_fails_or_is_refused_ends_a_subscribe_that_took_over_a_delete_as_it_ends_one_with_no_delete_running(boolean handsOver, String firstPosition) {
        assertThat(firstPositionOutcome(handsOver, firstPosition, true)).as("how a subscribe that took over a delete ended")
                .isEqualTo(firstPositionOutcome(handsOver, firstPosition, false));
    }

    private static String firstPositionOutcome(boolean handsOver, String firstPosition, boolean deleteRunning) {
        HeldDeleteStorage storage = new HeldDeleteStorage();
        storage.deleteFails = true;
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        feed.write();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        List<String> delivered = new CopyOnWriteArrayList<>();
        try {
            if (deleteRunning) {
                model.cancelSubscription(SUBSCRIPTION_ID);
                await().atMost(TIMEOUT).until(() -> storage.deleteAttempts.get() >= 1);
            }
            if (firstPosition.equals("fails")) {
                storage.savesToFail.set(1);
            } else {
                storage.storedByAnotherNodeBeforeAFirstPosition = checkpoint(1000);
            }
            @Nullable Subscription subscription = null;
            @Nullable Throwable subscribeThrew = null;
            try {
                subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered));
            } catch (Throwable throwable) {
                subscribeThrew = throwable;
            }
            Subscription returned = subscription;
            @Nullable Throwable startEnded = returned == null ? null : catchThrowable(() -> returned.waitUntilStarted().block(TIMEOUT));
            long writtenAfterTheSubscribe = feed.write();
            // Long enough for the event to be delivered and its position stored, where the subscription still runs
            await().pollDelay(Duration.ofMillis(300)).atMost(TIMEOUT).until(() -> true);
            return "subscribe threw " + describe(subscribeThrew) + ", waiting for the start ended with " + describe(startEnded)
                   + ", running " + model.isRunning(SUBSCRIPTION_ID) + ", event written afterwards delivered " + delivered.contains(String.valueOf(writtenAfterTheSubscribe))
                   + ", stored " + storage.stored.read(SUBSCRIPTION_ID).map(Checkpoint::asString).blockOptional(TIMEOUT).orElse("nothing");
        } finally {
            storage.deleteFails = false;
            model.shutdown();
        }
    }

    private static String describe(@Nullable Throwable throwable) {
        return throwable == null ? "nothing" : throwable.getClass().getSimpleName() + "(" + throwable.getMessage() + ")";
    }

    private static Checkpoint checkpoint(long position) {
        return new StringBasedCheckpoint(String.valueOf(position));
    }

    private static StartAt answer(String answer) {
        return answer.equals("now") ? StartAt.now() : StartAt.subscriptionModelDefault();
    }

    private static Function<CloudEvent, Mono<Void>> deliveredTo(List<String> delivered) {
        return event -> Mono.fromRunnable(() -> delivered.add(event.getId()));
    }

    private static List<String> ids(List<Long> positions) {
        return positions.stream().map(String::valueOf).toList();
    }

    private static CloudEvent eventAt(long position) {
        CloudEvent event = CloudEventBuilder.v1().withId(String.valueOf(position)).withSource(URI.create("urn:test")).withType("Something").build();
        return new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(String.valueOf(position)));
    }

    // Where a start position begins in a feed numbered from one, with the present as the last number written
    private static long startOf(StartAt startAt, long present) {
        StartAt resolved = startAt;
        while (resolved != null && resolved.isDynamic()) {
            resolved = resolved.get(new SubscriptionModelContext(ReactorDurableSubscriptionModelWrittenAfterTheCallTest.class));
        }
        if (resolved == null || resolved.isNow() || resolved.isDefault()) {
            return present;
        }
        return Long.parseLong(resolved.toString());
    }

    /**
     * Holds every delete until the test releases it.
     */
    private static final class HeldDeleteStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage stored;
        final CountDownLatch releaseDelete = new CountDownLatch(1);
        volatile boolean deleteFails;
        final AtomicInteger deleteAttempts = new AtomicInteger();
        final AtomicInteger saves = new AtomicInteger();
        // How many of the next saves fail, as during a short outage
        final AtomicInteger savesToFail = new AtomicInteger();
        // Stored for the id right after a delete applied, as another node recording a first position would
        volatile @Nullable Checkpoint storedByAnotherNodeOnceDeleted;
        // Stored for the id right before the next save on the condition that nothing is stored, as another node
        // recording a first position would
        volatile @Nullable Checkpoint storedByAnotherNodeBeforeAFirstPosition;
        // Set, the process ends right after a delete reached the store, so nothing asked of this storage afterwards
        // reaches it
        volatile boolean processEndsOnceDeleted;
        private volatile boolean processEnded;
        // Set, the first save on the condition that the stored version is not above a given one never reaches the
        // store, as a write still on its way there when the process ends
        volatile boolean firstVersionedSaveNeverArrives;
        // Set, that first save waits for it instead, and is then applied unless the process ended meanwhile
        volatile @Nullable CountDownLatch releaseFirstVersionedSave;
        private final AtomicBoolean versionedSaveHeld = new AtomicBoolean();
        // Set, the next read waits for it, and then answers what the store holds by then
        volatile @Nullable CountDownLatch releaseNextRead;
        final CountDownLatch nextReadEntered = new CountDownLatch(1);
        // Set, resolveFirstCheckpointRace keeps the earlier of the stored and the offered position, writing the
        // offered one when it is earlier, as the MongoDB storages do by operation time
        volatile boolean resolvesByOrder;
        final CountDownLatch deleteApplied = new CountDownLatch(1);
        // Counted down once a delete is held rather than failed
        final CountDownLatch deleteHeld = new CountDownLatch(1);
        // Whether a delete evaluates its condition, or refuses every condition but any(), as a storage written before
        // deletes took one does
        private final boolean conditionalDeletes;

        HeldDeleteStorage() {
            this(new InMemoryCheckpointStorage(), false);
        }

        HeldDeleteStorage(InMemoryCheckpointStorage stored, boolean conditionalDeletes) {
            this.stored = stored;
            this.conditionalDeletes = conditionalDeletes;
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            CountDownLatch release = releaseNextRead;
            if (release != null) {
                releaseNextRead = null;
                return Mono.fromCallable(() -> {
                            nextReadEntered.countDown();
                            return release.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .then(Mono.defer(() -> processEnded ? Mono.never() : stored.read(subscriptionId)));
            }
            return Mono.defer(() -> processEnded ? Mono.never() : stored.read(subscriptionId));
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            if (!resolvesByOrder) {
                return Mono.empty();
            }
            return Mono.defer(() -> processEnded ? Mono.never() : stored.read(subscriptionId)
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(storedNow -> storedNow.isPresent() && Long.parseLong(storedNow.get().asString()) <= Long.parseLong(candidate.asString())
                            ? Mono.just(storedNow.get())
                            // any() keeps the stored version, as the MongoDB storages do
                            : stored.save(subscriptionId, candidate, CheckpointWriteCondition.any())));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return Mono.defer(() -> {
                if (processEnded) {
                    return Mono.never();
                }
                saves.incrementAndGet();
                CountDownLatch release = releaseFirstVersionedSave;
                if ((firstVersionedSaveNeverArrives || release != null) && writeCondition instanceof CheckpointWriteCondition.NotOlderThan
                    && versionedSaveHeld.compareAndSet(false, true)) {
                    return release == null ? Mono.never() : Mono.fromCallable(() -> release.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                            .subscribeOn(Schedulers.boundedElastic())
                            .then(Mono.defer(() -> processEnded ? Mono.never() : stored.save(subscriptionId, checkpoint, writeCondition)));
                }
                if (savesToFail.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
                    return Mono.error(new IllegalStateException(SAVE_FAILED));
                }
                Checkpoint storedByAnotherNode = storedByAnotherNodeBeforeAFirstPosition;
                if (storedByAnotherNode != null && writeCondition instanceof CheckpointWriteCondition.IfAbsent) {
                    storedByAnotherNodeBeforeAFirstPosition = null;
                    return stored.save(subscriptionId, storedByAnotherNode).then(stored.save(subscriptionId, checkpoint, writeCondition));
                }
                return stored.save(subscriptionId, checkpoint, writeCondition);
            });
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return stored.evaluatesWriteConditions();
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.defer(() -> processEnded ? Mono.never() : stored.writeVersion(subscriptionId));
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return deleting(subscriptionId, () -> stored.delete(subscriptionId));
        }

        @Override
        public Mono<Void> delete(String subscriptionId, CheckpointWriteCondition condition) {
            return conditionalDeletes ? deleting(subscriptionId, () -> stored.delete(subscriptionId, condition)) : CheckpointStorage.super.delete(subscriptionId, condition);
        }

        @Override
        public boolean evaluatesDeleteConditions() {
            return conditionalDeletes;
        }

        private Mono<Void> deleting(String subscriptionId, Supplier<Mono<Void>> applied) {
            deleteAttempts.incrementAndGet();
            if (deleteFails) {
                return Mono.error(new IllegalStateException(DELETE_FAILED));
            }
            return Mono.fromCallable(() -> {
                        deleteHeld.countDown();
                        return releaseDelete.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    })
                    .subscribeOn(Schedulers.boundedElastic())
                    // Before the model hears that the delete ended, so nothing it starts then reaches the store
                    .then(Mono.defer(() -> processEnded ? Mono.<Void>never() : applied.get().doOnTerminate(() -> {
                        if (processEndsOnceDeleted) {
                            processEnded = true;
                        }
                        deleteApplied.countDown();
                    })))
                    .then(Mono.defer(() -> {
                        Checkpoint storedByAnotherNode = storedByAnotherNodeOnceDeleted;
                        return storedByAnotherNode == null ? Mono.empty() : stored.save(subscriptionId, storedByAnotherNode).then();
                    }));
        }
    }

    /**
     * A feed the durable model drives itself, which begins a subscription where its start position is when it is
     * subscribed to.
     */
    private static class Feed implements CheckpointAwareSubscriptionModel {
        final AtomicLong present = new AtomicLong();
        final Sinks.Many<CloudEvent> written = Sinks.many().replay().all();
        final AtomicInteger subscribed = new AtomicInteger();
        volatile Duration answerDelay = Duration.ZERO;
        volatile boolean readFails;
        volatile boolean answersNothing;
        // A read that never answers, as from a database that does not respond
        volatile boolean readHangs;
        volatile boolean firstReadHangs;
        // A read that answers only once releaseReads() is called, as from a database that responds late
        volatile boolean readHeld;
        final Sinks.Empty<Void> readsReleased = Sinks.empty();
        final AtomicInteger reads = new AtomicInteger();

        // How many subscriptions have begun reading from the feed
        int started() {
            return subscribed.get();
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.defer(() -> {
                subscribed.incrementAndGet();
                long start = startOf(startAt, present.get());
                return written.asFlux().filter(event -> Long.parseLong(event.getId()) > start);
            });
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return read(present::get);
        }

        // Where the feed is at the call, answered as late as globalCheckpoint() answers
        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            long atTheCall = present.get();
            return read(() -> atTheCall);
        }

        // Counted, and decided by the flags, when subscribed to, as a read of a database runs then
        private Mono<Checkpoint> read(LongSupplier position) {
            return Mono.defer(() -> {
                int read = reads.incrementAndGet();
                if (readHangs || (firstReadHangs && read == 1)) {
                    return Mono.never();
                } else if (readFails) {
                    return Mono.error(new IllegalStateException(POSITION_READ_FAILED));
                } else if (answersNothing) {
                    return Mono.empty();
                }
                Mono<Checkpoint> answer = Mono.fromSupplier(() -> new StringBasedCheckpoint(String.valueOf(position.getAsLong())));
                if (readHeld) {
                    return readsReleased.asMono().then(answer);
                }
                return answerDelay.isZero() ? answer : Mono.delay(answerDelay).then(answer);
            });
        }

        void releaseReads() {
            readsReleased.tryEmitEmpty();
        }

        synchronized long write() {
            long position = present.incrementAndGet();
            written.tryEmitNext(eventAt(position));
            return position;
        }
    }

    /**
     * A feed that manages named subscriptions of its own. A subscription handed to it while it runs begins where its
     * start position is then, and one handed to it while it is stopped begins where its start position is once it is
     * started.
     */
    private static final class NamedFeed extends Feed implements SubscriptionModel {
        final Map<String, Named> subscriptions = new ConcurrentHashMap<>();
        private final List<Long> log = new CopyOnWriteArrayList<>();
        private volatile boolean running;
        NamedFeed(boolean running) {
            this.running = running;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            return take(subscriptionId, startAt, action);
        }

        private synchronized Subscription take(String subscriptionId, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            Named named = new Named(startAt, action);
            if (subscriptions.putIfAbsent(subscriptionId, named) != null) {
                throw new DuplicateSubscriptionIdException(subscriptionId);
            }
            if (running) {
                begin(named);
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
        int started() {
            return (int) subscriptions.values().stream().filter(named -> named.start >= 0).count();
        }

        @Override
        synchronized long write() {
            long position = present.incrementAndGet();
            log.add(position);
            if (running) {
                subscriptions.values().stream().filter(named -> named.start >= 0 && position > named.start)
                        .forEach(named -> named.action.apply(eventAt(position)).block());
            }
            return position;
        }

        private void begin(Named named) {
            named.start = startOf(named.startAt, present.get());
            log.stream().filter(position -> position > named.start).forEach(position -> named.action.apply(eventAt(position)).block());
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            subscriptions.values().stream().filter(named -> named.start < 0).forEach(this::begin);
        }

        @Override
        public synchronized void stop() {
            running = false;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running && subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !running && subscriptions.containsKey(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        private static final class Named {
            final StartAt startAt;
            final Function<CloudEvent, Mono<Void>> action;
            volatile long start = -1;

            Named(StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
                this.startAt = startAt;
                this.action = action;
            }
        }
    }
}
