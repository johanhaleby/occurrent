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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * An event written after {@code subscribe(..)} or {@code start(..)} returned reaches a subscription that starts from the
 * subscription-model default or from {@link StartAt#now()}, also when its dynamic start position waits for the delete
 * of an earlier cancel and so resolves after that call returned.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelWrittenAfterTheCallTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final String POSITION_READ_FAILED = "The position of the feed cannot be read right now";

    /**
     * The model is stopped when the subscribe comes, and a cancel of the same id is still deleting its position. The
     * function runs only once the delete has ended, after the start here, and StartAt.now() and the model default both
     * start from where the feed was at the subscribe, read before it returned. So what is written between the
     * subscribe and the start is delivered as well as what is written after the start.
     */
    @ParameterizedTest
    @CsvSource({"false, now", "false, default", "true, now", "true, default"})
    void an_event_written_after_start_returned_reaches_a_subscription_whose_dynamic_start_position_waited_for_a_delete(boolean handsOver, String answer) {
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
            model.start(true);
            List<Long> writtenAfterTheStart = new ArrayList<>(List.of(feed.write(), feed.write()));
            storage.releaseDelete.countDown();
            // Not waitUntilStarted(), which a registration made while the model was stopped keeps waiting on once the
            // start has taken it over
            await().atMost(TIMEOUT).until(() -> feed.started() == 1);
            writtenAfterTheStart.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheStart))
                    .contains(String.valueOf(writtenBeforeTheStart));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The wrapped model answers where its feed is only some time after it is asked, as a model that asks a database
     * does. A subscribe whose dynamic start position waits for a delete reads where the feed is before it returns, and
     * on a thread that may block it waits for that answer, so what is written right after the return is delivered.
     */
    @ParameterizedTest
    @CsvSource({"false, now", "false, default", "true, now", "true, default"})
    void an_event_written_right_after_the_subscribe_returned_reaches_a_subscription_whose_dynamic_start_position_waited_for_a_delete_when_the_wrapped_model_answers_late(boolean handsOver, String answer) {
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
            List<Long> writtenAfterTheReturn = new ArrayList<>(List.of(feed.write(), feed.write()));
            storage.releaseDelete.countDown();
            subscription.waitUntilStarted().block(TIMEOUT);
            writtenAfterTheReturn.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheReturn.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheReturn));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A thread where Reactor does not allow blocking cannot wait for a read of where the feed is, so a subscribe there
     * with a dynamic start position that waits for a delete returns without waiting for it, and the function runs on
     * another thread once the delete has ended. One that answers StartAt.now() then starts from where the feed was
     * when that read answered, so what is written once it has answered and before the delete ended is delivered too.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_dynamic_start_position_answering_now_that_waits_for_a_delete_starts_from_the_read_at_the_subscribe_on_a_thread_that_may_not_block(boolean handsOver) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            Subscription subscription = requireNonNull(Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered)))
                    .subscribeOn(Schedulers.parallel())
                    .block(TIMEOUT));
            int startedWhileTheDeleteRan = feed.started();
            List<Long> writtenAfterTheReturn = new ArrayList<>(List.of(feed.write()));
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            writtenAfterTheReturn.add(feed.write());

            // Then
            assertThat(startedWhileTheDeleteRan).as("subscriptions that began reading from the feed while the delete ran").isZero();
            assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheReturn.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheReturn));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * As above with a dynamic start position that answers the model default, handed to a wrapped model that manages
     * named subscriptions. The subscribe returns without waiting for the read, and the subscription starts from where
     * the feed was when that read answered, once the delete has ended.
     */
    @Test
    void a_dynamic_start_position_answering_the_model_default_that_waits_for_a_delete_starts_from_the_read_at_the_subscribe_on_a_thread_that_may_not_block_when_handed_over() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        NamedFeed feed = new NamedFeed(true);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            Subscription subscription = requireNonNull(Mono.fromCallable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::subscriptionModelDefault), deliveredTo(delivered)))
                    .subscribeOn(Schedulers.parallel())
                    .block(TIMEOUT));
            int startedWhileTheDeleteRan = feed.started();
            long writtenWhileTheDeleteRan = feed.write();
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            long writtenAfterTheDelete = feed.write();

            // Then
            assertThat(startedWhileTheDeleteRan).as("subscriptions that began reading from the feed while the delete ran").isZero();
            assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheDelete)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(String.valueOf(writtenWhileTheDeleteRan), String.valueOf(writtenAfterTheDelete));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A dynamic start position that waits for a delete and answers StartAt.now() starts from where the feed was when
     * the subscribe returned, read before it returned. When that read fails or answers nothing there is no such
     * position, so the subscription is refused once the delete has ended, with what the read answered. Starting from
     * the present then instead would skip what was written between the subscribe and the end of the delete.
     */
    @ParameterizedTest
    @CsvSource({"false, fails", "false, empty", "true, fails", "true, empty"})
    void a_dynamic_start_position_that_waited_for_a_delete_and_answers_now_is_refused_once_the_delete_has_ended_when_the_read_at_the_call_could_not_answer(boolean handsOver, String read) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);

        try {
            // When
            feed.readFails = read.equals("fails");
            feed.answersNothing = read.equals("empty");
            Subscription subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), __ -> Mono.empty());
            feed.readFails = false;
            feed.answersNothing = false;
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(read.equals("fails") ? POSITION_READ_FAILED : "answered nothing");
            assertThat(feed.started()).as("subscriptions that began reading from the feed").isZero();
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The model drives the feed itself and is stopped when a subscribe from the model default comes, and the wrapped
     * model answers where its feed is only some time after it is asked. The model default means where the feed was
     * when the subscription was registered, so on a thread that may block the subscribe waits for that answer before it
     * returns, and what is written right after the return is delivered once the model is started.
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
     * checkpoint of the cancelled subscription, which lies ahead of the feed. A subscribe from the model default reads
     * no stored checkpoint while that delete is tried, since the delete removes it, and so neither waits for the delete
     * nor starts from that checkpoint. It starts from where the feed was at the call, and what is written right after
     * it returned is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_subscribe_from_the_model_default_starts_while_a_delete_keeps_failing_and_delivers_what_is_written_right_after_it_returned(boolean handsOver) {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("1000")).block(TIMEOUT);
        storage.deleteFails = true;
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
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
            Subscription subscription = subscribed.join();
            long writtenRightAfterTheReturn = feed.present.get() + 1;
            // On another thread, since a feed that manages named subscriptions writes only once the action and the
            // position write after it have ended, and that write waits for the delete
            CompletableFuture.runAsync(feed::write);
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended while the delete kept failing").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenRightAfterTheReturn)));
            assertThat(delivered).as("events delivered to the subscription").containsExactly(String.valueOf(writtenRightAfterTheReturn));
        } finally {
            model.shutdown();
        }
    }

    /**
     * The wrapped model answers where its feed is only some time after it is asked, and this model drives the feed
     * itself. A subscribe from the model default records where the feed was when it was registered, and on a thread
     * that may block it waits for that answer before it returns, so what is written right after the return is
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
     * A start of the model reads no position of its own, so a read of where the feed is that stops answering after the
     * subscribe holds up neither the start nor the subscription. A dynamic start position answering StartAt.now() that
     * waits for a delete runs once the delete has ended, and starts from where the feed was at the subscribe, which the
     * read answered before it hung. What is written after the start returned is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_start_of_the_model_returns_and_delivers_what_is_written_after_it_when_the_read_of_where_the_feed_is_hangs_behind_a_delete(boolean handsOver) {
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
        feed.readHangs = true;

        try {
            // When
            CompletableFuture<Void> started = CompletableFuture.runAsync(() -> model.start(true));
            Throwable startEnded = catchThrowable(() -> started.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            List<Long> writtenAfterTheStart = List.of(feed.write(), feed.write());
            storage.releaseDelete.countDown();

            // Then
            assertThat(startEnded).as("how the start ended while the read of where the feed is hung").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(writtenAfterTheStart));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A subscribe or a resume on a thread that may block waits for its read of where the feed is, so that a dynamic
     * start position waiting for a delete has a position to start from. Once the delete has ended there is no need to
     * wait for it, since the function then runs at the call and StartAt.now() means the present there, as it does
     * without a delete. So the end of the delete also ends the wait when the read never answers, and what is written
     * after the call returned is delivered.
     */
    @ParameterizedTest
    @CsvSource({"false, subscribe", "false, resume", "true, subscribe"})
    void a_call_whose_read_of_where_the_feed_is_never_answers_returns_once_the_delete_has_ended_for_a_dynamic_start_position_answering_now(boolean handsOver, String call) throws Exception {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = handsOver ? new NamedFeed(true) : new Feed();
        feed.readHangs = true;
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

        int readsBeforeTheCall = feed.reads.get();

        try {
            // When
            AtomicReference<@Nullable Thread> caller = new AtomicReference<>();
            CompletableFuture<Subscription> called = CompletableFuture.supplyAsync(() -> {
                caller.set(Thread.currentThread());
                return resumes
                        ? model.resumeSubscription(SUBSCRIPTION_ID)
                        : model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), action);
            });
            // The call has asked where the feed is and waits
            await().atMost(TIMEOUT).until(() -> feed.reads.get() > readsBeforeTheCall && isWaiting(caller.get()));
            storage.releaseDelete.countDown();
            Throwable callEnded = catchThrowable(() -> called.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            List<Long> writtenAfterTheCall = List.of(feed.write(), feed.write());

            // Then
            assertThat(callEnded).as("how the call ended once the delete had ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheCall.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(writtenAfterTheCall));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The wrapped model is stopped when the subscribe comes and answers where its feed is only some time after it is
     * asked. The subscribe waits for that answer, and the function runs once the delete has ended and starts from it,
     * so a start from a thread where Reactor does not allow blocking has no read to wait for, and what is written after
     * the start returned is delivered.
     */
    @Test
    void an_event_written_after_a_start_on_a_thread_that_may_not_block_returned_reaches_a_subscription_whose_dynamic_start_position_waited_for_a_delete() {
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
            List<Long> writtenAfterTheStart = new ArrayList<>(List.of(feed.write(), feed.write()));
            storage.releaseDelete.countDown();
            await().atMost(TIMEOUT).until(() -> feed.started() == 1);
            writtenAfterTheStart.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheStart));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * When this model drives the feed itself and is stopped, a dynamic start position that waits for a delete runs
     * once the delete has ended, also when the model is started on a thread where Reactor does not allow blocking. One
     * that answers StartAt.now() then starts from where the feed was when the subscribe registered it, so the start
     * waits on no read, and what is written after the start returned is delivered.
     */
    @Test
    void a_dynamic_start_position_answering_now_that_waits_for_a_delete_starts_once_it_has_ended_when_the_model_it_drives_is_started_on_a_thread_that_may_not_block() {
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
     * A pause that comes while the position write after an event waits for the delete of an earlier cancel resumes
     * after that event, since its action already ran. Resuming from where the subscription started would skip what was
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
     * A dynamic start position answering StartAt.now() that waits for a delete has not resolved when the
     * subscription is paused, so the resume starts the generation it begins from where the feed was at the subscribe,
     * as the first one would have. Reading where the feed is at the resume would skip what was written between the
     * subscribe and the resume.
     */
    @Test
    void a_resume_of_a_dynamic_start_position_answering_now_that_never_resolved_starts_from_where_the_feed_was_at_the_subscribe() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = new Feed();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered));
            long writtenBeforeThePause = feed.write();
            model.pauseSubscription(SUBSCRIPTION_ID);
            long writtenWhilePaused = feed.write();
            model.resumeSubscription(SUBSCRIPTION_ID);
            storage.releaseDelete.countDown();
            await().atMost(TIMEOUT).until(() -> feed.started() == 1);
            long writtenOnceStarted = feed.write();

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenOnceStarted)));
            assertThat(delivered).as("events delivered to the subscription")
                    .containsExactly(String.valueOf(writtenBeforeThePause), String.valueOf(writtenWhilePaused), String.valueOf(writtenOnceStarted));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * With startWhenNoStartPositionCanBeRecorded, a subscription from the model default whose read of where the feed
     * is answers nothing starts from the present, recording nothing. Behind the delete of an earlier cancel the model
     * reads no stored checkpoint, since the delete removes it, so the feed opens at the call, as it does with nothing
     * stored.
     */
    @Test
    void a_subscription_from_the_model_default_that_may_start_without_a_recorded_position_opens_the_feed_at_the_call_while_a_delete_runs() {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        Feed feed = new Feed();
        feed.answersNothing = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage,
                new ReactorDurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();

        try {
            // When
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), deliveredTo(delivered));
            List<Long> written = new ArrayList<>(List.of(feed.write(), feed.write()));
            storage.releaseDelete.countDown();
            written.add(feed.write());

            // Then
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(written.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsExactlyElementsOf(ids(written));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A start of the model starts every subscription registered while it was stopped without waiting for any of their
     * reads of where the feed is, so one read that never answers neither holds up the start nor the subscriptions
     * after it. The subscribe whose read hangs waits for it on its own thread, and a shutdown ends that wait.
     */
    @Test
    void a_start_of_the_model_returns_and_starts_the_other_subscriptions_when_one_read_of_where_the_feed_is_never_answers() throws Exception {
        // Given
        Feed feed = new Feed();
        feed.firstReadHangs = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, new InMemoryCheckpointStorage());
        model.stop();
        CompletableFuture<Subscription> hung = CompletableFuture.supplyAsync(() -> model.subscribe("hung", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()));
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
            assertThat(hung).as("subscribe whose read hangs").isNotDone();
        } finally {
            model.shutdown();
        }
        assertThat(catchThrowable(() -> hung.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).waitUntilStarted().block(TIMEOUT)))
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
     * A subscribe on a thread that may block waits for its read of where the feed is before it returns. A cancel of
     * the id ends that wait, along with the subscription, so a read that never answers does not hold the thread that
     * subscribed once nobody can use the subscription.
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
     * The wrapped model is stopped at the subscribe, and the read of where its feed is fails or answers nothing. A
     * dynamic start position answering StartAt.now() that waits for a delete has no position to start from once the
     * delete has ended, so the subscription is refused then, as on a running model. The start of the model goes ahead
     * and does not throw.
     */
    @ParameterizedTest
    @ValueSource(strings = {"fails", "empty"})
    void a_start_of_a_stopped_wrapped_model_goes_ahead_when_a_subscription_whose_read_at_the_subscribe_could_not_answer_is_refused_once_the_delete_has_ended(String read) {
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
            assertThat(thrown).as("how waiting for the start of the subscription ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(read.equals("fails") ? POSITION_READ_FAILED : "answered nothing");
            assertThat(feed.started()).as("subscriptions that began reading from the feed").isZero();
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    private static boolean isWaiting(@Nullable Thread thread) {
        return thread != null && (thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING);
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
        final InMemoryCheckpointStorage stored = new InMemoryCheckpointStorage();
        final CountDownLatch releaseDelete = new CountDownLatch(1);
        volatile boolean deleteFails;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return stored.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return stored.save(subscriptionId, checkpoint, writeCondition);
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
            if (deleteFails) {
                return Mono.error(new IllegalStateException("The storage cannot delete right now"));
            }
            return Mono.fromCallable(() -> releaseDelete.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .subscribeOn(Schedulers.boundedElastic())
                    .then(Mono.defer(() -> stored.delete(subscriptionId)));
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
            int read = reads.incrementAndGet();
            if (readHangs || (firstReadHangs && read == 1)) {
                return Mono.never();
            } else if (readFails) {
                return Mono.error(new IllegalStateException(POSITION_READ_FAILED));
            } else if (answersNothing) {
                return Mono.empty();
            }
            Mono<Checkpoint> answer = Mono.fromSupplier(() -> new StringBasedCheckpoint(String.valueOf(present.get())));
            return answerDelay.isZero() ? answer : Mono.delay(answerDelay).then(answer);
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
