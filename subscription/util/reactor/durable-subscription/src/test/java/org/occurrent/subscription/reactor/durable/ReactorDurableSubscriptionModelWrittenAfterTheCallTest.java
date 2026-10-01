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
     * The model is stopped when the subscribe comes, and a cancel of the same id is still deleting its position, so
     * the function runs only once the delete has ended, after {@code start(true)} returned. StartAt.now() on a stopped
     * model means where the feed is once it is started, so what is written after the start returned is delivered.
     * The model default means where the feed was at registration, so what is written before the start is delivered
     * too.
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
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheStart));
            if (answer.equals("default")) {
                assertThat(delivered).as("events delivered to the subscription").contains(String.valueOf(writtenBeforeTheStart));
            }
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
     * A thread where Reactor does not allow blocking cannot wait for a read of where the feed is, so a dynamic start
     * position that would wait for a delete runs at the call there instead, as it does without a delete and as it did
     * in 0.33.0. One that answers StartAt.now() starts from where the feed is at the call, and what is written after
     * the subscribe returned is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_dynamic_start_position_answering_now_that_would_wait_for_a_delete_starts_at_the_call_on_a_thread_that_may_not_block(boolean handsOver) {
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
            storage.releaseDelete.countDown();
            Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
            List<Long> writtenAfterTheReturn = List.of(feed.write(), feed.write());

            // Then
            assertThat(thrown).as("how waiting for the start of the subscription ended").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheReturn.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheReturn));
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * Handing the subscription to a wrapped model that manages named subscriptions reads the position of the model
     * default on the caller's thread before the subscribe returns, which a thread where Reactor does not allow blocking
     * refuses. A dynamic start position that would wait for a delete and answers the model default is refused there
     * the same way, from the subscribe itself, since its function runs at the call, as it does without a delete and as
     * it did in 0.33.0.
     */
    @Test
    void a_dynamic_start_position_answering_the_model_default_that_would_wait_for_a_delete_is_refused_by_the_subscribe_on_a_thread_that_may_not_block_when_handed_over() {
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
            assertThat(thrown).as("how the subscribe ended").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("which is not supported in thread");
            assertThat(feed.started()).as("subscriptions that began reading from the feed").isZero();
        } finally {
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * A dynamic start position that waits for a delete and answers StartAt.now() starts from where the feed was when
     * the subscribe returned, read before it returned. When that read fails or answers nothing there is no such
     * position, and starting from wherever the feed is once the delete has ended would skip what was written in
     * between. So the subscription is refused instead, as one from the model default is when its read cannot answer.
     */
    @ParameterizedTest
    @CsvSource({"false, fails", "false, empty", "true, fails", "true, empty"})
    void a_dynamic_start_position_that_waited_for_a_delete_and_answers_now_is_refused_when_the_read_at_the_call_could_not_answer(boolean handsOver, String read) {
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
            feed.write();
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
     * The function answers StartAt.now() while the wrapped model is still stopped, so the subscription is handed to
     * that model, which applies it when it starts. A start that comes while the hand-over is still under way does not
     * wait for it, on a thread that may block or not, since the hand-over runs the wrapped model's subscribe, which can
     * take any time. The wrapped model can then be running before it has the subscription, so what it gets answers
     * where the feed was at the subscribe once a start came meanwhile, and what is written after the start returned
     * is delivered.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_start_that_comes_while_a_dynamic_start_position_answering_now_is_handed_over_does_not_wait_for_it(boolean startMayBlock) throws Exception {
        // Given
        HeldDeleteStorage storage = new HeldDeleteStorage();
        NamedFeed feed = new NamedFeed(false);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
        model.cancelSubscription(SUBSCRIPTION_ID);
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), deliveredTo(delivered));
        feed.write();
        CountDownLatch handOverReached = new CountDownLatch(1);
        CountDownLatch releaseHandOver = new CountDownLatch(1);
        feed.beforeSubscribe = () -> {
            handOverReached.countDown();
            awaitReleased(releaseHandOver);
        };

        try {
            // When
            storage.releaseDelete.countDown();
            assertThat(handOverReached.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the hand-over reached the wrapped model").isTrue();
            CompletableFuture<Void> started = startMayBlock
                    ? CompletableFuture.runAsync(() -> model.start(true))
                    : Mono.<Void>fromRunnable(() -> model.start(true)).subscribeOn(Schedulers.parallel()).toFuture();
            Throwable startEnded = catchThrowable(() -> started.get(2, TimeUnit.SECONDS));
            List<Long> writtenAfterTheStart = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                writtenAfterTheStart.add(feed.write());
            }
            releaseHandOver.countDown();
            await().atMost(TIMEOUT).until(() -> feed.started() == 1);
            writtenAfterTheStart.add(feed.write());

            // Then
            assertThat(startEnded).as("how the start ended while the hand-over was held").isNull();
            await().atMost(TIMEOUT).until(() -> delivered.contains(String.valueOf(writtenAfterTheStart.getLast())));
            assertThat(delivered).as("events delivered to the subscription").containsAll(ids(writtenAfterTheStart));
        } finally {
            releaseHandOver.countDown();
            storage.releaseDelete.countDown();
            model.shutdown();
        }
    }

    /**
     * The wrapped model is stopped when the subscribe comes and answers where its feed is only some time after it is
     * asked. The start comes from a thread where Reactor does not allow blocking, so it cannot wait for a read of where
     * the feed is once it has started the wrapped model, and such a read could answer with a position from after the
     * start returned. The subscription starts from what the subscribe read and awaited instead, which is not after the
     * start, so what is written after the start returned is delivered, along with what was written between the
     * subscribe and the start.
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
     * When this model drives the feed itself and is stopped, a start from a thread where Reactor does not allow
     * blocking cannot wait for a read of where the feed is, so a dynamic start position that would wait for a delete
     * runs at the start instead, as it does without a delete and as it did in 0.33.0. One that answers StartAt.now()
     * starts from where the feed is then, and what is written after the start returned is delivered.
     */
    @Test
    void a_dynamic_start_position_answering_now_that_would_wait_for_a_delete_starts_when_the_model_it_drives_is_started_on_a_thread_that_may_not_block() {
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

    private static void awaitReleased(CountDownLatch latch) {
        try {
            latch.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
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
            if (readFails) {
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
        /**
         * Runs first in every named subscribe, outside this model's monitor, so a test can hold a subscribe open.
         */
        volatile Runnable beforeSubscribe = () -> {
        };

        NamedFeed(boolean running) {
            this.running = running;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            beforeSubscribe.run();
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
