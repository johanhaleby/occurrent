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
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscription from the subscription-model default that the durable model hands to a wrapped model managing named
 * subscriptions. The subscribe returns before the start position is read, and the durable model answers for the
 * subscription until the wrapped model has it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelHandOverTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final Duration RETURNS_AT_ONCE = Duration.ofSeconds(5);
    private static final String SUBSCRIPTION_ID = "sub";
    private static final String READ_FAILED = "The storage cannot read right now";
    private static final String REFUSED = "The wrapped model does not support this filter";

    private final HeldReadStorage storage = new HeldReadStorage();
    private final NamedFeed feed = new NamedFeed();
    private final ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(feed, storage);
    private final ExecutorService caller = Executors.newSingleThreadExecutor();
    private final LoggedByTheModel logged = new LoggedByTheModel();

    @AfterEach
    void shutdown() {
        storage.release.complete(null);
        logged.close();
        caller.shutdownNow();
        model.shutdown();
    }

    static Stream<Arguments> startPositionsAnsweringTheModelDefault() {
        return Stream.of(
                Arguments.of(Named.of("the model default", StartAt.subscriptionModelDefault())),
                Arguments.of(Named.of("a dynamic start position answering the model default", StartAt.dynamic(() -> StartAt.subscriptionModelDefault()))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("startPositionsAnsweringTheModelDefault")
    void a_subscribe_of_an_id_the_wrapped_model_holds_is_refused_at_the_call_and_stores_nothing(StartAt startAt) {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        feed.present.set(5);
        String storedBefore = storage.stored();
        int writesBefore = storage.writes.get();

        // When
        Throwable refused = catchThrowable(() -> model.subscribe(SUBSCRIPTION_ID, null, startAt, cloudEvent -> Mono.empty()));

        // Then
        assertThat(refused).as("the subscribe of the id the wrapped model holds").isExactlyInstanceOf(DuplicateSubscriptionIdException.class);
        assertThat(storage.stored()).as("the checkpoint stored for the id").isEqualTo(storedBefore);
        assertThat(storage.writes.get()).as("writes to storage").isEqualTo(writesBefore);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").containsExactly(SUBSCRIPTION_ID);
    }

    @Test
    void a_subscribe_from_the_model_default_returns_before_the_read_answers_and_the_model_answers_for_the_subscription_until_the_hand_over() throws Exception {
        // Given
        storage.holdNextRead();

        // When
        CompletableFuture<Subscription> subscribed = subscribeFromTheModelDefault();

        // Then
        assertThat(subscribed).as("the subscribe while the read is held").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(storage.heldReadEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the start position read is held").isTrue();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running while the read is held").isTrue();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).as("paused while the read is held").isFalse();
        assertThat(model.subscriptionIds()).as("subscription ids while the read is held").contains(SUBSCRIPTION_ID);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took while the read is held").isEmpty();

        storage.release.complete(null);
        assertThat(subscribed.join().waitUntilStarted().toFuture()).as("the start once the read answered").succeedsWithin(TIMEOUT);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once the read answered").isTrue();
    }

    @Test
    void a_pause_before_the_hand_over_leaves_the_subscription_paused_in_the_wrapped_model_until_it_is_resumed() throws Exception {
        // Given
        subscribedWhileTheReadIsHeld();

        // When
        model.pauseSubscription(SUBSCRIPTION_ID);
        boolean pausedWhileHeld = model.isPaused(SUBSCRIPTION_ID);
        storage.release.complete(null);

        // Then
        assertThat(pausedWhileHeld).as("paused while the read is held").isTrue();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(feed.isPaused(SUBSCRIPTION_ID)).as("paused in the wrapped model once handed over").isTrue());
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once handed over").isFalse();

        model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted().block(TIMEOUT);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once resumed").isTrue();
    }

    @Test
    void a_cancel_before_the_hand_over_leaves_nothing_running_or_stored_and_the_id_can_be_subscribed_again() throws Exception {
        // Given
        Subscription subscription = subscribedWhileTheReadIsHeld();

        // When
        model.cancelSubscription(SUBSCRIPTION_ID).block(TIMEOUT);
        storage.release.complete(null);

        // Then
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the cancelled subscription did not start").isInstanceOf(CancellationException.class);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        assertThat(storage.stored()).as("the checkpoint stored for the id").isEqualTo("-");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the cancel").isFalse();

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took once subscribed again").containsExactly(SUBSCRIPTION_ID);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once subscribed again").isTrue();
    }

    @Test
    void a_read_of_the_start_position_that_fails_fails_the_start_and_is_logged_as_an_error() throws Exception {
        // Given
        Subscription subscription = subscribedWhileTheReadIsHeld();

        // When
        storage.release.completeExceptionally(new IllegalStateException(READ_FAILED));

        // Then
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the subscription did not start").isInstanceOf(IllegalStateException.class).hasMessage(READ_FAILED);
        assertThat(logged.at(Level.ERROR)).as("errors logged").anySatisfy(message -> assertThat(message).startsWith("Subscription " + SUBSCRIPTION_ID + " could not be started"));
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the failed start").isFalse();
        assertThat(model.subscriptionIds()).as("subscription ids after the failed start").doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void a_refusal_of_the_wrapped_model_fails_the_start_and_is_logged_as_an_error() {
        // Given
        feed.refusal = new IllegalArgumentException(REFUSED);

        // When
        CompletableFuture<Subscription> subscribed = subscribeFromTheModelDefault();

        // Then
        assertThat(subscribed).as("the subscribe the wrapped model refuses").succeedsWithin(RETURNS_AT_ONCE);
        Throwable notStarted = catchThrowable(() -> subscribed.join().waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the subscription did not start").isInstanceOf(IllegalArgumentException.class).hasMessage(REFUSED);
        assertThat(logged.at(Level.ERROR)).as("errors logged").anySatisfy(message -> assertThat(message).startsWith("Subscription " + SUBSCRIPTION_ID + " could not be started"));
        assertThat(model.isRunning(SUBSCRIPTION_ID)).as("running after the refusal").isFalse();
        assertThat(model.subscriptionIds()).as("subscription ids after the refusal").doesNotContain(SUBSCRIPTION_ID);
    }

    @Test
    void a_subscription_from_the_model_default_with_nothing_stored_delivers_every_event_written_after_the_call_while_its_read_was_held() throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        storage.holdNextRead();
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(
                () -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller);
        assertThat(subscribed).as("the subscribe while the read is held").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(storage.heldReadEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the start position read is held").isTrue();

        // When
        long writtenFirst = feed.write();
        long writtenSecond = feed.write();
        storage.release.complete(null);
        subscribed.join().waitUntilStarted().block(TIMEOUT);
        long writtenAfter = feed.write();

        // Then
        await().atMost(TIMEOUT).until(() -> delivered.contains(writtenAfter));
        assertThat(delivered).as("events delivered").containsExactly(writtenFirst, writtenSecond, writtenAfter);
    }

    @Test
    void a_hand_over_the_cancel_ends_between_its_two_steps_fails_quietly_as_cancelled() throws Exception {
        // Given
        Subscription subscription = subscribedWhileTheReadIsHeld();
        CompletableFuture<Void> started = subscription.waitUntilStarted().toFuture();
        Thread cancelling = Thread.currentThread();
        AtomicBoolean askedByTheCancel = new AtomicBoolean();
        storage.whenDeleteConditionsAsked.set(() -> {
            askedByTheCancel.set(Thread.currentThread() == cancelling);
            storage.release.complete(null);
            catchThrowable(() -> started.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        });

        // When
        CompletableFuture<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID).toFuture();

        // Then
        assertThat(askedByTheCancel).as("the hand-over ran while the cancel was under way").isTrue();
        Throwable notStarted = catchThrowable(() -> subscription.waitUntilStarted().block(TIMEOUT));
        assertThat(notStarted).as("why the cancelled subscription did not start").isInstanceOf(CancellationException.class);
        assertThat(logged.at(Level.ERROR)).as("errors logged").noneSatisfy(message -> assertThat(message).startsWith("Subscription " + SUBSCRIPTION_ID + " could not be started"));
        assertThat(cancelled).as("the cancel").succeedsWithin(TIMEOUT);
        assertThat(feed.subscribedIds).as("subscriptions the wrapped model took").isEmpty();
        assertThat(storage.stored()).as("the checkpoint stored for the id").isEqualTo("-");

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        assertThat(feed.isRunning(SUBSCRIPTION_ID)).as("running in the wrapped model once subscribed again").isTrue();
    }

    private CompletableFuture<Subscription> subscribeFromTheModelDefault() {
        return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()), caller);
    }

    private static Function<CloudEvent, Mono<Void>> action(List<Long> delivered) {
        return cloudEvent -> Mono.fromRunnable(() -> delivered.add(Long.parseLong(cloudEvent.getId())));
    }

    private Subscription subscribedWhileTheReadIsHeld() throws Exception {
        storage.holdNextRead();
        CompletableFuture<Subscription> subscribed = subscribeFromTheModelDefault();
        assertThat(subscribed).as("the subscribe while the read is held").succeedsWithin(RETURNS_AT_ONCE);
        assertThat(storage.heldReadEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("the start position read is held").isTrue();
        return subscribed.join();
    }

    // Counts every save and delete, and holds the next read until release completes once holdNextRead is called. Runs
    // whenDeleteConditionsAsked once, on the thread that next asks whether it evaluates delete conditions.
    private static final class HeldReadStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        private final AtomicInteger writes = new AtomicInteger();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CountDownLatch heldReadEntered = new CountDownLatch(1);
        private final AtomicBoolean holdsNextRead = new AtomicBoolean();
        private final AtomicReference<@Nullable Runnable> whenDeleteConditionsAsked = new AtomicReference<>();

        private void holdNextRead() {
            holdsNextRead.set(true);
        }

        private String stored() {
            @Nullable Checkpoint checkpoint = storage.read(SUBSCRIPTION_ID).block();
            return checkpoint == null ? "-" : checkpoint.asString();
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                if (holdsNextRead.compareAndSet(true, false)) {
                    heldReadEntered.countDown();
                    return Mono.fromFuture(release, true).then(storage.read(subscriptionId));
                }
                return storage.read(subscriptionId);
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                writes.incrementAndGet();
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
        public boolean evaluatesDeleteConditions() {
            @Nullable Runnable asked = whenDeleteConditionsAsked.getAndSet(null);
            if (asked != null) {
                asked.run();
            }
            return storage.evaluatesDeleteConditions();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> {
                writes.incrementAndGet();
                return storage.delete(subscriptionId);
            });
        }
    }

    // Manages named subscriptions by id and refuses a duplicate as a real one does. Each reads the events written after
    // where it starts and moves past an event once its action for it has ended, and a pause stops the reading until a
    // resume. globalCheckpointAsOfNow() answers with where the feed is at the call.
    private static final class NamedFeed implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        private final AtomicLong present = new AtomicLong();
        private final Sinks.Many<CloudEvent> written = Sinks.many().replay().all();
        private final Map<String, Reading> readings = new ConcurrentHashMap<>();
        private final List<String> subscribedIds = new CopyOnWriteArrayList<>();
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
        // Where the feed is at the call, however late the Mono is subscribed to
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(new StringBasedCheckpoint(String.valueOf(present.get())));
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
                resolved = resolved.get(new SubscriptionModelContext(ReactorDurableSubscriptionModelHandOverTest.class));
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
