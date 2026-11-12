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

package org.occurrent.subscription.blocking.durable.catchup;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.AbstractCatchupSubscriptionModel.LIFECYCLE_WAIT;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A dynamic start-position function can wait for a call for the same subscription id made on another thread. The blocking subscription models ask the function before they take the claim or the lock for the id, so a
 * cancel made meanwhile returns at once and wins over the subscribe, and a pause on a catch-up model waits for the
 * subscribe, up to {@code LIFECYCLE_WAIT}, and then throws. The cases where the function waits for the cancel to return
 * run on a plain MongoDB model, which never held anything while it asked, and on the three wrappers that did. A cancel
 * made while the function runs can also wait for another call for the id to give up the claim or the lock, and get it
 * only once the function has returned. That cancel wins over the subscribe too. The case runs on the three wrappers
 * only, since a plain MongoDB model has no claim or lock for an id. The pause case runs on the two catch-up stacks, the
 * handover case on the catch-up over the durable model, and the case of a resume that waits for the subscribe on the
 * durable model. The case of a function that cancels its own id on the subscribing thread, which doesn't stop the
 * subscribe, runs on the two catch-up stacks.
 */
@Testcontainers
@Timeout(120)
@DisplayName("BlockingDynamicStartCancel")
@DisplayNameGeneration(ReplaceUnderscores.class)
class BlockingDynamicStartCancelMongoTest {

    // Longer than LIFECYCLE_WAIT, so a claim that gives up while the function still waits shows as a failed cancel
    private static final Duration FUNCTION_WAITS = LIFECYCLE_WAIT.plusSeconds(2);
    private static final Duration FUNCTION_WAITS_FOR_PAUSE = Duration.ofSeconds(30);
    private static final Duration EVENT_DELIVERY = Duration.ofSeconds(10);
    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    enum Stack {MONGO, DURABLE_OVER_MONGO, CATCHUP_OVER_MONGO, CATCHUP_OVER_DURABLE_OVER_MONGO}

    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private String eventCollectionName;
    private SpringMongoEventStore eventStore;
    private SpringMongoCheckpointStorage storage;
    private SubscriptionModel model;
    private ExecutorService otherThreads;
    private final String subscriptionId = UUID.randomUUID().toString();

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        eventCollectionName = requireNonNull(connectionString.getCollection());
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        storage = new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID());
        otherThreads = Executors.newCachedThreadPool();
    }

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
        otherThreads.shutdownNow();
        mongoClient.close();
    }

    @Nested
    @DisplayName("when the start-position function waits for a cancel of its own subscription made on another thread")
    class When_the_function_waits_for_a_cancel {

        @ParameterizedTest
        @EnumSource(Stack.class)
        @DisplayName("the cancel returns while the function waits for it")
        void the_cancel_returns_while_the_function_waits_for_it(Stack stack) {
            // Given
            model = build(stack);
            WaitsForCancel function = new WaitsForCancel();

            // When
            model.subscribe(subscriptionId, function.startAt(), event -> {
            });

            // Then
            function.assertTheCancelReturnedWhileTheFunctionWaited();
        }

        @ParameterizedTest
        @EnumSource(Stack.class)
        @DisplayName("the cancel returns when the function that waits is the inner one of a nested dynamic start position")
        void the_cancel_returns_when_the_function_that_waits_is_the_inner_one_of_a_nested_dynamic_start_position(Stack stack) {
            // Given
            model = build(stack);
            WaitsForCancel inner = new WaitsForCancel();

            // When
            model.subscribe(subscriptionId, StartAt.dynamic(context -> inner.startAt()), event -> {
            });

            // Then
            inner.assertTheCancelReturnedWhileTheFunctionWaited();
        }

        @ParameterizedTest
        @EnumSource(Stack.class)
        @DisplayName("the returned subscription never starts and the id is not running")
        void the_returned_subscription_never_starts_and_the_id_is_not_running(Stack stack) {
            // Given
            model = build(stack);
            Subscription returned = subscribeAndLetTheFunctionWaitForTheCancel(new CopyOnWriteArrayList<>());

            // When
            boolean started = returned.waitUntilStarted(Duration.ofMillis(500));

            // Then
            assertAll(
                    () -> assertThat(started).as("the returned subscription started").isFalse(),
                    () -> assertThat(model.isRunning(subscriptionId)).as("the id is running").isFalse()
            );
        }

        @ParameterizedTest
        @EnumSource(Stack.class)
        @DisplayName("no event reaches the action of the cancelled subscribe")
        void no_event_reaches_the_action_of_the_cancelled_subscribe(Stack stack) {
            // Given
            model = build(stack);
            List<CloudEvent> received = new CopyOnWriteArrayList<>();
            subscribeAndLetTheFunctionWaitForTheCancel(received);

            // When
            append();

            // Then an event that does not arrive can only be shown by waiting, so the action is watched for a while
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).until(received::isEmpty);
        }

        @ParameterizedTest
        @EnumSource(Stack.class)
        @DisplayName("the id can be subscribed again and the new subscription is delivered an event")
        void the_id_can_be_subscribed_again_and_the_new_subscription_is_delivered_an_event(Stack stack) {
            // Given
            model = build(stack);
            List<CloudEvent> cancelled = new CopyOnWriteArrayList<>();
            subscribeAndLetTheFunctionWaitForTheCancel(cancelled);
            List<CloudEvent> again = new CopyOnWriteArrayList<>();

            // When
            boolean started = model.subscribe(subscriptionId, StartAt.now(), again::add).waitUntilStarted(EVENT_DELIVERY);
            append();

            // Then
            await().atMost(EVENT_DELIVERY).until(() -> again.size() == 1);
            assertAll(
                    () -> assertThat(started).as("the new subscription started").isTrue(),
                    () -> assertThat(cancelled).as("events that reached the cancelled subscribe").isEmpty()
            );
        }

        @ParameterizedTest
        @EnumSource(Stack.class)
        @DisplayName("a function that waits for nothing delivers an event")
        void a_function_that_waits_for_nothing_delivers_an_event(Stack stack) {
            // Given
            model = build(stack);
            List<CloudEvent> received = new CopyOnWriteArrayList<>();

            // When
            boolean started = model.subscribe(subscriptionId, StartAt.dynamic(context -> StartAt.now()), received::add).waitUntilStarted(EVENT_DELIVERY);
            append();

            // Then
            await().atMost(EVENT_DELIVERY).until(() -> received.size() == 1);
            assertThat(started).isTrue();
        }

        private Subscription subscribeAndLetTheFunctionWaitForTheCancel(List<CloudEvent> received) {
            WaitsForCancel function = new WaitsForCancel();
            Subscription returned = model.subscribe(subscriptionId, function.startAt(), received::add);
            function.assertTheCancelReturnedWhileTheFunctionWaited();
            return returned;
        }
    }

    @Nested
    @DisplayName("when a cancel of its own subscription made while the start-position function runs waits behind another call for the id until the function has returned")
    class When_a_cancel_waits_behind_another_call_until_the_function_has_returned {

        private StorageThatHoldsTheFirstDelete storageThatHoldsTheFirstDelete;

        @BeforeEach
        void hold_the_first_delete_of_a_checkpoint() {
            storageThatHoldsTheFirstDelete = new StorageThatHoldsTheFirstDelete(mongoTemplate, "storage-" + UUID.randomUUID());
            storage = storageThatHoldsTheFirstDelete;
        }

        @ParameterizedTest
        @EnumSource(value = Stack.class, names = {"DURABLE_OVER_MONGO", "CATCHUP_OVER_MONGO", "CATCHUP_OVER_DURABLE_OVER_MONGO"})
        @DisplayName("the returned subscription never starts and the id is not running")
        void the_returned_subscription_never_starts_and_the_id_is_not_running(Stack stack) {
            // Given
            model = build(stack);
            Subscription returned = subscribeWhileACancelWaitsBehindAnotherCall(new CopyOnWriteArrayList<>());

            // When
            boolean started = returned.waitUntilStarted(Duration.ofMillis(500));

            // Then
            assertAll(
                    () -> assertThat(started).as("the returned subscription started").isFalse(),
                    () -> assertThat(model.isRunning(subscriptionId)).as("the id is running").isFalse()
            );
        }

        @ParameterizedTest
        @EnumSource(value = Stack.class, names = {"DURABLE_OVER_MONGO", "CATCHUP_OVER_MONGO", "CATCHUP_OVER_DURABLE_OVER_MONGO"})
        @DisplayName("no event reaches the action of the cancelled subscribe")
        void no_event_reaches_the_action_of_the_cancelled_subscribe(Stack stack) {
            // Given
            model = build(stack);
            List<CloudEvent> received = new CopyOnWriteArrayList<>();
            subscribeWhileACancelWaitsBehindAnotherCall(received);

            // When
            append();

            // Then an event that does not arrive can only be shown by waiting, so the action is watched for a while
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(received).as("events that reached the cancelled subscribe").isEmpty());
        }

        // The function starts another cancel, which holds the claim or the lock for the id while it deletes the checkpoint
        // and then throws, so that cancel stops nothing. The function then starts the cancel checked here and returns
        // once it waits for the claim or the lock, and the delete goes on once the subscribe waits behind it. The cancel
        // then gets the claim or the lock first, with the function returned and the subscribe not yet holding it. The
        // other call starts in the function, since a catch-up model takes the claim before it asks the function
        private Subscription subscribeWhileACancelWaitsBehindAnotherCall(List<CloudEvent> received) {
            AtomicBoolean asked = new AtomicBoolean();
            AtomicBoolean functionReturned = new AtomicBoolean();
            AtomicReference<Thread> cancelling = new AtomicReference<>();
            AtomicReference<CompletableFuture<Void>> otherCall = new AtomicReference<>();
            CompletableFuture<Void> cancel = new CompletableFuture<>();
            StartAt function = StartAt.dynamic(context -> {
                if (asked.compareAndSet(false, true)) {
                    otherCall.set(CompletableFuture.runAsync(() -> model.cancelSubscription(subscriptionId), otherThreads));
                    await().atMost(EVENT_DELIVERY).until(storageThatHoldsTheFirstDelete.firstDeleteStarted::isDone);
                    CompletableFuture.runAsync(() -> {
                        cancelling.set(Thread.currentThread());
                        model.cancelSubscription(subscriptionId);
                    }, otherThreads).whenComplete((__, error) -> {
                        if (error == null) {
                            cancel.complete(null);
                        } else {
                            cancel.completeExceptionally(error);
                        }
                    });
                    await().atMost(EVENT_DELIVERY).until(() -> waitsForALock(cancelling.get()));
                    functionReturned.set(true);
                }
                return StartAt.now();
            });
            AtomicReference<Thread> subscribing = new AtomicReference<>();
            CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> {
                subscribing.set(Thread.currentThread());
                return model.subscribe(subscriptionId, function, received::add);
            }, otherThreads);
            await().atMost(EVENT_DELIVERY).until(() -> functionReturned.get() && waitsForALock(subscribing.get()));

            storageThatHoldsTheFirstDelete.firstDeleteMayGoOn.complete(null);

            assertAll(
                    () -> assertThat(otherCall.get()).as("the other call holding the id").failsWithin(EVENT_DELIVERY)
                            .withThrowableThat().withStackTraceContaining("the first delete failed"),
                    () -> assertThat(cancel).as("the cancel made while the function ran").succeedsWithin(EVENT_DELIVERY),
                    () -> assertThat(subscribe).as("the subscribe").succeedsWithin(EVENT_DELIVERY)
            );
            return subscribe.join();
        }
    }

    @Nested
    @DisplayName("when the start-position function is asked during the handover and waits for a cancel of its own subscription made on another thread")
    class When_the_function_is_asked_during_the_handover {

        @Disabled("A cancel waits for the handover lock of the id, which the replay thread holds while the function, asked there during the handover, waits for the cancel. Red on main too.")
        @Test
        void the_cancel_returns_while_the_function_waits_for_it() {
            // Given
            model = build(Stack.CATCHUP_OVER_DURABLE_OVER_MONGO);
            append();
            WaitsForCancel function = new WaitsForCancel();

            // When
            model.subscribe(subscriptionId, function.startAtWhenTheHandoverAsks(Thread.currentThread().getName()), event -> {
            });

            // Then
            function.assertTheCancelReturnedWhileTheFunctionWaited();
        }
    }

    @Nested
    @DisplayName("when the start-position function waits for a pause of its own subscription made on another thread")
    class When_the_function_waits_for_a_pause {

        @ParameterizedTest
        @EnumSource(value = Stack.class, names = {"CATCHUP_OVER_MONGO", "CATCHUP_OVER_DURABLE_OVER_MONGO"})
        @DisplayName("the pause throws IllegalStateException after the lifecycle wait and the subscribe then completes")
        void throws_IllegalStateException_after_the_lifecycle_wait_and_the_subscribe_then_completes(Stack stack) {
            // Given
            model = build(stack);
            AtomicBoolean asked = new AtomicBoolean();
            CompletableFuture<PauseOutcome> pause = new CompletableFuture<>();
            StartAt function = StartAt.dynamic(context -> {
                if (asked.compareAndSet(false, true)) {
                    CompletableFuture.supplyAsync(() -> {
                        long start = System.nanoTime();
                        Throwable thrown = catchThrowable(() -> model.pauseSubscription(subscriptionId));
                        return new PauseOutcome(thrown, Duration.ofNanos(System.nanoTime() - start));
                    }, otherThreads).whenComplete((outcome, error) -> {
                        if (error == null) {
                            pause.complete(outcome);
                        } else {
                            pause.completeExceptionally(error);
                        }
                    });
                    waitFor(pause, FUNCTION_WAITS_FOR_PAUSE);
                }
                return StartAt.now();
            });

            // When
            Subscription returned = model.subscribe(subscriptionId, function, event -> {
            });

            // Then
            PauseOutcome outcome = pause.join();
            assertAll(
                    () -> assertThat(outcome.thrown()).as("what the pause threw").isInstanceOf(IllegalStateException.class),
                    () -> assertThat(outcome.waited()).as("how long the pause waited").isGreaterThanOrEqualTo(LIFECYCLE_WAIT),
                    () -> assertThat(returned.waitUntilStarted(EVENT_DELIVERY)).as("the subscribe started").isTrue(),
                    () -> assertThat(model.isPaused(subscriptionId)).as("the id is paused after the failed pause").isFalse()
            );
        }
    }

    @Nested
    @DisplayName("when a resume of the id is made on another thread while the start-position function of a paused subscribe runs")
    class When_a_resume_is_made_while_the_function_of_a_paused_subscribe_runs {

        @Test
        @DisplayName("the resume waits for the subscribe and then returns, and the subscription runs and is delivered an event")
        void the_resume_waits_for_the_subscribe_and_then_returns_and_the_subscription_runs_and_is_delivered_an_event() {
            // Given
            model = build(Stack.DURABLE_OVER_MONGO);
            List<CloudEvent> received = new CopyOnWriteArrayList<>();
            AtomicBoolean asked = new AtomicBoolean();
            AtomicBoolean resumeReturnedBeforeTheFunctionDid = new AtomicBoolean();
            AtomicReference<Thread> resuming = new AtomicReference<>();
            CompletableFuture<Subscription> resume = new CompletableFuture<>();
            StartAt function = StartAt.dynamic(context -> {
                if (asked.compareAndSet(false, true)) {
                    CompletableFuture.supplyAsync(() -> {
                        resuming.set(Thread.currentThread());
                        return model.resumeSubscription(subscriptionId);
                    }, otherThreads).whenComplete((resumed, error) -> {
                        if (error == null) {
                            resume.complete(resumed);
                        } else {
                            resume.completeExceptionally(error);
                        }
                    });
                    await().atMost(EVENT_DELIVERY).until(() -> resume.isDone() || waitsOnAFuture(resuming.get()));
                    resumeReturnedBeforeTheFunctionDid.set(resume.isDone());
                }
                return StartAt.now();
            });

            // When
            model.subscribePaused(subscriptionId, null, function, received::add);

            // Then
            assertThat(resume).as("the resume made while the function ran").succeedsWithin(EVENT_DELIVERY);
            Subscription resumed = resume.join();
            assertAll(
                    () -> assertThat(resumeReturnedBeforeTheFunctionDid).as("the resume returned before the function did").isFalse(),
                    () -> assertThat(resumed.waitUntilStarted(EVENT_DELIVERY)).as("the resumed subscription started").isTrue(),
                    () -> assertThat(model.isRunning(subscriptionId)).as("the id is running").isTrue(),
                    () -> assertThat(model.isPaused(subscriptionId)).as("the id is paused").isFalse()
            );
            append();
            await().atMost(EVENT_DELIVERY).until(() -> received.size() == 1);
        }
    }

    @Nested
    @DisplayName("when the start-position function cancels its own subscription id on the subscribing thread")
    class When_the_function_cancels_its_own_id_on_the_subscribing_thread {

        @ParameterizedTest
        @EnumSource(value = Stack.class, names = {"CATCHUP_OVER_MONGO", "CATCHUP_OVER_DURABLE_OVER_MONGO"})
        @DisplayName("the subscribe still subscribes and the subscription is delivered an event")
        void the_subscribe_still_subscribes_and_the_subscription_is_delivered_an_event(Stack stack) {
            // Given
            model = build(stack);
            List<CloudEvent> received = new CopyOnWriteArrayList<>();
            AtomicBoolean cancelled = new AtomicBoolean();
            StartAt function = StartAt.dynamic(context -> {
                if (cancelled.compareAndSet(false, true)) {
                    model.cancelSubscription(subscriptionId);
                }
                return StartAt.now();
            });

            // When
            Subscription returned = model.subscribe(subscriptionId, function, received::add);
            boolean started = returned.waitUntilStarted(Duration.ofSeconds(2));
            append();

            // Then
            assertAll(
                    () -> assertThat(cancelled).as("the function cancelled its own id").isTrue(),
                    () -> assertThat(started).as("the returned subscription started").isTrue(),
                    () -> assertThat(model.isRunning(subscriptionId)).as("the id is running").isTrue()
            );
            await().atMost(EVENT_DELIVERY).until(() -> received.size() == 1);
        }
    }

    @Nested
    @DisplayName("when a cancel made while the start-position function blocks on another thread throws")
    class When_a_cancel_made_while_the_function_blocks_throws {

        @Test
        @DisplayName("the cancel throws and the subscribe still subscribes and the subscription is delivered an event")
        void the_cancel_throws_and_the_subscribe_still_subscribes_and_the_subscription_is_delivered_an_event() {
            // Given
            SpringMongoSubscriptionModel mongo = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
            SpringMongoCheckpointStorage throwsOnDelete = new ThrowsOnDelete(mongoTemplate, "storage-" + UUID.randomUUID());
            model = new StreamCatchupSubscriptionModel(mongo, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(throwsOnDelete)));
            List<CloudEvent> received = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> functionStarted = new CompletableFuture<>();
            CompletableFuture<Void> releaseTheFunction = new CompletableFuture<>();
            StartAt function = StartAt.dynamic(context -> {
                functionStarted.complete(null);
                releaseTheFunction.orTimeout(30, SECONDS).join();
                return StartAt.now();
            });
            CompletableFuture<Subscription> subscribe = CompletableFuture.supplyAsync(() -> model.subscribe(subscriptionId, function, received::add), otherThreads);
            assertThat(functionStarted).as("the function asked").succeedsWithin(EVENT_DELIVERY);

            // When
            CompletableFuture<Void> cancel = CompletableFuture.runAsync(() -> model.cancelSubscription(subscriptionId), otherThreads);
            assertThat(cancel).as("the cancel").failsWithin(EVENT_DELIVERY)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(IllegalStateException.class)
                    .withMessageContaining("the delete failed");
            releaseTheFunction.complete(null);

            // Then
            assertThat(subscribe).as("the subscribe").succeedsWithin(EVENT_DELIVERY);
            Subscription returned = subscribe.join();
            boolean started = returned.waitUntilStarted(Duration.ofSeconds(2));
            assertAll(
                    () -> assertThat(started).as("the returned subscription started").isTrue(),
                    () -> assertThat(model.isRunning(subscriptionId)).as("the id is running").isTrue()
            );
            append();
            await().atMost(EVENT_DELIVERY).until(() -> received.size() == 1);
        }
    }

    private SubscriptionModel build(Stack stack) {
        SpringMongoSubscriptionModel mongo = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
        return switch (stack) {
            case MONGO -> mongo;
            case DURABLE_OVER_MONGO -> new DurableSubscriptionModel(mongo, storage);
            case CATCHUP_OVER_MONGO -> new StreamCatchupSubscriptionModel(mongo, eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
            case CATCHUP_OVER_DURABLE_OVER_MONGO ->
                    new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(mongo, storage), eventStore, new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage)));
        };
    }

    private void append() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType("test")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write(UUID.randomUUID().toString(), event);
    }

    private static void waitFor(CompletableFuture<?> future, Duration atMost) {
        try {
            future.get(atMost.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while the function waited", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("The call the function waited for did not return normally: " + e, e);
        }
    }

    private record PauseOutcome(Throwable thrown, Duration waited) {
    }

    // Parked in a timed CompletableFuture.get, which is how a resume waits for a subscribe whose function runs
    private static boolean waitsOnAFuture(Thread thread) {
        return thread != null && thread.getState() == Thread.State.TIMED_WAITING;
    }

    // Parked on a ReentrantLock, which the claim of a catch-up model and the lock of a durable model are. A thread
    // parked there is already queued for it, behind every thread that queued before
    private static boolean waitsForALock(Thread thread) {
        return thread != null && LockSupport.getBlocker(thread) instanceof AbstractQueuedSynchronizer;
    }

    private static final class ThrowsOnDelete extends SpringMongoCheckpointStorage {
        private ThrowsOnDelete(MongoTemplate mongoTemplate, String checkpointCollection) {
            super(mongoTemplate, checkpointCollection);
        }

        @Override
        public void delete(String subscriptionId) {
            throw new IllegalStateException("the delete failed");
        }
    }

    /**
     * Checkpoint storage whose first delete waits until the test lets it go on, so the call that deletes holds the claim
     * or the lock for its id meanwhile, and then throws. Every later delete goes on at once.
     */
    private static final class StorageThatHoldsTheFirstDelete extends SpringMongoCheckpointStorage {
        private final AtomicBoolean deleted = new AtomicBoolean();
        private final CompletableFuture<Void> firstDeleteStarted = new CompletableFuture<>();
        private final CompletableFuture<Void> firstDeleteMayGoOn = new CompletableFuture<>();

        private StorageThatHoldsTheFirstDelete(MongoTemplate mongoTemplate, String checkpointCollection) {
            super(mongoTemplate, checkpointCollection);
        }

        @Override
        public void delete(String subscriptionId) {
            if (deleted.compareAndSet(false, true)) {
                firstDeleteStarted.complete(null);
                firstDeleteMayGoOn.orTimeout(30, SECONDS).join();
                throw new IllegalStateException("the first delete failed");
            }
            super.delete(subscriptionId);
        }
    }

    /**
     * A start-position function that, the first time it is asked, cancels its own subscription id on another thread and
     * waits for that cancel to return, longer than {@code LIFECYCLE_WAIT}. Any later ask answers {@code now} at once.
     */
    private final class WaitsForCancel {
        private final AtomicBoolean asked = new AtomicBoolean();
        private final CompletableFuture<Void> cancelCompleted = new CompletableFuture<>();
        // Completed by the function once it stops waiting, which on the plain MongoDB model is on its executor after the
        // subscribe has returned
        private final CompletableFuture<Boolean> cancelReturnedWhileWaiting = new CompletableFuture<>();

        StartAt startAt() {
            return StartAt.dynamic(context -> {
                if (asked.compareAndSet(false, true)) {
                    cancelOnAnotherThreadAndWaitForIt();
                }
                return StartAt.now();
            });
        }

        // Answers beginning of time to the catch-up, to have it replay and hand over, and waits for the cancel the second
        // time the live delegate asks on the replay thread. The first of its asks there answers null, and the handover
        // asks again.
        StartAt startAtWhenTheHandoverAsks(String subscribingThread) {
            AtomicInteger asksOnTheReplayThread = new AtomicInteger();
            return StartAt.dynamic(context -> {
                if (context.subscriptionModelType() == StreamCatchupSubscriptionModel.class) {
                    return StartAtTime.beginningOfTime();
                }
                if (!Thread.currentThread().getName().equals(subscribingThread) && asksOnTheReplayThread.incrementAndGet() == 2) {
                    cancelOnAnotherThreadAndWaitForIt();
                }
                return null;
            });
        }

        private void cancelOnAnotherThreadAndWaitForIt() {
            CompletableFuture.runAsync(() -> model.cancelSubscription(subscriptionId), otherThreads).whenComplete((__, error) -> {
                if (error == null) {
                    cancelCompleted.complete(null);
                } else {
                    cancelCompleted.completeExceptionally(error);
                }
            });
            try {
                cancelCompleted.get(FUNCTION_WAITS.toMillis(), TimeUnit.MILLISECONDS);
                cancelReturnedWhileWaiting.complete(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                // assertTheCancelReturnedWhileTheFunctionWaited() decides whether the cancel returned, so the function only stops waiting
            } finally {
                cancelReturnedWhileWaiting.complete(false);
            }
        }

        // The cancel has to return before the function stops waiting for it. A cancel that returns only once the
        // subscribe has returned, because it waited for the subscribe, is the deadlock this guards against
        void assertTheCancelReturnedWhileTheFunctionWaited() {
            assertThat(cancelCompleted).as("the cancel made while the function waited").succeedsWithin(EVENT_DELIVERY);
            assertThat(cancelReturnedWhileWaiting).as("the cancel returned before the function stopped waiting for it").succeedsWithin(EVENT_DELIVERY).isEqualTo(true);
        }
    }
}
