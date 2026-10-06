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

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionModelShutdownException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A subscription handed to {@link ReactorMongoSubscriptionModel} that {@link ReactorDurableSubscriptionModel} starts
 * again there from an earlier first position is cancelled, or ends, while the Mongo model takes a cancel or a pause
 * that the durable model sends it for that subscription. Those calls go by id, so a subscribe of the id that came
 * before one of them ended would lose its subscription to it, or have it paused.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableMongoSubscriptionModelStartAgainTest {

    private static final String DATABASE = "reactordurablemongostartagain";
    private static final String SUBSCRIPTION_ID = "sub";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    // How far back the reactive Mongo model may start a new subscription, and half a second for the reply to arrive
    private static final Duration LONGER_THAN_A_NEW_SUBSCRIPTION_LOOKS_BACK =
            ReactorDurableMongoSubscriptionModelFixture.HOW_FAR_BACK_THE_REACTIVE_MONGO_MODEL_MAY_START.plusMillis(500);
    private static final String SCHEDULE_HOOK ="reactor-durable-mongo-start-again";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;

    private final String eventCollectionName = "events-" + UUID.randomUUID();
    private final AtomicLong eventsWritten = new AtomicLong();
    private final ExecutorService caller = Executors.newSingleThreadExecutor();
    private final ExecutorService canceller = Executors.newSingleThreadExecutor();
    private final ReactiveMongoTemplate template;
    private final ReactorMongoEventStore eventStore;
    private final HoldingMongoModel mongoModel;
    private final RaceStorage storage;
    private final ReactorDurableSubscriptionModel model;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    ReactorDurableMongoSubscriptionModelStartAgainTest() {
        template = new ReactiveMongoTemplate(mongoClient, DATABASE);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, DATABASE)))
                .timeRepresentation(timeRepresentation)
                .build();
        eventStore = new ReactorMongoEventStore(template, eventStoreConfig);
        mongoModel = new HoldingMongoModel(template, eventCollectionName, timeRepresentation);
        storage = new RaceStorage();
        model = new ReactorDurableSubscriptionModel(mongoModel, storage);
    }

    @AfterEach
    void shutdown() {
        mongoModel.letGoOfEveryHold();
        storage.deleteLetGo.countDown();
        caller.shutdownNow();
        canceller.shutdownNow();
        model.shutdown();
        template.remove(new Query(), eventCollectionName).block(TIMEOUT);
    }

    @Test
    void a_subscribe_of_the_id_before_its_cancel_completes_is_refused_while_the_mongo_model_takes_the_cancel_of_the_start_again() throws Exception {
        // Given
        Hold startAgainCancel = mongoModel.holdCancel(2);
        heldInTheCancelOfTheStartAgain(startAgainCancel);

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        CompletableFuture<Void> cancelCompleted = cancelled.toFuture();
        Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
        boolean completedWhileHeld = cancelCompleted.isDone();
        startAgainCancel.letGo();
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(refused).as("how the subscribe before the cancel completed ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(completedWhileHeld).as("the cancel completed while the Mongo model took the cancel of the start again").isFalse();
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the Mongo model").isTrue();
        });
    }

    @Test
    void a_subscribe_of_the_id_once_its_cancel_completes_delivers_every_event_written_after_it() throws Exception {
        // Given
        Hold startAgainCancel = mongoModel.holdCancel(2);
        heldInTheCancelOfTheStartAgain(startAgainCancel);

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        startAgainCancel.letGo();
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the Mongo model").isTrue();
        });
    }

    @Test
    void a_subscribe_of_the_id_while_the_mongo_model_takes_the_cancel_of_the_id_is_refused_once_the_start_again_has_ended() throws Exception {
        // Given
        Hold startAgainCancel = mongoModel.holdCancel(2);
        Subscription startedAgain = heldInTheCancelOfTheStartAgain(startAgainCancel);
        Hold cancelOfTheId = mongoModel.holdCancel(3);
        CompletableFuture<Mono<Void>> cancelling = CompletableFuture.supplyAsync(() -> model.cancelSubscription(SUBSCRIPTION_ID), canceller);
        cancelOfTheId.awaitReached();
        startAgainCancel.letGo();
        Throwable startAgainEnded = catchThrowable(() -> startedAgain.waitUntilStarted(TIMEOUT).block());

        // When
        Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
        cancelOfTheId.letGo();
        Mono<Void> cancelled = cancelling.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(startAgainEnded).as("how the start again ended").isInstanceOf(CancellationException.class);
            softly.assertThat(refused).as("how the subscribe while the Mongo model took the cancel of the id ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.failed()).as("how a later subscribe ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the Mongo model").isTrue();
        });
    }

    @Test
    void a_subscribe_of_the_id_while_the_mongo_model_takes_the_pause_kept_during_the_start_again_is_refused_and_the_cancel_completes_after_that_pause() throws Exception {
        // Given
        Hold startAgainCancel = mongoModel.holdCancel(2);
        heldInTheCancelOfTheStartAgain(startAgainCancel);
        model.pauseSubscription(SUBSCRIPTION_ID);
        Hold keptPause = mongoModel.holdNextPause();
        startAgainCancel.letGo();
        keptPause.awaitReached();

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        CompletableFuture<Void> cancelCompleted = cancelled.toFuture();
        Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
        boolean completedWhilePauseHeld = cancelCompleted.isDone();
        keptPause.letGo();
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(refused).as("how the subscribe while the Mongo model took the pause ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(completedWhilePauseHeld).as("the cancel completed while the Mongo model took the pause").isFalse();
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.failed()).as("how a later subscribe ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isPaused(SUBSCRIPTION_ID)).as("the later subscription paused in the Mongo model").isFalse();
        });
    }

    @Test
    void a_subscribe_of_the_id_while_the_mongo_model_takes_a_pause_of_the_id_is_refused_and_the_cancel_completes_after_that_pause() throws Exception {
        // Given
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
        Hold pause = mongoModel.holdNextPause();
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> model.pauseSubscription(SUBSCRIPTION_ID), canceller);
        pause.awaitReached();

        // When
        Mono<Void> cancelled = model.cancelSubscription(SUBSCRIPTION_ID);
        CompletableFuture<Void> cancelCompleted = cancelled.toFuture();
        Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
        boolean completedWhilePauseHeld = cancelCompleted.isDone();
        pause.letGo();
        // The Mongo model has cancelled the id by then, so the pause can fail, as one made after the cancel does
        catchThrowable(() -> pausing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        Throwable cancelFailed = catchThrowable(() -> cancelled.block(TIMEOUT));
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(refused).as("how the subscribe while the Mongo model took the pause ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(completedWhilePauseHeld).as("the cancel completed while the Mongo model took the pause").isFalse();
            softly.assertThat(cancelFailed).as("how the cancel ended").isNull();
            softly.assertThat(later.failed()).as("how a later subscribe ended").isNull();
            softly.assertThat(later.delivered()).as("events a later subscribe delivered, of %s written before it", later.writtenBefore())
                    .containsExactlyElementsOf(later.writtenAfter());
            softly.assertThat(mongoModel.isPaused(SUBSCRIPTION_ID)).as("the later subscription paused in the Mongo model").isFalse();
        });
    }

    @Test
    void a_subscribe_of_the_id_while_the_mongo_model_takes_the_cancel_of_a_start_again_whose_second_subscribe_failed_is_refused() throws Exception {
        // Given
        mongoModel.failingSubscribe = 3;
        Hold cancelOfTheFailedStart = mongoModel.holdCancel(3);
        Subscription startedAgain = startedAgainInTheMongoModel();
        cancelOfTheFailedStart.awaitReached();

        // When
        Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
        cancelOfTheFailedStart.letGo();
        Throwable startAgainEnded = catchThrowable(() -> startedAgain.waitUntilStarted(TIMEOUT).block());
        Later later = subscribeAgain();

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(refused).as("how the subscribe while the Mongo model took the cancel of the failed start ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(startAgainEnded).as("how the start again ended").hasMessage(HoldingMongoModel.FAILED_SUBSCRIBE);
            softly.assertThat(later.failed()).as("how a later subscribe ended").isNull();
            // The failed start keeps the delete it took over, so the later subscribe resumes from the checkpoint another
            // node stored and delivers earlier events too
            softly.assertThat(later.delivered()).as("events a later subscribe delivered").containsAll(later.writtenAfter());
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the later subscription running in the Mongo model").isTrue();
        });
    }

    @Test
    void a_shutdown_while_the_mongo_model_takes_the_cancel_of_the_start_again_ends_its_wait_until_started_with_the_shutdown() throws Exception {
        // Given
        Hold startAgainCancel = mongoModel.holdCancel(2);
        Subscription startedAgain = heldInTheCancelOfTheStartAgain(startAgainCancel);
        // The start again goes on from the thread of its cancel once that is let go, on tasks it schedules from there
        TasksScheduledFrom startAgain = new TasksScheduledFrom(requireNonNull(mongoModel.threadOfCancel(2)));
        Schedulers.onScheduleHook(SCHEDULE_HOOK, startAgain::counted);

        try {
            // When
            model.shutdown();
            startAgainCancel.letGo();
            // Asked once the start again has ended, so what it ended with comes first, see untilStartedOrShutDown
            await().atMost(TIMEOUT).until(startAgain::ended);
            Throwable ended = catchThrowable(() -> startedAgain.waitUntilStarted(TIMEOUT).block());

            // Then
            assertThat(ended).as("how the wait for the start of the subscription started again ended").isInstanceOf(SubscriptionModelShutdownException.class);
        } finally {
            Schedulers.resetOnScheduleHook(SCHEDULE_HOOK);
        }
    }

    @Test
    void a_cancel_the_mongo_model_blocks_on_interruptibly_after_the_second_subscribe_of_a_start_again_failed_runs_to_its_end() throws Exception {
        // Given
        mongoModel.failingSubscribe = 3;
        Hold cancelOfTheFailedStart = mongoModel.holdCancel(3, true);
        // The thread of the start again's cancel goes on to subscribe the second time
        Schedulers.Snapshot schedulers = Schedulers.setFactoryWithSnapshot(new Schedulers.Factory() {
            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                return new HeldBackScheduler(Schedulers.Factory.super.newBoundedElastic(threadCap, queuedTaskCap, threadFactory, ttlSeconds),
                        () -> mongoModel.threadOfCancel(2), cancelOfTheFailedStart.reached);
            }
        });

        try {
            startedAgainInTheMongoModel();
            cancelOfTheFailedStart.awaitReached();
            Thread startAgainCancel = requireNonNull(mongoModel.threadOfCancel(2));
            await().atMost(TIMEOUT).until(() -> doneWithTheStartAgain(startAgainCancel, cancelOfTheFailedStart));

            // When
            Throwable refused = catchThrowable(() -> subscribe(new CopyOnWriteArrayList<>()));
            cancelOfTheFailedStart.letGo();
            cancelOfTheFailedStart.awaitEnded();

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(cancelOfTheFailedStart.interrupted).as("the cancel of the failed start interrupted").isFalse();
                softly.assertThat(refused).as("how the subscribe while the Mongo model took the cancel of the failed start ended").hasCauseInstanceOf(DuplicateSubscriptionIdException.class);
            });
        } finally {
            mongoModel.letGoOfEveryHold();
            Schedulers.resetFrom(schedulers);
        }
    }

    @Test
    void a_cancel_the_mongo_model_blocks_on_interruptibly_after_the_second_subscribe_of_a_start_again_failed_ends_the_subscription_there() throws Exception {
        // Given
        mongoModel.failingSubscribe = 3;
        Hold cancelOfTheFailedStart = mongoModel.holdCancel(3, true);
        // The thread that the second subscribe fails on schedules what goes on from the failure, and records that task as
        // scheduled only once the cancel of the failed start is held
        Schedulers.Snapshot schedulers = Schedulers.setFactoryWithSnapshot(new Schedulers.Factory() {
            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                return new HeldBackScheduler(Schedulers.Factory.super.newBoundedElastic(threadCap, queuedTaskCap, threadFactory, ttlSeconds),
                        () -> mongoModel.threadOfSubscribe(3), cancelOfTheFailedStart.reached);
            }
        });

        try {
            Subscription startedAgain = startedAgainInTheMongoModel();
            cancelOfTheFailedStart.awaitReached();
            Thread failedSubscribe = requireNonNull(mongoModel.threadOfSubscribe(3));
            await().atMost(TIMEOUT).until(() -> doneWithTheStartAgain(failedSubscribe, cancelOfTheFailedStart));

            // When
            cancelOfTheFailedStart.letGo();
            cancelOfTheFailedStart.awaitEnded();
            Throwable startAgainEnded = catchThrowable(() -> startedAgain.waitUntilStarted(TIMEOUT).block());
            boolean runningAfterTheCancel = mongoModel.isRunning(SUBSCRIPTION_ID);
            Later later = subscribeAgain();

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(startAgainEnded).as("how the start again ended").hasMessage(HoldingMongoModel.FAILED_SUBSCRIBE);
                softly.assertThat(cancelOfTheFailedStart.interrupted).as("the cancel of the failed start interrupted").isFalse();
                softly.assertThat(runningAfterTheCancel).as("the subscription running in the Mongo model after the cancel of the failed start").isFalse();
                softly.assertThat(later.failed()).as("how a later subscribe ended").isNull();
                softly.assertThat(later.delivered()).as("events a later subscribe delivered").containsAll(later.writtenAfter());
            });
        } finally {
            mongoModel.letGoOfEveryHold();
            Schedulers.resetFrom(schedulers);
        }
    }

    @Test
    void a_cancel_the_mongo_model_blocks_on_interruptibly_after_its_cancel_of_a_start_again_failed_runs_to_its_end_and_ends_the_subscription_there() throws Exception {
        // Given
        Hold cancelOfTheFailedStart = mongoModel.holdCancel(3, true);
        Scheduler cancelling = Schedulers.newSingle("cancelling");
        // Fails on a thread of the Mongo model. The thread that subscribes to the failure records that task as scheduled
        // only once the cancel of the failed start is held.
        mongoModel.failCancel(2, Mono.<Void>fromCallable(() -> {
            throw new IllegalStateException("The cancel failed");
        }).subscribeOn(new HeldBackScheduler(cancelling, () -> mongoModel.threadOfCancel(2), cancelOfTheFailedStart.reached)));

        try {
            Subscription startedAgain = startedAgainInTheMongoModel();
            cancelOfTheFailedStart.awaitReached();
            Thread startAgainCancel = requireNonNull(mongoModel.threadOfCancel(2));
            await().atMost(TIMEOUT).until(() -> doneWithTheStartAgain(startAgainCancel, cancelOfTheFailedStart));

            // When
            cancelOfTheFailedStart.letGo();
            cancelOfTheFailedStart.awaitEnded();
            Throwable startAgainEnded = catchThrowable(() -> startedAgain.waitUntilStarted(TIMEOUT).block());

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(startAgainEnded).as("how the start again ended").hasMessage("The cancel failed");
                softly.assertThat(cancelOfTheFailedStart.interrupted).as("the cancel of the failed start interrupted").isFalse();
                softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID)).as("the subscription running in the Mongo model").isFalse();
            });
        } finally {
            mongoModel.letGoOfEveryHold();
            cancelling.dispose();
        }
    }

    @Test
    void a_cancel_the_mongo_model_blocks_on_interruptibly_after_its_cancel_of_a_start_again_failed_on_a_thread_of_its_own_ends_the_subscription_there() throws Exception {
        // Given
        Hold cancelOfTheFailedStart = mongoModel.holdCancel(3, true);
        Scheduler cancelling = Schedulers.newSingle("cancelling");
        Thread cancellingThread = requireNonNull(Mono.fromSupplier(Thread::currentThread).subscribeOn(cancelling).block(TIMEOUT));
        // The cancel of the start again fails on a thread of the Mongo model, which schedules what goes on from the
        // failure, and records that task as scheduled only once the cancel of the failed start is held
        mongoModel.failCancel(2, Mono.<Void>defer(() -> Mono.error(new IllegalStateException("The cancel failed"))).subscribeOn(cancelling));
        Schedulers.Snapshot schedulers = Schedulers.setFactoryWithSnapshot(new Schedulers.Factory() {
            @Override
            public Scheduler newBoundedElastic(int threadCap, int queuedTaskCap, ThreadFactory threadFactory, int ttlSeconds) {
                return new HeldBackScheduler(Schedulers.Factory.super.newBoundedElastic(threadCap, queuedTaskCap, threadFactory, ttlSeconds),
                        () -> cancellingThread, cancelOfTheFailedStart.reached);
            }
        });

        try {
            Subscription startedAgain = startedAgainInTheMongoModel();
            cancelOfTheFailedStart.awaitReached();
            await().atMost(TIMEOUT).until(() -> isIdle(cancellingThread));

            // When
            cancelOfTheFailedStart.letGo();
            cancelOfTheFailedStart.awaitEnded();
            Throwable startAgainEnded = catchThrowable(() -> startedAgain.waitUntilStarted(TIMEOUT).block());
            boolean runningAfterTheCancel = mongoModel.isRunning(SUBSCRIPTION_ID);
            Later later = subscribeAgain();

            // Then
            SoftAssertions.assertSoftly(softly -> {
                softly.assertThat(startAgainEnded).as("how the start again ended").hasMessage("The cancel failed");
                softly.assertThat(cancelOfTheFailedStart.interrupted).as("the cancel of the failed start interrupted").isFalse();
                softly.assertThat(runningAfterTheCancel).as("the subscription running in the Mongo model after the cancel of the failed start").isFalse();
                softly.assertThat(later.failed()).as("how a later subscribe ended").isNull();
                softly.assertThat(later.delivered()).as("events a later subscribe delivered").containsAll(later.writtenAfter());
            });
        } finally {
            mongoModel.letGoOfEveryHold();
            Schedulers.resetFrom(schedulers);
            cancelling.dispose();
        }
    }

    /**
     * A subscription from a dynamic start position waits to be handed to the Mongo model until a cancel of the id has
     * deleted the checkpoint and storage holds it again, and the Mongo model doesn't know the id until then. A pause
     * made meanwhile is kept and put in place once the Mongo model has the subscription, so nothing is delivered until a
     * resume, and the event written while it was paused is delivered after the resume. The dynamic start position
     * answers StartAt.now() or the subscription-model default.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void a_pause_while_the_subscription_waits_to_be_handed_to_the_mongo_model_is_kept_until_it_is_resumed(boolean fromTheDefault) throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = waitingToBeHandedOver(fromTheDefault ? StartAt::subscriptionModelDefault : StartAt::now, delivered);

        // When
        Throwable pauseFailed = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));
        storage.deleteLetGo.countDown();
        Throwable startFailed = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block());
        Throwable notPausedThere = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> mongoModel.isPaused(SUBSCRIPTION_ID)));
        long writtenWhilePaused = write();
        Throwable resumeFailed = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted(TIMEOUT).block());
        long writtenAfterTheResume = write();
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(writtenAfterTheResume)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(pauseFailed).as("how the pause while the subscription waited to be handed over ended").isNull();
            softly.assertThat(startFailed).as("how waiting for the subscription to start ended").isNull();
            softly.assertThat(notPausedThere).as("how waiting for the subscription to be paused in the Mongo model ended").isNull();
            softly.assertThat(resumeFailed).as("how the resume ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written after the resume ended").isNull();
            softly.assertThat(delivered).as("events delivered").contains(writtenWhilePaused, writtenAfterTheResume);
        });
    }

    /**
     * A resume of a subscription that waits to be handed to the Mongo model, and that nobody paused, is refused as for a
     * subscription that runs, and the subscription starts once it is handed over.
     */
    @Test
    void a_resume_while_the_subscription_waits_to_be_handed_to_the_mongo_model_is_refused_as_it_runs() throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = waitingToBeHandedOver(StartAt::now, delivered);

        // When
        Throwable refused = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID));
        storage.deleteLetGo.countDown();
        Throwable startFailed = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block());
        long written = write();
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(written)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(refused).as("how the resume while the subscription waited to be handed over ended").isInstanceOf(SubscriptionAlreadyRunningException.class);
            softly.assertThat(startFailed).as("how waiting for the subscription to start ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written once it started ended").isNull();
        });
    }

    /**
     * A cancel of a subscription that waits to be handed to the Mongo model ends it before the Mongo model has it.
     */
    @Test
    void a_cancel_while_the_subscription_waits_to_be_handed_to_the_mongo_model_ends_it_before_the_mongo_model_has_it() throws Exception {
        // Given
        Subscription subscription = waitingToBeHandedOver(StartAt::now, new CopyOnWriteArrayList<>());

        // When
        Throwable cancelThrew = catchThrowable(() -> model.cancelSubscription(SUBSCRIPTION_ID));
        storage.deleteLetGo.countDown();
        Throwable startEnded = catchThrowable(() -> subscription.waitUntilStarted(TIMEOUT).block());

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(cancelThrew).as("how the cancel while the subscription waited to be handed over ended").isNull();
            softly.assertThat(startEnded).as("how waiting for the subscription to start ended").isInstanceOf(CancellationException.class);
            softly.assertThat(mongoModel.isRunning(SUBSCRIPTION_ID) || mongoModel.isPaused(SUBSCRIPTION_ID)).as("the Mongo model holding the subscription").isFalse();
        });
    }

    /**
     * A pause of the id made while a subscribe that would wait to be handed to the Mongo model reads where the feed is,
     * and that the Mongo model is still taking when that subscribe goes on, has the subscribe refused at the call. The
     * subscribe checks for such a call and starts keeping a pause in one step, so the pause comes either before that
     * step, as here, or after it, and is then kept.
     */
    @Test
    void a_subscribe_that_would_wait_to_be_handed_to_the_mongo_model_is_refused_while_the_mongo_model_takes_a_pause_of_the_id_made_during_that_subscribe() throws Exception {
        // Given
        cancelledWhileTheDeleteIsHeld(() -> {
        });
        Hold readOfWhereTheFeedIs = mongoModel.holdNextReadOfWhereTheFeedIs();
        Hold pauseInTheMongoModel = mongoModel.holdNextPause();
        CompletableFuture<Subscription> subscribed = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now),
                action(new CopyOnWriteArrayList<>())), caller);
        readOfWhereTheFeedIs.awaitReached();

        // When
        CompletableFuture<Void> paused = CompletableFuture.runAsync(() -> model.pauseSubscription(SUBSCRIPTION_ID), canceller);
        pauseInTheMongoModel.awaitReached();
        readOfWhereTheFeedIs.letGo();
        @Nullable Throwable subscribeFailed = failureOf(subscribed);
        pauseInTheMongoModel.letGo();
        @Nullable Throwable pauseFailed = failureOf(paused);

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(subscribeFailed).as("how the subscribe made while the Mongo model took the pause ended").isInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(pauseFailed).as("how the pause, which the Mongo model took for an id it doesn't know, ended").isInstanceOf(UnknownSubscriptionException.class);
        });
    }

    /**
     * A subscribe of the id while a paused subscription of it waits to be handed to the Mongo model is refused at the
     * call, as the Mongo model refuses a subscribe of an id it has. The pause stays in place for the subscription that
     * waited, which delivers nothing until it is resumed.
     */
    @Test
    void a_subscribe_of_the_id_while_a_paused_subscription_of_it_waits_to_be_handed_to_the_mongo_model_is_refused_and_the_first_stays_paused() throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        Subscription waiting = waitingToBeHandedOver(StartAt::now, delivered);
        Throwable pauseFailed = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));

        // When
        @Nullable Throwable secondRefused = failureOf(CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(),
                action(new CopyOnWriteArrayList<>())), canceller));
        storage.deleteLetGo.countDown();
        Throwable startFailed = catchThrowable(() -> waiting.waitUntilStarted(TIMEOUT).block());
        Throwable notPausedThere = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> mongoModel.isPaused(SUBSCRIPTION_ID)));
        long writtenWhilePaused = write();
        Throwable resumeFailed = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted(TIMEOUT).block());
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(writtenWhilePaused)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(pauseFailed).as("how the pause while the subscription waited to be handed over ended").isNull();
            softly.assertThat(secondRefused).as("how a subscribe of the id while the paused subscription waited ended").isInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(startFailed).as("how waiting for the paused subscription to start ended").isNull();
            softly.assertThat(notPausedThere).as("how waiting for the subscription to be paused in the Mongo model ended").isNull();
            softly.assertThat(resumeFailed).as("how the resume ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written while it was paused ended").isNull();
        });
    }

    /**
     * A second subscribe of the id from a dynamic start position while the first waits to be handed to the Mongo model
     * is refused at the call, as the Mongo model refuses a subscribe of an id it has, and the first starts.
     */
    @Test
    void a_second_subscribe_of_the_id_while_the_first_waits_to_be_handed_to_the_mongo_model_is_refused_at_the_call() throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        Subscription waiting = waitingToBeHandedOver(StartAt::now, delivered);

        // When
        @Nullable Throwable secondRefused = failureOf(CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now),
                action(new CopyOnWriteArrayList<>())), canceller));
        storage.deleteLetGo.countDown();
        Throwable startFailed = catchThrowable(() -> waiting.waitUntilStarted(TIMEOUT).block());
        long written = write();
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(written)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(secondRefused).as("how the second subscribe of the id ended").isInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(startFailed).as("how waiting for the first subscription to start ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written once it started ended").isNull();
        });
    }

    /**
     * A subscribe of the id while another subscribe of it still runs its dynamic start position starts, as the Mongo
     * model takes it when nothing of the id waits to be handed over. The other subscribe then throws what its start
     * position threw, or, when the start position answers, the DuplicateSubscriptionIdException of the Mongo model, so
     * only one of the two delivers.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void a_subscribe_of_the_id_while_another_runs_its_start_position_starts_and_only_one_of_them_delivers(boolean theStartPositionThrows) throws Exception {
        // Given
        CountDownLatch inTheStartPosition = new CountDownLatch(1);
        CountDownLatch startPositionLetGo = new CountDownLatch(1);
        List<Long> deliveredToTheOther = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> other = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(() -> {
            inTheStartPosition.countDown();
            try {
                startPositionLetGo.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (theStartPositionThrows) {
                throw new IllegalStateException("The start position failed");
            }
            return StartAt.now();
        }), action(deliveredToTheOther)), caller);
        assertThat(inTheStartPosition.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("start position reached").isTrue();
        List<Long> delivered = new CopyOnWriteArrayList<>();

        // When
        Throwable subscribeFailed = catchThrowable(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), action(delivered)).waitUntilStarted(TIMEOUT).block());
        startPositionLetGo.countDown();
        @Nullable Throwable otherFailed = failureOf(other);
        long written = write();
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(written)));
        long writtenLast = write();
        Throwable lastNotDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(writtenLast)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(subscribeFailed).as("how the subscribe while the other ran its start position ended").isNull();
            softly.assertThat(otherFailed).as("how the other subscribe ended")
                    .isInstanceOf(theStartPositionThrows ? IllegalStateException.class : DuplicateSubscriptionIdException.class);
            softly.assertThat(notDelivered).as("how waiting for the event written once it started ended").isNull();
            softly.assertThat(lastNotDelivered).as("how waiting for the event written after it ended").isNull();
            softly.assertThat(Stream.of(delivered, deliveredToTheOther).filter(events -> !events.isEmpty()).count())
                    .as("subscriptions of the id that delivered").isLessThanOrEqualTo(1);
        });
    }

    /**
     * A subscribe of the id that still reads where the feed is when another subscription of it starts to wait to be
     * handed to the Mongo model is refused once it has read, since a pause kept for the waiting subscription would not
     * reach it. The pause holds the waiting subscription, which delivers once it is resumed.
     */
    @Test
    void a_subscribe_of_the_id_that_reads_where_the_feed_is_when_another_starts_to_wait_to_be_handed_to_the_mongo_model_is_refused_and_a_pause_holds_the_other() throws Exception {
        // Given
        cancelledWhileTheDeleteIsHeld(() -> {
        });
        Hold readOfWhereTheFeedIs = mongoModel.holdNextReadOfWhereTheFeedIs();
        List<Long> deliveredToTheReading = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> reading = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(),
                action(deliveredToTheReading)), canceller);
        readOfWhereTheFeedIs.awaitReached();
        List<Long> delivered = new CopyOnWriteArrayList<>();
        Subscription waiting = CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(StartAt::now), action(delivered)), caller)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Throwable pauseFailed = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));

        // When
        readOfWhereTheFeedIs.letGo();
        @Nullable Throwable readingRefused = failureOf(reading);
        storage.deleteLetGo.countDown();
        Throwable startFailed = catchThrowable(() -> waiting.waitUntilStarted(TIMEOUT).block());
        Throwable notPausedThere = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> mongoModel.isPaused(SUBSCRIPTION_ID)));
        long writtenWhilePaused = write();
        Throwable resumeFailed = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted(TIMEOUT).block());
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(writtenWhilePaused)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(pauseFailed).as("how the pause while the subscription waited to be handed over ended").isNull();
            softly.assertThat(readingRefused).as("how the subscribe that read where the feed is ended").isInstanceOf(DuplicateSubscriptionIdException.class);
            softly.assertThat(startFailed).as("how waiting for the paused subscription to start ended").isNull();
            softly.assertThat(notPausedThere).as("how waiting for the subscription to be paused in the Mongo model ended").isNull();
            softly.assertThat(resumeFailed).as("how the resume ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written while it was paused ended").isNull();
            softly.assertThat(deliveredToTheReading).as("events delivered to the refused subscribe").isEmpty();
        });
    }

    /**
     * On a stopped model, a subscription that waits to be handed to the Mongo model is paused, as the Mongo model
     * registers a subscription made while it is stopped. A pause is refused as for a paused subscription, and a resume
     * starts it once the Mongo model has it.
     */
    @Test
    void on_a_stopped_model_a_subscription_that_waits_to_be_handed_to_the_mongo_model_is_paused_and_a_resume_starts_it() throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        waitingToBeHandedOver(StartAt::now, delivered, model::stop);

        // When
        boolean pausedWhileWaiting = model.isPaused(SUBSCRIPTION_ID);
        Throwable pauseRefused = catchThrowable(() -> model.pauseSubscription(SUBSCRIPTION_ID));
        AtomicReference<@Nullable Subscription> resumed = new AtomicReference<>();
        Throwable resumeFailed = catchThrowable(() -> resumed.set(model.resumeSubscription(SUBSCRIPTION_ID)));
        storage.deleteLetGo.countDown();
        Throwable startFailed = catchThrowable(() -> requireNonNull(resumed.get()).waitUntilStarted(TIMEOUT).block());
        long written = write();
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(written)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(pausedWhileWaiting).as("whether the subscription that waited on the stopped model was paused").isTrue();
            softly.assertThat(pauseRefused).as("how the pause while it waited ended").isInstanceOf(SubscriptionNotRunningException.class);
            softly.assertThat(resumeFailed).as("how the resume while it waited ended").isNull();
            softly.assertThat(startFailed).as("how waiting for the resumed subscription to start ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written once it started ended").isNull();
        });
    }

    /**
     * On a stopped model, a subscription that waits to be handed to the Mongo model stays paused through a start that
     * resumes nothing, as one the Mongo model registered while it was stopped does, and a resume starts it.
     */
    @Test
    void on_a_stopped_model_a_subscription_that_waits_to_be_handed_to_the_mongo_model_stays_paused_through_a_start_that_resumes_nothing() throws Exception {
        // Given
        List<Long> delivered = new CopyOnWriteArrayList<>();
        waitingToBeHandedOver(StartAt::now, delivered, model::stop);

        // When
        model.start(false);
        boolean pausedWhileWaiting = model.isPaused(SUBSCRIPTION_ID);
        storage.deleteLetGo.countDown();
        Throwable notPausedThere = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> mongoModel.isPaused(SUBSCRIPTION_ID)));
        long writtenWhilePaused = write();
        Throwable resumeFailed = catchThrowable(() -> model.resumeSubscription(SUBSCRIPTION_ID).waitUntilStarted(TIMEOUT).block());
        Throwable notDelivered = catchThrowable(() -> await().atMost(TIMEOUT).until(() -> delivered.contains(writtenWhilePaused)));

        // Then
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(pausedWhileWaiting).as("whether the subscription that waited was paused after the start").isTrue();
            softly.assertThat(notPausedThere).as("how waiting for the subscription to be paused in the Mongo model ended").isNull();
            softly.assertThat(resumeFailed).as("how the resume ended").isNull();
            softly.assertThat(notDelivered).as("how waiting for the event written while it was paused ended").isNull();
        });
    }

    // Subscribes the id and cancels it while the delete of its stored checkpoint is held, then subscribes it again from a
    // dynamic start position that answers what startAt does. Returns that subscription, which waits for the delete.
    private Subscription waitingToBeHandedOver(Supplier<StartAt> startAt, List<Long> delivered) throws Exception {
        return waitingToBeHandedOver(startAt, delivered, () -> {
        });
    }

    // As above, and runs beforeTheSubscribe once the delete is held
    private Subscription waitingToBeHandedOver(Supplier<StartAt> startAt, List<Long> delivered, Runnable beforeTheSubscribe) throws Exception {
        cancelledWhileTheDeleteIsHeld(beforeTheSubscribe);
        return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.dynamic(startAt), action(delivered)), caller)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    // Subscribes the id and cancels it while the delete of its stored checkpoint is held, then runs then
    private void cancelledWhileTheDeleteIsHeld(Runnable then) throws Exception {
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), action(new CopyOnWriteArrayList<>())).waitUntilStarted(TIMEOUT).block();
        write();
        await().atMost(TIMEOUT).until(() -> storage.storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT).isPresent());
        storage.holdsDeletes = true;
        model.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();
        then.run();
    }

    // What the call failed with, or null when it returned
    private static @Nullable Throwable failureOf(CompletableFuture<?> call) throws InterruptedException, TimeoutException {
        try {
            call.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    // Subscribes the id and cancels it while the delete of its checkpoint is held. Subscribes it again from the model
    // default, and another node stores an earlier checkpoint right before that subscribe records its first, so the model
    // starts it again in the Mongo model from that one. Returns once the cancel the start again sends there is held.
    private Subscription heldInTheCancelOfTheStartAgain(Hold startAgainCancel) throws Exception {
        Subscription startedAgain = startedAgainInTheMongoModel();
        startAgainCancel.awaitReached();
        return startedAgain;
    }

    // Subscribes the id and cancels it while the delete of its checkpoint is held. Subscribes it again from the model
    // default, and another node stores an earlier checkpoint right before that subscribe records its first, so the model
    // starts it again in the Mongo model from that one. Returns the subscription that starts again once the delete is
    // let go.
    private Subscription startedAgainInTheMongoModel() throws Exception {
        List<Checkpoint> checkpoints = new CopyOnWriteArrayList<>();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.now(), cloudEvent -> Mono.fromRunnable(() -> checkpoints.add(CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(cloudEvent))))
                .waitUntilStarted(TIMEOUT).block();
        write();
        write();
        await().atMost(TIMEOUT).until(() -> checkpoints.size() == 2);
        // The subscribe below then records the position it reads from the Mongo model as its first
        storage.storage.delete(SUBSCRIPTION_ID).block(TIMEOUT);
        storage.holdsDeletes = true;
        model.cancelSubscription(SUBSCRIPTION_ID);
        assertThat(storage.deleteEntered.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("delete held").isTrue();
        storage.storedElsewhereBeforeIfAbsent = checkpoints.getFirst();
        Subscription startedAgain = subscribe(new CopyOnWriteArrayList<>());
        storage.deleteLetGo.countDown();
        return startedAgain;
    }

    // What a subscribe of the id from the model default delivered, of an event written before it and two written after,
    // and how it failed, if it did
    private record Later(List<Long> delivered, long writtenBefore, List<Long> writtenAfter, @Nullable Throwable failed) {
    }

    private Later subscribeAgain() throws InterruptedException {
        long writtenBefore = write();
        // A new subscription from the model default can also receive what was written shortly before it, so only one
        // that resumes from a checkpoint the cancel should have deleted receives what was written before this
        Thread.sleep(LONGER_THAN_A_NEW_SUBSCRIPTION_LOOKS_BACK.toMillis());
        List<Long> delivered = new CopyOnWriteArrayList<>();
        Throwable failed = catchThrowable(() -> subscribe(delivered).waitUntilStarted(TIMEOUT).block());
        List<Long> writtenAfter = List.of(write(), write());
        if (failed == null) {
            try {
                await().atMost(TIMEOUT).until(() -> delivered.contains(writtenAfter.getLast()));
            } catch (ConditionTimeoutException notDelivered) {
                // What was delivered instead is asserted next
            }
        }
        return new Later(delivered, writtenBefore, writtenAfter, failed);
    }

    // On a thread of its own, since a subscribe that takes over a delete can wait for a read
    private Subscription subscribe(List<Long> delivered) throws Exception {
        return CompletableFuture.supplyAsync(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), action(delivered)), caller)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private long write() {
        long position = eventsWritten.incrementAndGet();
        CloudEvent cloudEvent = CloudEventBuilder.v1()
                .withId(String.valueOf(position))
                .withSource(URI.create("urn:occurrent:test"))
                .withType("Written")
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write("stream", Flux.just(cloudEvent)).block(TIMEOUT);
        return position;
    }

    private static Function<CloudEvent, Mono<Void>> action(List<Long> delivered) {
        return cloudEvent -> Mono.fromRunnable(() -> delivered.add(Long.parseLong(cloudEvent.getId())));
    }

    // The thread that ran the cancel of the start again has ended that task, and either waits for a new one or runs the
    // held cancel of the failed start as its next one
    private static boolean doneWithTheStartAgain(Thread startAgainCancel, Hold cancelOfTheFailedStart) {
        return isIdle(startAgainCancel) || cancelOfTheFailedStart.heldOn == startAgainCancel;
    }

    // A scheduler thread that has gone back to waiting for its next task
    private static boolean isIdle(Thread thread) {
        return (thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING)
               && Arrays.stream(thread.getStackTrace()).noneMatch(frame -> frame.getClassName().startsWith("org.occurrent"));
    }

    // Holds the cancels of the numbers asked for, counted from the first, the next pause and the next read of where the
    // feed is, each before it reaches the Mongo model, until its hold is let go. Fails the subscribe of the number asked
    // for, counted from the first, and answers the cancels of the numbers asked for with a failure instead of reaching
    // the Mongo model.
    private static final class HoldingMongoModel extends ReactorMongoSubscriptionModel {
        private static final String FAILED_SUBSCRIBE = "The subscribe failed";
        private final AtomicInteger cancels = new AtomicInteger();
        private final AtomicInteger subscribes = new AtomicInteger();
        private final Map<Integer, Hold> heldCancels = new ConcurrentHashMap<>();
        private final Map<Integer, Thread> cancelThreads = new ConcurrentHashMap<>();
        private final Map<Integer, Thread> subscribeThreads = new ConcurrentHashMap<>();
        private final Map<Integer, Mono<Void>> failedCancels = new ConcurrentHashMap<>();
        private final List<Hold> holds = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Hold> heldPause = new AtomicReference<>();
        private final AtomicReference<@Nullable Hold> heldReadOfWhereTheFeedIs = new AtomicReference<>();
        private volatile int failingSubscribe = -1;

        private HoldingMongoModel(ReactiveMongoTemplate template, String eventCollectionName, TimeRepresentation timeRepresentation) {
            super(template, eventCollectionName, timeRepresentation);
        }

        private Hold holdCancel(int number) {
            return holdCancel(number, false);
        }

        private Hold holdCancel(int number, boolean interruptibly) {
            Hold hold = new Hold(interruptibly);
            holds.add(hold);
            heldCancels.put(number, hold);
            return hold;
        }

        private Hold holdNextPause() {
            Hold hold = new Hold(false);
            holds.add(hold);
            heldPause.set(hold);
            return hold;
        }

        private Hold holdNextReadOfWhereTheFeedIs() {
            Hold hold = new Hold(false);
            holds.add(hold);
            heldReadOfWhereTheFeedIs.set(hold);
            return hold;
        }

        private void letGoOfEveryHold() {
            holds.forEach(Hold::letGo);
        }

        // Held when asked, on the thread that asks, before the Mono is returned
        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            @Nullable Hold hold = heldReadOfWhereTheFeedIs.getAndSet(null);
            if (hold != null) {
                hold.hold();
            }
            return super.globalCheckpointAsOfNow();
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            int number = subscribes.incrementAndGet();
            subscribeThreads.put(number, Thread.currentThread());
            if (number == failingSubscribe) {
                throw new IllegalStateException(FAILED_SUBSCRIBE);
            }
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        private @Nullable Thread threadOfCancel(int number) {
            return cancelThreads.get(number);
        }

        private @Nullable Thread threadOfSubscribe(int number) {
            return subscribeThreads.get(number);
        }

        private void failCancel(int number, Mono<Void> failure) {
            failedCancels.put(number, failure);
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            int number = cancels.incrementAndGet();
            cancelThreads.put(number, Thread.currentThread());
            @Nullable Hold hold = heldCancels.remove(number);
            try {
                if (hold != null) {
                    hold.hold();
                }
                @Nullable Mono<Void> failure = failedCancels.remove(number);
                return failure != null ? failure : super.cancelSubscription(subscriptionId);
            } finally {
                if (hold != null) {
                    hold.ended.countDown();
                }
            }
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            @Nullable Hold hold = heldPause.getAndSet(null);
            if (hold != null) {
                hold.hold();
            }
            super.pauseSubscription(subscriptionId);
        }
    }

    // A call held where it is reached until it is let go. One held interruptibly fails when its thread is interrupted,
    // as a call blocked on a lock or a socket can, without doing what it was called for.
    private static final class Hold {
        private final boolean interruptibly;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch letGo = new CountDownLatch(1);
        private final CountDownLatch ended = new CountDownLatch(1);
        private volatile boolean interrupted;
        private volatile @Nullable Thread heldOn;

        private Hold(boolean interruptibly) {
            this.interruptibly = interruptibly;
        }

        private void hold() {
            heldOn = Thread.currentThread();
            reached.countDown();
            try {
                letGo.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
                Thread.currentThread().interrupt();
                if (interruptibly) {
                    throw new IllegalStateException("Interrupted while held", e);
                }
            }
        }

        private void awaitEnded() throws InterruptedException {
            assertThat(ended.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("held call ended").isTrue();
        }

        private void awaitReached() throws InterruptedException {
            assertThat(reached.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("call held").isTrue();
        }

        private void letGo() {
            letGo.countDown();
        }
    }

    // Holds the thread that holdsBack answers inside its next schedule(..), after the task is handed over and until the
    // latch until opens. The task can then end before that thread has recorded it as scheduled. Reactor's subscribeOn of
    // a Mono.fromCallable that throws then disposes the task from that thread, which interrupts the thread that runs it.
    private static final class HeldBackScheduler implements Scheduler {
        private final Scheduler scheduler;
        private final Supplier<@Nullable Thread> holdsBack;
        private final CountDownLatch until;

        private HeldBackScheduler(Scheduler scheduler, Supplier<@Nullable Thread> holdsBack, CountDownLatch until) {
            this.scheduler = scheduler;
            this.holdsBack = holdsBack;
            this.until = until;
        }

        @Override
        public Disposable schedule(Runnable task) {
            Disposable scheduled = scheduler.schedule(task);
            if (until.getCount() > 0 && Thread.currentThread() == holdsBack.get()) {
                try {
                    until.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return scheduled;
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            return scheduler.schedule(task, delay, unit);
        }

        @Override
        public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            return scheduler.schedulePeriodically(task, initialDelay, period, unit);
        }

        @Override
        public Worker createWorker() {
            return scheduler.createWorker();
        }

        @Override
        public void init() {
            scheduler.init();
        }

        @Override
        public void dispose() {
            scheduler.dispose();
        }

        @Override
        public boolean isDisposed() {
            return scheduler.isDisposed();
        }
    }

    // Counts the tasks the thread schedules, and those the tasks counted schedule, until each has run
    private static final class TasksScheduledFrom {
        private final Thread thread;
        private final AtomicInteger notEnded = new AtomicInteger();
        private final ThreadLocal<Boolean> runningOne = ThreadLocal.withInitial(() -> false);

        private TasksScheduledFrom(Thread thread) {
            this.thread = thread;
        }

        private Runnable counted(Runnable task) {
            if (Thread.currentThread() != thread && !runningOne.get()) {
                return task;
            }
            notEnded.incrementAndGet();
            return () -> {
                runningOne.set(true);
                try {
                    task.run();
                } finally {
                    runningOne.set(false);
                    notEnded.decrementAndGet();
                }
            };
        }

        private boolean ended() {
            return notEnded.get() == 0 && isIdle(thread);
        }
    }

    // Holds every delete while holdsDeletes is set, until deleteLetGo opens. Stores a checkpoint of another node right
    // before the next write on the condition that nothing is stored, and settles a first-position race in favour of
    // what is stored.
    private static final class RaceStorage implements CheckpointStorage {
        private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        private final CountDownLatch deleteEntered = new CountDownLatch(1);
        private final CountDownLatch deleteLetGo = new CountDownLatch(1);
        private volatile boolean holdsDeletes;
        private volatile @Nullable Checkpoint storedElsewhereBeforeIfAbsent;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return storage.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return storage.read(subscriptionId);
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
            return Mono.defer(() -> {
                if (!holdsDeletes) {
                    return storage.delete(subscriptionId);
                }
                holdsDeletes = false;
                deleteEntered.countDown();
                return Mono.fromCallable(() -> deleteLetGo.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .subscribeOn(Schedulers.boundedElastic())
                        .then(Mono.defer(() -> storage.delete(subscriptionId)));
            });
        }
    }
}
