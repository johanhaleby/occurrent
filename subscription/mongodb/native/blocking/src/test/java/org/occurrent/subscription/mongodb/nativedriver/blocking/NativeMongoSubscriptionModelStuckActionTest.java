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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.slf4j.LoggerFactory;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A pause of a {@link NativeMongoSubscriptionModel} subscription waits for a running action for one second at most, so
 * an action that is stuck outlives the pause. A resumed subscription must not call its action while that call still
 * runs, however many times it was paused and resumed meanwhile, and a shutdown must end the wait of a resumed run.
 */
@Testcontainers
@Timeout(30)
@DisplayName("NativeMongoSubscriptionModelStuckAction")
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelStuckActionTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CountDownLatch releaseFirstCallOfE1 = new CountDownLatch(1);
    private final AtomicBoolean firstCallOfE1 = new AtomicBoolean(true);
    private final AtomicReference<Thread> stuckThread = new AtomicReference<>();
    // Loses the history as the next change stream opens
    private final AtomicBoolean historyLostWhenOpening = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger mostInFlight = new AtomicInteger();
    private final CopyOnWriteArrayList<String> calls = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Thread> executorThreads = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Throwable> uncaught = new CopyOnWriteArrayList<>();
    private final NameDefined e1 = nameDefined("name1");
    private final NameDefined e2 = nameDefined("name2");
    private final NameDefined e3 = nameDefined("name3");
    private MongoClient mongoClient;
    private MongoCollection<Document> eventCollection;
    private MongoDatabase database;
    private MongoEventStore mongoEventStore;
    private ThreadPoolExecutor subscriptionExecutor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".stuckAction");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
        subscriptionExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<>(), runnable -> {
            Thread thread = new Thread(runnable);
            thread.setUncaughtExceptionHandler((__, throwable) -> uncaught.add(throwable));
            executorThreads.add(thread);
            return thread;
        });
        subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatLosesItsHistoryWhenOpening(), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor, NativeMongoSubscriptionModelConfig.withConfig());
    }

    @AfterEach
    void shutdown() {
        // The stuck call ignores interrupts, so the model's shutdown would otherwise wait for it
        releaseFirstCallOfE1.countDown();
        subscriptionModel.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void a_resumed_subscription_calls_its_action_only_once_the_call_that_outlived_the_pause_has_returned() {
        // Given: the first call for E1 is stuck for longer than a pause waits
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));

        // When: paused, which returns while that call still runs, then resumed, and a second event is written
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(Duration.ofSeconds(3), () -> calls.size() > 1);

        // Then: nothing else was called while the first call was stuck
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(TEN_SECONDS, () -> calls.contains("E2 returned"));
        assertThat(calls).as("calls once the stuck call was released").containsExactly("E1 started", "E1 returned", "E1 started", "E1 returned", "E2 started", "E2 returned");
        assertThat(mostInFlight).as("most calls running at once").hasValue(1);
    }

    @Test
    void a_second_pause_and_resume_while_the_resumed_run_waits_still_holds_back_the_action_until_the_stuck_call_has_returned() {
        // Given: the first call for E1 is stuck for longer than a pause waits
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();

        // When: paused and resumed again while the first resumed run waits for the stuck call, and a second event is written
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(Duration.ofSeconds(3), () -> calls.size() > 1);

        // Then: nothing else was called while the first call was stuck
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(TEN_SECONDS, () -> calls.contains("E2 returned"));
        assertThat(calls).as("calls once the stuck call was released").containsExactly("E1 started", "E1 returned", "E1 started", "E1 returned", "E2 started", "E2 returned");
        assertThat(mostInFlight).as("most calls running at once").hasValue(1);
    }

    @Test
    void a_resumed_run_paused_again_while_it_waits_for_the_stuck_call_ends_and_frees_its_thread() {
        // Given: the first call for E1 is stuck, and a resumed run waits for it, so two executor threads are busy
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
        await().atMost(FIVE_SECONDS).until(() -> subscriptionExecutor.getActiveCount() == 2);

        // When
        subscriptionModel.pauseSubscription(subscriptionId);

        // Then: the run that waited ended and freed its thread, while the stuck call still runs
        waitAtMostFor(FIVE_SECONDS, () -> subscriptionExecutor.getActiveCount() == 1);
        assertThat(subscriptionExecutor.getActiveCount()).as("executor threads in use while the first call for E1 is stuck").isEqualTo(1);
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");

        // And: a resume after that still holds back the action until the stuck call has returned
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(Duration.ofSeconds(3), () -> calls.size() > 1);
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(TEN_SECONDS, () -> calls.contains("E2 returned"));
        assertThat(calls).as("calls once the stuck call was released").containsExactly("E1 started", "E1 returned", "E1 started", "E1 returned", "E2 started", "E2 returned");
        assertThat(mostInFlight).as("most calls running at once").hasValue(1);
    }

    @Test
    void a_cancel_while_a_resumed_run_waits_for_the_stuck_call_ends_that_run_and_frees_its_thread() {
        // Given: the first call for E1 is stuck, and a resumed run waits for it, so two executor threads are busy
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
        await().atMost(FIVE_SECONDS).until(() -> subscriptionExecutor.getActiveCount() == 2);

        // When
        subscriptionModel.cancelSubscription(subscriptionId);

        // Then: the run that waited ended and freed its thread, while the stuck call still runs
        waitAtMostFor(FIVE_SECONDS, () -> subscriptionExecutor.getActiveCount() == 1);
        assertThat(subscriptionExecutor.getActiveCount()).as("executor threads in use while the first call for E1 is stuck").isEqualTo(1);
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");

        // And: nothing is delivered to the cancelled subscription once the stuck call has returned
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(Duration.ofSeconds(2), () -> calls.size() > 2);
        assertThat(calls).as("calls once the stuck call was released").containsExactly("E1 started", "E1 returned");
    }

    @Test
    void a_run_paused_while_it_waits_two_runs_behind_the_stuck_call_ends_and_frees_its_thread() {
        // Given: the first call for E1 is stuck, and the third run waits for it through the second, which was paused while it waited
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("second run started").isTrue();
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("third run started").isTrue();
        await().atMost(FIVE_SECONDS).until(() -> subscriptionExecutor.getActiveCount() == 2);

        // When
        subscriptionModel.pauseSubscription(subscriptionId);

        // Then: the third run ended and freed its thread, while the stuck call still runs
        waitAtMostFor(FIVE_SECONDS, () -> subscriptionExecutor.getActiveCount() == 1);
        assertThat(subscriptionExecutor.getActiveCount()).as("executor threads in use while the first call for E1 is stuck").isEqualTo(1);
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
    }

    @Test
    void a_stop_and_start_while_the_call_for_the_first_event_is_stuck_returns_promptly() {
        // Given: the first call for E1 is stuck for longer than a pause waits
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));

        // When
        subscriptionModel.stop();
        CompletableFuture<Void> starting = CompletableFuture.runAsync(() -> subscriptionModel.start(true));

        // Then: the start waited for the resumed run to start, not for the stuck call
        assertThat(starting).as("start while the first call for E1 is stuck").succeedsWithin(Duration.ofSeconds(5));
        assertThat(calls).as("calls while the first call for E1 is stuck").containsExactly("E1 started");
    }

    @Test
    void an_interrupt_of_a_resumed_run_that_waits_for_the_stuck_call_reaches_the_retry_strategy_of_that_run_as_an_interrupted_exception_and_a_pause_and_resume_delivers_again() {
        // Given: the first call for E1 is stuck, a resumed run waits for it, and the retry strategy records the errors it is given
        List<Throwable> givenToTheRetryStrategy = new CopyOnWriteArrayList<>();
        subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatLosesItsHistoryWhenOpening(), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(100).onError((Throwable error) -> givenToTheRetryStrategy.add(error))));
        Logger modelLog = (Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        modelLog.addAppender(appender);
        try {
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(e1));
            await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
            subscriptionModel.pauseSubscription(subscriptionId);
            assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
            await().atMost(FIVE_SECONDS).until(() -> subscriptionExecutor.getActiveCount() == 2);
            Thread waiting = await().atMost(FIVE_SECONDS).until(this::threadOfTheResumedRun, thread -> thread != null);

            // When: the thread of the waiting run is interrupted, with no pause or cancel
            waiting.interrupt();

            // Then: the retry strategy of the run was given an interrupted exception, as for an interrupted read of the change stream
            waitAtMostFor(FIVE_SECONDS, () -> givenToTheRetryStrategy.stream().anyMatch(MongoInterruptedException.class::isInstance));
            assertThat(givenToTheRetryStrategy).as("errors given to the retry strategy").filteredOn(MongoInterruptedException.class::isInstance).first()
                    .satisfies(error -> assertThat(error).hasMessageContaining(subscriptionId).hasMessageContaining("was interrupted while it waited for an action call of a run it replaced"));
            waitAtMostFor(FIVE_SECONDS, () -> warningsContaining(appender, "Will restart!").stream().anyMatch(event -> event.getFormattedMessage().contains("MongoInterruptedException")));
            assertThat(warningsContaining(appender, "Will restart!")).as("warnings that the run restarts").anySatisfy(event -> assertThat(event.getFormattedMessage()).contains(subscriptionId).contains("MongoInterruptedException"));
            assertThat(messagesAt(appender, Level.WARN)).as("warnings of the model").noneMatch(message -> message.contains("so this run stops and delivers nothing"));

            // And: a pause and a resume once the stuck call has returned starts a run that delivers an event written after the resume
            releaseFirstCallOfE1.countDown();
            subscriptionModel.pauseSubscription(subscriptionId);
            assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("run started by the resume").isTrue();
            mongoEventStore.write("3", 0, serialize(e3));
            waitAtMostFor(TEN_SECONDS, () -> calls.contains("E3 returned"));
            assertThat(calls).as("calls once the subscription was paused and resumed").contains("E3 started", "E3 returned");
        } finally {
            modelLog.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void a_shutdown_while_a_resumed_run_waits_for_the_stuck_call_ends_the_wait_of_that_run() {
        // Given: the first call for E1 is stuck, and a resumed run waits for it
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);
        assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
        List<Thread> waiting = executorThreads.stream().filter(thread -> thread != stuckThread.get() && thread.isAlive()).toList();
        assertThat(waiting).as("executor threads other than the one running the stuck call").isNotEmpty();

        // When: shut down, which waits for the executor while the stuck call ignores its interrupts
        long startedShuttingDown = System.nanoTime();
        CompletableFuture<Void> shuttingDown = CompletableFuture.runAsync(subscriptionModel::shutdown);

        // Then: the close of the cancel ended the wait of the run long before the executor gave up on it
        waitAtMostFor(Duration.ofSeconds(2), () -> waiting.stream().noneMatch(Thread::isAlive));
        assertThat(waiting).as("threads still waiting two seconds after the shutdown started").noneMatch(Thread::isAlive);
        assertThat(shuttingDown).as("shutdown while a call is stuck").succeedsWithin(Duration.ofSeconds(8));
        assertThat(Duration.ofNanos(System.nanoTime() - startedShuttingDown)).as("time to shut down while a call is stuck").isLessThan(Duration.ofSeconds(8));
        assertThat(stuckThread.get().isAlive()).as("thread of the stuck call, which ignores interrupts").isTrue();
    }

    @Test
    void a_history_loss_as_the_resumed_run_opens_asks_the_listener_only_once_the_stuck_call_has_returned() {
        // Given: the first call for E1 is stuck, and a listener that takes a subscription over when its history is lost
        List<Checkpoint> askedAbout = new CopyOnWriteArrayList<>();
        subscriptionModel.addHistoryLossListener(takingOver(askedAbout));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.pauseSubscription(subscriptionId);

        // When: resumed, and its history is lost as the resumed run opens its change stream
        historyLostWhenOpening.set(true);
        subscriptionModel.resumeSubscription(subscriptionId);
        waitAtMostFor(Duration.ofSeconds(3), () -> !askedAbout.isEmpty());

        // Then: the listener was not asked while the first call for E1 is stuck
        assertThat(askedAbout).as("positions the listener was asked about while the first call for E1 is stuck").isEmpty();
        releaseFirstCallOfE1.countDown();
        waitAtMostFor(TEN_SECONDS, () -> !askedAbout.isEmpty());
        assertThat(askedAbout).as("positions the listener was asked about once the stuck call was released").singleElement().isNotNull();
        waitAtMostFor(FIVE_SECONDS, () -> subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused after the listener took it over").isTrue();
        assertThat(mostInFlight).as("most calls running at once").hasValue(1);
    }

    @Test
    void a_start_while_the_resumed_run_that_lost_its_history_as_it_opened_waits_for_the_stuck_call_returns_promptly() {
        // Given: the first call for E1 is stuck, and a listener that takes a subscription over when its history is lost
        List<Checkpoint> askedAbout = new CopyOnWriteArrayList<>();
        subscriptionModel.addHistoryLossListener(takingOver(askedAbout));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
        subscriptionModel.stop();

        // When: started, and the history is lost as the resumed run opens its change stream
        historyLostWhenOpening.set(true);
        CompletableFuture<Void> starting = CompletableFuture.runAsync(() -> subscriptionModel.start(true));

        // Then: the start didn't wait for the stuck call, which the run that never started waits for
        assertThat(starting).as("start while the first call for E1 is stuck").succeedsWithin(Duration.ofSeconds(5));
        assertThat(askedAbout).as("positions the listener was asked about while the first call for E1 is stuck").isEmpty();
    }

    @Test
    void an_action_that_resumes_its_own_subscription_after_a_history_loss_and_waits_until_it_started_gets_past_the_wait() {
        // Given: restarts after a history loss are on, and the action of E1 pauses its own subscription, resumes it as
        // its history is lost as the resumed run opens, and waits with no timeout for the resumed run to start
        subscriptionModel = new NativeMongoSubscriptionModel(database, collectionThatLosesItsHistoryWhenOpening(), TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true));
        String subscriptionId = UUID.randomUUID().toString();
        CountDownLatch gotPastTheWait = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
            if (!cloudEvent.getId().equals(e1.eventId())) {
                return;
            }
            try {
                subscriptionModel.pauseSubscription(subscriptionId);
                historyLostWhenOpening.set(true);
                subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted();
                gotPastTheWait.countDown();
            } catch (Throwable throwable) {
                failure.set(throwable);
            }
        }).waitUntilStarted(Duration.ofSeconds(10));

        // When
        mongoEventStore.write("1", 0, serialize(e1));

        // Then: the call got past the wait, although the resumed run waits for that very call to return
        boolean gotPast = awaitUnlessInterrupted(gotPastTheWait, Duration.ofSeconds(8));
        assertThat(gotPast).as("the action got past waitUntilStarted() on the subscription it resumed").isTrue();
        assertThat(failure).as("failure of the action").hasValue(null);
    }

    @Test
    void a_resumed_run_that_waits_for_the_stuck_call_logs_the_wait_and_then_that_it_goes_on() {
        // Given: the first call for E1 is stuck
        Logger modelLog = (Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        modelLog.addAppender(appender);
        try {
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(e1));
            await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
            subscriptionModel.pauseSubscription(subscriptionId);

            // When: resumed, which makes a run that waits for the stuck call
            assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
            waitAtMostFor(FIVE_SECONDS, () -> messagesAt(appender, Level.WARN).stream().anyMatch(message -> message.contains("waits for an action call")));

            // Then: it said so while the call was stuck, naming the subscription, and said it went on once the call returned
            assertThat(messagesAt(appender, Level.WARN)).as("warnings while the first call for E1 is stuck").singleElement().satisfies(message -> {
                assertThat(message).contains(subscriptionId);
                assertThat(message).contains("waits for an action call");
            });
            releaseFirstCallOfE1.countDown();
            waitAtMostFor(FIVE_SECONDS, () -> messagesAt(appender, Level.WARN).size() > 1);
            assertThat(messagesAt(appender, Level.WARN)).as("warnings once the stuck call was released").hasSize(2).last().satisfies(message -> {
                assertThat(message).contains(subscriptionId);
                assertThat(message).containsPattern("waited \\d+ ms");
            });
        } finally {
            modelLog.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    @Timeout(60)
    void a_resumed_run_that_waits_for_the_stuck_call_warns_again_after_ten_seconds_and_once_more_after_twenty_seconds_after_that() {
        // Given: the first call for E1 is stuck
        Logger modelLog = (Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        modelLog.addAppender(appender);
        try {
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), this::action).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(e1));
            await().atMost(FIVE_SECONDS).until(() -> calls.contains("E1 started"));
            subscriptionModel.pauseSubscription(subscriptionId);

            // When: resumed, which makes a run that waits for the stuck call, and the call stays stuck
            assertThat(subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10))).as("resumed run started").isTrue();
            waitAtMostFor(Duration.ofSeconds(45), () -> warningsContaining(appender, "still waits for an action call").size() >= 2);

            // Then: it said so when the wait began, and again after ten seconds and after twenty more
            List<ILoggingEvent> began = warningsContaining(appender, "waits for an action call of a run it replaced to return before it goes on");
            List<ILoggingEvent> stillWaiting = warningsContaining(appender, "still waits for an action call");
            assertThat(began).as("warnings when the wait began").hasSize(1);
            assertThat(stillWaiting).as("warnings while the first call for E1 stays stuck").hasSize(2);
            long sinceTheWaitBegan = began.getFirst().getTimeStamp();
            assertThat(stillWaiting.get(0).getTimeStamp() - sinceTheWaitBegan).as("milliseconds from the start of the wait to the first repeated warning").isGreaterThanOrEqualTo(9_500);
            assertThat(stillWaiting.get(1).getTimeStamp() - sinceTheWaitBegan).as("milliseconds from the start of the wait to the second repeated warning").isGreaterThanOrEqualTo(29_500);
            assertThat(stillWaiting).allSatisfy(event -> assertThat(event.getFormattedMessage()).contains(subscriptionId));
        } finally {
            modelLog.detachAppender(appender);
            appender.stop();
        }
    }

    // The executor thread that runs the resumed run, which is in the code of the model while it waits for the stuck call
    private @Nullable Thread threadOfTheResumedRun() {
        return executorThreads.stream()
                .filter(thread -> thread != stuckThread.get() && thread.isAlive())
                .filter(thread -> Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getClassName().contains("ChangeStreamSubscriptions")))
                .findFirst().orElse(null);
    }

    private static List<ILoggingEvent> warningsContaining(ListAppender<ILoggingEvent> appender, String text) {
        synchronized (appender) {
            return appender.list.stream().filter(event -> event.getLevel() == Level.WARN && event.getFormattedMessage().contains(text)).toList();
        }
    }

    private static List<String> messagesAt(ListAppender<ILoggingEvent> appender, Level level) {
        synchronized (appender) {
            return appender.list.stream().filter(event -> event.getLevel() == level).map(ILoggingEvent::getFormattedMessage).toList();
        }
    }

    private static HistoryLossListener takingOver(List<Checkpoint> askedAbout) {
        return new HistoryLossListener() {
            @Override
            public HistoryLossHandling handleHistoryLoss(String subscriptionId, Checkpoint lostFrom) {
                askedAbout.add(lostFrom);
                return HistoryLossHandling.takeOver(() -> {
                });
            }

            @Override
            public void restartingAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent) {
            }
        };
    }

    // Opens the change streams of the real collection, except that the next one the model opens throws that its history was lost
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatLosesItsHistoryWhenOpening() {
        MongoCollection<Document> wrapped = spy(eventCollection);
        doAnswer(invocation -> {
            ChangeStreamIterable<Document> recorded = spy((ChangeStreamIterable<Document>) invocation.callRealMethod());
            doAnswer(cursorInvocation -> {
                if (historyLostWhenOpening.compareAndSet(true, false)) {
                    throw historyLostError();
                }
                return cursorInvocation.callRealMethod();
            }).when(recorded).cursor();
            return recorded;
        }).when(wrapped).watch(anyList(), eq(Document.class));
        return wrapped;
    }

    private static MongoCommandException historyLostError() {
        List<BsonElement> elements = List.of(new BsonElement("code", new BsonInt32(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE)), new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")));
        return new MongoCommandException(new BsonDocument(elements), new ServerAddress());
    }

    private void action(CloudEvent cloudEvent) {
        String name = cloudEvent.getId().equals(e1.eventId()) ? "E1" : cloudEvent.getId().equals(e3.eventId()) ? "E3" : "E2";
        mostInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        calls.add(name + " started");
        try {
            if (name.equals("E1") && firstCallOfE1.getAndSet(false)) {
                stuckThread.set(Thread.currentThread());
                awaitUninterruptibly(releaseFirstCallOfE1);
            }
        } finally {
            calls.add(name + " returned");
            inFlight.decrementAndGet();
        }
    }

    private static boolean awaitUnlessInterrupted(CountDownLatch latch, Duration timeout) {
        try {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitAtMostFor(Duration timeout, Callable<Boolean> condition) {
        try {
            await().atMost(timeout).until(condition);
        } catch (ConditionTimeoutException ignored) {
            // The assertions that follow fail with what the model did instead
        }
    }

    private static NameDefined nameDefined(String name) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name);
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}
