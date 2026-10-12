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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A subscription held paused with nothing stored, whose id another catch-up instance has since stored a position for,
 * replays from that position when it is resumed, or when the model it is made through is started again. Two
 * dispatching {@link CatchupSubscriptionModel} instances in stream mode, over one event store and one checkpoint
 * storage, stand in for two nodes.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupResumeReplayStreamModeMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String REPLAY_THREAD_PREFIX = "occurrent-catchup-";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private String eventCollectionName;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private CatchupSubscriptionModel catchupA;
    private CatchupSubscriptionModel catchupB;
    private DurableSubscriptionModel durableB;
    private BlockingStorage storage;
    private FlakyReader readerOfB;
    private FailingLiveModel liveOfB;
    private final ErrorLog errorLog = new ErrorLog();
    private final Logger modelLogger = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private final CountDownLatch releaseTheClaim = new CountDownLatch(1);
    private final String subscriptionId = UUID.randomUUID().toString();
    private final CopyOnWriteArrayList<String> receivedByB = new CopyOnWriteArrayList<>();
    private final AtomicInteger inAction = new AtomicInteger();
    private final AtomicInteger maxInAction = new AtomicInteger();

    @BeforeEach
    void create_instances() {
        Awaitility.setDefaultPollInterval(Duration.ofMillis(10));
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
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        storage = new BlockingStorage(new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID()));
        readerOfB = new FlakyReader(eventStore);
        liveOfB = new FailingLiveModel(mongoTemplate, eventCollectionName);
        errorLog.start();
        modelLogger.addAppender(errorLog);
    }

    @AfterEach
    void shutdown() {
        Awaitility.reset();
        modelLogger.detachAppender(errorLog);
        errorLog.stop();
        storage.unblock();
        releaseA.countDown();
        releaseTheClaim.countDown();
        if (catchupA != null) {
            catchupA.shutdown();
        }
        if (catchupB != null) {
            catchupB.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_pause_through_the_dispatcher_that_comes_while_the_resume_replay_hands_over_leaves_the_subscription_paused() throws Exception {
        // Given B has replayed the history another node's position leaves out, and is about to hand over
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        CompletableFuture<Subscription> resumed = CompletableFuture.supplyAsync(() -> catchupB.resumeSubscription(subscriptionId));
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
        storage.blockReadsOn(thread -> thread.getName().startsWith(REPLAY_THREAD_PREFIX));
        releaseB.countDown();
        assertThat(storage.blocked.await(10, SECONDS)).as("the handover reads the storage").isTrue();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay has ended, and the live delegate is not resumed yet").isFalse();

        // When the subscription is paused in that window, off the test thread since the pause may wait for the handover
        AtomicReference<Thread> pausingThread = new AtomicReference<>();
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> {
            pausingThread.set(Thread.currentThread());
            catchupB.pauseSubscription(subscriptionId);
        });
        awaitParkedIn(pausing, pausingThread, "pauseSubscription");
        storage.unblock();

        // Then the pause returned without failing
        assertThat(failureOf(pausing)).as("the pause returned without throwing").isNull();
        awaitHandover(resumed);
        // And the subscription is paused, and holds back what is written after the pause
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused after the pause returned").isTrue();
        append("afterPause", 4);
        await("nothing written after the pause reaches B while it is paused").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterPause"));
    }

    @Test
    void a_pause_through_the_dispatcher_that_comes_while_a_replay_a_subscribe_started_hands_over_leaves_the_subscription_paused() throws Exception {
        // Given a replay a subscribe started has delivered the history, and its handover waits for the storage
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        CountDownLatch inH3 = new CountDownLatch(1);
        CountDownLatch releaseH3 = new CountDownLatch(1);
        Subscription subscription = catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), recording("h3", inH3, releaseH3));
        assertThat(inH3.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
        storage.blockReadsOn(thread -> thread.getName().startsWith(REPLAY_THREAD_PREFIX));
        releaseH3.countDown();
        assertThat(storage.blocked.await(10, SECONDS)).as("the handover reads the storage").isTrue();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay has ended, and the live delegate does not have the subscription yet").isFalse();

        // When the subscription is paused in that window, off the test thread since the pause may wait for the handover
        AtomicReference<Thread> pausingThread = new AtomicReference<>();
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> {
            pausingThread.set(Thread.currentThread());
            catchupB.pauseSubscription(subscriptionId);
        });
        awaitParkedIn(pausing, pausingThread, "pauseSubscription");
        storage.unblock();

        // Then the pause returned without failing
        assertThat(failureOf(pausing)).as("the pause returned without throwing").isNull();
        ((CatchupSubscription) subscription).delegatedSubscription().get(10, SECONDS);
        // And the subscription is paused, and holds back what is written after the pause
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused after the pause returned").isTrue();
        assertThat(receivedByB).as("the history the replay delivered").contains("h1", "h2", "h3");
        append("afterPause", 4);
        await("nothing written after the pause reaches B while it is paused").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterPause"));
    }

    @Test
    void a_resume_through_the_dispatcher_after_a_cancel_that_the_live_delegate_failed_replays_from_the_position_another_node_stored() throws Exception {
        // Given the live delegate fails the cancel while it still holds the subscription paused
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)), CancelFailingModel::new);

        // When the subscription is cancelled, and then resumed
        assertThatThrownBy(() -> catchupB.cancelSubscription(subscriptionId)).as("the cancel").hasMessageContaining(CancelFailingModel.UNAVAILABLE);
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("the position another node stored is kept by the failed cancel").isTrue();
        catchupB.resumeSubscription(subscriptionId);
        append("afterResume", 4);

        // Then the history after the stored position is delivered, with what is written after the resume
        await("B delivers the history after the stored position and what is written after the resume").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterResume"));
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
    }

    @Test
    void the_subscription_reads_as_running_and_not_catching_up_while_the_resume_replay_hands_over_through_the_dispatcher() throws Exception {
        // Given B has replayed the history another node's position leaves out, and its handover waits for the storage
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        CompletableFuture<Subscription> resumed = CompletableFuture.supplyAsync(() -> catchupB.resumeSubscription(subscriptionId));
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
        storage.blockReadsOn(thread -> thread.getName().startsWith(REPLAY_THREAD_PREFIX));
        releaseB.countDown();
        assertThat(storage.blocked.await(10, SECONDS)).as("the handover reads the storage").isTrue();

        // When the subscription is asked about in that window
        boolean running = catchupB.isRunning(subscriptionId);
        boolean paused = catchupB.isPaused(subscriptionId);
        boolean catchingUp = catchupB.isCatchingUp(subscriptionId);
        storage.unblock();
        awaitHandover(resumed);

        // Then it reads as running or paused
        assertThat(running || paused).as("the subscription is running or paused while the replay hands over").isTrue();
        // And as running, though no longer catching up
        assertThat(running).as("the subscription is running while the replay hands over").isTrue();
        assertThat(catchingUp).as("the replay has ended, so the subscription is not catching up").isFalse();
        // And it keeps running once the handover is done
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the handover").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the handover").isFalse();
    }

    @Test
    void a_pause_through_the_dispatcher_that_comes_right_after_a_failed_resume_handover_gave_up_the_lock_holds_the_replay_until_the_subscription_is_resumed() throws Exception {
        // Given the storage fails the first read the handover of the resume replay makes, and the replay thread
        // is descheduled right after it has given up the handover lock
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        GapLock gapLock = new GapLock(storage);
        handoverLocksOf(catchupB).put(subscriptionId, gapLock);
        storage.failTheNextHandoverReads(1);
        gapLock.armed = true;
        catchupB.resumeSubscription(subscriptionId);
        assertThat(gapLock.inGap.await(10, SECONDS)).as("the handover failed and the replay thread gave up the lock").isTrue();

        // When the subscription is paused in that gap
        Throwable pauseFailure = catchThrowable(() -> catchupB.pauseSubscription(subscriptionId));
        boolean paused = catchupB.isPaused(subscriptionId);
        boolean running = catchupB.isRunning(subscriptionId);
        gapLock.leaveGap.countDown();

        // Then the pause returned without failing
        assertThat(pauseFailure).as("the pause returned without throwing").isNull();
        // And the subscription is paused right away
        assertThat(paused).as("the subscription is paused after the pause returned").isTrue();
        assertThat(running || paused).as("the subscription is running or paused in that gap").isTrue();
        // And what is written after the pause is held back
        append("afterPause", 4);
        await("nothing written after the pause reaches B while it is paused").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterPause"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is still paused").isTrue();

        // And a later resume delivers the history and what was written after the pause
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers what was written after the pause").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterPause"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
        assertThat(storage.handoverReadFailures.get()).as("the read in the handover failed once").isEqualTo(1);
    }

    @Test
    void start_with_resume_through_the_dispatcher_replays_from_the_position_another_catch_up_stored_as_a_resume_does() throws Exception {
        // Given B holds the subscription paused, and another node stored a position for it
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        catchupB.stop();

        // When B is started again, resuming what it holds paused
        catchupB.start(true);
        append("afterStart", 4);

        // Then everything after the stored position is delivered, not only what is written from now on
        await("B delivers what is written after the start").atMost(10, SECONDS).until(() -> receivedByB.contains("afterStart"));
        assertThat(receivedByB).as("the history after the stored position").contains("h2", "h3", "afterBRegistered");
    }

    @Test
    void two_resumes_through_the_dispatcher_at_the_same_time_never_run_the_action_concurrently() throws Exception {
        // Given an action that stays in its first call until a second call comes, or a second has passed
        AtomicInteger inAction = new AtomicInteger();
        AtomicInteger maxInAction = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstCall = new CountDownLatch(1);
        CountDownLatch secondCall = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(event -> {
            maxInAction.accumulateAndGet(inAction.incrementAndGet(), Math::max);
            try {
                receivedByB.add(nameOf(event));
                if (calls.incrementAndGet() == 1) {
                    firstCall.countDown();
                    awaitUpTo(secondCall, 1);
                } else {
                    secondCall.countDown();
                }
            } finally {
                inAction.decrementAndGet();
            }
        });
        // And the second resume reads the storage once the first one's replay is in the action, or a second has passed
        // without that happening. A second resume that waits for the first one never gets as far as the read.
        storage.holdReadsOn(thread -> thread.getName().equals("resume-2"), firstCall);

        // When B is resumed from two threads at once
        Thread first = Thread.ofPlatform().name("resume-1").start(() -> catchupB.resumeSubscription(subscriptionId));
        Thread second = Thread.ofPlatform().name("resume-2").start(() -> catchupB.resumeSubscription(subscriptionId));
        first.join(10_000);
        second.join(10_000);

        // Then the history after the stored position is delivered
        await("B delivers the history after the stored position").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        // And no two calls of the action overlapped
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_start_through_the_dispatcher_that_fails_listing_the_replays_a_resume_runs_stops_the_model_again_as_a_start_whose_live_delegate_fails_does() throws Exception {
        // Given B is stopped, holds a subscription paused, and its live delegate fails the first time it is asked if one is paused
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        catchupB.stop();
        liveOfB.failTheNextIsPausedOn(Thread.currentThread());

        // When B is started, resuming what it holds paused, then the start fails
        assertThatThrownBy(() -> catchupB.start(true))
                .as("the start")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(FailingLiveModel.UNAVAILABLE);

        // Then a replay of a subscription made afterwards is held, as in a stopped model
        String laterId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<String> receivedByLater = new CopyOnWriteArrayList<>();
        catchupB.subscribe(laterId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByLater.add(nameOf(event)));
        assertThat(catchupB.isPaused(laterId)).as("the subscription made after the failed start is paused").isTrue();
        await("nothing is delivered to the subscription made after the failed start").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByLater).isEmpty());

        // And a start that succeeds runs both replays
        catchupB.start(true);
        await("B delivers the history of both subscriptions").atMost(10, SECONDS).untilAsserted(() -> {
            assertThat(receivedByLater).contains("h3");
            assertThat(receivedByB).as("the history after the stored position").contains("h2", "h3", "afterBRegistered");
        });
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_while_a_failed_resume_replay_waits_to_run_again_ends_the_retries() throws Exception {
        // Given the event store fails every read of the resume replay, so it waits to run again
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        await("the replay is run again after it failed").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(3));

        Thread replayThread = requireNonNull(readerOfB.lastFailedOn, "the thread the last read failed on");
        int failuresAtResume = readerOfB.failures.get();

        // When the subscription is resumed at the present
        catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(requireNonNull(durableB.globalCheckpoint())));

        // Then the replay thread ends, so no replay runs again, and an attempt that was reading when the resume came fails at most once
        awaitEnded(replayThread);
        await("no replay is run after the resume at a position").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).as("the reads that failed after the resume at a position").isLessThanOrEqualTo(failuresAtResume + 1));
        // And the subscription runs from that position
        append("afterResume", 4);
        await("B delivers what is written after the resume").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterResume"));
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the subscription is not catching up").isFalse();
    }

    @Test
    void a_stop_through_the_dispatcher_that_comes_right_after_a_failed_resume_handover_gave_up_the_lock_holds_the_replay_until_a_start_that_resumes() throws Exception {
        // Given the storage fails the first read the handover of the resume replay makes, and the replay thread
        // is descheduled right after it has given up the handover lock
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        GapLock gapLock = new GapLock(storage);
        handoverLocksOf(catchupB).put(subscriptionId, gapLock);
        storage.failTheNextHandoverReads(1);
        gapLock.armed = true;
        catchupB.resumeSubscription(subscriptionId);
        assertThat(gapLock.inGap.await(10, SECONDS)).as("the handover failed and the replay thread gave up the lock").isTrue();

        // When the model is stopped in that gap
        Throwable stopFailure = catchThrowable(() -> catchupB.stop());
        boolean pausedInTheGap = catchupB.isPaused(subscriptionId);
        boolean catchingUpInTheGap = catchupB.isCatchingUp(subscriptionId);
        gapLock.leaveGap.countDown();

        // Then the stop returned without failing
        assertThat(stopFailure).as("the stop returned without throwing").isNull();
        // And the replay is held, so the subscription is paused and not catching up
        assertThat(pausedInTheGap).as("the subscription is paused after the stop returned").isTrue();
        assertThat(catchingUpInTheGap).as("the replay is held after the stop returned").isFalse();
        // And it stays held, so what is written after the stop is not delivered, once the replay thread left the gap
        append("afterStop", 4);
        await("nothing written after the stop reaches B while the replay is held").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterStop"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is still paused").isTrue();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay does not run").isFalse();

        // And a start that resumes delivers the history and what was written after the stop
        catchupB.start(true);
        await("B delivers what was written after the stop").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterStop"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the start that resumes").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the start that resumes").isTrue();
        assertThat(storage.handoverReadFailures.get()).as("the read in the handover failed once").isEqualTo(1);
    }

    @Test
    void a_cancel_through_the_dispatcher_that_comes_right_after_a_failed_resume_handover_gave_up_the_lock_ends_the_replay_and_the_id_can_be_subscribed_again() throws Exception {
        // Given the storage fails the first read the handover of the resume replay makes, and the replay thread
        // is descheduled right after it has given up the handover lock
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        GapLock gapLock = new GapLock(storage);
        handoverLocksOf(catchupB).put(subscriptionId, gapLock);
        storage.failTheNextHandoverReads(1);
        gapLock.armed = true;
        catchupB.resumeSubscription(subscriptionId);
        assertThat(gapLock.inGap.await(10, SECONDS)).as("the handover failed and the replay thread gave up the lock").isTrue();

        // When the subscription is cancelled in that gap
        Throwable cancelFailure = catchThrowable(() -> catchupB.cancelSubscription(subscriptionId));
        gapLock.leaveGap.countDown();

        // Then the cancel returned without failing
        assertThat(cancelFailure).as("the cancel returned without throwing").isNull();
        // And the replay does not run again, so what is written after the cancel is not delivered, and nothing is left running or paused
        append("afterCancel", 4);
        await("nothing written after the cancel reaches B").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterCancel"));
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay does not run").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription does not run").isFalse();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(storage.handoverReadFailures.get()).as("the read in the handover failed once").isEqualTo(1);

        // And the id can be subscribed again, and the new subscription delivers
        CopyOnWriteArrayList<String> receivedByNew = new CopyOnWriteArrayList<>();
        catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByNew.add(nameOf(event)));
        await("the new subscription delivers the history and what was written after the cancel").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByNew).contains("h1", "h2", "h3", "afterCancel"));
        assertThat(receivedByB).as("what the cancelled subscription received").doesNotContain("afterCancel");
    }

    @Test
    void a_replay_thread_that_is_interrupted_while_a_failed_resume_replay_waits_to_run_again_holds_the_replay_and_logs_it_until_the_subscription_is_resumed() throws Exception {
        // Given the event store fails every read of the resume replay, so it waits to run again
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        // And it has failed often enough for the wait before the next run to last about two seconds
        await("the replay is run again after it failed").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(6));
        Thread replayThread = requireNonNull(readerOfB.lastFailedOn, "the thread the last read failed on");
        awaitAsleep(replayThread);
        assertThat(replayThread.getName()).as("the thread that waits to run again").startsWith(REPLAY_THREAD_PREFIX);

        // When that thread is interrupted while it waits, and the event store would serve a replay that ran now
        replayThread.interrupt();
        readerOfB.failTheNext(0);

        // Then the replay is held and the interruption is logged at ERROR
        await("the interruption is logged").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(errorLog.messagesAbout(subscriptionId))
                        .filteredOn(message -> message.contains("was interrupted while it waited to run again"))
                        .hasSize(1));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused").isTrue();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay is held, not waiting to run again").isFalse();
        awaitEnded(replayThread);
        await("nothing is delivered while the replay is held").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).isEmpty());

        // And a resume runs it and delivers the history
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the resume").isTrue();
    }

    @Test
    void a_resume_through_the_dispatcher_that_comes_after_a_pause_during_the_resume_replay_drops_the_pause_so_the_replay_hands_over_to_a_running_subscription() throws Exception {
        // Given B's resume replay is blocked while it reads the history
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        CompletableFuture.runAsync(() -> catchupB.resumeSubscription(subscriptionId));
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();

        // When the subscription is paused and then resumed during the replay
        catchupB.pauseSubscription(subscriptionId);
        catchupB.resumeSubscription(subscriptionId);
        releaseB.countDown();

        // Then the replay hands over and the subscription runs without being paused
        await("the replay has ended").atMost(10, SECONDS).until(() -> !catchupB.isCatchingUp(subscriptionId));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the handover").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the handover").isTrue();
        // And what is written afterwards is delivered
        append("afterResume", 4);
        await("B delivers what is written after the resume").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterResume"));
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_waits_for_the_action_the_resume_replay_is_in_before_the_live_delegate_calls_it() throws Exception {
        // Given B's resume replay is in the action, blocked on h2
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();

        // When the subscription is resumed at the present from another thread, and an event is written after that position
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        append("afterReposition", 4);
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");

        // Then the resume has not returned, and the live delegate has not called the action
        assertSoftly(softly -> {
            softly.assertThat(repositioning.isDone()).as("the resume at a position returned while the replay was in the action").isFalse();
            softly.assertThat(receivedByB).as("what the action received while the replay was in it").doesNotContain("afterReposition");
            softly.assertThat(maxInAction.get()).as("the most calls of the action in flight at once while the replay was in it").isEqualTo(1);
        });
        // And once the action returns, the resume returns and the event written after the position is delivered
        releaseH2.countDown();
        repositioning.get(10, SECONDS);
        await("B delivers what is written after the position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterReposition"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_waits_for_the_action_a_held_resume_replay_is_in_before_the_live_delegate_calls_it() throws Exception {
        // Given B's resume replay is blocked in the action on h2, and a stop has held it
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        catchupB.stop();
        catchupB.start(false);
        assertThat(catchupB.isPaused(subscriptionId)).as("the replay is held").isTrue();

        // When the subscription is resumed at the present from another thread, and an event is written after that position
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        append("afterReposition", 4);
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");

        // Then the resume has not returned, and the live delegate has not called the action
        assertSoftly(softly -> {
            softly.assertThat(repositioning.isDone()).as("the resume at a position returned while the held replay was in the action").isFalse();
            softly.assertThat(receivedByB).as("what the action received while the held replay was in it").doesNotContain("afterReposition");
            softly.assertThat(maxInAction.get()).as("the most calls of the action in flight at once while the held replay was in it").isEqualTo(1);
        });
        // And once the action returns, the resume returns and the event written after the position is delivered
        releaseH2.countDown();
        repositioning.get(10, SECONDS);
        await("B delivers what is written after the position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterReposition"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_from_inside_the_action_of_its_own_resume_replay_is_refused_and_the_replay_goes_on() throws Exception {
        // Given an action that resumes the subscription at the present when the replay delivers h2
        AtomicReference<Throwable> repositionFailure = new AtomicReference<>();
        CountDownLatch repositionTried = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(event -> {
            String name = nameOf(event);
            receivedByB.add(name);
            if (name.equals("h2")) {
                repositionFailure.set(catchThrowable(() -> catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(requireNonNull(durableB.globalCheckpoint())))));
                repositionTried.countDown();
            }
        });

        // When B is resumed from the position another node stored
        catchupB.resumeSubscription(subscriptionId);

        // Then the resume from inside the action is refused
        assertThat(repositionTried.await(10, SECONDS)).as("the action tried the resume at a position").isTrue();
        assertThat(repositionFailure.get()).as("the resume at a position from inside the action")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(subscriptionId);
        // And the replay goes on and delivers the history after the stored position
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        // And the subscription runs once the replay has handed over
        append("afterReplay", 4);
        await("B delivers what is written after the replay").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterReplay"));
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
    }

    @Test
    void a_resume_through_the_dispatcher_while_a_replay_a_subscribe_started_runs_returns_without_failing_and_the_subscription_runs() throws Exception {
        // Given a replay a subscribe started is blocked in the action on h1
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        CountDownLatch inH1 = new CountDownLatch(1);
        CountDownLatch releaseH1 = new CountDownLatch(1);
        Subscription subscription = catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), recording("h1", inH1, releaseH1));
        assertThat(inH1.await(10, SECONDS)).as("B's replay reaches h1").isTrue();

        // When the subscription is resumed during the replay
        Throwable resumeFailure = catchThrowable(() -> catchupB.resumeSubscription(subscriptionId));
        releaseH1.countDown();

        // Then the resume returned without failing
        assertThat(resumeFailure).as("the resume during the replay").isNull();
        // And the subscription receives the history, then what is written after the replay
        ((CatchupSubscription) subscription).delegatedSubscription().get(10, SECONDS);
        append("afterResume", 3);
        await("B delivers the history and what is written after the replay").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h1", "h2", "afterResume"));
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
    }

    @Test
    void a_resume_that_comes_while_a_resume_at_a_position_waits_for_the_action_of_a_held_replay_waits_for_it_and_the_action_never_runs_twice() throws Exception {
        // Given a resume at a position waits for the action a held resume replay is in
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        catchupB.stop();
        catchupB.start(false);
        assertThat(catchupB.isPaused(subscriptionId)).as("the replay is held").isTrue();
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");
        assertThat(repositioning.isDone()).as("the resume at a position waits for the action").isFalse();

        // When the subscription is resumed from another thread
        AtomicReference<Thread> resumingThread = new AtomicReference<>();
        CompletableFuture<Subscription> resuming = CompletableFuture.supplyAsync(() -> {
            resumingThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId);
        });

        // Then neither call returns while the action is blocked, and the action is not entered again
        awaitParkedIn(resuming, resumingThread, "claim");
        await("neither call returns while the action is blocked").during(2, SECONDS).atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(repositioning.isDone()).as("the resume at a position returned while the action was blocked").isFalse();
            assertThat(resuming.isDone()).as("the resume returned while the action was blocked").isFalse();
            assertThat(maxInAction.get()).as("the most calls of the action in flight at once while it was blocked").isEqualTo(1);
            assertThat(receivedByB).as("what the action received while it was blocked").containsExactly("h2");
        });
        // And once the action returns, the resume at a position returns without failing
        releaseH2.countDown();
        assertThat(failureOf(repositioning)).as("the resume at a position").isNull();
        // And the resume returns after it
        assertThat(failureOf(resuming)).as("the resume that waited, which finds the subscription running").isInstanceOf(SubscriptionAlreadyRunningException.class);
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
        // And the subscription runs from the position, so what is written after it is delivered once
        append("afterReposition", 4);
        await("B delivers what is written after the position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).containsExactly("h2", "afterReposition"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once, after the resumes").isEqualTo(1);
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("no replay runs").isFalse();
    }

    @Test
    void a_start_that_resumes_and_comes_while_a_resume_at_a_position_waits_for_the_action_of_a_held_replay_waits_for_it_and_the_action_never_runs_twice() throws Exception {
        // Given a resume at a position waits for the action a held resume replay is in
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        catchupB.stop();
        catchupB.start(false);
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");
        assertThat(repositioning.isDone()).as("the resume at a position waits for the action").isFalse();

        // When the model is started again, resuming what it holds paused, from another thread
        AtomicReference<Thread> startingThread = new AtomicReference<>();
        CompletableFuture<Void> starting = CompletableFuture.runAsync(() -> {
            startingThread.set(Thread.currentThread());
            catchupB.start(true);
        });

        // Then neither call returns while the action is blocked, and the action is not entered again
        awaitParkedIn(starting, startingThread, "claim");
        await("neither call returns while the action is blocked").during(2, SECONDS).atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(repositioning.isDone()).as("the resume at a position returned while the action was blocked").isFalse();
            assertThat(starting.isDone()).as("the start returned while the action was blocked").isFalse();
            assertThat(maxInAction.get()).as("the most calls of the action in flight at once while it was blocked").isEqualTo(1);
            assertThat(receivedByB).as("what the action received while it was blocked").containsExactly("h2");
        });
        // And once the action returns, the resume at a position returns without failing
        releaseH2.countDown();
        assertThat(failureOf(repositioning)).as("the resume at a position").isNull();
        // And the start returns after it
        assertThat(failureOf(starting)).as("the start that waited").isNull();
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
        // And the subscription runs from the position, so what is written after it is delivered once
        append("afterReposition", 4);
        await("B delivers what is written after the position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).containsExactly("h2", "afterReposition"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once, after the start").isEqualTo(1);
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("no replay runs").isFalse();
    }

    @Test
    void a_resume_that_comes_right_after_a_resume_at_a_position_held_the_replay_waits_for_it_and_the_action_never_runs_twice() throws Exception {
        // Given B's resume replay is in the action, blocked on h2
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        PausingLock pausingLock = new PausingLock("repositioning");
        handoverLocksOf(catchupB).put(subscriptionId, pausingLock);
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        pausingLock.armed = true;

        // And a resume at a position is descheduled right after it took the replay
        CompletableFuture<Subscription> repositioning = new CompletableFuture<>();
        Thread.ofPlatform().name("repositioning").start(() -> {
            try {
                repositioning.complete(catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present)));
            } catch (Throwable e) {
                repositioning.completeExceptionally(e);
            }
        });
        assertThat(pausingLock.paused.await(10, SECONDS)).as("the resume at a position took the replay").isTrue();

        // When the subscription is resumed from another thread
        AtomicReference<Thread> resumingThread = new AtomicReference<>();
        CompletableFuture<Subscription> resuming = CompletableFuture.supplyAsync(() -> {
            resumingThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId);
        });

        // Then the resume waits, and the action is not entered again
        awaitParkedIn(resuming, resumingThread, "claim");
        await("the resume does not return while the resume at a position is under way").during(2, SECONDS).atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(resuming.isDone()).as("the resume returned while the resume at a position was under way").isFalse();
            assertThat(maxInAction.get()).as("the most calls of the action in flight at once while it was blocked").isEqualTo(1);
            assertThat(receivedByB).as("what the action received while it was blocked").containsExactly("h2");
        });
        // And once the resume at a position goes on and the action returns, the resume at a position returns without failing
        pausingLock.resume.countDown();
        releaseH2.countDown();
        assertThat(failureOf(repositioning)).as("the resume at a position").isNull();
        // And the resume returns after it
        assertThat(failureOf(resuming)).as("the resume that waited, which finds the subscription running").isInstanceOf(SubscriptionAlreadyRunningException.class);
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
        // And the subscription runs from the position, so what is written after it is delivered once
        append("afterReposition", 4);
        await("B delivers what is written after the position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).containsExactly("h2", "afterReposition"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once, after the resumes").isEqualTo(1);
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("no replay runs").isFalse();
    }

    @Test
    void a_resume_at_a_position_while_the_action_of_the_resume_replay_never_returns_fails_after_ten_seconds_and_changes_nothing() throws Exception {
        // Given B's resume replay is in the action, blocked on h2
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        boolean runningBefore = catchupB.isRunning(subscriptionId);
        boolean pausedBefore = catchupB.isPaused(subscriptionId);
        boolean catchingUpBefore = catchupB.isCatchingUp(subscriptionId);
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        append("afterReposition", 4);

        // When the subscription is resumed at the present from another thread, and the action is not released within the bound
        AtomicLong waitedMillis = new AtomicLong();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            try {
                return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
            } finally {
                waitedMillis.set(MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS));
            }
        });

        // Then it fails with IllegalStateException, after the bound and not long after it
        waitUpTo(repositioning, 30);
        assertThat(repositioning.isDone()).as("the resume at a position gave up on the action within 30 seconds").isTrue();
        assertThat(failureOf(repositioning)).as("the resume at a position").isInstanceOf(IllegalStateException.class).hasMessageContaining(subscriptionId);
        assertThat(waitedMillis.get()).as("the milliseconds the resume at a position waited").isBetween(9_900L, 20_000L);
        // And it logs at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR for the subscription")
                .anyMatch(message -> message.contains("was not resumed from the given position") && message.contains("10000 ms"));
        // And the subscription is as it was
        assertThat(catchupB.isRunning(subscriptionId)).as("running").isEqualTo(runningBefore);
        assertThat(catchupB.isPaused(subscriptionId)).as("paused").isEqualTo(pausedBefore);
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("catching up").isEqualTo(catchingUpBefore);
        // And the live delegate has not called the action
        await("the live delegate does not call the action while it is blocked").during(2, SECONDS).atMost(5, SECONDS).untilAsserted(() -> {
            assertThat(receivedByB).as("what the action received while it was blocked").containsExactly("h2");
            assertThat(maxInAction.get()).as("the most calls of the action in flight at once while it was blocked").isEqualTo(1);
        });
        // And once the action returns, the replay goes on and delivers what is left, one call at a time
        releaseH2.countDown();
        await("B delivers the rest of the history and what was written after the position").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h3", "afterBRegistered", "afterReposition"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_resume_at_a_position_that_the_action_hands_to_another_thread_and_waits_for_fails_after_ten_seconds_and_the_replay_goes_on() throws Exception {
        // Given an action that hands a resume at the present to another thread and waits for it when it delivers h2
        AtomicReference<Throwable> handedOffFailure = new AtomicReference<>();
        CountDownLatch handedOffReturned = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(event -> {
            String name = nameOf(event);
            receivedByB.add(name);
            if (name.equals("h2") && handedOffReturned.getCount() > 0) {
                Checkpoint present = requireNonNull(durableB.globalCheckpoint());
                try {
                    CompletableFuture.supplyAsync(() -> catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present))).get(60, SECONDS);
                } catch (ExecutionException e) {
                    handedOffFailure.set(e.getCause());
                } catch (Exception e) {
                    handedOffFailure.set(e);
                }
                handedOffReturned.countDown();
            }
        }));

        // When B is resumed from the position another node stored
        long start = System.nanoTime();
        catchupB.resumeSubscription(subscriptionId);

        // Then the handed-off call fails with IllegalStateException, after the bound
        assertThat(handedOffReturned.await(30, SECONDS)).as("the action got the answer of the handed-off call").isTrue();
        long waitedMillis = MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS);
        assertThat(handedOffFailure.get()).as("the handed-off resume at a position").isInstanceOf(IllegalStateException.class).hasMessageContaining(subscriptionId);
        assertThat(waitedMillis).as("the milliseconds until the handed-off call failed").isBetween(9_900L, 25_000L);
        // And it logs at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR for the subscription")
                .anyMatch(message -> message.contains("was not resumed from the given position"));
        // And the subscription is not left stalled, since the replay goes on and delivers the history
        await("B delivers the history after the stored position").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        append("afterReplay", 4);
        await("B delivers what is written after the replay").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterReplay"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void the_subscription_reads_as_running_and_not_paused_while_a_resume_at_a_position_through_the_dispatcher_waits_for_the_action_of_the_replay_it_holds() throws Exception {
        // Given B's resume replay is in the action, blocked on h2
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();

        // When the subscription is resumed at the present from another thread, and waits for the action
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");

        // Then the subscription reads as running and not paused, since it runs once the resume has returned
        assertThat(repositioning.isDone()).as("the resume at a position returned while the replay was in the action").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("running while the resume at a position waits").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("paused while the resume at a position waits").isFalse();
        // And it still does once the resume has returned
        releaseH2.countDown();
        repositioning.get(10, SECONDS);
        assertThat(catchupB.isRunning(subscriptionId)).as("running once the resume at a position returned").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("paused once the resume at a position returned").isFalse();
    }

    @Test
    void a_subscription_that_reads_as_paused_because_a_stop_held_its_replay_in_the_action_still_reads_as_paused_while_a_resume_at_a_position_waits_for_the_action_and_runs_once_the_resume_has_returned() throws Exception {
        // Given B's resume replay is blocked in the action on h2, and a stop has held it
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        catchupB.stop();
        catchupB.start(false);
        boolean runningBefore = catchupB.isRunning(subscriptionId);
        assertThat(catchupB.isPaused(subscriptionId)).as("the replay is held").isTrue();

        // When the subscription is resumed at the present from another thread, and waits for the action
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");

        // Then the subscription reads as it did before, since it did not run
        assertThat(repositioning.isDone()).as("the resume at a position returned while the held replay was in the action").isFalse();
        assertThat(catchupB.isPaused(subscriptionId)).as("paused while the resume at a position waits").isTrue();
        assertThat(catchupB.isRunning(subscriptionId)).as("running while the resume at a position waits").isEqualTo(runningBefore);
        // And it runs once the action has returned and the resume has returned
        releaseH2.countDown();
        repositioning.get(10, SECONDS);
        assertThat(catchupB.isRunning(subscriptionId)).as("running once the resume at a position returned").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("paused once the resume at a position returned").isFalse();
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_whose_live_delegate_fails_after_waiting_for_the_action_of_the_resume_replay_leaves_the_replay_running_and_it_hands_over() throws Exception {
        // Given B's resume replay is in the action, blocked on h2, and the live delegate fails the next resume
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        Subscription handleOfTheReplay = catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        liveOfB.failTheNextResumes(1);

        // When the subscription is resumed at the present from another thread, and waits for the action
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present)));
        append("duringTheCall", 4);
        waitUpTo(repositioning, 1);
        assertThat(repositioning.isDone()).as("the resume at a position returned while the replay was in the action").isFalse();
        // And the action returns, so the live delegate is asked to resume and fails
        releaseH2.countDown();
        Throwable failure = failureOf(repositioning);
        append("afterTheFailedCall", 5);

        // Then what the live delegate threw reaches the caller
        assertThat(failure).as("the resume at a position").isInstanceOf(IllegalStateException.class).hasMessageContaining(FailingLiveModel.UNAVAILABLE);
        // And the subscription runs, with its replay as it was
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the failed resume at a position").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("paused after the failed resume at a position").isFalse();
        // And the replay hands over, so the handle of the first resume starts
        assertThat(handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
        // And no event is lost, whether written before the call, during it or after it
        await("B delivers the history and what was written during and after the failed call").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "duringTheCall", "afterTheFailedCall"));
        assertThat(liveOfB.resumeFailures.get()).as("the resumes of the live delegate that failed").isEqualTo(1);
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_whose_live_delegate_fails_leaves_a_replay_held_by_a_pause_held_until_the_subscription_is_resumed() throws Exception {
        // Given B's resume replay is in the action on h3, and a pause during the replay holds it once the history is delivered
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
        catchupB.pauseSubscription(subscriptionId);
        releaseB.countDown();
        await("the replay is held by the pause").atMost(10, SECONDS).until(() -> childOf(catchupB).hasParkedReplay(subscriptionId));
        liveOfB.failTheNextResumes(1);

        // When the subscription is resumed at the present
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        Throwable failure = catchThrowable(() -> catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present)));
        append("afterTheFailedCall", 4);

        // Then what the live delegate threw reaches the caller
        assertThat(failure).as("the resume at a position").isInstanceOf(IllegalStateException.class).hasMessageContaining(FailingLiveModel.UNAVAILABLE);
        // And the subscription is still paused, with the replay held
        assertThat(catchupB.isPaused(subscriptionId)).as("paused after the failed resume at a position").isTrue();
        assertThat(childOf(catchupB).hasParkedReplay(subscriptionId)).as("the replay is still held after the failed resume at a position").isTrue();
        await("nothing written after the failed call reaches B while it is paused").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterTheFailedCall"));
        // And a later resume runs the replay, so every event arrives
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers the history and what was written after the failed call").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterTheFailedCall"));
        assertThat(catchupB.isPaused(subscriptionId)).as("paused after the resume").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the resume").isTrue();
    }

    @Test
    void a_replay_put_back_after_a_failed_resume_at_a_position_through_the_dispatcher_does_not_deliver_the_event_that_was_in_the_action_again() throws Exception {
        // Given B's resume replay is in the action, blocked on h2, and the live delegate fails the next resume
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h2", inH2, releaseH2));
        Subscription handleOfTheReplay = catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        liveOfB.failTheNextResumes(1);

        // When the subscription is resumed at the present from another thread, and the action returns, so the live delegate fails
        CompletableFuture<Subscription> repositioning = resumeAtThePresentFromAnotherThread();
        releaseH2.countDown();
        Throwable failure = failureOf(repositioning);

        // Then the replay is put back and hands over, and delivers the rest of the history
        assertThat(failure).as("the resume at a position").isInstanceOf(IllegalStateException.class).hasMessageContaining(FailingLiveModel.UNAVAILABLE);
        assertThat(handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
        await("B delivers the rest of the history").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h3", "afterBRegistered"));
        // And h2 was delivered once, since the held replay stores the position of the event in its action, and the replay put back after the failed live resume starts after it
        assertThat(receivedByB).as("what B received").containsOnlyOnce("h2");
    }

    @Test
    void a_resume_at_a_position_through_the_dispatcher_whose_live_delegate_throws_an_error_after_waiting_for_the_action_of_the_resume_replay_rethrows_it_and_leaves_the_replay_running() throws Exception {
        // Given B's resume replay is in the action, blocked on h2, and the live delegate throws an error on the next resume
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h2", inH2, releaseH2));
        Subscription handleOfTheReplay = catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        AssertionError error = new AssertionError("The live delegate broke");
        liveOfB.throwOnTheNextResume(error);

        // When the subscription is resumed at the present from another thread, and the action returns, so the live delegate throws
        CompletableFuture<Subscription> repositioning = resumeAtThePresentFromAnotherThread();
        releaseH2.countDown();
        Throwable failure = failureOf(repositioning);

        // Then the error reaches the caller
        assertThat(failure).as("the resume at a position").isSameAs(error);
        // And the replay runs as before
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the resume at a position threw an error").isTrue();
        assertThat(childOf(catchupB).hasParkedReplay(subscriptionId)).as("a replay is held after the resume at a position threw an error").isFalse();
        // And it hands over, so the handle of the first resume starts and the rest of the history arrives
        assertThat(handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
        await("B delivers the rest of the history").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h3", "afterBRegistered"));
        assertThat(liveOfB.resumeFailures.get()).as("the resumes of the live delegate that threw").isEqualTo(1);
    }

    @Test
    void a_catch_up_listener_that_fails_when_the_resume_replay_starts_through_the_dispatcher_leaves_the_subscription_paused_and_a_later_resume_runs_the_replay() throws Exception {
        // Given the listener of the subscription fails the first time a catch-up starts
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        FailingCatchupListener listener = new FailingCatchupListener(1);
        catchupB.listenForCatchup(subscriptionId, listener);

        // When the subscription is resumed, so its resume replay starts
        catchThrowable(() -> catchupB.resumeSubscription(subscriptionId));

        // Then nothing runs the replay, so the subscription does not read as running
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the replay failed to start").isFalse();
        // And the failure is logged
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR").isNotEmpty();
        // And a later resume runs the replay, which hands over, so every event arrives
        Subscription resumed = catchupB.resumeSubscription(subscriptionId);
        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(15))).as("the replay handed over").isTrue();
        append("afterTheResume", 4);
        await("B delivers the history and what was written after the resume").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterTheResume"));
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the later resume").isTrue();
        assertThat(listener.starts.get()).as("the catch-ups the listener was told about").isEqualTo(2);
    }

    @Test
    void a_catch_up_listener_that_fails_when_a_replay_is_put_back_after_a_failed_resume_at_a_position_through_the_dispatcher_leaves_the_subscription_paused_and_a_later_resume_runs_the_replay() throws Exception {
        // Given B's resume replay is in the action, blocked on h2, the live delegate fails the next resume, and the listener fails the second time a catch-up starts
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h2", inH2, releaseH2));
        FailingCatchupListener listener = new FailingCatchupListener(2);
        catchupB.listenForCatchup(subscriptionId, listener);
        Subscription handleOfTheReplay = catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        liveOfB.failTheNextResumes(1);

        // When the subscription is resumed at the present from another thread, and the action returns, so the live delegate fails and the replay is put back
        CompletableFuture<Subscription> repositioning = resumeAtThePresentFromAnotherThread();
        releaseH2.countDown();
        Throwable failure = failureOf(repositioning);

        // Then what the live delegate threw reaches the caller
        assertThat(failure).as("the resume at a position").isInstanceOf(IllegalStateException.class).hasMessageContaining(FailingLiveModel.UNAVAILABLE);
        // And nothing runs the replay, so the subscription does not read as running
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the put back replay failed to start").isFalse();
        // And the failure is logged
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR").isNotEmpty();
        // And a later resume runs the replay, which hands over, so the rest of the history arrives
        catchupB.resumeSubscription(subscriptionId);
        assertThat(handleOfTheReplay.waitUntilStarted(Duration.ofSeconds(15))).as("the handle of the replay started").isTrue();
        await("B delivers the rest of the history").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h3", "afterBRegistered"));
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the later resume").isTrue();
    }

    @Test
    void a_catch_up_listener_that_fails_when_a_subscribe_through_the_dispatcher_starts_its_replay_leaves_the_subscription_paused_and_a_later_resume_runs_the_replay() throws Exception {
        // Given a history, and a listener that fails the first time a catch-up starts
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        FailingCatchupListener listener = new FailingCatchupListener(1);
        catchupB.listenForCatchup(subscriptionId, listener);

        // When the subscription is made, so its replay starts
        catchThrowable(() -> catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByB.add(nameOf(event))));

        // Then nothing runs the replay, so the subscription does not read as running
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the replay failed to start").isFalse();
        // And the failure is logged
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR").isNotEmpty();
        // And a later resume runs the replay, which hands over, so every event arrives
        Subscription resumed = catchupB.resumeSubscription(subscriptionId);
        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(15))).as("the replay handed over").isTrue();
        append("afterTheResume", 3);
        await("B delivers the history and what was written after the resume").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h1", "h2", "h3", "afterTheResume"));
        assertThat(catchupB.isRunning(subscriptionId)).as("running after the later resume").isTrue();
    }

    @Test
    void a_pause_through_the_dispatcher_during_a_replay_a_subscribe_started_returns_without_failing_and_holds_back_what_is_written_after_it_until_the_subscription_is_resumed() throws Exception {
        // Given a replay a subscribe started is blocked in the action on h2
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        Subscription subscription = catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), recording("h2", inH2, releaseH2));
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();

        // When the subscription is paused during the replay
        Throwable pauseFailure = catchThrowable(() -> catchupB.pauseSubscription(subscriptionId));
        releaseH2.countDown();

        // Then the pause returned without failing
        assertThat(pauseFailure).as("the pause during the replay").isNull();
        // And the replay delivers the history, and the subscription is paused in the live delegate once the replay has handed over
        ((CatchupSubscription) subscription).delegatedSubscription().get(10, SECONDS);
        await("the live delegate holds the subscription paused").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(liveOfB.isPaused(subscriptionId)).isTrue());
        assertThat(receivedByB).as("the history the replay delivered").contains("h1", "h2", "h3");
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused after the replay handed over").isTrue();
        // And what is written after that is not delivered while it is paused
        append("afterPause", 3);
        await("nothing written after the pause reaches B while it is paused").during(2, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterPause"));
        // And a resume delivers it, with the history
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers the history and what was written after the pause").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h1", "h2", "h3", "afterPause"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the resume").isTrue();
    }

    @Test
    void a_pause_through_the_dispatcher_of_a_replay_a_subscribe_started_that_a_failing_catch_up_listener_holds_is_refused_as_already_paused_and_a_later_resume_runs_the_replay() throws Exception {
        // Given a history, and a subscribe whose replay is held because the listener failed when it started
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        FailingCatchupListener listener = new FailingCatchupListener(1);
        catchupB.listenForCatchup(subscriptionId, listener);
        catchThrowable(() -> catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByB.add(nameOf(event))));
        assertThat(catchupB.isPaused(subscriptionId)).as("the held replay reads as paused").isTrue();

        // When the subscription is paused
        Throwable pauseFailure = catchThrowable(() -> catchupB.pauseSubscription(subscriptionId));

        // Then the pause is refused as the subscription already being paused
        assertThat(pauseFailure).as("the pause of the held replay").isInstanceOf(SubscriptionNotRunningException.class).hasMessageContaining("already paused");
        // And the replay is still held
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is still paused").isTrue();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription does not run").isFalse();
        assertThat(receivedByB).as("nothing is delivered while the replay is held").isEmpty();
        // And a resume, with the listener no longer failing, runs the replay and delivers the history and what is written after it
        Subscription resumed = catchupB.resumeSubscription(subscriptionId);
        assertThat(resumed.waitUntilStarted(Duration.ofSeconds(15))).as("the replay handed over").isTrue();
        append("afterTheResume", 3);
        await("B delivers the history and what was written after the resume").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h1", "h2", "h3", "afterTheResume"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the resume").isTrue();
    }

    @Test
    void a_cancel_through_the_dispatcher_during_a_replay_a_subscribe_started_ends_the_replay_so_nothing_written_after_it_is_delivered_and_the_id_can_be_subscribed_again() throws Exception {
        // Given a replay a subscribe started is blocked in the action on h2
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), recording("h2", inH2, releaseH2));
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();

        // When the subscription is cancelled during the replay, and an event is written after the cancel
        Throwable cancelFailure = catchThrowable(() -> catchupB.cancelSubscription(subscriptionId));
        append("afterCancel", 3);
        releaseH2.countDown();

        // Then the cancel returned without failing
        assertThat(cancelFailure).as("the cancel during the replay").isNull();
        // And the replay does not hand over, so what is written after the cancel is not delivered, and nothing is left running or paused
        await("nothing written after the cancel reaches B").during(2, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterCancel"));
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay does not run").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription does not run").isFalse();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(liveOfB.isRunning(subscriptionId)).as("the live delegate does not run the subscription").isFalse();
        // And the id can be subscribed again, and the new subscription delivers
        CopyOnWriteArrayList<String> receivedByNew = new CopyOnWriteArrayList<>();
        catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByNew.add(nameOf(event)));
        await("the new subscription delivers the history and what was written after the cancel").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByNew).contains("h1", "h2", "h3", "afterCancel"));
    }

    @Test
    void a_cancel_through_the_dispatcher_of_a_replay_a_subscribe_started_that_a_failing_catch_up_listener_holds_ends_the_replay_and_the_id_can_be_subscribed_again() throws Exception {
        // Given a history, and a subscribe whose replay is held because the listener failed when it started
        givenTwoNodes();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        catchupB.listenForCatchup(subscriptionId, new FailingCatchupListener(1));
        catchThrowable(() -> catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByB.add(nameOf(event))));
        assertThat(childOf(catchupB).hasParkedReplay(subscriptionId)).as("the replay is held").isTrue();

        // When the subscription is cancelled
        Throwable cancelFailure = catchThrowable(() -> catchupB.cancelSubscription(subscriptionId));

        // Then the cancel returned without failing
        assertThat(cancelFailure).as("the cancel of the held replay").isNull();
        // And the replay is gone, so nothing runs or is paused
        assertThat(childOf(catchupB).hasParkedReplay(subscriptionId)).as("the replay is still held").isFalse();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription does not run").isFalse();
        assertThat(receivedByB).as("nothing is delivered after the cancel").isEmpty();
        // And the id can be subscribed again, and the new subscription delivers
        CopyOnWriteArrayList<String> receivedByNew = new CopyOnWriteArrayList<>();
        catchupB.subscribe(subscriptionId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByNew.add(nameOf(event)));
        await("the new subscription delivers the history").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByNew).contains("h1", "h2", "h3"));
    }

    @Test
    void a_catch_up_listener_that_fails_when_a_failed_resume_replay_runs_again_through_the_dispatcher_holds_the_replay_and_a_later_resume_runs_it() throws Exception {
        // Given the handover of the resume replay fails once, and the listener fails the second time a catch-up starts, which is when the replay runs again after its backoff
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        FailingCatchupListener listener = new FailingCatchupListener(2);
        catchupB.listenForCatchup(subscriptionId, listener);
        storage.failTheNextHandoverReads(1);

        // When the subscription is resumed
        catchupB.resumeSubscription(subscriptionId);
        await("the listener is told the replay runs again").atMost(10, SECONDS).until(() -> listener.starts.get() >= 2);

        // Then the replay is held, so the subscription reads as paused and not running
        await("the replay is held").atMost(10, SECONDS).untilAsserted(() -> assertThat(catchupB.isPaused(subscriptionId)).isTrue());
        assertThat(childOf(catchupB).hasParkedReplay(subscriptionId)).as("the replay is held").isTrue();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription runs").isFalse();
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay runs").isFalse();
        // And the failure of the listener is logged at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR about the listener").filteredOn(message -> message.contains("catch-up listener")).hasSize(1);
        // And what is written while the replay is held is not delivered
        append("afterHeld", 4);
        await("nothing written while the replay is held reaches B").during(2, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).doesNotContain("afterHeld"));
        // And a resume, with the listener no longer failing, runs the replay and delivers it
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers what was written while the replay was held").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterHeld"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running after the resume").isTrue();
        assertThat(storage.handoverReadFailures.get()).as("the read in the handover failed once").isEqualTo(1);
        assertThat(listener.starts.get()).as("the catch-ups the listener was told about").isEqualTo(3);
    }

    @Test
    void a_pause_through_the_dispatcher_that_comes_after_a_cancel_of_a_resume_replay_that_waits_to_run_again_does_not_hold_the_cancelled_replay_so_a_new_subscribe_for_the_id_runs_and_can_be_paused() throws Exception {
        // Given the storage fails the first read the handover of the resume replay makes, and the replay thread
        // is descheduled right after it has given up the handover lock, so the replay waits to run again
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        GapLock gapLock = new GapLock(storage);
        handoverLocksOf(catchupB).put(subscriptionId, gapLock);
        storage.failTheNextHandoverReads(1);
        gapLock.armed = true;
        catchupB.resumeSubscription(subscriptionId);
        assertThat(gapLock.inGap.await(10, SECONDS)).as("the handover failed and the replay thread gave up the lock").isTrue();

        // When the subscription is cancelled and then paused while the replay waits, and the id is subscribed again
        catchupB.cancelSubscription(subscriptionId);
        catchThrowable(() -> catchupB.pauseSubscription(subscriptionId));
        gapLock.leaveGap.countDown();
        CopyOnWriteArrayList<String> receivedByNew = new CopyOnWriteArrayList<>();
        catchupB.subscribe(subscriptionId, StartAt.now(), event -> receivedByNew.add(nameOf(event)));

        // Then the cancelled replay was not held, so the new subscription is not paused
        assertThat(childOf(catchupB).hasParkedReplay(subscriptionId)).as("the cancelled replay is held").isFalse();
        assertThat(catchupB.isPaused(subscriptionId)).as("the new subscription is paused").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the new subscription runs").isTrue();
        // And it delivers what is written after it started
        append("afterTheSubscribe", 4);
        await("the new subscription delivers what was written after it started").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByNew).contains("afterTheSubscribe"));
        // And a pause of it succeeds and pauses it
        Throwable secondPauseFailure = catchThrowable(() -> catchupB.pauseSubscription(subscriptionId));
        assertThat(secondPauseFailure).as("the pause of the new subscription").isNull();
        assertThat(catchupB.isPaused(subscriptionId)).as("the new subscription is paused after the pause").isTrue();
        assertThat(receivedByB).as("what the cancelled subscription received").doesNotContain("afterTheSubscribe");
    }

    // The subscription is resumed at the present from another thread, which returns once it waits for the action of the replay it holds
    private CompletableFuture<Subscription> resumeAtThePresentFromAnotherThread() {
        Checkpoint present = requireNonNull(durableB.globalCheckpoint());
        AtomicReference<Thread> repositioningThread = new AtomicReference<>();
        CompletableFuture<Subscription> repositioning = CompletableFuture.supplyAsync(() -> {
            repositioningThread.set(Thread.currentThread());
            return catchupB.resumeSubscription(subscriptionId, StartAt.checkpoint(present));
        });
        awaitParkedIn(repositioning, repositioningThread, "awaitTheActions");
        return repositioning;
    }

    @Test
    void a_resume_of_a_stopped_model_that_cannot_get_the_claim_of_the_subscription_says_the_model_was_started_and_leaves_the_subscription_paused() throws Exception {
        // Given a subscription on a stopped model, and another call that holds the claim of the subscription for longer than a call waits
        givenTwoNodes();
        append("h1", 0);
        catchupB.subscribe(subscriptionId, StartAt.subscriptionModelDefault(), event -> receivedByB.add(nameOf(event)));
        await("the live delegate runs the subscription").atMost(10, SECONDS).until(() -> liveOfB.isRunning(subscriptionId));
        catchupB.stop();
        assertThat(liveOfB.isRunning()).as("the live delegate runs before the resume").isFalse();
        holdTheClaimOf(subscriptionId);

        // When the subscription is resumed
        Throwable resume = catchThrowable(() -> catchupB.resumeSubscription(subscriptionId));

        // Then the resume fails with IllegalStateException that says the model was started, not that nothing was changed
        assertThat(resume).as("the resume").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(subscriptionId).hasMessageContaining("started").hasMessageNotContaining("Nothing was changed");
        // And the model is started, with the subscription not resumed
        assertThat(liveOfB.isRunning()).as("the live delegate runs after the resume").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused").isTrue();
        // And it logs the same at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR for the subscription")
                .anyMatch(message -> message.contains("10000 ms") && message.contains("started"));
        // And once the other call returns, a resume resumes the subscription
        releaseTheClaim.countDown();
        catchupB.resumeSubscription(subscriptionId);
        append("afterResume", 4);
        await("B delivers what is written after the resume").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterResume"));
    }

    @Test
    void a_stop_that_cannot_get_the_claim_of_a_subscription_says_the_model_is_stopped_and_a_start_that_resumes_delivers_the_rest_of_the_history() throws Exception {
        // Given B's resume replay is in the action, blocked on h2, and another call holds the claim of the subscription for longer than a call waits
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        holdTheClaimOf(subscriptionId);

        // When the model is stopped
        Throwable stop = catchThrowable(() -> catchupB.stop());

        // Then the stop fails with IllegalStateException that says the model is stopped, not that nothing was changed
        assertThat(stop).as("the stop").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(subscriptionId).hasMessageContaining("stopped").hasMessageNotContaining("Nothing was changed");
        // And the live delegate is stopped
        assertThat(liveOfB.isRunning()).as("the live delegate runs after the stop").isFalse();
        // And it logs the same at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR for the subscription")
                .anyMatch(message -> message.contains("10000 ms") && message.contains("stopped"));
        // And once the other call and the action return, the replay is held having delivered nothing more
        releaseTheClaim.countDown();
        releaseH2.countDown();
        await("the replay is held").atMost(10, SECONDS).until(() -> catchupB.isPaused(subscriptionId));
        assertThat(receivedByB).as("what the action received").containsExactly("h2");
        // And a start that resumes delivers the rest of the history and what is written afterwards, one call at a time
        catchupB.start(true);
        append("afterStart", 4);
        await("B delivers the rest of the history and what was written after the start").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterStart"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_start_that_resumes_and_cannot_get_the_claim_of_a_parked_replay_says_the_model_was_started_and_still_resumes_the_other_subscriptions() throws Exception {
        // Given B's resume replay of one subscription is parked by a stop, with another call holding its claim for longer than a call waits
        CountDownLatch inH2 = new CountDownLatch(1);
        CountDownLatch releaseH2 = new CountDownLatch(1);
        CopyOnWriteArrayList<String> receivedByOther = new CopyOnWriteArrayList<>();
        String other = subscriptionId + "-other";
        givenAnotherNodeStoredAPosition(countingEntries(recording("h2", inH2, releaseH2)));
        catchupB.subscribePaused(other, null, StartAt.subscriptionModelDefault(), event -> receivedByOther.add(nameOf(event)));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(inH2.await(10, SECONDS)).as("B's replay reaches h2").isTrue();
        catchupB.stop();
        assertThat(catchupB.isPaused(subscriptionId)).as("the replay is held").isTrue();
        holdTheClaimOf(subscriptionId);

        // When the model is started with the subscriptions resumed
        Throwable start = catchThrowable(() -> catchupB.start(true));
        append("afterStart", 4);

        // Then the start fails with IllegalStateException that says the model was started, not that nothing was changed
        // And the model is started, with the other subscription resumed
        assertSoftly(softly -> {
            softly.assertThat(start).as("the start").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(subscriptionId).hasMessageContaining("started").hasMessageNotContaining("Nothing was changed");
            softly.assertThat(liveOfB.isRunning()).as("the live delegate runs after the start").isTrue();
            softly.assertThat(liveOfB.isRunning(other)).as("the other subscription runs after the start").isTrue();
        });
        // And the other subscription delivers what is written after the start
        await("the other subscription delivers what is written after the start").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByOther).contains("afterStart"));
        // And the parked replay delivered nothing more, but runs when it is resumed once the other call has returned
        assertThat(receivedByB).as("what the action of the parked replay received").containsExactly("h2");
        releaseTheClaim.countDown();
        releaseH2.countDown();
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers the rest of the history").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        assertThat(maxInAction.get()).as("the most calls of the action in flight at once").isEqualTo(1);
    }

    @Test
    void a_start_that_resumes_and_cannot_get_the_claim_of_a_replay_held_by_a_pause_says_the_replay_was_not_run_again_without_blaming_a_stop() throws Exception {
        // Given B's resume replay is held by a pause during the replay, with no stop, and another call holds its claim for longer than a call waits
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        catchupB.resumeSubscription(subscriptionId);
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();
        catchupB.pauseSubscription(subscriptionId);
        releaseB.countDown();
        await("the replay is held by the pause").atMost(10, SECONDS).until(() -> childOf(catchupB).hasParkedReplay(subscriptionId));
        holdTheClaimOf(subscriptionId);

        // When the model is started with the subscriptions resumed
        Throwable start = catchThrowable(() -> catchupB.start(true));

        // Then the start fails with IllegalStateException that says the replay held for the subscription wasn't run again, and doesn't blame a stop
        assertThat(start).as("the start").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(subscriptionId)
                .hasMessageContaining("The catch-up replay held for the subscription wasn't run again")
                .hasMessageNotContaining("its stop");
        // And the replay runs when it is resumed once the other call has returned
        releaseTheClaim.countDown();
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers the history after the stored position").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
    }

    @Test
    void a_pause_and_a_cancel_that_cannot_get_the_claim_of_a_subscription_say_nothing_was_changed_and_leave_the_subscription_running() throws Exception {
        // Given a running subscription, and another call that holds its claim for longer than a call waits
        givenTwoNodes();
        append("h1", 0);
        catchupB.subscribe(subscriptionId, StartAt.subscriptionModelDefault(), event -> receivedByB.add(nameOf(event)));
        await("the live delegate runs the subscription").atMost(10, SECONDS).until(() -> liveOfB.isRunning(subscriptionId));
        holdTheClaimOf(subscriptionId);

        // When the subscription is paused and cancelled
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> catchupB.pauseSubscription(subscriptionId));
        CompletableFuture<Void> cancelling = CompletableFuture.runAsync(() -> catchupB.cancelSubscription(subscriptionId));

        // Then both fail with IllegalStateException that says nothing was changed
        assertThat(failureOf(pausing, 30)).as("the pause").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(subscriptionId).hasMessageContaining("Nothing was changed");
        assertThat(failureOf(cancelling, 30)).as("the cancel").isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(subscriptionId).hasMessageContaining("Nothing was changed");
        // And they log it at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR for the subscription")
                .hasSize(2).allMatch(message -> message.contains("10000 ms"));
        // And the subscription runs as it did, and delivers what is written afterwards
        assertThat(catchupB.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("paused").isFalse();
        append("afterTheCalls", 4);
        await("B delivers what is written after the calls").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterTheCalls"));
    }

    @Test
    void no_claim_is_left_behind_after_subscriptions_are_made_paused_resumed_and_cancelled_and_after_unknown_ids_are_cancelled_and_paused() throws Exception {
        // Given a history
        givenTwoNodes();
        append("h1", 0);

        // When subscriptions are made, paused, resumed and cancelled, and unknown ids are cancelled and paused
        for (int i = 0; i < 25; i++) {
            String id = subscriptionId + "-dynamic-" + i;
            catchupB.subscribe(id, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> {
            });
            await("the replay of " + id + " has handed over").atMost(10, SECONDS).until(() -> !catchupB.isCatchingUp(id) && liveOfB.isRunning(id));
            catchupB.pauseSubscription(id);
            catchupB.resumeSubscription(id);
            await("the replay of " + id + " has handed over again").atMost(10, SECONDS).until(() -> !catchupB.isCatchingUp(id) && liveOfB.isRunning(id));
            catchupB.cancelSubscription(id);
        }
        for (int i = 0; i < 100; i++) {
            String unknown = subscriptionId + "-unknown-" + i;
            catchupB.cancelSubscription(unknown);
            assertThatThrownBy(() -> catchupB.pauseSubscription(unknown)).as("the pause of an unknown subscription").isInstanceOf(UnknownSubscriptionException.class);
        }

        // Then the calls have left no claim behind
        assertThat(claimsOf(catchupB)).as("the claims left").isEmpty();
    }

    // Holds the claim of the subscription on another thread until releaseTheClaim is counted down
    private void holdTheClaimOf(String id) throws Exception {
        AbstractCatchupSubscriptionModel child = childOf(catchupB);
        CountDownLatch held = new CountDownLatch(1);
        Thread.ofPlatform().daemon().start(() -> {
            try (AbstractCatchupSubscriptionModel.Claim ignored = child.claim(id)) {
                held.countDown();
                awaitUninterrupted(releaseTheClaim);
            }
        });
        assertThat(held.await(5, SECONDS)).as("the other call holds the claim of " + id).isTrue();
    }

    private static AbstractCatchupSubscriptionModel childOf(CatchupSubscriptionModel dispatcher) throws Exception {
        Field child = CatchupSubscriptionModel.class.getDeclaredField("streamCatchupSubscriptionModel");
        child.setAccessible(true);
        return (AbstractCatchupSubscriptionModel) child.get(dispatcher);
    }

    // The claims the children of the dispatcher share, which hold an entry for each subscription some call holds or waits for
    private static Map<?, ?> claimsOf(CatchupSubscriptionModel dispatcher) throws Exception {
        Field claims = AbstractCatchupSubscriptionModel.class.getDeclaredField("lifecycleClaims");
        claims.setAccessible(true);
        return (Map<?, ?>) claims.get(childOf(dispatcher));
    }

    // B holds the subscription paused with nothing stored. A replays from the start and stalls in its second event,
    // with the position after the first stored. An event is written after B registered.
    private void givenAnotherNodeStoredAPosition(Consumer<CloudEvent> actionOfB) throws Exception {
        givenTwoNodes();
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), actionOfB);
        catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
        assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("what A's replay stored is a global checkpoint").isTrue();
        append("afterBRegistered", 3);
    }

    // As above, over a live delegate of B made from the durable one
    private void givenAnotherNodeStoredAPosition(Consumer<CloudEvent> actionOfB, Function<DurableSubscriptionModel, CheckpointAwareSubscriptionModel> liveDelegateOfB) throws Exception {
        givenTwoNodes(liveDelegateOfB);
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), actionOfB);
        catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
        assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("what A's replay stored is a global checkpoint").isTrue();
        append("afterBRegistered", 3);
    }

    private void givenTwoNodes(Function<DurableSubscriptionModel, CheckpointAwareSubscriptionModel> liveDelegateOfB) {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);
        durableB = new DurableSubscriptionModel(liveOfB, storage);
        catchupB = new CatchupSubscriptionModel(liveDelegateOfB.apply(durableB), readerOfB, config);
    }

    private void givenTwoNodes() {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchupA = new CatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);
        durableB = new DurableSubscriptionModel(liveOfB, storage);
        catchupB = new CatchupSubscriptionModel(durableB, readerOfB, config);
    }

    private Consumer<CloudEvent> countingEntries(Consumer<CloudEvent> action) {
        return event -> {
            maxInAction.accumulateAndGet(inAction.incrementAndGet(), Math::max);
            try {
                action.accept(event);
            } finally {
                inAction.decrementAndGet();
            }
        };
    }

    private Consumer<CloudEvent> recording(String stallOn, CountDownLatch stalled, CountDownLatch release) {
        return event -> {
            String name = nameOf(event);
            receivedByB.add(name);
            if (name.equals(stallOn) && stalled.getCount() > 0) {
                stalled.countDown();
                awaitUninterrupted(release);
            }
        };
    }

    private static void awaitHandover(CompletableFuture<Subscription> resumed) throws Exception {
        ((CatchupSubscription) resumed.get(10, SECONDS)).delegatedSubscription().get(10, SECONDS);
    }

    // The handover locks the children of the dispatcher share
    @SuppressWarnings("unchecked")
    private static ConcurrentMap<String, ReentrantLock> handoverLocksOf(CatchupSubscriptionModel dispatcher) throws Exception {
        Field child = CatchupSubscriptionModel.class.getDeclaredField("streamCatchupSubscriptionModel");
        child.setAccessible(true);
        Field locks = AbstractCatchupSubscriptionModel.class.getDeclaredField("handoverLocks");
        locks.setAccessible(true);
        return (ConcurrentMap<String, ReentrantLock>) locks.get(child.get(dispatcher));
    }

    // Waits until the call has returned or its thread waits inside the method, as a call does while another call or an action holds it up
    private static void awaitParkedIn(CompletableFuture<?> call, AtomicReference<Thread> callingThread, String method) {
        await("the call waits inside " + method).atMost(10, SECONDS).until(() -> {
            Thread thread = callingThread.get();
            if (call.isDone()) {
                return true;
            }
            if (thread == null || (thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TIMED_WAITING)) {
                return false;
            }
            return Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals(method));
        });
    }

    // A replay thread sleeps while it waits to run again
    private static void awaitAsleep(Thread thread) {
        await("the replay thread waits to run again").atMost(10, SECONDS).until(() -> thread.getState() == Thread.State.TIMED_WAITING);
    }

    private static void awaitEnded(Thread thread) {
        await("the replay thread ends").atMost(10, SECONDS).until(() -> !thread.isAlive());
    }

    private static void waitUpTo(CompletableFuture<?> future, int seconds) {
        try {
            future.get(seconds, SECONDS);
        } catch (TimeoutException | ExecutionException ignored) {
            // Looked at by failureOf, once the future has had its time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // What the future failed with, or null when it completed normally
    private static @Nullable Throwable failureOf(CompletableFuture<?> future) throws InterruptedException, TimeoutException {
        return failureOf(future, 10);
    }

    private static @Nullable Throwable failureOf(CompletableFuture<?> future, int seconds) throws InterruptedException, TimeoutException {
        try {
            future.get(seconds, SECONDS);
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static void awaitUninterrupted(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitUpTo(CountDownLatch latch, int seconds) {
        try {
            latch.await(seconds, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private SpringMongoSubscriptionModel springModel() {
        return new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, TimeRepresentation.RFC_3339_STRING);
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    private void append(String name, int secondsAfterStart) {
        DomainEvent event = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0, secondsAfterStart), "name", name);
        eventStore.write(UUID.randomUUID().toString(), cloudEventConverter.toCloudEvents(List.of(event)));
    }

    /**
     * A wrapper of the model it is made over, whose first cancel throws without reaching that model, so the model still
     * holds the subscription. Every other call, and every later cancel, goes through.
     */
    private static final class CancelFailingModel implements CheckpointAwareSubscriptionModel, SubscriptionModelWrapper {
        static final String UNAVAILABLE = "The live delegate cannot cancel";
        private final CheckpointAwareSubscriptionModel delegate;
        private final AtomicBoolean failed = new AtomicBoolean();

        CancelFailingModel(CheckpointAwareSubscriptionModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            if (failed.compareAndSet(false, true)) {
                throw new IllegalStateException(UNAVAILABLE);
            }
            delegate.cancelSubscription(subscriptionId);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribePaused(subscriptionId, filter, startAt, action);
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            delegate.start(resumeSubscriptionsAutomatically);
        }

        @Override
        public boolean isRunning() {
            return delegate.isRunning();
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return delegate.isRunning(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return delegate.isPaused(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return delegate.resumeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            delegate.pauseSubscription(subscriptionId);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return delegate.globalCheckpoint();
        }

        @Override
        public boolean canResumeFrom(Checkpoint checkpoint) {
            return delegate.canResumeFrom(checkpoint);
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }

    /**
     * The live delegate as it is, except that the first question whether a subscription is paused fails on a chosen
     * thread, and that a chosen number of resumes fail.
     */
    private static final class FailingLiveModel extends SpringMongoSubscriptionModel {
        static final String UNAVAILABLE = "The live delegate is unavailable";
        private volatile @Nullable Thread failIsPausedOn;
        private final AtomicInteger resumeFailuresLeft = new AtomicInteger();
        private volatile @Nullable Error resumeError;
        final AtomicInteger resumeFailures = new AtomicInteger();

        void failTheNextResumes(int times) {
            resumeFailuresLeft.set(times);
        }

        void throwOnTheNextResume(Error error) {
            resumeError = error;
        }

        private void failIfAsked() {
            Error error = resumeError;
            if (error != null) {
                resumeError = null;
                resumeFailures.incrementAndGet();
                throw error;
            }
            if (resumeFailuresLeft.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
                resumeFailures.incrementAndGet();
                throw new IllegalStateException(UNAVAILABLE);
            }
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            failIfAsked();
            return super.resumeSubscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            failIfAsked();
            return super.resumeSubscription(subscriptionId, startAt);
        }

        FailingLiveModel(MongoTemplate mongoTemplate, String eventCollection) {
            super(mongoTemplate, eventCollection, TimeRepresentation.RFC_3339_STRING);
        }

        void failTheNextIsPausedOn(Thread thread) {
            failIsPausedOn = thread;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            if (Thread.currentThread() == failIsPausedOn) {
                failIsPausedOn = null;
                throw new IllegalStateException(UNAVAILABLE);
            }
            return super.isPaused(subscriptionId);
        }
    }

    /**
     * Reads the event store as it is, except that the history read of a replay fails a set number of times.
     */
    private static final class FlakyReader implements EventStoreQueries, PositionOrderedReader {
        private final SpringMongoEventStore delegate;
        private final AtomicInteger failuresLeft = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        volatile @Nullable Thread lastFailedOn;

        FlakyReader(SpringMongoEventStore delegate) {
            this.delegate = delegate;
        }

        void failTheNext(int times) {
            failuresLeft.set(times);
        }

        @Override
        public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            if (Thread.currentThread().getName().startsWith(REPLAY_THREAD_PREFIX) && failuresLeft.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
                failures.incrementAndGet();
                lastFailedOn = Thread.currentThread();
                throw new IllegalStateException("The event store is unavailable");
            }
            return delegate.readInPositionOrder(filter, range);
        }

        @Override
        public long currentPosition() {
            return delegate.currentPosition();
        }

        @Override
        public boolean writesPosition() {
            return delegate.writesPosition();
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return delegate.query(filter, skip, limit, sortBy);
        }

        @Override
        public long count(Filter filter) {
            return delegate.count(filter);
        }

        @Override
        public boolean exists(Filter filter) {
            return delegate.exists(filter);
        }
    }

    /**
     * Fails the chosen time it is told a catch-up started, and counts how many times it is told.
     */
    private static final class FailingCatchupListener implements CatchupListener {
        private final int failOnStart;
        final AtomicInteger starts = new AtomicInteger();

        FailingCatchupListener(int failOnStart) {
            this.failOnStart = failOnStart;
        }

        @Override
        public void catchupStarted(Object episode) {
            if (starts.incrementAndGet() == failOnStart) {
                throw new IllegalStateException("The listener failed");
            }
        }

        @Override
        public void historyRead(Object episode) {
        }
    }

    /**
     * Keeps what the catch-up models log at ERROR.
     */
    private static final class ErrorLog extends AppenderBase<ILoggingEvent> {
        private final CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            if (event.getLevel() == Level.ERROR) {
                messages.add(event.getFormattedMessage());
            }
        }

        List<String> messagesAbout(String subscriptionId) {
            return messages.stream().filter(message -> message.contains(subscriptionId)).toList();
        }
    }

    /**
     * The handover lock of an id, except that the replay thread waits once it has given the lock up for good after its
     * handover failed, which stands in for a scheduler that deschedules it there.
     */
    private static final class GapLock extends ReentrantLock {
        private final BlockingStorage storage;
        volatile boolean armed = false;
        final CountDownLatch inGap = new CountDownLatch(1);
        final CountDownLatch leaveGap = new CountDownLatch(1);

        GapLock(BlockingStorage storage) {
            this.storage = storage;
        }

        @Override
        public void unlock() {
            super.unlock();
            if (armed && !isHeldByCurrentThread() && Thread.currentThread().getName().startsWith(REPLAY_THREAD_PREFIX) && storage.handoverReadFailures.get() > 0) {
                armed = false;
                inGap.countDown();
                awaitUpTo(leaveGap, 10);
            }
        }
    }

    /**
     * The handover lock of an id, except that a thread of a chosen name waits once it has given the lock up, which
     * stands in for a scheduler that deschedules it there.
     */
    private static final class PausingLock extends ReentrantLock {
        private final String threadName;
        volatile boolean armed = false;
        final CountDownLatch paused = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);

        PausingLock(String threadName) {
            this.threadName = threadName;
        }

        @Override
        public void unlock() {
            super.unlock();
            if (armed && !isHeldByCurrentThread() && Thread.currentThread().getName().equals(threadName)) {
                armed = false;
                paused.countDown();
                awaitUpTo(resume, 30);
            }
        }
    }

    /**
     * Reads the storage as it is, except that a read on a chosen thread either waits to be released or is held back
     * until something else has happened.
     */
    private static final class BlockingStorage implements CheckpointStorage {
        private final CheckpointStorage delegate;
        private volatile Predicate<Thread> blockOn = thread -> false;
        private volatile @Nullable CountDownLatch holdUntil;
        final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger handoverReadFailuresLeft = new AtomicInteger();
        final AtomicInteger handoverReadFailures = new AtomicInteger();

        BlockingStorage(CheckpointStorage delegate) {
            this.delegate = delegate;
        }

        // A read on such a thread waits until unblock()
        void blockReadsOn(Predicate<Thread> predicate) {
            blockOn = predicate;
        }

        // A read on such a thread waits until the latch is counted down, but no longer than a second
        void holdReadsOn(Predicate<Thread> predicate, CountDownLatch latch) {
            holdUntil = latch;
            blockOn = predicate;
        }

        void unblock() {
            blockOn = thread -> false;
            release.countDown();
        }

        // The next reads made while a resume replay hands over to the live delegate throw
        void failTheNextHandoverReads(int times) {
            handoverReadFailuresLeft.set(times);
        }

        private static boolean handsOverToTheLiveDelegate() {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("resumeTheLiveDelegate")));
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            if (handoverReadFailuresLeft.get() > 0 && handsOverToTheLiveDelegate() && handoverReadFailuresLeft.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
                handoverReadFailures.incrementAndGet();
                throw new IllegalStateException("The storage is unavailable");
            }
            if (blockOn.test(Thread.currentThread())) {
                CountDownLatch until = holdUntil;
                if (until != null) {
                    awaitUpTo(until, 1);
                } else {
                    blocked.countDown();
                    awaitUninterrupted(release);
                }
            }
            return delegate.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public boolean evaluatesWriteConditionsFor(String subscriptionId) {
            return delegate.evaluatesWriteConditionsFor(subscriptionId);
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            return delegate.resolveFirstCheckpointRace(subscriptionId, candidate);
        }
    }
}
