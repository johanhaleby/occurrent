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
import io.cloudevents.core.builder.CloudEventBuilder;
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
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
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

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A subscription held paused with nothing stored, whose id another catch-up instance has since stored a position for,
 * replays from that position when it is resumed, or when the model it is made through is started again. Two
 * {@link StreamCatchupSubscriptionModel} instances over one event store and one checkpoint storage stand in for two
 * nodes.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupResumeReplayMongoTest {

    /*
     * The state model these tests check. A replay a resume started has one state per subscription id, and runAgainAfter
     * moves it after a failure, under the handover lock of the id.
     *
     * LIVE          no resume replay, the live delegate holds the subscription, running or paused
     * REPLAYING     the replay reads history, so isCatchingUp and isRunning, and isPaused only once a pause was asked
     * HANDING_OVER  the replay stays current under the handover lock until the live delegate has resumed, isCatchingUp
     *               is false and lifecycle calls wait for the lock
     * BACKING_OFF   after a failure the replay stays current and its thread sleeps in slices of at most 100 ms,
     *               isCatchingUp and isRunning, not isPaused
     * HELD          the replay is parked and isPaused, it runs again only on resumeSubscription(id) or start(true)
     *
     * pauseSubscription
     *   LIVE          the live delegate pauses
     *   REPLAYING     the pause is recorded and applied at handover, a second one changes nothing
     *   BACKING_OFF   goes to HELD
     *   HELD          the live delegate throws SubscriptionNotRunningException
     * resumeSubscription(id)
     *   LIVE          replays first when paused and a catch-up position is stored, else the live delegate resumes
     *   REPLAYING     returns the running handle and drops a pause that was asked
     *   BACKING_OFF   returns the running handle
     *   HELD          goes to REPLAYING
     * resumeSubscription(id, startAt) on the dispatcher
     *   LIVE          the live delegate repositions
     *   other states  the replay ends, then the live delegate repositions
     * cancelSubscription
     *   every state   the replay is gone
     * stop()
     *   LIVE          the live delegate stops
     *   REPLAYING     goes to HELD, also when a pause was asked
     *   BACKING_OFF   goes to HELD
     *   HELD          stays HELD
     * start(false)
     *   LIVE          the live delegate starts
     *   REPLAYING     unchanged, also when a pause was asked
     *   BACKING_OFF   unchanged
     *   HELD          stays HELD
     * start(true)
     *   LIVE          the live delegate starts, then each paused id resumes, and one that runs by then counts as resumed
     *   REPLAYING     unchanged, but a pause that was asked is dropped as resumeSubscription(id) drops it
     *   BACKING_OFF   unchanged
     *   HELD          goes to REPLAYING
     * shutdown
     *   every state   the replay ends silently
     * replay or handover fails, logged at ERROR
     *   REPLAYING     goes to BACKING_OFF, or to HELD when stopped or a pause was asked
     * backoff timer wakes, decided under the lock
     *   BACKING_OFF   goes to REPLAYING, or to HELD when stopped or paused, or is gone when cancelled, repositioned or shut down
     * handover finds the live delegate already running the subscription (SubscriptionAlreadyRunningException)
     *   REPLAYING     the replay ends with no retry, logged at ERROR
     * subscribe again with the id of a subscription made through the model that the live delegate holds
     *   every state   DuplicateSubscriptionIdException, and the subscription and its replays are untouched
     *
     * No event is lost, though one can arrive twice. A subscription never stalls silently, so it is not paused with
     * no pending retry unless the caller asked, and every failure is logged at ERROR. A pause or a stop wins over a
     * pending retry, checked under the lock when the backoff ends.
     */

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
    private StreamCatchupSubscriptionModel catchupA;
    private StreamCatchupSubscriptionModel catchupB;
    private DurableSubscriptionModel durableB;
    private BlockingStorage storage;
    private FlakyReader readerOfB;
    private ScriptedLiveModel liveOfB;
    private final ErrorLog errorLog = new ErrorLog();
    private final Logger modelLogger = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
    private final CountDownLatch releaseA = new CountDownLatch(1);
    private final String subscriptionId = UUID.randomUUID().toString();
    private final CopyOnWriteArrayList<String> receivedByB = new CopyOnWriteArrayList<>();

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
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        storage = new BlockingStorage(new SpringMongoCheckpointStorage(mongoTemplate, "storage-" + UUID.randomUUID()));
        readerOfB = new FlakyReader(eventStore);
        liveOfB = new ScriptedLiveModel(mongoTemplate, eventCollectionName);
        errorLog.start();
        modelLogger.addAppender(errorLog);
    }

    @AfterEach
    void shutdown() {
        modelLogger.detachAppender(errorLog);
        errorLog.stop();
        liveOfB.releaseTheListing();
        storage.unblock();
        releaseA.countDown();
        if (catchupA != null) {
            catchupA.shutdown();
        }
        if (catchupB != null) {
            catchupB.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_pause_that_comes_while_the_resume_replay_hands_over_leaves_the_subscription_paused() throws Exception {
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
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> catchupB.pauseSubscription(subscriptionId));
        waitUpTo(pausing, 1);
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
    void start_with_resume_replays_from_the_position_another_catch_up_stored_as_a_resume_does() throws Exception {
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
    void a_resume_replay_that_fails_is_run_again_until_it_has_delivered_the_history_and_the_subscription_runs() throws Exception {
        // Given the event store fails the first read of the resume replay
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(1);

        // When B is resumed
        catchupB.resumeSubscription(subscriptionId);

        // Then the replay runs again and delivers the history after the stored position
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        assertThat(readerOfB.failures.get()).as("the read failed once").isEqualTo(1);
        // And goes live
        append("afterRetry", 4);
        await("B delivers what is written after the replay").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterRetry"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
    }

    @Test
    void cancelling_the_subscription_while_a_failed_resume_replay_waits_to_run_again_stops_the_retries() throws Exception {
        // Given the event store fails every read of the resume replay
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        await("the replay is run again after it failed").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(3));

        // When the subscription is cancelled
        catchupB.cancelSubscription(subscriptionId);
        // An attempt that was reading when the cancel came can still fail once
        await().pollDelay(500, MILLISECONDS).until(() -> true);
        int failuresAtCancel = readerOfB.failures.get();

        // Then no attempt runs again, and nothing is delivered
        await("no replay is run after the cancel").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isEqualTo(failuresAtCancel));
        assertThat(receivedByB).as("nothing is delivered after the cancel").isEmpty();
    }

    @Test
    void two_resumes_at_the_same_time_never_run_the_action_concurrently() throws Exception {
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
    void a_resume_over_a_live_delegate_that_cannot_be_repositioned_is_refused_naming_that_delegate() throws Exception {
        // Given B's live delegate has no way to be resumed at a position
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)), DelegatingModel::new);

        // When B is resumed from the position another node stored, then the resume is refused
        assertThatThrownBy(() -> catchupB.resumeSubscription(subscriptionId))
                .as("the resume of a subscription whose live delegate cannot be repositioned")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(DelegatingModel.class.getSimpleName());
    }

    @Test
    void a_cancel_that_comes_while_the_subscription_is_being_made_leaves_nothing_that_replays_for_a_later_subscription_with_the_same_id() throws Exception {
        // Given a history, and a catch-up model whose live delegate holds back the first subscribePaused
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        durableB = new DurableSubscriptionModel(springModel(), storage);
        HoldingModel holdingModel = new HoldingModel(durableB);
        catchupB = new StreamCatchupSubscriptionModel(holdingModel, eventStore, config);
        CopyOnWriteArrayList<String> receivedByTheCancelledAction = new CopyOnWriteArrayList<>();
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        Thread subscribing = Thread.ofPlatform().name("subscribing").start(() ->
                catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), event -> receivedByTheCancelledAction.add(nameOf(event))));
        assertThat(holdingModel.enteredSubscribePaused.await(10, SECONDS)).as("the live delegate is asked to hold the subscription").isTrue();

        // When the subscription is cancelled while that is still running, off the test thread since the cancel may wait for it
        CompletableFuture<Void> cancelling = CompletableFuture.runAsync(() -> catchupB.cancelSubscription(subscriptionId));
        waitUpTo(cancelling, 1);
        holdingModel.releaseSubscribePaused.countDown();
        subscribing.join(10_000);
        assertThat(failureOf(cancelling)).as("the cancel returned without throwing").isNull();

        // And a new subscription with that id is made on the live delegate itself, held paused, with a position stored
        CopyOnWriteArrayList<String> receivedByTheNewAction = new CopyOnWriteArrayList<>();
        durableB.cancelSubscription(subscriptionId);
        durableB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), event -> receivedByTheNewAction.add(nameOf(event)));
        storage.save(subscriptionId, GlobalCheckpoint.of(1));

        // And it is resumed through the catch-up model
        catchupB.resumeSubscription(subscriptionId);

        // Then the action of the cancelled subscription receives nothing
        await("the cancelled subscription's action receives nothing").during(2, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByTheCancelledAction).isEmpty());
    }

    @Test
    void a_resume_from_a_time_another_catch_up_stored_delivers_an_event_with_an_earlier_time_written_while_the_subscription_was_paused() throws Exception {
        // Given B holds the subscription paused, with nothing stored
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchupA = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);
        durableB = new DurableSubscriptionModel(springModel(), storage);
        catchupB = new StreamCatchupSubscriptionModel(durableB, eventStore, config);
        appendAt("h1", OffsetDateTime.parse("2026-01-01T00:00:00Z"));
        appendAt("h2", OffsetDateTime.parse("2026-01-01T00:00:01Z"));
        appendAt("h3", OffsetDateTime.parse("2026-01-01T00:00:02Z"));
        catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), event -> receivedByB.add(nameOf(event)));
        // And A replays by time and stalls in its second event, with the time of the first stored
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        catchupA.subscribe(subscriptionId, StartAtTime.offsetDateTime(OffsetDateTime.parse("2025-12-31T00:00:00Z")), event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        });
        assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
        assertThat(CatchupTimeCheckpoint.isCatchupTimeCheckpoint(requireNonNull(storage.read(subscriptionId)))).as("what A's replay stored is a catch-up time checkpoint").isTrue();
        // And an event is written that has a time earlier than the one stored, after the live start A read
        appendAt("late", OffsetDateTime.parse("2025-12-31T12:00:00Z"));

        // When B is resumed, and the replay has handed over to the live subscription
        CatchupSubscription resumed = (CatchupSubscription) catchupB.resumeSubscription(subscriptionId);
        resumed.delegatedSubscription().get(10, SECONDS);
        appendAt("afterResume", OffsetDateTime.parse("2026-01-01T00:00:04Z"));

        // Then everything written while the subscription was paused is delivered, also what has the earlier time
        await("B delivers what is written after the resume").atMost(10, SECONDS).until(() -> receivedByB.contains("afterResume"));
        assertThat(receivedByB).as("what B received").contains("late", "h2", "h3", "afterResume");
    }

    @Test
    void a_resume_replay_whose_handover_fails_reading_the_storage_is_run_again_and_the_subscription_goes_live() throws Exception {
        // Given the storage fails the first read the handover of the resume replay makes
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        storage.failTheNextHandoverReads(1);

        // When B is resumed
        catchupB.resumeSubscription(subscriptionId);

        // Then the history after the stored position is delivered
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3"));
        // And the handover is tried again, so the subscription goes live and delivers what was written after the history it replayed
        append("afterRetry", 4);
        await("B delivers what is written after the handover failed").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterBRegistered", "afterRetry"));
        assertThat(storage.handoverReadFailures.get()).as("the read in the handover failed once").isEqualTo(1);
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        // And the failure is logged at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR").isNotEmpty();
    }

    @Test
    void a_resume_replay_whose_live_delegate_fails_to_resume_is_run_again_and_the_subscription_goes_live() throws Exception {
        // Given the live delegate fails the first resume
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        liveOfB.failTheNextResumes(1);

        // When B is resumed
        catchupB.resumeSubscription(subscriptionId);

        // Then the history after the stored position is delivered
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3"));
        // And the handover is tried again, so the subscription goes live and delivers what was written after the history it replayed
        append("afterRetry", 4);
        await("B delivers what is written after the resume failed").atMost(15, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterBRegistered", "afterRetry"));
        assertThat(liveOfB.resumeFailures.get()).as("the resume of the live delegate failed once").isEqualTo(1);
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        // And the failure is logged at ERROR
        assertThat(errorLog.messagesAbout(subscriptionId)).as("what was logged at ERROR").isNotEmpty();
    }

    @Test
    void a_pause_while_a_failed_resume_replay_waits_to_run_again_holds_it_until_the_subscription_is_resumed() throws Exception {
        // Given the event store fails every read of the resume replay, so it waits to run again
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        await("the replay is run again after it failed").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(3));

        // When the subscription is paused, and the event store would serve a replay that ran now
        assertThatCode(() -> catchupB.pauseSubscription(subscriptionId)).as("the pause").doesNotThrowAnyException();
        readerOfB.failTheNext(0);

        // Then the subscription is paused, and the replay does not run again
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused after the pause returned").isTrue();
        await("nothing is delivered while the subscription is paused").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).isEmpty());
        assertThat(catchupB.isCatchingUp(subscriptionId)).as("the replay is not running").isFalse();

        // And a later resume delivers the history
        catchupB.resumeSubscription(subscriptionId);
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the resume").isFalse();
    }

    @Test
    void a_stop_and_a_start_without_resuming_while_a_failed_resume_replay_waits_to_run_again_leave_it_held_until_a_start_that_resumes() throws Exception {
        // Given the event store fails every read of the resume replay, so it waits to run again
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        await("the replay is run again after it failed").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(3));

        // When B is stopped and started again without resuming, and the event store would serve a replay that ran now
        catchupB.stop();
        catchupB.start(false);
        readerOfB.failTheNext(0);

        // Then the replay does not run, and the subscription is paused
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is paused after the start").isTrue();
        await("nothing is delivered while the replay is held").during(3, SECONDS).atMost(6, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).isEmpty());

        // And a start that resumes runs it
        catchupB.start(true);
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused after the start that resumes").isFalse();
    }

    @Test
    void a_start_that_fails_listing_the_replays_a_resume_runs_stops_the_model_again_as_a_start_whose_live_delegate_fails_does() throws Exception {
        // Given B is stopped, holds a subscription paused, and its live delegate fails the first time it is asked if one is paused
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        catchupB.stop();
        liveOfB.failTheNextIsPausedOn(Thread.currentThread());

        // When B is started, resuming what it holds paused, then the start fails
        assertThatThrownBy(() -> catchupB.start(true))
                .as("the start")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ScriptedLiveModel.UNAVAILABLE);

        // Then a replay of a subscription made afterwards is held, as in a stopped model
        String laterId = UUID.randomUUID().toString();
        CopyOnWriteArrayList<String> receivedByLater = new CopyOnWriteArrayList<>();
        catchupB.subscribe(laterId, null, StartAt.checkpoint(GlobalCheckpoint.of(0)), event -> receivedByLater.add(nameOf(event)));
        assertThat(catchupB.isPaused(laterId)).as("the subscription made after the failed start is paused").isTrue();
        await("nothing is delivered to the subscription made after the failed start").during(2, SECONDS).atMost(5, SECONDS)
                .untilAsserted(() -> assertThat(receivedByLater).isEmpty());

        // And a start that succeeds runs both replays
        catchupB.start(true);
        await("B delivers the history of both subscriptions").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByLater).contains("h3"));
        assertThat(receivedByB).as("the history after the stored position").contains("h2", "h3", "afterBRegistered");
    }

    @Test
    void a_duplicate_live_subscribe_that_fails_leaves_the_subscription_replaying_from_the_position_another_node_stored_on_resume() throws Exception {
        aFailedDuplicateSubscribeLeavesTheSubscriptionReplayingOnResume(StartAt.subscriptionModelDefault());
    }

    @Test
    void a_duplicate_catch_up_subscribe_that_fails_leaves_the_subscription_replaying_from_the_position_another_node_stored_on_resume() throws Exception {
        aFailedDuplicateSubscribeLeavesTheSubscriptionReplayingOnResume(StartAt.checkpoint(GlobalCheckpoint.of(0)));
    }

    private void aFailedDuplicateSubscribeLeavesTheSubscriptionReplayingOnResume(StartAt duplicateStartAt) throws Exception {
        // Given B runs the subscription live, after the history was written
        givenTwoNodes(durable -> durable);
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        catchupB.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), event -> receivedByB.add(nameOf(event)));

        // When the id is subscribed again, which the live delegate refuses
        CopyOnWriteArrayList<String> receivedByDuplicate = new CopyOnWriteArrayList<>();
        assertThatThrownBy(() -> catchupB.subscribe(subscriptionId, null, duplicateStartAt, event -> receivedByDuplicate.add(nameOf(event))))
                .as("the second subscribe of an id that runs")
                .isInstanceOf(DuplicateSubscriptionIdException.class);
        // And the subscription is paused, and another node stores a position for it
        catchupB.pauseSubscription(subscriptionId);
        anotherNodeStoresAPosition();
        append("afterBRegistered", 3);

        // And B is resumed
        catchupB.resumeSubscription(subscriptionId);

        // Then the history after the stored position is delivered to the first subscription
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        assertThat(receivedByDuplicate).as("what the refused subscription received").isEmpty();
    }

    @Test
    void a_duplicate_subscribe_while_the_resume_replay_runs_is_refused_and_the_replay_still_hands_over() throws Exception {
        // Given B's resume replay has delivered h3 and stays in the action
        CountDownLatch bIsStalled = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        givenAnotherNodeStoredAPosition(recording("h3", bIsStalled, releaseB));
        CatchupSubscription resumed = (CatchupSubscription) catchupB.resumeSubscription(subscriptionId);
        assertThat(bIsStalled.await(10, SECONDS)).as("B's replay reaches h3").isTrue();

        // When the id is subscribed again
        CopyOnWriteArrayList<String> receivedByDuplicate = new CopyOnWriteArrayList<>();
        assertThatThrownBy(() -> catchupB.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), event -> receivedByDuplicate.add(nameOf(event))))
                .as("the second subscribe of an id whose resume replay runs")
                .isInstanceOf(DuplicateSubscriptionIdException.class);

        // Then the replay hands over, and the subscription goes live
        releaseB.countDown();
        resumed.delegatedSubscription().get(10, SECONDS);
        append("afterHandover", 4);
        await("B delivers what is written after the handover").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered", "afterHandover"));
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(receivedByDuplicate).as("what the refused subscription received").isEmpty();
    }

    @Test
    void a_start_that_resumes_does_not_fail_when_the_replay_it_ran_again_hands_over_before_it_resumes_that_subscription() throws Exception {
        // Given a resume replay that failed and was held by a stop
        givenAnotherNodeStoredAPosition(event -> receivedByB.add(nameOf(event)));
        readerOfB.failTheNext(Integer.MAX_VALUE);
        catchupB.resumeSubscription(subscriptionId);
        await("the replay is run again after it failed").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(readerOfB.failures.get()).isGreaterThanOrEqualTo(2));
        catchupB.stop();
        readerOfB.failTheNext(0);
        // And a start that resumes has listed the subscription the live delegate holds paused, and waits
        CompletableFuture<Void> starting = new CompletableFuture<>();
        Thread starter = Thread.ofPlatform().name("start-with-resume").unstarted(() -> {
            try {
                catchupB.start(true);
                starting.complete(null);
            } catch (Throwable e) {
                starting.completeExceptionally(e);
            }
        });
        liveOfB.holdTheListingOn(starter);
        starter.start();
        assertThat(liveOfB.listed.await(10, SECONDS)).as("the start has listed what the live delegate holds paused").isTrue();

        // When the replay the start ran again has handed over
        await("B delivers the history after the stored position").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("h2", "h3", "afterBRegistered"));
        await("the live delegate runs the subscription").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(liveOfB.isRunning(subscriptionId)).isTrue());
        liveOfB.releaseTheListing();

        // Then the start returns without failing
        assertThat(failureOf(starting)).as("the start that resumes").isNull();
        // And the subscription runs
        append("afterStart", 4);
        await("B delivers what is written after the start").atMost(10, SECONDS)
                .untilAsserted(() -> assertThat(receivedByB).contains("afterStart"));
        assertThat(catchupB.isPaused(subscriptionId)).as("the subscription is not paused").isFalse();
        assertThat(catchupB.isRunning(subscriptionId)).as("the subscription is running").isTrue();
    }

    // B holds the subscription paused with nothing stored. A replays from the start and stalls in its second event,
    // with the position after the first stored. An event is written after B registered.
    private void givenAnotherNodeStoredAPosition(Consumer<CloudEvent> actionOfB) throws Exception {
        givenAnotherNodeStoredAPosition(actionOfB, durable -> durable);
    }

    private void givenAnotherNodeStoredAPosition(Consumer<CloudEvent> actionOfB, Function<DurableSubscriptionModel, CheckpointAwareSubscriptionModel> liveDelegateOfB) throws Exception {
        givenTwoNodes(liveDelegateOfB);
        append("h1", 0);
        append("h2", 1);
        append("h3", 2);
        catchupB.subscribePaused(subscriptionId, null, StartAt.subscriptionModelDefault(), actionOfB);
        anotherNodeStoresAPosition();
        append("afterBRegistered", 3);
    }

    private void givenTwoNodes(Function<DurableSubscriptionModel, CheckpointAwareSubscriptionModel> liveDelegateOfB) {
        CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1));
        catchupA = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(springModel(), storage), eventStore, config);
        durableB = new DurableSubscriptionModel(liveOfB, storage);
        catchupB = new StreamCatchupSubscriptionModel(liveDelegateOfB.apply(durableB), readerOfB, config);
    }

    // A replays from the start and stalls in its second event, with the position after the first stored
    private void anotherNodeStoresAPosition() throws Exception {
        AtomicInteger deliveredToA = new AtomicInteger();
        CountDownLatch aIsStalled = new CountDownLatch(1);
        Consumer<CloudEvent> stallingA = event -> {
            if (deliveredToA.incrementAndGet() > 1) {
                aIsStalled.countDown();
                awaitUninterrupted(releaseA);
            }
        };
        catchupA.subscribe(subscriptionId, StartAt.checkpoint(GlobalCheckpoint.of(0)), stallingA);
        assertThat(aIsStalled.await(10, SECONDS)).as("A's replay reaches its second event").isTrue();
        assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(subscriptionId))).as("what A's replay stored is a global checkpoint").isTrue();
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
        try {
            future.get(10, SECONDS);
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

    // The converter gives an event the time it is written, so a test that needs a given time sets it on the cloud event
    private void appendAt(String name, OffsetDateTime time) {
        DomainEvent event = new NameDefined(UUID.randomUUID().toString(), time.toLocalDateTime(), "name", name);
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(event)).stream().map(cloudEvent -> CloudEventBuilder.v1(cloudEvent).withTime(time).build()).toList();
        eventStore.write(UUID.randomUUID().toString(), cloudEvents);
    }

    /**
     * The live delegate as it is, except that it can fail a resume, fail the first question whether a subscription is
     * paused on a chosen thread, or hold the listing of the paused subscriptions of a start after it answered.
     */
    private static final class ScriptedLiveModel extends SpringMongoSubscriptionModel {
        static final String UNAVAILABLE = "The live delegate is unavailable";
        private final AtomicInteger resumeFailuresLeft = new AtomicInteger();
        final AtomicInteger resumeFailures = new AtomicInteger();
        private volatile @Nullable Thread failIsPausedOn;
        private volatile @Nullable Thread holdTheListingOn;
        final CountDownLatch listed = new CountDownLatch(1);
        private final CountDownLatch releaseListing = new CountDownLatch(1);

        ScriptedLiveModel(MongoTemplate mongoTemplate, String eventCollection) {
            super(mongoTemplate, eventCollection, TimeRepresentation.RFC_3339_STRING);
        }

        void failTheNextResumes(int times) {
            resumeFailuresLeft.set(times);
        }

        void failTheNextIsPausedOn(Thread thread) {
            failIsPausedOn = thread;
        }

        void holdTheListingOn(Thread thread) {
            holdTheListingOn = thread;
        }

        void releaseTheListing() {
            holdTheListingOn = null;
            releaseListing.countDown();
        }

        private void failIfAsked() {
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

        @Override
        public boolean isPaused(String subscriptionId) {
            Thread asked = Thread.currentThread();
            if (asked == failIsPausedOn) {
                failIsPausedOn = null;
                throw new IllegalStateException(UNAVAILABLE);
            }
            boolean paused = super.isPaused(subscriptionId);
            if (asked == holdTheListingOn && StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("subscriptionsTheLiveDelegateHoldsPaused")))) {
                holdTheListingOn = null;
                listed.countDown();
                awaitUninterrupted(releaseListing);
            }
            return paused;
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

    /**
     * Reads the event store as it is, except that the history read of a replay fails a set number of times.
     */
    private static final class FlakyReader implements EventStoreQueries, PositionOrderedReader {
        private final SpringMongoEventStore delegate;
        private final AtomicInteger failuresLeft = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();

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
     * Passes everything on to the model it is made over, and says nothing about being a wrapper of it, so no
     * capability of that model can be found through it.
     */
    private static class DelegatingModel implements CheckpointAwareSubscriptionModel {
        final CheckpointAwareSubscriptionModel delegate;

        DelegatingModel(CheckpointAwareSubscriptionModel delegate) {
            this.delegate = delegate;
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
        public void cancelSubscription(String subscriptionId) {
            delegate.cancelSubscription(subscriptionId);
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
    }

    /**
     * A wrapper of the model it is made over, which waits before it holds the first subscription paused.
     */
    private static final class HoldingModel extends DelegatingModel implements SubscriptionModelWrapper {
        final CountDownLatch enteredSubscribePaused = new CountDownLatch(1);
        final CountDownLatch releaseSubscribePaused = new CountDownLatch(1);

        HoldingModel(CheckpointAwareSubscriptionModel delegate) {
            super(delegate);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (enteredSubscribePaused.getCount() > 0) {
                enteredSubscribePaused.countDown();
                awaitUninterrupted(releaseSubscribePaused);
            }
            return super.subscribePaused(subscriptionId, filter, startAt, action);
        }

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }
    }
}
