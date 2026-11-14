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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.invocation.Invocation;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.blocking.StartRefusalReportingSubscriptions.StartRefusalListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A {@link NativeMongoSubscriptionModel} subscribed at the present records where the present is when MongoDB's reply
 * to the operation time question has no {@code operationTime}, by reading its change stream once. These tests stub the
 * reply, and either let that read go to the real replica set or answer it themselves, so a refusal and a read that
 * returns an event can be arranged. The read is the first {@code watch(..)} with an empty pipeline, so every
 * subscription here has a filter, which gives the change stream it opens a pipeline of its own.
 */
@Testcontainers
@Timeout(30)
@DisplayName("NativeMongoSubscriptionModelNoOperationTime")
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelNoOperationTimeTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicInteger repliesWithoutOperationTime = new AtomicInteger();
    // What answers the read that records the present, or nothing to let it go to the real change stream
    private final AtomicReference<Supplier<MongoChangeStreamCursor<ChangeStreamDocument<Document>>>> presentRead = new AtomicReference<>();
    private final AtomicInteger presentReads = new AtomicInteger();
    private final AtomicBoolean historyLost = new AtomicBoolean();
    // Loses the history as the next change stream with the subscription's pipeline opens, rather than when its cursor is read
    private final AtomicBoolean historyLostWhenOpening = new AtomicBoolean();
    // The change streams the model opened with its subscription's pipeline, as spies, so the position each opened at can be read back
    private final List<ChangeStreamIterable<Document>> opened = new CopyOnWriteArrayList<>();
    private final List<CountDownLatch> heldReads = new CopyOnWriteArrayList<>();
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> eventCollection;
    private MongoEventStore mongoEventStore;
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private ThreadPoolExecutor subscriptionExecutor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".noOperationTime");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
        subscriptionExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<>());
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        modelLog.detachAppender(logged);
        logged.stop();
        heldReads.forEach(CountDownLatch::countDown);
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void a_pause_and_resume_after_the_action_of_the_first_event_threw_delivers_that_event_again_when_mongodb_sent_no_operation_time() {
        // Given: a backoff of 2 s between attempts, so the pause comes during the first backoff
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), RetryStrategy.fixed(2000), Duration.ofSeconds(5));
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger failures = new AtomicInteger();
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
            if (failing.get()) {
                failures.incrementAndGet();
                throw new IllegalStateException("expected");
            }
            handled.add(cloudEvent.getId());
        }).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        NameDefined e2 = nameDefined("name2");
        mongoEventStore.write("1", 0, serialize(e1));
        await().atMost(FIVE_SECONDS).until(() -> failures.get() >= 1);

        // When: paused while the retry backs off, then resumed once the listener works again
        subscriptionModel.pauseSubscription(subscriptionId);
        failing.set(false);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e2.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId());
        assertThat(repliesWithoutOperationTime).as("replies without an operation time").hasValueGreaterThan(0);
    }

    @Test
    void a_pause_and_resume_while_the_action_of_the_second_of_two_events_in_one_write_fails_does_not_deliver_the_first_again() {
        // Given: two events written together, so the position recorded for the present is before both
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), RetryStrategy.fixed(2000), Duration.ofSeconds(5));
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger failuresOnE2 = new AtomicInteger();
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        NameDefined e1 = nameDefined("name1");
        NameDefined e2 = nameDefined("name2");
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
            if (cloudEvent.getId().equals(e2.eventId()) && failing.get()) {
                failuresOnE2.incrementAndGet();
                throw new IllegalStateException("expected");
            }
            handled.add(cloudEvent.getId());
        }).waitUntilStarted(Duration.ofSeconds(10));
        List<CloudEvent> both = new ArrayList<>(serialize(e1));
        both.addAll(serialize(e2));
        mongoEventStore.write("1", 0, both);
        await().atMost(FIVE_SECONDS).until(() -> handled.contains(e1.eventId()) && failuresOnE2.get() >= 1);

        // When: paused while the second event's action fails, then resumed once it works again
        subscriptionModel.pauseSubscription(subscriptionId);
        failing.set(false);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e2.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId());
        assertThat(repliesWithoutOperationTime).as("replies without an operation time").hasValueGreaterThan(0);
    }

    @Test
    void an_empty_read_of_the_change_stream_records_its_resume_token_as_the_present_and_an_event_written_while_paused_is_delivered() {
        // Given: a real change stream that has nothing to read, so the resume token is the one the server sent with the empty batch
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        // When
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(repliesWithoutOperationTime).as("replies without an operation time").hasValueGreaterThan(0);
        assertThat(callsTo("startAfter", opened.get(0))).as("positions the first change stream opened after").singleElement()
                .isInstanceOfSatisfying(BsonDocument.class, resumeToken -> assertThat(resumeToken).containsKey("_data"));
        assertThat(callsTo("startAtOperationTime", opened.get(0))).as("operation times the first change stream opened at").isEmpty();
        assertThat(handled).containsExactly(e1.eventId());
    }

    @Test
    void a_read_of_the_change_stream_that_returns_an_event_records_the_commit_time_of_that_event_as_the_present() {
        // Given: an event that the read returns, so the present is its commit time
        NameDefined e0 = nameDefined("name0");
        ChangeStreamDocument<Document> readOfE0;
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> observer = eventCollection.watch().cursor()) {
            mongoEventStore.write("0", 0, serialize(e0));
            AtomicReference<ChangeStreamDocument<Document>> observed = new AtomicReference<>();
            await().atMost(FIVE_SECONDS).until(() -> {
                observed.compareAndSet(null, observer.tryNext());
                return observed.get() != null;
            });
            readOfE0 = observed.get();
        }
        presentRead.set(() -> cursorReading(readOfE0, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();

        // When
        subscriptionModel.subscribe(UUID.randomUUID().toString(), filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(callsTo("startAtOperationTime", opened.get(0))).as("operation times the first change stream opened at").containsExactly(readOfE0.getClusterTime());
        assertThat(handled).containsExactly(e0.eventId(), e1.eventId());
    }

    @Test
    void a_pause_and_resume_right_after_subscribe_skips_no_event_written_before_the_read_that_records_the_present_returns() {
        // Given: a read that returns when released, from a position taken before any event was written, and a later position for any other read
        BsonDocument resumeTokenBeforeTheEvents = realResumeToken();
        AtomicReference<BsonDocument> resumeTokenAfterTheEvents = new AtomicReference<>();
        CountDownLatch released = new CountDownLatch(1);
        heldReads.add(released);
        AtomicInteger reads = new AtomicInteger();
        presentRead.set(() -> reads.getAndIncrement() == 0 ? cursorHeldUntil(released, resumeTokenBeforeTheEvents) : cursorReading(null, resumeTokenAfterTheEvents.get()));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        // When: paused and resumed while the read is held, with two events written and a third after it returns
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));
        subscriptionModel.pauseSubscription(subscriptionId);
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        NameDefined e1 = nameDefined("name1");
        NameDefined e2 = nameDefined("name2");
        NameDefined e3 = nameDefined("name3");
        mongoEventStore.write("1", 0, serialize(e1));
        mongoEventStore.write("2", 0, serialize(e2));
        resumeTokenAfterTheEvents.set(realResumeToken());
        released.countDown();
        resumed.waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("3", 0, serialize(e3));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e3.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId(), e3.eventId());
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
    }

    @Test
    void a_running_model_refuses_a_subscription_at_the_present_when_the_read_of_the_change_stream_returns_no_event_and_no_resume_token() {
        // Given
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        // When
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));

        // Then
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                .hasMessageContaining("MongoDB gave neither an operation time nor a change stream position").hasMessageContaining(subscriptionId);
        await().atMost(FIVE_SECONDS).until(() -> refused.contains(subscriptionId));
        assertThat(refused).containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(opened.size()).as("number of change streams opened for the subscription").isZero();
    }

    @Test
    void a_running_model_refuses_a_subscription_at_the_present_when_the_read_of_the_change_stream_returns_an_event_without_a_commit_time() {
        // Given
        ChangeStreamDocument<Document> withoutCommitTime = mock(ChangeStreamDocument.class);
        presentRead.set(() -> cursorReading(withoutCommitTime, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        String subscriptionId = UUID.randomUUID().toString();

        // When
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });

        // Then
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        await().atMost(FIVE_SECONDS).until(() -> refused.contains(subscriptionId));
        assertThat(refused).containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
    }

    @Test
    void a_stopped_model_refuses_a_subscription_at_the_present_when_the_read_of_the_change_stream_returns_no_event_and_no_resume_token_and_start_throws() {
        // Given
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        subscriptionModel.stop();
        String subscriptionId = UUID.randomUUID().toString();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused until the model starts").isTrue();

        // When
        Throwable thrown = catchThrowable(() -> subscriptionModel.start(true));

        // Then
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                .hasMessageContaining("MongoDB gave neither an operation time nor a change stream position").hasMessageContaining(subscriptionId);
        assertThat(catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)))).isExactlyInstanceOf(UnsupportedStartAtException.class);
        await().atMost(FIVE_SECONDS).until(() -> refused.contains(subscriptionId));
        assertThat(refused).containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
    }

    @Test
    void a_stopped_model_refuses_a_subscription_at_the_present_when_the_read_of_the_change_stream_returns_an_event_without_a_commit_time_and_start_throws() {
        // Given
        ChangeStreamDocument<Document> withoutCommitTime = mock(ChangeStreamDocument.class);
        presentRead.set(() -> cursorReading(withoutCommitTime, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        subscriptionModel.stop();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });

        // When
        Throwable thrown = catchThrowable(() -> subscriptionModel.start(true));

        // Then
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        await().atMost(FIVE_SECONDS).until(() -> refused.contains(subscriptionId));
        assertThat(refused).containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
    }

    @Test
    void resuming_a_subscription_that_has_run_at_the_present_is_held_paused_when_the_read_of_the_change_stream_returns_no_event_and_no_resume_token_and_resuming_it_asks_again() {
        // Given: a subscription that has run, and a server whose change stream gives no position now
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));

        // When
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());

        // Then: held paused, with the handle it returned told, and not refused
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position")
                .hasMessageContaining("It is paused, and resuming it asks MongoDB again.").hasMessageNotContaining("Subscribe it at a checkpoint");
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(refused).as("start refusals told").isEmpty();
        waitAtMostFor(FIVE_SECONDS, () -> !errorsLoggedAbout(subscriptionId).isEmpty());
        assertThat(errorsLoggedAbout(subscriptionId)).as("errors logged").singleElement().satisfies(message -> {
            assertThat(message).contains("Cannot open the change stream of subscription").contains("at the present");
            assertThat(message).contains("It is paused, and resuming it asks MongoDB again.");
        });

        // And when it is resumed once the server gives a position again
        int readsBeforeTheResume = presentReads.get();
        BsonDocument position = realResumeToken();
        presentRead.set(() -> cursorReading(null, position));
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(presentReads.get() - readsBeforeTheResume).as("reads of the change stream that records the present for the resume").isEqualTo(1);
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
        assertThat(refused).as("start refusals told").isEmpty();
    }

    @Test
    void a_dynamic_position_that_answers_a_checkpoint_opens_at_it_and_is_not_refused_when_the_read_of_the_change_stream_gives_no_position() {
        // Given: an event written after the checkpoint, and a server whose change stream gives no position
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();

        // When
        Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), filter(), StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)), cloudEvent -> handled.add(cloudEvent.getId()));
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(thrown).as("thrown by the handle").isNull();
        assertThat(handled).containsExactly(e1.eventId());
        assertThat(callsTo("startAtOperationTime", opened.get(0))).containsExactly(checkpoint.operationTime);
        assertThat(refused).isEmpty();
    }

    @Test
    void a_history_loss_listener_is_asked_about_the_resume_token_the_empty_read_recorded_and_the_subscription_it_takes_over_ends_paused() {
        // Given: a subscription at now whose read of the change stream is empty and gives a resume token, and a listener that takes it over
        BsonDocument resumeToken = realResumeToken();
        presentRead.set(() -> cursorReading(null, resumeToken));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<Checkpoint> askedAbout = new CopyOnWriteArrayList<>();
        AtomicInteger restarts = new AtomicInteger();
        subscriptionModel.addHistoryLossListener(new HistoryLossListener() {
            @Override
            public HistoryLossHandling handleHistoryLoss(String subscriptionId, Checkpoint lostFrom) {
                askedAbout.add(lostFrom);
                return HistoryLossHandling.takeOver(() -> {
                });
            }

            @Override
            public void restartingAfterHistoryLoss(String subscriptionId, Checkpoint restartedFrom, BooleanSupplier stillCurrent) {
                restarts.incrementAndGet();
            }
        });
        String subscriptionId = UUID.randomUUID().toString();

        // When: the history is lost on the first read of the subscription's change stream, before any batch came back
        historyLost.set(true);
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });
        waitAtMostFor(TEN_SECONDS, () -> !askedAbout.isEmpty() && subscriptionModel.isPaused(subscriptionId));

        // Then
        assertThat(askedAbout).as("positions the listener was asked about").containsExactly(new MongoResumeTokenCheckpoint(resumeToken));
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(restarts).as("restarts from now the listener was told about").hasValue(0);
    }

    @Test
    void a_run_that_restarts_from_now_after_its_history_was_lost_is_held_paused_when_the_read_of_the_change_stream_gives_no_position_and_resuming_it_asks_again() {
        // Given: a subscription that has started
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        Subscription started = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));
        started.waitUntilStarted(Duration.ofSeconds(10));

        // When: its history is lost while the server gives no position to restart from
        presentRead.set(() -> cursorReading(null, null));
        historyLost.set(true);
        waitAtMostFor(TEN_SECONDS, () -> subscriptionModel.isPaused(subscriptionId));

        // Then
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(1)))).as("thrown by the handle it started on").isNull();
        assertThat(refused).as("start refusals told").isEmpty();

        // And when it is resumed once the server gives a position again
        presentRead.set(null);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(handled).containsExactly(e1.eventId());
        assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
        assertThat(refused).isEmpty();
    }

    @Test
    void a_resumed_run_that_loses_its_history_as_it_opens_is_held_paused_when_the_read_of_the_change_stream_gives_no_position_and_resuming_it_asks_again() {
        // Given: a paused subscription
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);

        // When: its history is lost as the resumed run opens its change stream, while the server gives no position to restart from
        subscriptionModel.resumeSubscription(subscriptionId);
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > 0 && subscriptionModel.isPaused(subscriptionId));

        // Then
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(refused).as("start refusals told").isEmpty();

        // And when it is resumed once the server gives a position again
        presentRead.set(null);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(handled).containsExactly(e1.eventId());
        assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
        assertThat(refused).isEmpty();
    }

    @Test
    void a_held_subscription_stays_held_paused_when_it_is_resumed_and_the_read_of_the_change_stream_still_gives_no_position() {
        // Given: a subscription held paused after its history was lost while the server gave no position
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        presentRead.set(() -> cursorReading(null, null));
        historyLost.set(true);
        waitAtMostFor(TEN_SECONDS, () -> subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("held paused before the resume").isTrue();
        int readsBeforeTheResume = presentReads.get();

        // When: it is resumed without a position and the server still gives none
        subscriptionModel.resumeSubscription(subscriptionId);
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > readsBeforeTheResume && subscriptionModel.isPaused(subscriptionId));

        // Then
        assertThat(presentReads.get()).as("reads of the change stream that records the present, the resume asking again").isGreaterThan(readsBeforeTheResume);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(refused).as("start refusals told").isEmpty();
    }

    @Test
    void a_subscription_at_a_checkpoint_that_loses_its_history_as_it_first_opens_is_held_paused_when_the_read_of_the_change_stream_gives_no_position() {
        // Given: a checkpoint, and a server whose change stream gives no position
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        // When: its history is lost as it opens for the first time
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.checkpoint(checkpoint), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > 0 && subscriptionModel.isPaused(subscriptionId));
        Throwable thrownByTheHandle = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(1)));
        Throwable thrownWithoutATimeout = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(5), () -> subscription.waitUntilStarted()));

        // Then
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(thrownByTheHandle).as("thrown by the handle it was subscribed on").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(thrownWithoutATimeout).as("thrown by the handle it was subscribed on, waiting with no timeout").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");

        // And when it is resumed once the server gives a position again
        presentRead.set(null);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(handled).containsExactly(e1.eventId());
        assertThat(refused).isEmpty();
    }

    @Test
    void the_handle_of_a_subscription_held_paused_before_it_started_answers_that_it_started_once_a_resume_has_started_it() {
        // Given: a subscription made at a checkpoint, held paused after its history was lost as it first opened, so its handle throws
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        String subscriptionId = UUID.randomUUID().toString();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.checkpoint(checkpoint), cloudEvent -> {
        });
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > 0 && subscriptionModel.isPaused(subscriptionId));
        Throwable thrownWhileHeld = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(1)));
        assertThat(thrownWhileHeld).as("thrown by the handle while the subscription is held").isExactlyInstanceOf(UnsupportedStartAtException.class);

        // When: the server gives a position again and a resume starts it
        presentRead.set(null);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

        // Then: the handle it was subscribed on answers that it started, and does not throw
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(1))).as("handle it was subscribed on answers started").isTrue();
        assertThat(catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(5), () -> subscription.waitUntilStarted()))).as("thrown by the handle it was subscribed on, waiting with no timeout").isNull();
    }

    @Test
    void a_held_subscription_made_at_a_checkpoint_is_held_paused_again_when_it_is_resumed_and_the_read_of_the_change_stream_still_gives_no_position() {
        // Given: a subscription made at a checkpoint, held paused after its history was lost as it first opened
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.checkpoint(checkpoint), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > 0 && subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("held paused before the resume").isTrue();
        int readsBeforeTheResume = presentReads.get();

        // When: it is resumed without a position and the server still gives none
        subscriptionModel.resumeSubscription(subscriptionId);
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > readsBeforeTheResume);
        waitAtMostFor(TEN_SECONDS, () -> subscriptionModel.isPaused(subscriptionId) || refused.contains(subscriptionId));

        // Then
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(presentReads.get() - readsBeforeTheResume).as("reads of the change stream that records the present for the resume, the read at the opening and none by the question").isEqualTo(1);

        // And when it is resumed once the server gives a position again
        presentRead.set(null);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(handled).as("events handled").containsExactly(e1.eventId());
        assertThat(refused).as("start refusals told").isEmpty();
    }

    @Test
    void a_held_subscription_resumed_at_the_present_is_held_paused_again_when_the_read_of_the_change_stream_still_gives_no_position_and_resuming_it_asks_again() {
        // Given: a subscription held paused after its history was lost while the server gave no position
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        presentRead.set(() -> cursorReading(null, null));
        historyLost.set(true);
        waitAtMostFor(TEN_SECONDS, () -> subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("held paused before the resume").isTrue();
        int readsBeforeTheResume = presentReads.get();

        // When: it is resumed at the present and the server still gives no position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());

        // Then: held paused again, with the handle it returned told, and not refused
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position")
                .hasMessageContaining("It is paused, and resuming it asks MongoDB again.").hasMessageNotContaining("Subscribe it at a checkpoint");
        assertThat(presentReads.get() - readsBeforeTheResume).as("reads of the change stream that records the present for the resume").isEqualTo(1);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(refused).as("start refusals told").isEmpty();

        // And when it is resumed once the server gives a position again
        BsonDocument position = realResumeToken();
        presentRead.set(() -> cursorReading(null, position));
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(presentReads.get() - readsBeforeTheResume).as("reads of the change stream that records the present since the first resume").isEqualTo(2);
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
        assertThat(refused).as("start refusals told").isEmpty();
    }

    @Test
    void a_dynamic_resume_that_resolves_to_the_present_is_held_paused_when_the_read_of_the_change_stream_gives_no_position_and_resuming_it_asks_again() {
        // Given: a subscription that has run and is paused, and a server whose change stream gives no position now
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));

        // When: it is resumed at a position that resolves to the present
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(ctx -> StartAt.now()));

        // Then: held paused, with the handle it returned told, and not refused
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(refused).as("start refusals told").isEmpty();

        // And when it is resumed once the server gives a position again
        int readsBeforeTheResume = presentReads.get();
        BsonDocument position = realResumeToken();
        presentRead.set(() -> cursorReading(null, position));
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(presentReads.get() - readsBeforeTheResume).as("reads of the change stream that records the present for the second resume").isEqualTo(1);
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
        assertThat(refused).as("start refusals told").isEmpty();
    }

    @Test
    void a_dynamic_resume_that_loses_its_history_as_it_opens_falls_back_to_the_restart_from_now_and_is_held_paused_when_the_read_of_the_change_stream_gives_no_position() {
        // Given: a subscription that has run and is paused, a checkpoint, and a server whose change stream gives no position now
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);

        // When: it is resumed at a position that resolves to a checkpoint, whose history is lost as the change stream opens
        subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)));
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() > 0 && subscriptionModel.isPaused(subscriptionId));

        // Then: the restart from now asked MongoDB, which gave no position, so it is held paused and not refused
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(refused).as("start refusals told").isEmpty();

        // And when it is resumed once the server gives a position again
        BsonDocument position = realResumeToken();
        presentRead.set(() -> cursorReading(null, position));
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));
        assertThat(presentReads).as("reads of the change stream that records the present, the resume asking again").hasValue(2);
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
        assertThat(refused).as("start refusals told").isEmpty();
    }

    @Test
    void a_subscription_at_the_present_is_refused_when_the_read_of_the_change_stream_gives_no_position_and_the_retry_strategy_maps_errors() {
        // Given: a retry strategy that wraps every error it sees, and a server whose change stream gives no position
        AtomicInteger mapped = new AtomicInteger();
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), mappingErrors(mapped));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        String subscriptionId = UUID.randomUUID().toString();

        // When
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(FIVE_SECONDS, () -> refused.contains(subscriptionId));

        // Then
        assertThat(thrown).as("thrown by the handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(mapped).as("errors the retry strategy mapped").hasValue(0);
    }

    @Test
    void a_resume_of_a_subscription_at_the_present_is_refused_without_reading_the_present_again_when_the_read_at_the_subscribe_gave_no_position_and_the_retry_strategy_maps_errors() {
        // Given: a stopped model, a retry strategy that wraps every error it sees, and a subscription at now whose read at the subscribe gave no position
        AtomicInteger mapped = new AtomicInteger();
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), mappingErrors(mapped));
        subscriptionModel.stop();
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        BsonDocument position = realResumeToken();
        presentRead.set(() -> cursorReading(null, position));

        // When: it is resumed once the server gives a position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(FIVE_SECONDS, () -> refused.contains(subscriptionId));

        // Then: refused with the answer of the read at the subscribe, and not asked again
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(mapped).as("errors the retry strategy mapped").hasValue(0);
    }

    @Test
    void the_read_of_the_change_stream_that_records_the_present_closes_its_cursor_when_it_returns_a_position() {
        // Given
        BsonDocument position = realResumeToken();
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = cursorReading(null, position);
        presentRead.set(() -> cursor);
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));

        // When
        subscriptionModel.subscribe(UUID.randomUUID().toString(), filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));

        // Then
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        verify(cursor).close();
    }

    @Test
    void the_read_of_the_change_stream_that_records_the_present_closes_its_cursor_when_it_returns_no_position() {
        // Given
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = cursorReading(null, null);
        presentRead.set(() -> cursor);
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));

        // When
        Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), filter(), StartAt.now(), cloudEvent -> {
        });
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));

        // Then
        assertThat(thrown).as("thrown by the handle").isExactlyInstanceOf(UnsupportedStartAtException.class);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        verify(cursor).close();
    }

    @Test
    void a_dynamic_position_that_resolves_to_the_present_is_refused_when_the_read_of_the_change_stream_gives_no_position() {
        // Given: a server whose change stream gives no position
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        String subscriptionId = UUID.randomUUID().toString();

        // When
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> StartAt.now()), cloudEvent -> {
        });
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(TEN_SECONDS, () -> refused.contains(subscriptionId));

        // Then
        assertThat(thrown).as("thrown by the handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isFalse();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(cursorsOpened()).as("cursors of the subscription's change streams opened").isZero();
    }

    @Test
    void a_dynamic_position_that_resolves_to_the_present_on_a_stopped_model_is_refused_when_the_read_of_the_change_stream_gave_no_position_and_skips_no_event_written_since() {
        // Given: a stopped model, and a subscription whose position resolves to the present while the read at the subscribe gave no position
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        subscriptionModel.stop();
        String subscriptionId = UUID.randomUUID().toString();
        List<String> handled = new CopyOnWriteArrayList<>();
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> StartAt.now()), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        presentRead.set(null);
        mongoEventStore.write("1", 0, serialize(nameDefined("written since the subscribe")));

        // When
        catchThrowable(() -> subscriptionModel.start(true));
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));

        // Then: refused, and not started at a present later than the subscribe, which would skip the event
        assertThat(thrown).as("thrown by the handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(handled).as("events delivered").isEmpty();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
    }

    @Test
    void a_resume_at_now_on_a_stopped_model_reads_the_present_for_itself_and_is_not_refused_when_the_read_at_the_subscribe_gave_no_position() {
        // Given: a stopped model, and a subscription at now whose read at the subscribe gave no position
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        subscriptionModel.stop();
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        presentRead.set(null);

        // When: it is resumed at now once the server gives a position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(thrown).as("thrown by the resume handle").isNull();
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
    }

    @Test
    void a_dynamic_resume_that_falls_back_to_the_position_the_subscription_was_made_with_is_refused_and_skips_no_event_written_since_when_the_read_at_the_subscribe_gave_no_position() {
        // Given: a stopped model, a subscription at now whose read at the subscribe gave no position, and an event written since
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        subscriptionModel.stop();
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        List<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        presentRead.set(null);
        mongoEventStore.write("1", 0, serialize(nameDefined("written since the subscribe")));

        // When: it is resumed at a position that resolves to a checkpoint the change stream can't open at
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))));
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(FIVE_SECONDS, () -> refused.contains(subscriptionId));

        // Then: refused, and not opened at a present later than the subscribe, which would skip the event
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
        assertThat(handled).as("events delivered").isEmpty();
        assertThat(cursorsOpened()).as("cursors of the subscription's change streams opened").isZero();
    }

    @Test
    void a_resume_at_now_of_a_dynamic_subscription_that_started_when_the_read_at_the_subscribe_gave_no_position_reads_the_present_for_itself_and_is_not_refused() {
        // Given: a subscription made at a dynamic position that resolves to a checkpoint, whose read at the subscribe gave no position, and that is paused
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        presentRead.set(null);
        subscriptionModel.pauseSubscription(subscriptionId);

        // When: it is resumed at now once the server gives a position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(thrown).as("thrown by the resume handle").isNull();
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
    }

    @Test
    void a_run_of_a_dynamic_subscription_that_started_when_the_read_at_the_subscribe_gave_no_position_restarts_from_now_after_its_history_was_lost_and_is_not_held_paused() {
        // Given: a subscription made at a dynamic position that resolves to a checkpoint, whose read at the subscribe gave no position, and that has started
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)), cloudEvent -> handled.add(cloudEvent.getId())).waitUntilStarted(Duration.ofSeconds(10));
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        presentRead.set(null);

        // When: its history is lost once the server gives a position, and the restart from now has opened its change stream
        historyLost.set(true);
        waitAtMostFor(TEN_SECONDS, () -> cursorsOpened() == 2 || subscriptionModel.isPaused(subscriptionId));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isFalse();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
    }

    @Test
    void a_resume_of_a_run_held_paused_after_its_history_was_lost_waits_for_the_read_at_the_subscribe_still_running_and_then_reads_the_present_for_itself_and_is_not_refused() {
        // Given: a dynamic subscription on a stopped model whose read at the subscribe is held, resumed at a checkpoint that loses its history, while the server gives no position to restart from
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        CountDownLatch released = new CountDownLatch(1);
        heldReads.add(released);
        AtomicInteger reads = new AtomicInteger();
        presentRead.set(() -> reads.getAndIncrement() == 0 ? cursorHeldUntil(released, null) : cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        subscriptionModel.stop();
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe, held").hasValue(1);
        subscriptionModel.resumeSubscription(subscriptionId, StartAt.checkpoint(checkpoint)).waitUntilStarted(Duration.ofSeconds(10));
        historyLost.set(true);
        waitAtMostFor(TEN_SECONDS, () -> presentReads.get() == 2 && subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("held paused before the resume").isTrue();
        presentRead.set(null);

        // When: it is resumed while the read at the subscribe is still held
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        boolean startedWhileHeld = resumed.waitUntilStarted(Duration.ofMillis(500));
        boolean runningWhileHeld = subscriptionModel.isRunning(subscriptionId);
        released.countDown();
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(startedWhileHeld).as("started while the read at the subscribe was held").isFalse();
        assertThat(runningWhileHeld).as("running while the read at the subscribe was held").isTrue();
        assertThat(thrown).as("thrown by the resume handle").isNull();
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
    }

    @Test
    void a_dynamic_resume_that_falls_back_to_a_dynamic_position_that_resolves_to_a_checkpoint_opens_at_it_and_is_not_refused_when_the_read_at_the_subscribe_gave_no_position() {
        // Given: a stopped model, a subscription made at a dynamic position that resolves to a checkpoint, whose read at the subscribe gave no position, and an event written since
        MongoOperationTimeCheckpoint checkpoint = new MongoOperationTimeCheckpoint(requireNonNull(MongoCommons.operationTimeAfter(database.runCommand(MongoCommons.CURRENT_OPERATION_TIME_COMMAND))));
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        subscriptionModel.stop();
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        presentRead.set(null);
        NameDefined e1 = nameDefined("name1");
        mongoEventStore.write("1", 0, serialize(e1));

        // When: it is resumed at a position that resolves to a checkpoint the change stream can't open at
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))));
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e1.eventId()));

        // Then
        assertThat(thrown).as("thrown by the resume handle").isNull();
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isTrue();
        assertThat(handled).as("events delivered").containsExactly(e1.eventId());
    }

    @Test
    void a_dynamic_resume_that_falls_back_to_a_dynamic_position_that_now_resolves_to_the_present_is_refused_and_skips_no_event_written_since_when_the_read_at_the_subscribe_gave_no_position() {
        // Given: a subscription made at a dynamic position that fails until released and then resolves to the present, whose read at the subscribe gave no position, that is paused before it opened, and an event written since
        BsonDocument recorded = realResumeToken();
        AtomicBoolean released = new AtomicBoolean();
        AtomicInteger evaluations = new AtomicInteger();
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> {
            evaluations.incrementAndGet();
            if (!released.get()) {
                throw new IllegalStateException("not yet");
            }
            return StartAt.now();
        }), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1 && evaluations.get() > 0);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        subscriptionModel.pauseSubscription(subscriptionId);
        released.set(true);
        presentRead.set(() -> cursorReading(null, recorded));
        mongoEventStore.write("1", 0, serialize(nameDefined("written since the subscribe")));

        // When: it is resumed at a position that resolves to a checkpoint the change stream can't open at, once the server gives a position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))));
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(FIVE_SECONDS, () -> refused.contains(subscriptionId));

        // Then: refused, and not opened at a present later than the subscribe, which would skip the event
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(cursorsOpened()).as("cursors of the subscription's change streams opened").isZero();
        assertThat(handled).as("events delivered").isEmpty();
    }

    @Test
    void a_dynamic_position_that_resolves_to_the_present_is_refused_when_it_is_paused_and_resumed_before_it_opened_and_the_read_at_the_subscribe_gave_no_position() {
        // Given: a subscription made at a dynamic position that fails until released and then resolves to the present, whose read at the subscribe gave no position, that is paused before it opened, and an event written since
        BsonDocument recorded = realResumeToken();
        AtomicBoolean released = new AtomicBoolean();
        AtomicInteger evaluations = new AtomicInteger();
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.dynamic(ctx -> {
            evaluations.incrementAndGet();
            if (!released.get()) {
                throw new IllegalStateException("not yet");
            }
            return StartAt.now();
        }), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1 && evaluations.get() > 0);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe").hasValue(1);
        subscriptionModel.pauseSubscription(subscriptionId);
        released.set(true);
        presentRead.set(() -> cursorReading(null, recorded));
        mongoEventStore.write("1", 0, serialize(nameDefined("written since the subscribe")));

        // When: it is resumed, once the server gives a position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(FIVE_SECONDS, () -> refused.contains(subscriptionId));

        // Then: refused, and not opened at a present later than the subscribe, which would skip the event
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(cursorsOpened()).as("cursors of the subscription's change streams opened").isZero();
        assertThat(handled).as("events delivered").isEmpty();
    }

    @Test
    void a_subscription_at_now_that_is_paused_while_the_read_at_the_subscribe_runs_is_refused_when_resumed_and_the_read_gave_no_position() {
        // Given: a subscription at now whose read at the subscribe is held, then paused, and the read let go to give no position
        BsonDocument recorded = realResumeToken();
        CountDownLatch released = new CountDownLatch(1);
        heldReads.add(released);
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> held = cursorHeldUntil(released, null);
        AtomicInteger reads = new AtomicInteger();
        presentRead.set(() -> reads.getAndIncrement() == 0 ? held : cursorReading(null, recorded));
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));
        waitAtMostFor(FIVE_SECONDS, () -> presentReads.get() == 1);
        assertThat(presentReads).as("reads of the change stream that records the present at the subscribe, held").hasValue(1);
        subscriptionModel.pauseSubscription(subscriptionId);
        released.countDown();
        waitAtMostFor(FIVE_SECONDS, () -> mockingDetails(held).getInvocations().stream().anyMatch(invocation -> invocation.getMethod().getName().equals("getResumeToken")));
        // The read has answered, and the subscription takes its answer in a few more steps that only end with its task on the executor
        await().atMost(FIVE_SECONDS).until(() -> subscriptionExecutor.getActiveCount() == 0);
        mongoEventStore.write("1", 0, serialize(nameDefined("written since the subscribe")));

        // When: it is resumed, once the server gives a position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(FIVE_SECONDS, () -> refused.contains(subscriptionId));

        // Then: refused, and not opened at a present later than the subscribe, which would skip the event
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(cursorsOpened()).as("cursors of the subscription's change streams opened").isZero();
        assertThat(handled).as("events delivered").isEmpty();
    }

    @Test
    void a_resume_at_the_present_that_is_held_paused_tells_the_start_refusal_listeners_it_is_held_and_not_that_it_is_refused() {
        // Given: a subscription that has run, and a server whose change stream gives no position now
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        RecordingStartRefusalListener listener = new RecordingStartRefusalListener();
        subscriptionModel.addStartRefusalListener(listener);
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));

        // When
        subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        waitAtMostFor(TEN_SECONDS, () -> !listener.heldPaused.isEmpty());

        // Then: the listeners were told it is held paused, with the refusal a handle throws, and not that it is refused
        assertThat(listener.heldPaused).as("subscriptions the listener was told are held paused").containsExactly(subscriptionId);
        assertThat(listener.heldPausedWith).as("refusals the listener was told with").singleElement()
                .satisfies(refusal -> assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position"));
        assertThat(listener.refused).as("start refusals told").isEmpty();
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
    }

    @Test
    void a_run_held_paused_after_it_waited_for_a_stuck_call_and_its_handle_answered_true_still_tells_the_start_refusal_listeners_it_is_held() {
        // Given: the call of the action for the first event is stuck, and a listener of start refusals
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        RecordingStartRefusalListener listener = new RecordingStartRefusalListener();
        subscriptionModel.addStartRefusalListener(listener);
        CountDownLatch releaseTheStuckCall = new CountDownLatch(1);
        heldReads.add(releaseTheStuckCall);
        CountDownLatch actionCalled = new CountDownLatch(1);
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
            actionCalled.countDown();
            awaitUninterruptibly(releaseTheStuckCall);
        }).waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("1", 0, serialize(nameDefined("name1")));
        assertThat(awaitUnlessInterrupted(actionCalled, Duration.ofSeconds(10))).as("action called for the first event").isTrue();

        // When: it is paused, which returns while that call still runs, and resumed as its history is lost as the resumed run opens, while the server gives no position to restart from
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
        boolean startedWhileTheCallIsStuck = resumed.waitUntilStarted(Duration.ofSeconds(10));
        waitAtMostFor(TEN_SECONDS, () -> !historyLostWhenOpening.get());
        int readsWhileTheCallIsStuck = presentReads.get();
        List<String> toldWhileTheCallIsStuck = List.copyOf(listener.heldPaused);

        // And: the stuck call returns, so the restart from the present reads no position
        releaseTheStuckCall.countDown();
        waitAtMostFor(TEN_SECONDS, () -> !listener.heldPaused.isEmpty());

        // Then: the handle had answered true, and the listeners were told only once the run was held, and not that it is refused
        assertThat(startedWhileTheCallIsStuck).as("resume handle answered while the call is stuck").isTrue();
        assertThat(readsWhileTheCallIsStuck).as("reads of the change stream that records the present while the call is stuck").isZero();
        assertThat(toldWhileTheCallIsStuck).as("subscriptions the listener was told are held paused while the call is stuck").isEmpty();
        assertThat(listener.heldPaused).as("subscriptions the listener was told are held paused").containsExactly(subscriptionId);
        assertThat(listener.refused).as("start refusals told").isEmpty();
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
    }

    @Test
    void a_cancel_of_a_resume_at_now_whose_read_of_the_present_waits_closes_that_read_and_frees_its_thread() {
        // Given: a subscription that has run, and a resume at now whose read blocks until its cursor is closed
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100), Duration.ofSeconds(30));
        BsonDocument resumeToken = realResumeToken();
        presentRead.set(() -> cursorReading(null, resumeToken));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        BlockingRead blocking = new BlockingRead();
        presentRead.set(() -> blocking.cursor);
        subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        assertThat(awaitUnlessInterrupted(blocking.entered, Duration.ofSeconds(10))).as("read of the present entered").isTrue();

        // When
        subscriptionModel.cancelSubscription(subscriptionId);

        // Then
        assertThat(awaitUnlessInterrupted(blocking.returned, Duration.ofSeconds(2))).as("read of the present returned within two seconds of the cancel").isTrue();
        verify(blocking.cursor, atLeastOnce()).close();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
    }

    @Test
    void a_pause_of_a_resume_at_now_whose_read_of_the_present_waits_closes_that_read_and_frees_its_thread() {
        // Given: a subscription that has run, and a resume at now whose read blocks until its cursor is closed
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100), Duration.ofSeconds(30));
        BsonDocument resumeToken = realResumeToken();
        presentRead.set(() -> cursorReading(null, resumeToken));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        BlockingRead blocking = new BlockingRead();
        presentRead.set(() -> blocking.cursor);
        subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        assertThat(awaitUnlessInterrupted(blocking.entered, Duration.ofSeconds(10))).as("read of the present entered").isTrue();

        // When
        subscriptionModel.pauseSubscription(subscriptionId);

        // Then
        assertThat(awaitUnlessInterrupted(blocking.returned, Duration.ofSeconds(2))).as("read of the present returned within two seconds of the pause").isTrue();
        verify(blocking.cursor, atLeastOnce()).close();
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
    }

    @Test
    void a_cancel_of_a_subscription_at_now_whose_read_of_the_present_at_the_subscribe_waits_closes_that_read() {
        // Given: a subscription at now whose read at the subscribe blocks until its cursor is closed
        BlockingRead blocking = new BlockingRead();
        presentRead.set(() -> blocking.cursor);
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100), Duration.ofSeconds(30));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        });
        assertThat(awaitUnlessInterrupted(blocking.entered, Duration.ofSeconds(10))).as("read of the present at the subscribe entered").isTrue();

        // When
        subscriptionModel.cancelSubscription(subscriptionId);

        // Then
        assertThat(awaitUnlessInterrupted(blocking.returned, Duration.ofSeconds(2))).as("read of the present at the subscribe returned within two seconds of the cancel").isTrue();
        verify(blocking.cursor, atLeastOnce()).close();
        await().atMost(FIVE_SECONDS).until(() -> subscriptionExecutor.getActiveCount() == 0);
    }

    @Test
    void start_true_on_a_stopped_model_throws_the_refusal_of_a_resumed_run_held_paused_even_when_the_run_has_left_the_running_subscriptions_before_the_refusal_is_released() {
        // Given: a subscription that has run, a stopped model, and a server whose change stream gives no position now
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.stop();
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);
        HoldsTheLeaveOfAHeldRun holdsTheLeave = new HoldsTheLeaveOfAHeldRun();
        replaceRunningSubscriptionsOf(subscriptionModel, holdsTheLeave);

        // When: start(true) resumes it, its history is lost as the resumed run opens, and the thread that holds the run paused is held right after the run left the running subscriptions
        CompletableFuture<Void> starting = CompletableFuture.runAsync(() -> subscriptionModel.start(true));
        assertThat(awaitUnlessInterrupted(holdsTheLeave.held, Duration.ofSeconds(10))).as("run held paused left the running subscriptions").isTrue();
        Throwable thrown = catchThrowable(() -> starting.get(5, TimeUnit.SECONDS));
        holdsTheLeave.release.countDown();

        // Then
        assertThat(thrown).as("thrown by start(true) while the thread that holds the run is held").isInstanceOf(ExecutionException.class).hasCauseExactlyInstanceOf(UnsupportedStartAtException.class);
        assertThat(thrown.getCause()).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position").hasMessageContaining(subscriptionId)
                .hasMessageContaining("It is paused, and resuming it asks MongoDB again.").hasMessageNotContaining("Subscribe it at a checkpoint");
        await().atMost(FIVE_SECONDS).until(() -> subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
    }

    @Test
    void start_true_on_a_stopped_model_throws_when_the_read_of_the_present_of_a_resumed_run_gives_no_position() {
        // Given: a subscription that has run, a stopped model, and a server whose change stream gives no position now
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.stop();
        presentRead.set(() -> cursorReading(null, null));
        historyLostWhenOpening.set(true);

        // When
        Throwable thrown = catchThrowable(() -> subscriptionModel.start(true));

        // Then
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining(subscriptionId)
                .hasMessageContaining("It is paused, and resuming it asks MongoDB again.").hasMessageNotContaining("Subscribe it at a checkpoint");
        await().atMost(FIVE_SECONDS).until(() -> subscriptionModel.isPaused(subscriptionId));
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
    }

    @Test
    void a_start_refusal_listener_that_throws_when_told_a_run_is_held_paused_does_not_keep_the_next_listener_from_being_told() {
        // Given: a subscription that has run, a first listener that throws when told a run is held, and a second that records it
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        subscriptionModel.addStartRefusalListener(new StartRefusalListener() {
            @Override
            public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            }

            @Override
            public void handleStartHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
                throw new IllegalStateException("listener failed");
            }
        });
        RecordingStartRefusalListener listener = new RecordingStartRefusalListener();
        subscriptionModel.addStartRefusalListener(listener);
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));

        // When
        subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        waitAtMostFor(TEN_SECONDS, () -> !listener.heldPaused.isEmpty());

        // Then
        assertThat(listener.heldPaused).as("subscriptions the listener after the one that throws was told are held paused").containsExactly(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
    }

    // A read of the present whose cursor blocks in tryNext() until it is closed, for far longer than any wait of the test
    private final class BlockingRead {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch returned = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = blockingUntilClosed();

        @SuppressWarnings("unchecked")
        private MongoChangeStreamCursor<ChangeStreamDocument<Document>> blockingUntilClosed() {
            heldReads.add(closed);
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> mock = mock(MongoChangeStreamCursor.class);
            doAnswer(invocation -> {
                entered.countDown();
                closed.await(20, TimeUnit.SECONDS);
                returned.countDown();
                return null;
            }).when(mock).tryNext();
            doAnswer(invocation -> {
                closed.countDown();
                return null;
            }).when(mock).close();
            return mock;
        }
    }

    // The thread that removes a run held paused from the running subscriptions is held right after it did
    private static final class HoldsTheLeaveOfAHeldRun extends ConcurrentHashMap<String, Object> {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean first = new AtomicBoolean(true);

        @Override
        public boolean remove(Object key, Object value) {
            boolean removed = super.remove(key, value);
            if (removed && first.getAndSet(false)) {
                held.countDown();
                awaitUnlessInterrupted(release, Duration.ofSeconds(10));
            }
            return removed;
        }
    }

    // The map of the running subscriptions can't be reached from a public method, so it is replaced through the private fields
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void replaceRunningSubscriptionsOf(NativeMongoSubscriptionModel model, ConcurrentHashMap replacement) {
        try {
            Field subscriptionsField = NativeMongoSubscriptionModel.class.getDeclaredField("subscriptions");
            subscriptionsField.setAccessible(true);
            Object subscriptions = subscriptionsField.get(model);
            Field runningField = subscriptions.getClass().getDeclaredField("runningSubscriptions");
            runningField.setAccessible(true);
            replacement.putAll((Map) runningField.get(subscriptions));
            runningField.set(subscriptions, replacement);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class RecordingStartRefusalListener implements StartRefusalListener {
        private final List<String> refused = new CopyOnWriteArrayList<>();
        private final List<String> heldPaused = new CopyOnWriteArrayList<>();
        private final List<UnsupportedStartAtException> heldPausedWith = new CopyOnWriteArrayList<>();

        @Override
        public void handleStartRefusal(String subscriptionId, UnsupportedStartAtException refusal) {
            refused.add(subscriptionId);
        }

        @Override
        public void handleStartHeldPaused(String subscriptionId, UnsupportedStartAtException refusal) {
            heldPausedWith.add(refusal);
            heldPaused.add(subscriptionId);
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

    private List<String> errorsLoggedAbout(String subscriptionId) {
        synchronized (logged) {
            return logged.list.stream().filter(event -> event.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).filter(message -> message.contains(subscriptionId)).toList();
        }
    }

    private static RetryStrategy mappingErrors(AtomicInteger mapped) {
        return RetryStrategy.fixed(100).mapError(error -> {
            mapped.incrementAndGet();
            return new RuntimeException("wrapped", error);
        });
    }

    private long cursorsOpened() {
        return opened.stream().filter(iterable -> mockingDetails(iterable).getInvocations().stream().anyMatch(invocation -> invocation.getMethod().getName().equals("cursor"))).count();
    }

    private NativeMongoSubscriptionModel modelOver(MongoDatabase databaseToAsk, MongoCollection<Document> collectionToWatch, RetryStrategy retryStrategy) {
        return modelOver(databaseToAsk, collectionToWatch, retryStrategy, Duration.ofSeconds(1));
    }

    private NativeMongoSubscriptionModel modelOver(MongoDatabase databaseToAsk, RetryStrategy retryStrategy, Duration maxAwaitTime) {
        return modelOver(databaseToAsk, eventCollection, retryStrategy, maxAwaitTime);
    }

    private NativeMongoSubscriptionModel modelOver(MongoDatabase databaseToAsk, MongoCollection<Document> collectionToWatch, RetryStrategy retryStrategy, Duration maxAwaitTime) {
        return new NativeMongoSubscriptionModel(databaseToAsk, collectionToWatch, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(retryStrategy).restartSubscriptionsOnChangeStreamHistoryLost(true).maxAwaitTime(maxAwaitTime));
    }

    private static AgnosticSubscriptionFilter filter() {
        return AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));
    }

    /**
     * Wraps {@code database} so that the reply to the operation time question has no {@code operationTime}, as a server
     * that doesn't send one answers. Every other command gets its real reply.
     */
    private MongoDatabase databaseThatSendsNoOperationTime() {
        MongoDatabase wrapped = spy(database);
        doAnswer(invocation -> {
            if (MongoCommons.CURRENT_OPERATION_TIME_COMMAND.equals(invocation.getArgument(0))) {
                repliesWithoutOperationTime.incrementAndGet();
                return new Document("ok", 1.0);
            }
            return invocation.callRealMethod();
        }).when(wrapped).runCommand(any(Bson.class));
        return wrapped;
    }

    /**
     * Wraps the event collection so that the change stream with an empty pipeline, which is the read that records the
     * present, is answered by {@link #presentRead} when it holds an answer and by the real collection otherwise. A
     * change stream with a pipeline is the real one, as a spy kept in {@link #opened}, which also loses its history
     * once {@link #historyLost} is set, or when it opens once {@link #historyLostWhenOpening} is set.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatAnswersThePresentRead() {
        MongoCollection<Document> wrapped = spy(eventCollection);
        doAnswer(invocation -> {
            List<Bson> pipeline = invocation.getArgument(0);
            if (pipeline.isEmpty()) {
                Supplier<MongoChangeStreamCursor<ChangeStreamDocument<Document>>> answer = presentRead.get();
                return answer == null ? invocation.callRealMethod() : iterableReading(readCounted(answer));
            }
            ChangeStreamIterable<Document> recorded = spy((ChangeStreamIterable<Document>) invocation.callRealMethod());
            doAnswer(cursorInvocation -> {
                if (historyLostWhenOpening.compareAndSet(true, false)) {
                    throw historyLostError();
                }
                return losingItsHistoryWhenAsked((MongoChangeStreamCursor<ChangeStreamDocument<Document>>) cursorInvocation.callRealMethod());
            }).when(recorded).cursor();
            opened.add(recorded);
            return recorded;
        }).when(wrapped).watch(anyList(), eq(Document.class));
        return wrapped;
    }

    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> readCounted(Supplier<MongoChangeStreamCursor<ChangeStreamDocument<Document>>> answer) {
        presentReads.incrementAndGet();
        return answer.get();
    }

    @SuppressWarnings("unchecked")
    private static ChangeStreamIterable<Document> iterableReading(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
        ChangeStreamIterable<Document> iterable = mock(ChangeStreamIterable.class, RETURNS_SELF);
        doReturn(cursor).when(iterable).cursor();
        return iterable;
    }

    @SuppressWarnings("unchecked")
    private static MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursorReading(ChangeStreamDocument<Document> document, BsonDocument resumeToken) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock(MongoChangeStreamCursor.class);
        doReturn(document).when(cursor).tryNext();
        doReturn(resumeToken).when(cursor).getResumeToken();
        return cursor;
    }

    @SuppressWarnings("unchecked")
    private static MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursorHeldUntil(CountDownLatch released, BsonDocument resumeToken) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock(MongoChangeStreamCursor.class);
        doAnswer(invocation -> {
            released.await(20, TimeUnit.SECONDS);
            return null;
        }).when(cursor).tryNext();
        doReturn(resumeToken).when(cursor).getResumeToken();
        return cursor;
    }

    @SuppressWarnings("unchecked")
    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> losingItsHistoryWhenAsked(MongoChangeStreamCursor<ChangeStreamDocument<Document>> real) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = mock(MongoChangeStreamCursor.class, delegatesTo(real));
        doAnswer(invocation -> {
            if (historyLost.compareAndSet(true, false)) {
                throw historyLostError();
            }
            return real.tryNext();
        }).when(cursor).tryNext();
        return cursor;
    }

    private static MongoCommandException historyLostError() {
        List<BsonElement> elements = List.of(new BsonElement("code", new BsonInt32(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE)), new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")));
        return new MongoCommandException(new BsonDocument(elements), new ServerAddress());
    }

    /**
     * A resume token the server sends with an empty batch, before anything was written.
     */
    private BsonDocument realResumeToken() {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = eventCollection.watch().cursor()) {
            assertThat(cursor.tryNext()).isNull();
            return requireNonNull(cursor.getResumeToken());
        }
    }

    /**
     * The arguments of the calls of {@code method} on a spy of {@link #opened}.
     */
    private static List<Object> callsTo(String method, ChangeStreamIterable<Document> iterable) {
        return mockingDetails(iterable).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals(method))
                .map((Invocation invocation) -> invocation.getArguments()[0])
                .toList();
    }

    private static void waitAtMostFor(Duration timeout, Callable<Boolean> condition) {
        try {
            await().dontCatchUncaughtExceptions().atMost(timeout).until(condition);
        } catch (ConditionTimeoutException ignored) {
            // The assertions that follow fail with what the model did instead, and an exception that ends a thread of the model is not this wait's to throw
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
