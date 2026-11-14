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
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions.HistoryLossListener;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
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
    private ExecutorService subscriptionExecutor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".noOperationTime");
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), new EventStoreConfig(TimeRepresentation.RFC_3339_STRING));
        subscriptionExecutor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void shutdown() {
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
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), RetryStrategy.fixed(2000));
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
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), RetryStrategy.fixed(2000));
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
        // Given: a read that returns when released, from a position taken before any event was written
        BsonDocument resumeTokenBeforeTheEvents = realResumeToken();
        CountDownLatch released = new CountDownLatch(1);
        heldReads.add(released);
        presentRead.set(() -> cursorHeldUntil(released, resumeTokenBeforeTheEvents));
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
        released.countDown();
        resumed.waitUntilStarted(Duration.ofSeconds(10));
        mongoEventStore.write("3", 0, serialize(e3));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e3.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId(), e3.eventId());
    }

    @Test
    void a_pause_and_resume_right_after_subscribe_delivers_every_event_written_once_it_started() {
        // Given
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();

        // When
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> handled.add(cloudEvent.getId()));
        subscriptionModel.pauseSubscription(subscriptionId);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        NameDefined e1 = nameDefined("name1");
        NameDefined e2 = nameDefined("name2");
        mongoEventStore.write("1", 0, serialize(e1));
        mongoEventStore.write("2", 0, serialize(e2));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e2.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId());
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
    void resuming_a_subscription_that_has_run_at_the_present_is_refused_when_the_read_of_the_change_stream_returns_no_event_and_no_resume_token() {
        // Given: a subscription that has run, and a server whose change stream gives no position now
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime(), collectionThatAnswersThePresentRead(), RetryStrategy.fixed(100));
        List<String> refused = new CopyOnWriteArrayList<>();
        subscriptionModel.addStartRefusalListener((id, refusal) -> refused.add(id));
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, filter(), StartAt.now(), cloudEvent -> {
        }).waitUntilStarted(Duration.ofSeconds(10));
        subscriptionModel.pauseSubscription(subscriptionId);
        presentRead.set(() -> cursorReading(null, null));

        // When
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());

        // Then
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        await().atMost(FIVE_SECONDS).until(() -> refused.contains(subscriptionId));
        assertThat(refused).containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
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
        AtomicBoolean startedAnswer = new AtomicBoolean(true);
        Throwable thrownByTheHandle = catchThrowable(() -> startedAnswer.set(subscription.waitUntilStarted(Duration.ofSeconds(1))));

        // Then
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isTrue();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").contains(subscriptionId);
        assertThat(refused).as("start refusals told").isEmpty();
        assertThat(thrownByTheHandle).as("thrown by the handle it was subscribed on").isNull();
        assertThat(startedAnswer).as("answer of the handle it was subscribed on").isFalse();

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
    void a_held_subscription_resumed_at_the_present_is_refused_when_the_read_of_the_change_stream_still_gives_no_position() {
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

        // When: it is resumed at the present and the server still gives no position
        Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
        Throwable thrown = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
        waitAtMostFor(TEN_SECONDS, () -> refused.contains(subscriptionId));

        // Then
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
        assertThat(refused).as("start refusals told").containsExactly(subscriptionId);
        assertThat(subscriptionModel.subscriptionIds()).as("known subscriptions").doesNotContain(subscriptionId);
        assertThat(subscriptionModel.isPaused(subscriptionId)).as("paused").isFalse();
        assertThat(subscriptionModel.isRunning(subscriptionId)).as("running").isFalse();
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
        long cursorsOpened = opened.stream().filter(iterable -> mockingDetails(iterable).getInvocations().stream().anyMatch(invocation -> invocation.getMethod().getName().equals("cursor"))).count();
        assertThat(cursorsOpened).as("cursors of the subscription's change streams opened").isZero();
    }

    private NativeMongoSubscriptionModel modelOver(MongoDatabase databaseToAsk, RetryStrategy retryStrategy) {
        return modelOver(databaseToAsk, eventCollection, retryStrategy);
    }

    private NativeMongoSubscriptionModel modelOver(MongoDatabase databaseToAsk, MongoCollection<Document> collectionToWatch, RetryStrategy retryStrategy) {
        return new NativeMongoSubscriptionModel(databaseToAsk, collectionToWatch, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(retryStrategy).restartSubscriptionsOnChangeStreamHistoryLost(true).maxAwaitTime(Duration.ofSeconds(1)));
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
