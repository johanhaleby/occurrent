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

package org.occurrent.subscription.mongodb.spring.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
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
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A {@link SpringMongoSubscriptionModel} subscribed at the present records where the present is when MongoDB's reply
 * to the operation time question has no {@code operationTime}, by reading its change stream once. These tests stub the
 * reply, and either let that read go to the real replica set or answer it themselves, so a refusal and a read that
 * returns an event can be arranged. The read is the change stream without a pipeline, so every subscription here has a
 * filter, which gives the change stream it opens a pipeline of its own.
 */
@Testcontainers
@Timeout(30)
@DisplayName("SpringMongoSubscriptionModelNoOperationTime")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelNoOperationTimeTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicInteger repliesWithoutOperationTime = new AtomicInteger();
    private final TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
    // What answers the read that records the present, or nothing to let it go to the real change stream
    private final AtomicReference<Supplier<MongoChangeStreamCursor<ChangeStreamDocument<Document>>>> presentRead = new AtomicReference<>();
    // The change streams the model opened with its subscription's pipeline, as spies, so the position each opened at can be read back
    private final List<ChangeStreamIterable<Document>> opened = new CopyOnWriteArrayList<>();
    private final AtomicInteger presentReads = new AtomicInteger();
    private final List<CountDownLatch> heldReads = new CopyOnWriteArrayList<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private MongoClient mongoClient;
    private MongoTemplate mongoTemplate;
    private String eventCollectionName;
    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        logged.start();
        modelLog.addAppender(logged);
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".noOperationTime");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        eventCollectionName = connectionString.getCollection();
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollectionName).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).eventStoreCapabilities(STREAM, DCB).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @AfterEach
    void shutdown() {
        modelLog.detachAppender(logged);
        logged.stop();
        heldReads.forEach(CountDownLatch::countDown);
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void a_pause_and_resume_after_the_action_of_the_first_event_threw_delivers_that_event_again_when_mongodb_sent_no_operation_time() {
        // Given: a backoff of 2 s between attempts, so the pause comes during the first backoff
        subscriptionModel = modelOver(RetryStrategy.fixed(2000));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(2000));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> observer = eventCollection().watch().cursor()) {
            mongoEventStore.write("0", 0, serialize(e0));
            AtomicReference<ChangeStreamDocument<Document>> observed = new AtomicReference<>();
            await().atMost(FIVE_SECONDS).until(() -> {
                observed.compareAndSet(null, observer.tryNext());
                return observed.get() != null;
            });
            readOfE0 = observed.get();
        }
        presentRead.set(() -> cursorReading(readOfE0, null));
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
        assertThat(thrown).as("thrown by the resume handle").isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("MongoDB gave neither an operation time nor a change stream position");
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
    void a_dynamic_resume_that_resolves_to_the_present_is_held_paused_when_the_read_of_the_change_stream_gives_no_position_and_resuming_it_asks_again() {
        // Given: a subscription that has run and is paused, and a server whose change stream gives no position now
        subscriptionModel = modelOver(RetryStrategy.fixed(100));
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
    void a_subscription_at_the_present_is_refused_when_the_read_of_the_change_stream_gives_no_position_and_the_retry_strategy_maps_errors() {
        // Given: a retry strategy that wraps every error it sees, and a server whose change stream gives no position
        AtomicInteger mapped = new AtomicInteger();
        presentRead.set(() -> cursorReading(null, null));
        subscriptionModel = modelOver(mappingErrors(mapped));
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
        subscriptionModel = modelOver(mappingErrors(mapped));
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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));

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
        subscriptionModel = modelOver(RetryStrategy.fixed(100));

        // When
        Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), filter(), StartAt.now(), cloudEvent -> {
        });
        Throwable thrown = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofSeconds(10)));

        // Then
        assertThat(thrown).as("thrown by the handle").isExactlyInstanceOf(UnsupportedStartAtException.class);
        assertThat(presentReads).as("reads of the change stream that records the present").hasValue(1);
        verify(cursor).close();
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

    private SpringMongoSubscriptionModel modelOver(RetryStrategy retryStrategy) {
        return new SpringMongoSubscriptionModel(templateThatSendsNoOperationTime(), withConfig(eventCollectionName, timeRepresentation).retryStrategy(retryStrategy).maxAwaitTime(Duration.ofSeconds(1)));
    }

    private static AgnosticSubscriptionFilter filter() {
        return AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));
    }

    private MongoCollection<Document> eventCollection() {
        return mongoTemplate.getDb().getCollection(eventCollectionName);
    }

    /**
     * Answers the operation time question with a reply that has no {@code operationTime}, as a server that doesn't send
     * one does, and hands the model an event collection whose reads are answered by {@link #presentRead} when it holds
     * an answer. Every other command gets its real reply.
     */
    private MongoTemplate templateThatSendsNoOperationTime() {
        return new MongoTemplate(mongoClient, requireNonNull(mongoTemplate.getDb().getName())) {
            @Override
            public Document executeCommand(Document command) {
                if (MongoCommons.CURRENT_OPERATION_TIME_COMMAND.equals(command)) {
                    repliesWithoutOperationTime.incrementAndGet();
                    return new Document("ok", 1.0);
                }
                return super.executeCommand(command);
            }

            @Override
            public MongoDatabase getDb() {
                MongoDatabase wrapped = spy(super.getDb());
                doAnswer(invocation -> collectionThatAnswersThePresentRead(requireNonNull((MongoCollection<Document>) invocation.callRealMethod()))).when(wrapped).getCollection(eq(eventCollectionName));
                return wrapped;
            }
        };
    }

    /**
     * Wraps the event collection so that the change stream without a pipeline, which is the read that records the
     * present, is answered by {@link #presentRead} when it holds an answer and by the real collection otherwise. A
     * change stream with a pipeline is the real one, as a spy kept in {@link #opened}.
     */
    private MongoCollection<Document> collectionThatAnswersThePresentRead(MongoCollection<Document> real) {
        MongoCollection<Document> wrapped = spy(real);
        doAnswer(invocation -> {
            Supplier<MongoChangeStreamCursor<ChangeStreamDocument<Document>>> answer = presentRead.get();
            return answer == null ? invocation.callRealMethod() : iterableReading(readCounted(answer));
        }).when(wrapped).watch(eq(Document.class));
        doAnswer(invocation -> {
            ChangeStreamIterable<Document> realOpen = (ChangeStreamIterable<Document>) invocation.callRealMethod();
            List<Bson> pipeline = invocation.getArgument(0);
            if (pipeline.isEmpty()) {
                // The read that records the present, which the driver also routes through here
                return realOpen;
            }
            ChangeStreamIterable<Document> recorded = spy(realOpen);
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

    /**
     * A resume token the server sends with an empty batch, before anything was written.
     */
    private BsonDocument realResumeToken() {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = eventCollection().watch().cursor()) {
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
