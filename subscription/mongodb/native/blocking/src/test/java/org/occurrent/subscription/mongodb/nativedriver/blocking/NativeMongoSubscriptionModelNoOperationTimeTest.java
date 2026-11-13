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
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.core.ConditionTimeoutException;
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
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * A {@link NativeMongoSubscriptionModel} subscribed at the present opens its change stream at the present when
 * MongoDB's reply to the operation time question has no {@code operationTime}, and has recorded no position then.
 * These tests stub that reply and check that the commit time of the first event is recorded before its action runs,
 * and that recording it never moves a position back that an earlier event has recorded.
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
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void a_pause_and_resume_after_the_action_of_the_first_event_threw_delivers_that_event_again_when_mongodb_sent_no_operation_time() {
        // Given: a backoff of 2 s between attempts, so the pause comes during the first backoff
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime());
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
    }

    @Test
    void recording_the_commit_time_of_an_event_never_moves_back_a_position_that_an_earlier_event_recorded() {
        // Given: two events written together, which share one transaction and so one commit time
        subscriptionModel = modelOver(databaseThatSendsNoOperationTime());
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
        List<BsonTimestamp> commitTimes = new ArrayList<>();
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> observer = eventCollection.watch().cursor()) {
            List<CloudEvent> both = new ArrayList<>(serialize(e1));
            both.addAll(serialize(e2));
            mongoEventStore.write("1", 0, both);
            await().atMost(FIVE_SECONDS).until(() -> {
                ChangeStreamDocument<Document> observed = observer.tryNext();
                if (observed != null) {
                    commitTimes.add(observed.getClusterTime());
                }
                return commitTimes.size() == 2;
            });
        }
        assertThat(commitTimes).as("commit times of the two events in the change stream").hasSize(2).doesNotContainNull().containsOnly(commitTimes.get(0));
        await().atMost(FIVE_SECONDS).until(() -> handled.contains(e1.eventId()) && failuresOnE2.get() >= 1);

        // When: paused while the second event's action fails, then resumed once it works again
        subscriptionModel.pauseSubscription(subscriptionId);
        failing.set(false);
        subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
        waitAtMostFor(TEN_SECONDS, () -> handled.contains(e2.eventId()));

        // Then
        assertThat(handled).containsExactly(e1.eventId(), e2.eventId());
    }

    private NativeMongoSubscriptionModel modelOver(MongoDatabase databaseToAsk) {
        return new NativeMongoSubscriptionModel(databaseToAsk, eventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(2000)));
    }

    /**
     * Wraps {@code database} so that the reply to the operation time question has no {@code operationTime}, as a server
     * that doesn't send one answers. Every other command gets its real reply.
     */
    private MongoDatabase databaseThatSendsNoOperationTime() {
        MongoDatabase wrapped = spy(database);
        doAnswer(invocation -> {
            if (MongoCommons.CURRENT_OPERATION_TIME_COMMAND.equals(invocation.getArgument(0))) {
                return new Document("ok", 1.0);
            }
            return invocation.callRealMethod();
        }).when(wrapped).runCommand(any(Bson.class));
        return wrapped;
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
