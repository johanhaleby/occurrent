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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ConnectionString;
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
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.retry.RetryStrategy.exponentialBackoff;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Tests that {@link NativeMongoSubscriptionModel} survives the same class of MongoDB operational failures
 * (leader elections/failovers, transient network errors, change stream history lost) that
 * {@code SpringMongoSubscriptionModel} has been hardened against in production, and that it recovers gap-free.
 */
@Testcontainers
@Timeout(20)
public class NativeMongoSubscriptionModelResilienceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".resilience"));

    private MongoEventStore mongoEventStore;
    private ObjectMapper objectMapper;
    private MongoClient mongoClient;
    private ExecutorService subscriptionExecutor;
    private MongoDatabase database;
    private MongoCollection<Document> realEventCollection;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".resilience");
        this.mongoClient = MongoClients.create(connectionString);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig config = new EventStoreConfig(timeRepresentation);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        realEventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), config);
        subscriptionExecutor = Executors.newCachedThreadPool();
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    /**
     * Wraps {@code realEventCollection} so that the very first {@code watch(...)} call throws {@code exception},
     * simulating a change-stream disruption (a failover, a transient network error, history lost, ...), while every
     * subsequent call behaves exactly like the real collection. Mirrors how {@code SpringMongoSubscriptionModelTest}
     * injects the same class of failure.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatFailsOnce(RuntimeException exception) {
        MongoCollection<Document> throwingCollection = mock(MongoCollection.class);
        when(throwingCollection.watch(anyList(), eq(Document.class)))
                .thenThrow(exception)
                .thenAnswer(invocation -> realEventCollection.watch((List<? extends Bson>) invocation.getArgument(0), Document.class));
        return throwingCollection;
    }

    /**
     * Wraps {@code realEventCollection} so that {@code watch(...)} succeeds (registering the subscription normally),
     * but the cursor throws {@code exception} as soon as it is iterated, simulating a change-stream disruption that
     * happens mid-subscription rather than at cursor-open time.
     */
    @SuppressWarnings("unchecked")
    private MongoCollection<Document> collectionThatFailsDuringIteration(RuntimeException exception) {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> throwingCursor = mock(MongoChangeStreamCursor.class);
        doThrow(exception).when(throwingCursor).forEachRemaining(any());
        ChangeStreamIterable<Document> throwingIterable = mock(ChangeStreamIterable.class);
        when(throwingIterable.cursor()).thenReturn(throwingCursor);
        MongoCollection<Document> throwingCollection = mock(MongoCollection.class);
        when(throwingCollection.watch(anyList(), eq(Document.class))).thenReturn(throwingIterable);
        return throwingCollection;
    }

    private static MongoCommandException changeStreamHistoryLostException() {
        List<BsonElement> elements = new ArrayList<>();
        elements.add(new BsonElement("code", new BsonInt32(286)));
        elements.add(new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")));
        return new MongoCommandException(new BsonDocument(elements), new ServerAddress());
    }

    /**
     * Simulates the class of error a driver surfaces during a replica-set primary election/failover: the change
     * stream cursor becomes unusable and a socket-level read fails until a new primary is elected.
     */
    private static MongoSocketReadException failoverLikeException() {
        return new MongoSocketReadException("expected: simulated primary election/failover", new ServerAddress(), new java.io.IOException("Connection reset by peer"));
    }

    @Nested
    @DisplayName("ChangeStreamHistoryLost")
    class ChangeStreamHistoryLostTest {

        @Test
        void restarts_subscription_from_now_when_configured_to_do_so() {
            // Given
            MongoCollection<Document> throwingCollection = collectionThatFailsOnce(changeStreamHistoryLostException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true).retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }

        @Test
        void removes_the_subscription_entry_when_history_is_lost_mid_subscription_and_not_configured_to_restart() {
            // Given: unlike collectionThatFailsOnce, the failure happens once the cursor is already registered in
            // runningSubscriptions, proving the entry isn't leaked once the subscription gives up.
            MongoCollection<Document> throwingCollection = collectionThatFailsDuringIteration(changeStreamHistoryLostException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String subscriptionId = UUID.randomUUID().toString();

            // When
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(2));

            // Then
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
                assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
            });
            // The strongest proof the entry isn't leaked: the id is free to reuse. If it were still in either map,
            // this would throw IllegalArgumentException("Subscription ... is already defined.").
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(2));
        }

        @Test
        void does_not_restart_subscription_when_not_configured_to_do_so() {
            // Given
            MongoCollection<Document> throwingCollection = collectionThatFailsOnce(changeStreamHistoryLostException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            boolean started = subscriptionModel.subscribe(subscriptionId, state::add).waitUntilStarted(Duration.ofSeconds(2));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            assertThat(started).isFalse();
            await().atMost(Duration.ofSeconds(1)).during(Duration.ofMillis(500)).untilAsserted(() -> assertThat(state).isEmpty());
            assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse();
        }
    }

    @Nested
    @DisplayName("Failover / transient errors")
    class FailoverTest {

        @Test
        void restarts_and_resumes_gap_free_after_a_failover_like_error() {
            // Given
            MongoCollection<Document> throwingCollection = collectionThatFailsOnce(failoverLikeException());
            subscriptionModel = new NativeMongoSubscriptionModel(database, throwingCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }
    }

    @Nested
    @DisplayName("Pause/resume")
    class PauseResumeTest {

        @Test
        void resume_continues_from_last_delivered_position_instead_of_replaying_from_the_original_start_at() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, subscriptionExecutor,
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            // StartAt.now() is the scenario the Spring "supplier" fix targets: reusing a stale position on resume
            // must not replay (or skip) events relative to where the subscription actually left off.
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            mongoEventStore.write("1", 1, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2")));
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

            // Then: the event written while paused is delivered exactly once after resume, and the first event is
            // not redelivered.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));
            assertThat(state).extracting(CloudEvent::getType).containsExactly(NameDefined.class.getName(), NameWasChanged.class.getName());
        }
    }

    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
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
