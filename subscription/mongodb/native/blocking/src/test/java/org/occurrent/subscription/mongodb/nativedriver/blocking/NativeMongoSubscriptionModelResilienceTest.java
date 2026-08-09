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
import com.mongodb.MongoSocketReadException;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.*;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.internal.ExecutorShutdown;
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
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
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
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

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

    /**
     * Tests ADR 116, "A refused write throws, and it must never be retried": a delivery action throwing
     * {@link CheckpointWriteConditionNotFulfilledException} must not be retried on either retry loop, the
     * subscription it belongs to must stay known and pausable rather than forgotten, and no other subscription on
     * the same model is affected.
     */
    @Nested
    @DisplayName("Checkpoint write refusal (ADR 116)")
    class CheckpointWriteRefusalTest {

        private CheckpointWriteConditionNotFulfilledException refusal(String subscriptionId) {
            return new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(5), CheckpointWriteCondition.notOlderThan(3));
        }

        /**
         * ADR 116 has the refusal reach "an executor's uncaught handler" once it is logged, on the outer restart
         * loop's dispatcher thread. Awaitility catches uncaught exceptions from every thread by default and
         * rethrows them into the polling {@code await()} call, which would otherwise turn this test's own
         * provoked, expected escape into a spurious failure. A thread factory that recognizes exactly this
         * exception keeps the assertion on the model's behaviour instead of on Awaitility's global safety net.
         */
        private ExecutorService dispatcherTolerantOfTheExpectedRefusalEscape() {
            return Executors.newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable);
                thread.setUncaughtExceptionHandler((t, throwable) -> {
                    if (!(throwable instanceof CheckpointWriteConditionNotFulfilledException)) {
                        throw new AssertionError("Unexpected uncaught exception on subscription dispatcher thread " + t, throwable);
                    }
                });
                return thread;
            });
        }

        @Test
        void delivery_action_throwing_the_refusal_is_invoked_exactly_once() {
            // Given
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherTolerantOfTheExpectedRefusalEscape(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String subscriptionId = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            // A seed event the action must deliver successfully, so the model's tracked change-stream position is a
            // concrete resume token before the target event arrives. Without it, a broken exclusion's restart would
            // resolve "start from now" at restart time and never rediscover the already-past target event, letting
            // this test pass by accident instead of by proving the exclusion holds.
            NameDefined seedEvent = new NameDefined(UUID.randomUUID().toString(), now, "seed", "seed");
            NameDefined targetEvent = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "target", "target");
            AtomicInteger targetInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(subscriptionId, event -> {
                if (event.getId().equals(targetEvent.eventId())) {
                    targetInvocations.incrementAndGet();
                    throw refusal(subscriptionId);
                }
                delivered.add(event);
            }).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(seedEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).hasSize(1));
            mongoEventStore.write("2", 0, serialize(targetEvent));

            // Then: exactly one invocation, and it stays that way well past what a retry backoff, or an unbounded
            // restart loop reopening the change stream from the same concrete position, would allow.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));
            // during(), not pollDelay()+atMost(): the latter only needs one truthy poll and returns as soon as it
            // sees one, so a redelivery landing between polls would go unnoticed. during() re-checks continuously
            // and fails the moment the count moves off 1, which is what "stays that way" actually means.
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));
        }

        @Test
        void subscription_stays_known_and_pausable_and_a_resume_redelivers_the_refused_event() {
            // Given
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherTolerantOfTheExpectedRefusalEscape(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String subscriptionId = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            // A seed event the action must deliver successfully, so the model's tracked change-stream position
            // is a concrete resume token before the target event arrives. Without it, the position would still be
            // the unresolved "start from now" it was subscribed with, since a refusal aborts the action before that
            // position is advanced, and a resume would then start from "now" at resume time, after the target event
            // rather than before it.
            NameDefined seedEvent = new NameDefined(UUID.randomUUID().toString(), now, "seed", "seed");
            NameDefined targetEvent = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "target", "target");
            AtomicInteger targetInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(subscriptionId, event -> {
                if (event.getId().equals(targetEvent.eventId()) && targetInvocations.getAndIncrement() == 0) {
                    throw refusal(subscriptionId);
                }
                delivered.add(event);
            }).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(seedEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).hasSize(1));
            mongoEventStore.write("2", 0, serialize(targetEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(targetInvocations.get()).isEqualTo(1));

            // Then: the subscription is still known (pause succeeds rather than throwing UnknownSubscriptionException
            // or SubscriptionNotRunningException), and a resume redelivers the event the refusal aborted.
            subscriptionModel.pauseSubscription(subscriptionId);
            assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).contains(targetEvent.eventId()));
            assertThat(targetInvocations.get()).isEqualTo(2);
        }

        @Test
        void a_refused_write_on_one_subscription_does_not_stop_delivery_on_another() {
            // Given
            subscriptionModel = new NativeMongoSubscriptionModel(database, realEventCollection, TimeRepresentation.RFC_3339_STRING, dispatcherTolerantOfTheExpectedRefusalEscape(),
                    NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(exponentialBackoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS), 2)));
            String refusedSubscriptionId = UUID.randomUUID().toString();
            String healthySubscriptionId = UUID.randomUUID().toString();
            AtomicInteger refusedInvocations = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> healthyState = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(refusedSubscriptionId, event -> {
                refusedInvocations.incrementAndGet();
                throw refusal(refusedSubscriptionId);
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.subscribe(healthySubscriptionId, healthyState::add).waitUntilStarted(Duration.ofSeconds(10));

            LocalDateTime now = LocalDateTime.now();

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
                assertThat(refusedInvocations.get()).isEqualTo(1);
                assertThat(healthyState).hasSize(1);
            });
            mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "name2", "name2")));

            // Then: the healthy subscription keeps delivering, and the refused one still hasn't retried.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(healthyState).hasSize(2));
            assertThat(refusedInvocations.get()).isEqualTo(1);
        }
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
