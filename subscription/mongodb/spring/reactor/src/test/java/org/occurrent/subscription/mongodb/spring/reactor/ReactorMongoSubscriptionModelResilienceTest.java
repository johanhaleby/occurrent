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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * Tests that {@link ReactorMongoSubscriptionModel} survives the same class of MongoDB operational failures
 * (leader elections/failovers, transient network errors, change stream history lost) that
 * {@code SpringMongoSubscriptionModel} has been hardened against in production, and that it recovers gap-free.
 */
@Testcontainers
@Timeout(20)
public class ReactorMongoSubscriptionModelResilienceTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                    .withReplicaSet()
                    .withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactiveresilience"));

    private MongoClient mongoClient;
    private ReactiveMongoOperations realMongoOperations;
    private ReactorMongoEventStore mongoEventStore;
    private ObjectMapper objectMapper;
    private CopyOnWriteArrayList<Disposable> disposables;

    @BeforeEach
    void createEventStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactiveresilience");
        mongoClient = MongoClients.create(connectionString);
        realMongoOperations = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore((ReactiveMongoTemplate) realMongoOperations, eventStoreConfig);
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void shutdown() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    /**
     * Wraps {@code realMongoOperations} so that the very first {@code changeStream(...)} call errors with
     * {@code exception}, simulating a change-stream disruption (a failover, a transient network error, history
     * lost, ...), while every subsequent call behaves exactly like the real operations. Mirrors how
     * {@code SpringMongoSubscriptionModelTest} and {@code NativeMongoSubscriptionModelResilienceTest} inject the
     * same class of failure.
     */
    @SuppressWarnings("unchecked")
    private ReactiveMongoOperations operationsThatFailOnce(RuntimeException exception) {
        ReactiveMongoOperations throwingOperations = mock(ReactiveMongoOperations.class);
        when(throwingOperations.changeStream(eq("events"), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenReturn(Flux.error(exception))
                .thenAnswer(invocation -> realMongoOperations.changeStream("events", invocation.getArgument(1), Document.class));
        return throwingOperations;
    }

    /**
     * Wraps {@code realMongoOperations} so that the very first {@code changeStream(...)} call delivers exactly one
     * real event and then errors with {@code exception}, simulating a disruption that happens right after a
     * subscription has genuinely delivered something, while every subsequent call behaves exactly like the real
     * operations. Used to prove a retry resumes from the position of that delivered event rather than replaying it.
     */
    @SuppressWarnings("unchecked")
    private ReactiveMongoOperations operationsThatFailAfterDeliveringOneEvent(RuntimeException exception) {
        ReactiveMongoOperations throwingOperations = mock(ReactiveMongoOperations.class);
        when(throwingOperations.changeStream(eq("events"), any(ChangeStreamOptions.class), eq(Document.class)))
                .thenAnswer(invocation -> realMongoOperations.changeStream("events", invocation.getArgument(1), Document.class).take(1).concatWith(Flux.error(exception)))
                .thenAnswer(invocation -> realMongoOperations.changeStream("events", invocation.getArgument(1), Document.class));
        return throwingOperations;
    }

    // Wrapped in UncategorizedMongoDbException since that's how Spring Data actually translates driver exceptions,
    // exercising the same unwrap branch in ReactorMongoSubscriptionModel.isChangeStreamHistoryLost(...) that a raw
    // MongoCommandException would skip. Mirrors SpringMongoSubscriptionModelTest's equivalent injection.
    private static UncategorizedMongoDbException changeStreamHistoryLostException() {
        List<BsonElement> elements = new ArrayList<>();
        elements.add(new BsonElement("code", new BsonInt32(286)));
        elements.add(new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")));
        return new UncategorizedMongoDbException("expected", new MongoCommandException(new BsonDocument(elements), new ServerAddress()));
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
            ReactiveMongoOperations throwingOperations = operationsThatFailOnce(changeStreamHistoryLostException());
            ReactorMongoSubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(throwingOperations, "events", TimeRepresentation.RFC_3339_STRING,
                    ReactorMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(true).backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            disposables.add(subscriptionModel.subscribe().subscribe(state::add));

            // When: wait for the retry to actually happen (the second changeStream(...) call, delegating to the real
            // operations) before writing, since the retry is scheduled asynchronously with backoff and there is no
            // readiness signal on the bare Flux to synchronize against otherwise.
            verify(throwingOperations, timeout(5000).times(2)).changeStream(eq("events"), any(ChangeStreamOptions.class), eq(Document.class));

            // Then: keep writing new events until one is observed. The restart resumes from "now" with no resume
            // marker, so a single write can race ahead of the server actually establishing the new cursor and be
            // missed for good instead of just delayed.
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() -> {
                if (state.isEmpty()) {
                    mongoEventStore.write(UUID.randomUUID().toString(), 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();
                }
                assertThat(state).isNotEmpty();
            });
        }

        @Test
        void does_not_restart_subscription_when_not_configured_to_do_so() {
            // Given
            ReactiveMongoOperations throwingOperations = operationsThatFailOnce(changeStreamHistoryLostException());
            ReactorMongoSubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(throwingOperations, "events", TimeRepresentation.RFC_3339_STRING,
                    ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            disposables.add(subscriptionModel.subscribe().subscribe(state::add, errors::add));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();

            // Then: wait for the terminal error signal, the definitive point delivery has stopped, instead of a fixed sleep.
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(errors).hasSize(1));
            assertThat(state).isEmpty();
        }
    }

    @Nested
    @DisplayName("Failover / transient errors")
    class FailoverTest {

        @Test
        void restarts_and_resumes_gap_free_after_a_failover_like_error() {
            // Given
            ReactiveMongoOperations throwingOperations = operationsThatFailOnce(failoverLikeException());
            ReactorMongoSubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(throwingOperations, "events", TimeRepresentation.RFC_3339_STRING,
                    ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            disposables.add(subscriptionModel.subscribe().subscribe(state::add));

            // When: wait for the retry to actually happen before writing, see the comment in ChangeStreamHistoryLostTest.
            verify(throwingOperations, timeout(5000).times(2)).changeStream(eq("events"), any(ChangeStreamOptions.class), eq(Document.class));

            // Then: keep writing new events until one is observed, see the comment in ChangeStreamHistoryLostTest.
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(100, MILLIS)).untilAsserted(() -> {
                if (state.isEmpty()) {
                    mongoEventStore.write(UUID.randomUUID().toString(), 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();
                }
                assertThat(state).isNotEmpty();
            });
        }
    }

    @Nested
    @DisplayName("Position tracking")
    class PositionTrackingTest {

        @Test
        void tracks_the_position_of_the_last_change_stream_document_received_so_a_retry_does_not_replay_it() {
            // Given: subscribe from an explicit position (an operation time from before event 1 exists, not "now",
            // which is a no-op that applies no explicit position at all) so every (re)connect genuinely resumes from
            // whatever position is currently tracked, rather than opening a plain, position-less change stream that
            // happens to pick up recent writes regardless of tracking. The first changeStream(...) call delivers
            // event 1 for real, then errors, forcing a retry.
            LocalDateTime now = LocalDateTime.now();
            ReactorMongoSubscriptionModel realModel = new ReactorMongoSubscriptionModel(realMongoOperations, "events", TimeRepresentation.RFC_3339_STRING);
            StartAt beforeEvent1 = StartAt.subscriptionPosition(realModel.globalSubscriptionPosition()
                    .blockOptional()
                    .orElseThrow(() -> new IllegalStateException("globalSubscriptionPosition() completed empty, the server may be prohibiting the hostInfo command")));
            ReactiveMongoOperations throwingOperations = operationsThatFailAfterDeliveringOneEvent(failoverLikeException());
            ReactorMongoSubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(throwingOperations, "events", TimeRepresentation.RFC_3339_STRING,
                    ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)));

            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            disposables.add(subscriptionModel.subscribe(beforeEvent1).subscribe(state::add));

            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"))).block();
            await().atMost(5, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

            // When: the retry's changeStream(...) call (the second one) has to actually happen before writing event
            // 2, so the retry's own resumeAt/startAfter position is what's being exercised here, not the original.
            verify(throwingOperations, timeout(5000).times(2)).changeStream(eq("events"), any(ChangeStreamOptions.class), eq(Document.class));
            mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(1), "name2", "name2"))).block();

            // Then: event 2 is delivered, and event 1 is not replayed by the retry resuming from the original,
            // pre-event-1 position instead of the tracked one.
            await().atMost(5, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));
            assertThat(state).extracting(CloudEvent::getId).doesNotHaveDuplicates();
        }
    }

    @Nested
    @DisplayName("waitUntilStarted")
    class WaitUntilStartedTest {

        @Test
        void fails_instead_of_hanging_when_the_change_stream_cannot_even_be_created() {
            // Given: changeStream(...) throws synchronously (not a Flux that errors on subscription), simulating a
            // failure while building the change stream itself, e.g. an invalid filter. doOnSubscribe never gets a
            // chance to complete the started signal since the change stream Flux is never created to subscribe to.
            // Uses a non-restartable error (history lost, restart-on-history-lost off by default) so the failure is
            // terminal instead of retried forever, matching the only way a real failure here can actually terminate.
            UncategorizedMongoDbException historyLost = changeStreamHistoryLostException();
            ReactiveMongoOperations throwingOperations = mock(ReactiveMongoOperations.class);
            when(throwingOperations.changeStream(eq("events"), any(ChangeStreamOptions.class), eq(Document.class))).thenThrow(historyLost);
            ReactorMongoSubscriptionModel subscriptionModel = new ReactorMongoSubscriptionModel(throwingOperations, "events", TimeRepresentation.RFC_3339_STRING);

            // When
            Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> Mono.empty());

            // Then
            StepVerifier.create(subscription.waitUntilStarted())
                    .expectErrorMatches(throwable -> throwable == historyLost)
                    .verify(Duration.ofSeconds(5));
        }
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
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
