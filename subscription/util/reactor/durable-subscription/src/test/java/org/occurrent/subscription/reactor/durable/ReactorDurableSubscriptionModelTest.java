/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.subscription.reactor.durable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.functional.Not.not;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class ReactorDurableSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    private EventStore mongoEventStore;
    private ReactorDurableSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private CheckpointStorage storage;

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));
    private MongoClient mongoClient;
    private ReactorMongoSubscriptionModel springReactorSubscriptionForMongoDB;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        mongoClient = MongoClients.create(connectionString);
        reactiveMongoTemplate = new ReactiveMongoTemplate(MongoClients.create(connectionString), Objects.requireNonNull(connectionString.getDatabase()));
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        springReactorSubscriptionForMongoDB = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events", timeRepresentation);
        storage = new ReactorCheckpointStorage(reactiveMongoTemplate, RESUME_TOKEN_COLLECTION);
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, storage);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void dispose() {
        if (subscription != null) {
            subscription.shutdown();
        }
        mongoClient.close();
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_calls_action_for_each_new_event() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscription.subscribe("test", cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)));
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_stores_every_event_position_by_default() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        AtomicInteger numberOfWritesToSubscriptionStorage = new AtomicInteger(0);

        CheckpointStorage checkpointStorage = new CheckpointStorage() {
            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return Mono.empty();
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                return Mono.fromRunnable(numberOfWritesToSubscriptionStorage::incrementAndGet).thenReturn(checkpoint);
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.empty();
            }
        };
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, checkpointStorage);
        subscription.subscribe(UUID.randomUUID().toString(), e -> Mono.fromRunnable(() -> state.add(e)));

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_stores_every_n_events_as_defined_in_config() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        AtomicInteger numberOfWritesToSubscriptionStorage = new AtomicInteger(0);

        CheckpointStorage checkpointStorage = new CheckpointStorage() {
            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return Mono.empty();
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                return Mono.fromRunnable(numberOfWritesToSubscriptionStorage::incrementAndGet).thenReturn(checkpoint);
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.empty();
            }
        };
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, checkpointStorage, new ReactorDurableSubscriptionModelConfig(3));
        subscription.subscribe(UUID.randomUUID().toString(), e -> Mono.fromRunnable(() -> state.add(e)));

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            assertThat(state).hasSize(3);
            assertThat(numberOfWritesToSubscriptionStorage).hasValue(2); // 1 event and one for global checkpoint
        });
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_allows_resuming_events_from_where_it_left_off() throws Exception {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscription.subscribe(subscriberId, cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)));
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        // The subscription is async so we need to wait for it
        await().atMost(Durations.ONE_SECOND).until(not(state::isEmpty));
        // Simulate a restart: shut the model down and start a fresh one sharing the same position storage so it resumes
        // from the persisted position rather than from the original start.
        subscription.shutdown();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
        // Shutting the durable model down also shuts down the model it wraps, because the subscriptions live there
        // now, so a restart builds a new one of those too. That is what a restarted application does anyway.
        springReactorSubscriptionForMongoDB = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, storage);
        subscription.subscribe(subscriberId, cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)));

        // Then
        await().atMost(Durations.TWO_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_allows_resuming_events_from_where_it_left_when_first_event_for_subscription_fails_the_first_time() {
        // Given
        LocalDateTime now = LocalDateTime.now();

        AtomicInteger counter = new AtomicInteger();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        Runnable stream = () -> subscription.subscribe(subscriberId, cloudEvent -> {
            if (counter.incrementAndGet() == 1) {
                // We simulate error on first event
                return Mono.error(new IllegalArgumentException("Expected"));
            } else {
                state.add(cloudEvent);
                return Mono.empty();
            }
        });
        stream.run();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        // The subscription is async so we need to wait for it
        await().atMost(Durations.ONE_SECOND).and().dontCatchUncaughtExceptions().untilAtomic(counter, equalTo(1));
        // Nothing re-subscribes here on purpose. The wrapped model retries the action, so the event that failed is
        // delivered on a later attempt and the subscription keeps its id.
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
        assertThat(subscription.isRunning(subscriberId)).as("the subscription survived the action that failed").isTrue();
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getSimpleName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}
