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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.functional.Not.not;
import static org.occurrent.time.TimeConversion.toLocalDateTime;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@Timeout(20)
@DisplayNameGeneration(DisplayNameGenerator.Simple.class)
@Testcontainers
public class ReactorCheckpointStorageTest {

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    @Container
    private static final MongoDBContainer mongoDBContainer =
            new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                    .withReplicaSet()
                    .withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";
    private static final String ID_FIELD = "_id";
    private static final String CHECKPOINT_FIELD = "checkpoint";
    private static final String LEGACY_CHECKPOINT_FIELD = "subscriptionPosition";

    private EventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private CopyOnWriteArrayList<Disposable> disposables;
    private MongoClient mongoClient;
    private ReactorCheckpointStorage storage;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        mongoClient = MongoClients.create(connectionString);
        reactiveMongoTemplate = new ReactiveMongoTemplate(MongoClients.create(connectionString), Objects.requireNonNull(connectionString.getDatabase()));
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        storage = new ReactorCheckpointStorage(reactiveMongoTemplate, RESUME_TOKEN_COLLECTION);
        subscription = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events", timeRepresentation);
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @Nested
    @DisplayName("legacy subscriptionPosition field migration")
    class LegacyCheckpointFieldMigration {

        @Test
        void reads_legacy_subscription_position_field_as_string_based_checkpoint() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            reactiveMongoTemplate.getCollection(RESUME_TOKEN_COLLECTION)
                    .flatMap(collection -> Mono.from(collection.insertOne(
                            new Document(ID_FIELD, subscriptionId).append(LEGACY_CHECKPOINT_FIELD, "legacy-position-value"))))
                    .block();

            // When
            Checkpoint checkpoint = storage.read(subscriptionId).block();

            // Then
            assertThat(checkpoint).isInstanceOfSatisfying(StringBasedCheckpoint.class,
                    stringBasedCheckpoint -> assertThat(stringBasedCheckpoint.asString()).isEqualTo("legacy-position-value"));
        }

        @Test
        void save_removes_legacy_subscription_position_field_and_writes_new_checkpoint_field() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            reactiveMongoTemplate.getCollection(RESUME_TOKEN_COLLECTION)
                    .flatMap(collection -> Mono.from(collection.insertOne(
                            new Document(ID_FIELD, subscriptionId).append(LEGACY_CHECKPOINT_FIELD, "legacy-position-value"))))
                    .block();

            // When
            storage.save(subscriptionId, new StringBasedCheckpoint("new-position-value")).block();

            // Then
            Document rawDocument = reactiveMongoTemplate.findOne(new Query(where(ID_FIELD).is(subscriptionId)), Document.class, RESUME_TOKEN_COLLECTION).block();
            assertAll(
                    () -> assertThat(rawDocument.getString(CHECKPOINT_FIELD)).isEqualTo("new-position-value"),
                    () -> assertThat(rawDocument.containsKey(LEGACY_CHECKPOINT_FIELD)).isFalse()
            );
        }

        @Test
        void save_and_read_round_trips_string_based_checkpoint_using_new_checkpoint_field() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            StringBasedCheckpoint expected = new StringBasedCheckpoint("brand-new-value");

            // When
            storage.save(subscriptionId, expected).block();
            Checkpoint actual = storage.read(subscriptionId).block();

            // Then
            Document rawDocument = reactiveMongoTemplate.findOne(new Query(where(ID_FIELD).is(subscriptionId)), Document.class, RESUME_TOKEN_COLLECTION).block();
            assertAll(
                    () -> assertThat(actual).isEqualTo(expected),
                    () -> assertThat(rawDocument.getString(CHECKPOINT_FIELD)).isEqualTo("brand-new-value"),
                    () -> assertThat(rawDocument.containsKey(LEGACY_CHECKPOINT_FIELD)).isFalse()
            );
        }

        @Test
        void saving_twice_is_idempotent_and_never_reintroduces_legacy_field() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();

            // When
            storage.save(subscriptionId, new StringBasedCheckpoint("first-value")).block();
            storage.save(subscriptionId, new StringBasedCheckpoint("second-value")).block();

            // Then
            Document rawDocument = reactiveMongoTemplate.findOne(new Query(where(ID_FIELD).is(subscriptionId)), Document.class, RESUME_TOKEN_COLLECTION).block();
            assertAll(
                    () -> assertThat(rawDocument.getString(CHECKPOINT_FIELD)).isEqualTo("second-value"),
                    () -> assertThat(rawDocument.containsKey(LEGACY_CHECKPOINT_FIELD)).isFalse()
            );
        }
    }

    @RepeatedIfExceptionsTest(repeats = 2)
    void reactive_persistent_spring_subscription_allows_deleting_subscription_position() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        disposeAfterTest(subscription.subscribe().flatMap(ce -> {
            state.add(ce);
            return storage.save(subscriberId, CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(ce));
        }).subscribe());
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        // The subscription is async so we need to wait for it
        await().atMost(ONE_SECOND).until(not(state::isEmpty));

        storage.delete(subscriberId).block();

        // Then
        assertThat(reactiveMongoTemplate.count(new Query(), RESUME_TOKEN_COLLECTION).block()).isZero();
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

    private void disposeAfterTest(Disposable disposable) {
        disposables.add(disposable);
    }
}