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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.PositionAwareCloudEvent;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

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
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.functional.Not.not;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class ReactorSubscriptionPositionStorageTest {

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    private EventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private CopyOnWriteArrayList<Disposable> disposables;
    private MongoClient mongoClient;
    private ReactorSubscriptionPositionStorage storage;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        mongoClient = MongoClients.create(connectionString);
        reactiveMongoTemplate = new ReactiveMongoTemplate(MongoClients.create(connectionString), Objects.requireNonNull(connectionString.getDatabase()));
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        storage = new ReactorSubscriptionPositionStorage(reactiveMongoTemplate, RESUME_TOKEN_COLLECTION);
        subscription = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events", timeRepresentation);
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @RepeatedIfExceptionsTest(repeats = 2)
    void reactive_persistent_spring_subscription_allows_deleting_subscription_position() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        disposeAfterTest(subscription.subscribe().flatMap(ce -> {
            state.add(ce);
            return storage.save(subscriberId, PositionAwareCloudEvent.getSubscriptionPositionOrThrowIAE(ce));
        }).subscribe());
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");

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
                .withTime(toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }

    private void disposeAfterTest(Disposable disposable) {
        disposables.add(disposable);
    }
}