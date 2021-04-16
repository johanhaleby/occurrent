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
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.reactor.SubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorSubscriptionPositionStorage;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    private EventStore mongoEventStore;
    private ReactorDurableSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private CopyOnWriteArrayList<Disposable> disposables;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
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
        SubscriptionPositionStorage storage = new ReactorSubscriptionPositionStorage(reactiveMongoTemplate, RESUME_TOKEN_COLLECTION);
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, storage);
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_calls_action_for_each_new_event() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        disposeAfterTest(subscription.subscribe("test", cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent))).subscribe());
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

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
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        AtomicInteger numberOfWritesToSubscriptionStorage = new AtomicInteger(0);

        SubscriptionPositionStorage subscriptionPositionStorage = new SubscriptionPositionStorage() {
            @Override
            public Mono<SubscriptionPosition> read(String subscriptionId) {
                return Mono.empty();
            }

            @Override
            public Mono<SubscriptionPosition> save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
                return Mono.fromRunnable(numberOfWritesToSubscriptionStorage::incrementAndGet).thenReturn(subscriptionPosition);
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.empty();
            }
        };
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, subscriptionPositionStorage);
        disposeAfterTest(subscription.subscribe(UUID.randomUUID().toString(), e -> Mono.fromRunnable(() -> state.add(e))).subscribe());

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
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        AtomicInteger numberOfWritesToSubscriptionStorage = new AtomicInteger(0);

        SubscriptionPositionStorage subscriptionPositionStorage = new SubscriptionPositionStorage() {
            @Override
            public Mono<SubscriptionPosition> read(String subscriptionId) {
                return Mono.empty();
            }

            @Override
            public Mono<SubscriptionPosition> save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
                return Mono.fromRunnable(numberOfWritesToSubscriptionStorage::incrementAndGet).thenReturn(subscriptionPosition);
            }

            @Override
            public Mono<Void> delete(String subscriptionId) {
                return Mono.empty();
            }
        };
        subscription = new ReactorDurableSubscriptionModel(springReactorSubscriptionForMongoDB, subscriptionPositionStorage, new ReactorDurableSubscriptionModelConfig(3));
        disposeAfterTest(subscription.subscribe(UUID.randomUUID().toString(), e -> Mono.fromRunnable(() -> state.add(e))).subscribe());

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            assertThat(state).hasSize(3);
            assertThat(numberOfWritesToSubscriptionStorage).hasValue(2); // 1 event and one for global subscription position
        });
    }

    @Test
    void reactor_subscription_with_automatic_position_persistence_allows_resuming_events_from_where_it_left_off() throws Exception {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        Function<CloudEvent, Mono<Void>> function = cloudEvents -> Mono.fromRunnable(() -> state.add(cloudEvents));
        Disposable subscription1 = disposeAfterTest(subscription.subscribe(subscriberId, function).subscribe());
        Thread.sleep(200);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        // The subscription is async so we need to wait for it
        await().atMost(Durations.ONE_SECOND).until(not(state::isEmpty));
        subscription1.dispose();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
        disposeAfterTest(subscription.subscribe(subscriberId, function).subscribe());

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
        }).subscribe();
        stream.run();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
        // The subscription is async so we need to wait for it
        await().atMost(Durations.ONE_SECOND).and().dontCatchUncaughtExceptions().untilAtomic(counter, equalTo(1));
        // Since an exception occurred we need to run the stream again
        stream.run();
        mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
        mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
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

    private Disposable disposeAfterTest(Disposable disposable) {
        disposables.add(disposable);
        return disposable;
    }
}