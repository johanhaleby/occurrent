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
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
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

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.is;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class ReactorMongoSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);

    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscription;
    private ObjectMapper objectMapper;
    private CopyOnWriteArrayList<Disposable> disposables;

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));
    private MongoClient mongoClient;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        ReactiveMongoTemplate reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, Objects.requireNonNull(connectionString.getDatabase()));
        subscription = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, "events", TimeRepresentation.RFC_3339_STRING);
        ReactiveTransactionManager reactiveMongoTransactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(reactiveMongoTransactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        objectMapper = new ObjectMapper();
        disposables = new CopyOnWriteArrayList<>();
    }

    @AfterEach
    void dispose() {
        disposables.forEach(Disposable::dispose);
        mongoClient.close();
    }

    @Test
    void reactive_spring_subscription_calls_listener_for_each_new_event() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        disposeAfterTest(subscription.subscribe().flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent))).subscribe());
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

    @Nested
    @DisplayName("SubscriptionFilter for BsonMongoDBFilterSpecification")
    class MongoBsonFilterSpecificationTest {
        @Test
        void using_bson_query_for_type_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscription.subscribe(MongoFilterSpecification.MongoBsonFilterSpecification.filter().type(Filters::eq, NameDefined.class.getSimpleName()))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();
            Thread.sleep(200);
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getSimpleName());
        }

        @Test
        void using_bson_query_dsl_composition_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            subscription.subscribe(MongoFilterSpecification.MongoBsonFilterSpecification.filter().id(Filters::eq, nameDefined2.getEventId()).and().type(Filters::eq, NameDefined.class.getSimpleName()))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.getEventId(), NameDefined.class.getSimpleName()));
        }

        @Test
        void using_bson_query_native_mongo_filters_composition_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            subscription.subscribe(MongoFilterSpecification.MongoBsonFilterSpecification.filter(match(and(eq("fullDocument.id", nameDefined2.getEventId()), eq("fullDocument.type", NameDefined.class.getSimpleName())))))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.getEventId(), NameDefined.class.getSimpleName()));
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter for JsonMongoDBFilterSpecification")
    class MongoJsonFilterSpecificationTest {
        @Test
        void using_json_query_for_type_with_reactive_spring_subscription() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscription.subscribe(MongoFilterSpecification.MongoJsonFilterSpecification.filter("{ $match : { \"" + MongoFilterSpecification.FULL_DOCUMENT + ".type\" : \"" + NameDefined.class.getSimpleName() + "\" } }"))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getSimpleName());
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter using OccurrentSubscriptionFilter")
    class OccurrentSubscriptionFilterTest {

        @Test
        void using_occurrent_subscription_filter_for_type() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();

            subscription.subscribe(OccurrentSubscriptionFilter.filter(Filter.type(NameDefined.class.getSimpleName())))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getSimpleName());
        }

        @Test
        void using_occurrent_subscription_filter_dsl_composition() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            Filter filter = Filter.id(nameDefined2.getEventId()).and(Filter.type(NameDefined.class.getSimpleName()));
            subscription.subscribe(OccurrentSubscriptionFilter.filter(filter))
                    .flatMap(cloudEvent -> Mono.fromRunnable(() -> state.add(cloudEvent)))
                    .subscribe();

            Thread.sleep(200);

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1)).block();
            mongoEventStore.write("1", 1, serialize(nameWasChanged1)).block();
            mongoEventStore.write("2", 0, serialize(nameDefined2)).block();
            mongoEventStore.write("2", 1, serialize(nameWasChanged2)).block();

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.getEventId(), NameDefined.class.getSimpleName()));
        }
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
                .withId(e.getEventId())
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