/*
 * Copyright 2020 Johan Haleby
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringBlockingMongoEventStore;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.functional.Not;
import org.occurrent.subscription.mongodb.MongoDBFilterSpecification;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.time.TimeConversion;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.OccurrentSubscriptionFilter;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.is;
import static org.occurrent.functional.Not.not;
import static org.occurrent.subscription.mongodb.MongoDBFilterSpecification.BsonMongoDBFilterSpecification.filter;

@Testcontainers
public class SpringBlockingSubscriptionForMongoDBTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8");
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private EventStore mongoEventStore;
    private SpringBlockingSubscriptionForMongoDB subscription;
    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).build();
        mongoEventStore = new SpringBlockingMongoEventStore(mongoTemplate, eventStoreConfig);
        subscription = new SpringBlockingSubscriptionForMongoDB(mongoTemplate, connectionString.getCollection(), timeRepresentation);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        subscription.shutdown();
    }

    @Test
    void blocking_spring_subscription_calls_listener_for_each_new_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscription.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void blocking_spring_subscription_allows_cancelling_subscription() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscription.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        // The subscription is async so we need to wait for it
        await().atMost(ONE_SECOND).until(Not.not(state::isEmpty));
        subscription.cancelSubscription(subscriberId);

        // Then
        assertThat(mongoTemplate.getCollection(RESUME_TOKEN_COLLECTION).countDocuments()).isZero();
    }

    @Nested
    @DisplayName("SubscriptionFilter for BsonMongoDBFilterSpecification")
    class BsonMongoDBFilterSpecificationTest {
        @Test
        void using_bson_query_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscription.subscribe(subscriberId, MongoDBFilterSpecification.BsonMongoDBFilterSpecification.filter().type(Filters::eq, NameDefined.class.getName()), state::add)
                    .waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getName());
        }

        @Test
        void using_bson_query_dsl_composition() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            subscription.subscribe(subscriberId, MongoDBFilterSpecification.BsonMongoDBFilterSpecification.filter().id(Filters::eq, nameDefined2.getEventId()).type(Filters::eq, NameDefined.class.getName()), state::add
            )
                    .waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.getEventId(), NameDefined.class.getName()));
        }

        @Test
        void using_bson_query_native_mongo_filters_composition() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            subscription.subscribe(subscriberId, MongoDBFilterSpecification.BsonMongoDBFilterSpecification.filter(match(and(eq("fullDocument.id", nameDefined2.getEventId()), eq("fullDocument.type", NameDefined.class.getName())))), state::add
            )
                    .waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.getEventId(), NameDefined.class.getName()));
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter for JsonMongoDBFilterSpecification")
    class JsonMongoDBFilterSpecificationTest {
        @Test
        void using_json_query_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscription.subscribe(subscriberId, MongoDBFilterSpecification.JsonMongoDBFilterSpecification.filter("{ $match : { \"" + MongoDBFilterSpecification.FULL_DOCUMENT + ".type\" : \"" + NameDefined.class.getName() + "\" } }"), state::add)
                    .waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getName());
        }

    }

    @Nested
    @DisplayName("SubscriptionFilter using OccurrentSubscriptionFilter")
    class OccurrentSubscriptionFilterTest {

        @Test
        void using_occurrent_subscription_filter_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscription.subscribe(subscriberId, OccurrentSubscriptionFilter.filter(Filter.type(NameDefined.class.getName())), state::add).waitUntilStarted();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(2));
            assertThat(state).extracting(CloudEvent::getType).containsOnly(NameDefined.class.getName());
        }

        @Test
        void using_occurrent_subscription_filter_dsl_composition() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name4");

            Filter filter = Filter.id(nameDefined2.getEventId()).and(Filter.type(NameDefined.class.getName()));
            subscription.subscribe(subscriberId, OccurrentSubscriptionFilter.filter(filter), state::add).waitUntilStarted();

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.getEventId(), NameDefined.class.getName()));
        }
    }


    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.getTimestamp()).atZone(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}