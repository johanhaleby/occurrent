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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.github.artsok.RepeatedIfExceptionsTest;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoJsonFilterSpecification;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.functional.Not.not;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.FULL_DOCUMENT;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoBsonFilterSpecification.filter;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
public class SpringMongoSubscriptionPositionStorageTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private EventStore mongoEventStore;
    private DurableSubscriptionModel subscriptionModel;
    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;
    private MongoClient mongoClient;
    private SpringMongoSubscriptionModel springMongoSubscriptionModel;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        TimeRepresentation timeRepresentation = RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(connectionString.getCollection()).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        springMongoSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, connectionString.getCollection(), timeRepresentation);
        SpringMongoSubscriptionPositionStorage storage = new SpringMongoSubscriptionPositionStorage(mongoTemplate, RESUME_TOKEN_COLLECTION);
        this.subscriptionModel = new DurableSubscriptionModel(springMongoSubscriptionModel, storage);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        mongoClient.close();
    }

    @Test
    void blocking_spring_subscription_calls_listener_for_each_new_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(4, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void blocking_spring_subscription_retries_failed_writes() {
        // Given
        AtomicInteger counter = new AtomicInteger();

        SpringMongoSubscriptionPositionStorage storage = new SpringMongoSubscriptionPositionStorage(mongoTemplate, RESUME_TOKEN_COLLECTION) {

            @Override
            void persistDocumentStreamPosition(String subscriptionId, Document document) {
                if (counter.getAndIncrement() == 0) {
                    throw new IllegalStateException("expected");
                }
                super.persistDocumentStreamPosition(subscriptionId, document);
            }
        };

        StringBasedSubscriptionPosition expectedPosition = new StringBasedSubscriptionPosition("hello");

        // When
        storage.save("subscriptionId", expectedPosition);

        // Then
        SubscriptionPosition actualPosition = storage.read("subscriptionId");
        assertThat(actualPosition).isEqualTo(expectedPosition);
    }

    @Test
    void blocking_spring_subscription_stores_every_event_by_default() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        AtomicInteger numberOfWritesToBlockingSubscriptionStorage = new AtomicInteger(0);
        SubscriptionPositionStorage storage = new SubscriptionPositionStorage() {

            @Override
            public SubscriptionPosition read(String subscriptionId) {
                return null;
            }

            @Override
            public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
                numberOfWritesToBlockingSubscriptionStorage.incrementAndGet();
                return subscriptionPosition;
            }

            @Override
            public void delete(String subscriptionId) {

            }

            @Override
            public boolean exists(String subscriptionId) {
                return false;
            }
        };
        subscriptionModel = new DurableSubscriptionModel(springMongoSubscriptionModel, storage);
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(4, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            assertThat(state).hasSize(3);
            assertThat(numberOfWritesToBlockingSubscriptionStorage).hasValue(4); // 3 events and one for global subscription position
        });
    }

    @Test
    void blocking_spring_subscription_stores_every_n_events_was_defined_config() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        AtomicInteger numberOfWritesToBlockingSubscriptionStorage = new AtomicInteger(0);
        SubscriptionPositionStorage storage = new SubscriptionPositionStorage() {

            @Override
            public SubscriptionPosition read(String subscriptionId) {
                return null;
            }

            @Override
            public SubscriptionPosition save(String subscriptionId, SubscriptionPosition subscriptionPosition) {
                numberOfWritesToBlockingSubscriptionStorage.incrementAndGet();
                return subscriptionPosition;
            }

            @Override
            public void delete(String subscriptionId) {

            }

            @Override
            public boolean exists(String subscriptionId) {
                return false;
            }
        };
        subscriptionModel = new DurableSubscriptionModel(springMongoSubscriptionModel, storage, new DurableSubscriptionModelConfig(3));
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(4, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            assertThat(state).hasSize(3);
            assertThat(numberOfWritesToBlockingSubscriptionStorage).hasValue(2); // 1 event and one for global subscription position
        });
    }

    @RepeatedIfExceptionsTest(repeats = 2, suspend = 500)
    void blocking_spring_subscription_allows_resuming_events_from_where_it_left_off() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        cancelSubscription(subscriptionModel, subscriberId);
        // The subscription is async so we need to wait for it
        await("state not to be empty").atMost(4, SECONDS).until(not(state::isEmpty));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));
        subscriptionModel.subscribe(subscriberId, state::add);

        // Then
        await("state to has size equal to 3").atMost(5, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void blocking_spring_subscription_allows_resuming_events_from_where_it_left_when_first_event_for_subscription_fails_the_first_time() {
        // Given
        LocalDateTime now = LocalDateTime.now();

        AtomicInteger counter = new AtomicInteger();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();

        // We recreate the subscription model with a retry strategy of "none" so that we can fail and not retry when exceptions are thrown
        Supplier<DurableSubscriptionModel> createSubscriptionModelWithoutRetry = () -> {
            SpringMongoSubscriptionModel springMongoSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, "events", RFC_3339_STRING, RetryStrategy.none());
            SpringMongoSubscriptionPositionStorage storage = new SpringMongoSubscriptionPositionStorage(mongoTemplate, RESUME_TOKEN_COLLECTION);
            return new DurableSubscriptionModel(springMongoSubscriptionModel, storage);
        };

        subscriptionModel = createSubscriptionModelWithoutRetry.get();
        Consumer<SubscriptionModel> stream = subscriptionModel -> subscriptionModel.subscribe(subscriberId, cloudEvent -> {
            if (counter.incrementAndGet() == 1) {
                // We simulate error on first event
                throw new IllegalArgumentException("Expected");
            } else {
                state.add(cloudEvent);
            }
        }).waitUntilStarted();

        stream.accept(subscriptionModel);
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        // The subscription is async so we need to wait for it
        await().atMost(ONE_SECOND).and().dontCatchUncaughtExceptions().untilAtomic(counter, equalTo(1));
        // Since an exception occurred we need to run the stream again, but first we need to close the old subscription
        subscriptionModel.shutdown();
        subscriptionModel = createSubscriptionModelWithoutRetry.get();
        stream.accept(subscriptionModel);
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(4, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @RepeatedIfExceptionsTest(repeats = 2, suspend = 500)
    void blocking_spring_subscription_allows_cancelling_subscription() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        // The subscription is async so we need to wait for it
        await().atMost(ONE_SECOND).until(not(state::isEmpty));
        subscriptionModel.cancelSubscription(subscriberId);

        // Then
        assertThat(mongoTemplate.getCollection(RESUME_TOKEN_COLLECTION).countDocuments()).isZero();
    }

    @Test
    void using_bson_query_for_type() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriberId, filter().type(Filters::eq, NameDefined.class.getName()), state::add)
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

        subscriptionModel.subscribe(subscriberId, filter().id(Filters::eq, nameDefined2.getEventId()).type(Filters::eq, NameDefined.class.getName()), state::add
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

        subscriptionModel.subscribe(subscriberId, filter(match(and(eq("fullDocument.id", nameDefined2.getEventId()), eq("fullDocument.type", NameDefined.class.getName())))), state::add
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
    void using_json_query_for_type() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriberId, MongoJsonFilterSpecification.filter("{ $match : { \"" + FULL_DOCUMENT + ".type\" : \"" + NameDefined.class.getName() + "\" } }"), state::add)
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

    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }

    private static void cancelSubscription(DelegatingSubscriptionModel subscriptionModel, String subscriberId) {
        subscriptionModel.getDelegatedSubscriptionModelRecursively().cancelSubscription(subscriberId);
    }
}