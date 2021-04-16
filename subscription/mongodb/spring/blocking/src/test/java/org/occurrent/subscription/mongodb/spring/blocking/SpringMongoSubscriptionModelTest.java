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
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.functional.Not;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.PositionAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionPosition;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.time.TimeConversion;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.groups.Tuple.tuple;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.occurrent.filter.Filter.all;
import static org.occurrent.filter.Filter.id;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoBsonFilterSpecification.filter;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

@Testcontainers
public class SpringMongoSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
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
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, connectionString.getCollection(), timeRepresentation);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
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
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void blocking_spring_subscription_calls_listeners_in_order() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), e -> {
            if (e.getId().equals("1")) {
                sleep(500);
            }
            state.add(e);
        }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        NameDefined nameDefined1 = new NameDefined("1", now, "name1");
        NameDefined nameDefined2 = new NameDefined("2", now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged("3", now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void resumes_stream_after_deletion_of_events_from_event_store() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // When

        // Now we delete the events
        mongoEventStore.delete(all());
        // And write some additional events
        mongoEventStore.write("1", 0, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(15), "name4")));
        mongoEventStore.write("3", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name5")));

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(5));
    }

    @Test
    void resumes_stream_after_deletion_of_event_that_subscription_has_not_received_yet() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        String eventId1 = UUID.randomUUID().toString();
        String eventId2 = UUID.randomUUID().toString();
        NameDefined nameDefined1 = new NameDefined(eventId1, now, "name1");
        NameDefined nameDefined2 = new NameDefined(eventId2, now.plusSeconds(2), "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));

        await("first event").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        SubscriptionPosition subscriptionPosition = PositionAwareCloudEvent.getSubscriptionPositionOrThrowIAE(state.get(0));

        subscriptionModel.cancelSubscription(subscriptionId);

        // When
        mongoEventStore.delete(id(eventId1)); // Delete event that subscription hasn't received yet
        assertThat(mongoEventStore.count()).isZero();

        // Write a new event and the resume subscription
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        subscriptionModel.subscribe(subscriptionId, StartAt.subscriptionPosition(subscriptionPosition), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
                    assertThat(state).hasSize(2);
                    assertThat(state.get(1).getId()).isEqualTo(eventId2);
                }
        );
    }

    @Test
    void blocking_spring_subscription_throws_iae_when_subscription_already_exists_and_subscription_model_is_started() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted();

        // When
        Throwable throwable = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted());

        // Then
        assertAll(
                () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Subscription " + subscriptionId + " is already defined."),
                () -> assertThat(subscriptionModel.isRunning(subscriptionId)).describedAs("is running").isTrue(),
                () -> assertThat(subscriptionModel.isPaused(subscriptionId)).describedAs("is paused").isFalse()
        );
    }

    @Test
    void blocking_spring_subscription_throws_iae_when_subscription_already_exists_and_subscription_model_is_stopped() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted();
        subscriptionModel.stop();

        // When
        Throwable throwable = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted());

        // Then
        assertAll(
                () -> assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Subscription " + subscriptionId + " is already defined."),
                () -> assertThat(subscriptionModel.isRunning(subscriptionId)).describedAs("is running").isFalse(),
                () -> assertThat(subscriptionModel.isPaused(subscriptionId)).describedAs("is paused").isTrue()
        );
    }

    @Test
    void blocking_spring_subscription_retries_listener_on_failure() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        AtomicInteger counter = new AtomicInteger();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), c -> {
            if (counter.getAndIncrement() <= 1) {
                throw new IllegalStateException("expected");
            }
            state.add(c);
        }).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
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

    @Nested
    @DisplayName("Lifecycle")
    class LifeCycleTest {

        @Test
        void blocking_spring_subscription_allows_cancelling_a_subscription() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            // The subscription is async so we need to wait for it
            await().atMost(ONE_SECOND).until(Not.not(state::isEmpty));
            subscriptionModel.cancelSubscription(subscriberId);

            // Then
            assertThat(mongoTemplate.getCollection(RESUME_TOKEN_COLLECTION).countDocuments()).isZero();
        }

        @Test
        void blocking_spring_subscription_is_running_returns_false_when_subscription_is_not_started() {
            boolean running = subscriptionModel.isRunning(UUID.randomUUID().toString());

            assertThat(running).isFalse();
        }

        @Test
        void blocking_spring_subscription_is_running_returns_false_when_subscription_is_paused() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            }).waitUntilStarted(Duration.ofSeconds(5));
            ;
            subscriptionModel.pauseSubscription(subscriptionId);

            // When
            boolean running = subscriptionModel.isRunning(subscriptionId);

            // Then
            assertThat(running).isFalse();
        }

        @Test
        void blocking_spring_subscription_is_running_returns_true_when_subscription_is_running() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            }).waitUntilStarted(Duration.ofSeconds(5));
            ;

            // When
            boolean running = subscriptionModel.isRunning(subscriptionId);

            // Then
            assertThat(running).isTrue();
        }

        @Test
        void blocking_spring_subscription_is_paused_returns_false_when_subscription_is_not_started() {
            boolean running = subscriptionModel.isPaused(UUID.randomUUID().toString());

            assertThat(running).isFalse();
        }

        @Test
        void blocking_spring_subscription_is_paused_returns_false_when_subscription_is_running() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            }).waitUntilStarted(Duration.ofSeconds(5));
            ;

            // When
            boolean paused = subscriptionModel.isPaused(subscriptionId);

            // Then
            assertThat(paused).isFalse();
        }

        @Test
        void blocking_spring_subscription_is_paused_returns_true_when_subscription_is_paused() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            }).waitUntilStarted(Duration.ofSeconds(5));
            ;
            subscriptionModel.pauseSubscription(subscriptionId);

            // When
            boolean paused = subscriptionModel.isPaused(subscriptionId);

            // Then
            assertThat(paused).isTrue();
        }

        @Test
        void blocking_spring_subscription_allows_stopping_and_starting_all_subscriptions() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

            CountDownLatch waitUntilStopped = new CountDownLatch(1);
            // When
            subscriptionModel.stop(waitUntilStopped::countDown);

            if (!waitUntilStopped.await(10, SECONDS)) {
                throw new IllegalStateException("Failed to stop subscription model");
            }

            // Then
            subscriptionModel.start();

            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));

            await("state").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
        }

        @Test
        void blocking_spring_subscription_allows_stopping_and_starting_all_subscriptions_when_not_waiting_for_stopped() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

            // When
            subscriptionModel.stop();

            // Then
            subscriptionModel.start();

            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));

            await("state").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
        }

        @Test
        void blocking_spring_subscription_allows_pausing_and_resuming_individual_subscriptions() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> subscription1State = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> subscription2State = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, subscription1State::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            subscriptionModel.subscribe(UUID.randomUUID().toString(), subscription2State::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

            // When
            subscriptionModel.pauseSubscription(subscriptionId);

            mongoEventStore.write("1", 0, serialize(nameDefined1));

            await("subscription2 received event").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(subscription2State).hasSize(1));
            Thread.sleep(200); // We wait a little bit longer to give subscription some time to receive the event (even though it shouldn't!)
            assertThat(subscription1State).isEmpty();

            // Then
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted();

            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));

            await("subscription1 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription1State).extracting(CloudEvent::getId).containsExactly(nameDefined2.getEventId(), nameWasChanged1.getEventId()));
            await("subscription2 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription2State).extracting(CloudEvent::getId).containsExactly(nameDefined1.getEventId(), nameDefined2.getEventId(), nameWasChanged1.getEventId()));
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter for BsonMongoDBFilterSpecification")
    class MongoBsonFilterSpecificationTest {
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

            subscriptionModel.subscribe(subscriberId, filter().id(Filters::eq, nameDefined2.getEventId()).type(Filters::eq, NameDefined.class.getName()), state::add)
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
    }

    @Nested
    @DisplayName("SubscriptionFilter for JsonMongoDBFilterSpecification")
    class MongoJsonFilterSpecificationTest {
        @Test
        void using_json_query_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, MongoFilterSpecification.MongoJsonFilterSpecification.filter("{ $match : { \"" + MongoFilterSpecification.FULL_DOCUMENT + ".type\" : \"" + NameDefined.class.getName() + "\" } }"), state::add)
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
            subscriptionModel.subscribe(subscriberId, OccurrentSubscriptionFilter.filter(Filter.type(NameDefined.class.getName())), state::add).waitUntilStarted();
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
            subscriptionModel.subscribe(subscriberId, OccurrentSubscriptionFilter.filter(filter), state::add).waitUntilStarted();

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


    @Nested
    @DisplayName("ChangeStreamHistoryLost")
    class ChangeStreamHistoryLostTest {

        @SuppressWarnings("unchecked")
        @Timeout(value = 20, unit = SECONDS)
        @Test
        void restarts_subscription_when_change_stream_history_is_lost_when_configured_to_do_so() {
            // Given
            MongoTemplate mongoTemplateSpy = spy(mongoTemplate);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection<Document> mongoCollection = (MongoCollection<Document>) mock(MongoCollection.class);

            List<BsonElement> elements = new ArrayList<>();
            elements.add(new BsonElement("code", new BsonInt32(286)));
            elements.add(new BsonElement("codeName", new BsonString("ChangeStreamHistoryLost")));

            // Called in org.springframework.data.mongodb.core.messaging.ChangeStreamTask#initCursor
            when(mongoTemplateSpy.getDb()).thenReturn(mongoDatabase).thenCallRealMethod();
            when(mongoDatabase.getCollection("events")).thenReturn(mongoCollection);
            when(mongoCollection.watch(any(Class.class))).thenThrow(new UncategorizedMongoDbException("expected", new MongoCommandException(new BsonDocument(elements), new ServerAddress())));

            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted();

            // When
            mongoEventStore.write("1", serialize(new NameDefined(UUID.randomUUID().toString(), now, "name1")));

            // Then
            await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }

    }


    private Stream<CloudEvent> serialize(DomainEvent e) {
        return Stream.of(CloudEventBuilder.v1()
                .withId(e.getEventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.getTimestamp()).atOffset(UTC))
                .withSubject(e.getName())
                .withDataContentType("application/json")
                .withData(CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}