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
import com.mongodb.*;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.functional.Not;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.time.TimeConversion;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;

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
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoBsonFilterSpecification.filter;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

@Testcontainers
public class SpringMongoSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withReuse(true);
    private static final String RESUME_TOKEN_COLLECTION = "ack";

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private SpringMongoEventStore mongoEventStore;
    private SpringMongoSubscriptionModel subscriptionModel;
    private ObjectMapper objectMapper;
    private MongoTemplate mongoTemplate;
    private String eventCollectionName;
    private TimeRepresentation timeRepresentation;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        this.eventCollectionName = connectionString.getCollection();
        this.timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollectionName).transactionConfig(mongoTransactionManager).timeRepresentation(timeRepresentation).eventStoreCapabilities(STREAM, DCB).build();
        mongoEventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, timeRepresentation);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
    }

    @Test
    void blocking_spring_subscription_delivers_events_when_max_await_time_is_configured() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        SpringMongoSubscriptionModel configuredSubscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventCollectionName, timeRepresentation).maxAwaitTime(Duration.ofMillis(500)));
        try {
            configuredSubscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));

            // Then
            await().atMost(5, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
        } finally {
            configuredSubscriptionModel.shutdown();
        }
    }

    @Test
    void blocking_spring_subscription_calls_listener_for_dcb_written_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

        // When
        mongoEventStore.append(serialize(nameDefined).stream()
                .map(event -> DcbCloudEvents.withTags(event, List.of(Tag.parse("name:1"))))
                .toList());

        // Then
        await().atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
            assertThat(state).hasSize(1);
            assertThat(DcbCloudEvents.getTags(state.get(0))).containsExactly(Tag.parse("name:1"));
            assertThat(OccurrentCloudEventExtension.getPosition(state.get(0))).isPositive();
        });
    }

    @Test
    void resumes_stream_after_deletion_of_events_from_event_store() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // When

        // Now we delete the events
        mongoEventStore.delete(all());
        // And write some additional events
        mongoEventStore.write("1", 0, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(15), "name", "name4")));
        mongoEventStore.write("3", 0, serialize(new NameDefined(UUID.randomUUID().toString(), now, "name5", "name5")));

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
        NameDefined nameDefined1 = new NameDefined(eventId1, now, "name", "name1");
        NameDefined nameDefined2 = new NameDefined(eventId2, now.plusSeconds(2), "name3", "name3");

        mongoEventStore.write("1", 0, serialize(nameDefined1));

        await("first event").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        Checkpoint checkpoint = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(state.get(0));

        subscriptionModel.cancelSubscription(subscriptionId);

        // When
        mongoEventStore.delete(id(eventId1)); // Delete event that subscription hasn't received yet
        assertThat(mongoEventStore.count()).isZero();

        // Write a new event and the resume subscription
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        subscriptionModel.subscribe(subscriptionId, StartAt.checkpoint(checkpoint), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

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

    @Nested
    @DisplayName("Auto startup")
    class AutoStartupTest {

        private SpringMongoSubscriptionModel notAutoStarted;

        @BeforeEach
        void create_a_model_that_does_not_start_itself() {
            notAutoStarted = new SpringMongoSubscriptionModel(mongoTemplate,
                    withConfig(eventCollectionName, timeRepresentation).autoStartup(false));
        }

        @AfterEach
        void shutdown_the_model_that_does_not_start_itself() {
            notAutoStarted.shutdown();
        }

        @Test
        void a_model_configured_not_to_auto_start_is_not_running() {
            assertAll(
                    () -> assertThat(notAutoStarted.isRunning()).isFalse(),
                    () -> assertThat(notAutoStarted.isAutoStartup()).isFalse()
            );
        }

        @Test
        void subscribing_on_a_model_that_did_not_auto_start_registers_the_subscription_as_paused() {
            String subscriptionId = UUID.randomUUID().toString();

            notAutoStarted.subscribe(subscriptionId, __ -> {
            });

            assertAll(
                    () -> assertThat(notAutoStarted.isPaused(subscriptionId)).isTrue(),
                    () -> assertThat(notAutoStarted.isRunning(subscriptionId)).isFalse()
            );
        }

        @Test
        void a_subscription_registered_before_start_receives_events_once_resumed() {
            String subscriptionId = UUID.randomUUID().toString();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            notAutoStarted.subscribe(subscriptionId, state::add);

            // Nothing arrives while it is still paused
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            await().during(ONE_SECOND).atMost(FIVE_SECONDS).until(state::isEmpty);

            notAutoStarted.resumeSubscription(subscriptionId).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2")));

            await().atMost(FIVE_SECONDS).until(Not.not(state::isEmpty));
        }

        @Test
        void the_default_still_auto_starts() {
            assertAll(
                    () -> assertThat(subscriptionModel.isRunning()).isTrue(),
                    () -> assertThat(subscriptionModel.isAutoStartup()).isTrue()
            );
        }
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
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            // The subscription is async so we need to wait for it
            await().atMost(ONE_SECOND).until(Not.not(state::isEmpty));
            subscriptionModel.cancelSubscription(subscriberId);

            // Then
            assertThat(mongoTemplate.getCollection(RESUME_TOKEN_COLLECTION).countDocuments()).isZero();
        }

        @Test
        void blocking_spring_subscription_allows_stopping_and_starting_all_subscriptions() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

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
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

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
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

            subscriptionModel.subscribe(subscriberId, filter().id(Filters::eq, nameDefined2.eventId()).type(Filters::eq, NameDefined.class.getName()), state::add)
                    .waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getName()));
        }

        @Test
        void using_bson_query_native_mongo_filters_composition() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

            subscriptionModel.subscribe(subscriberId, filter(match(and(eq("fullDocument.id", nameDefined2.eventId()), eq("fullDocument.type", NameDefined.class.getName())))), state::add
                    )
                    .waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(ONE_SECOND).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getName()));
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
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

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
    @DisplayName("SubscriptionFilter using StreamSubscriptionFilter")
    class StreamSubscriptionFilterTest {

        @Test
        void using_occurrent_subscription_filter_dsl_composition() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

            Filter filter = Filter.id(nameDefined2.eventId()).and(Filter.type(NameDefined.class.getName()));
            subscriptionModel.subscribe(subscriberId, StreamSubscriptionFilter.filter(filter), state::add).waitUntilStarted();

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getName()));
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter using AgnosticSubscriptionFilter")
    class AgnosticSubscriptionFilterTest {

        @Test
        void using_occurrent_subscription_filter_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, AgnosticSubscriptionFilter.filter(Filter.type(NameDefined.class.getName())), state::add).waitUntilStarted();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

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
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

            Filter filter = Filter.id(nameDefined2.eventId()).and(Filter.type(NameDefined.class.getName()));
            subscriptionModel.subscribe(subscriberId, AgnosticSubscriptionFilter.filter(filter), state::add).waitUntilStarted();

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId, CloudEvent::getType).containsOnly(tuple(nameDefined2.eventId(), NameDefined.class.getName()));
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
            mongoEventStore.write("1", serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            // Restart-recovery await: after the mocked failure the subscription model restarts the change stream, which
            // can take longer than a couple of seconds on a loaded CI machine. Awaitility short-circuits on success.
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }

    }

    @Nested
    @DisplayName("MongoException")
    class MongoExceptionTest {

        @Timeout(value = 20, unit = SECONDS)
        @Test
        void restarts_subscription_on_mongo_query_exception() {
            List<BsonElement> elements = new ArrayList<>();
            elements.add(new BsonElement("code", new BsonInt32(11600)));
            elements.add(new BsonElement("codeName", new BsonString("InterruptedAtShutdown")));

            UncategorizedMongoDbException exception = new UncategorizedMongoDbException("expected", new MongoQueryException(new BsonDocument(elements), new ServerAddress()));
            
            assertSubscriptionIsRestartedForException(exception);
        }

        @Timeout(value = 20, unit = SECONDS)
        @Test
        void restarts_subscription_on_DataAccessResourceFailureException() {
            DataAccessResourceFailureException exception = new DataAccessResourceFailureException("expected", new MongoTimeoutException("timed out"));
            assertSubscriptionIsRestartedForException(exception);
        }

        @Timeout(value = 20, unit = SECONDS)
        @Test
        void restarts_subscription_on_non_DataAccessException() {
            var exception = new IllegalStateException("Cursor com.mongodb.client.internal.MongoChangeStreamCursorImpl@3ab4fcd8 is not longer open");

            assertSubscriptionIsRestartedForException(exception);
        }

        @SuppressWarnings("unchecked")
        private void assertSubscriptionIsRestartedForException(Exception exception) {
            // Given
            MongoTemplate mongoTemplateSpy = spy(mongoTemplate);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection<Document> mongoCollection = (MongoCollection<Document>) mock(MongoCollection.class);

            // Called in org.springframework.data.mongodb.core.messaging.ChangeStreamTask#initCursor
            when(mongoTemplateSpy.getDb()).thenReturn(mongoDatabase).thenCallRealMethod();
            when(mongoDatabase.getCollection("events")).thenReturn(mongoCollection);
            when(mongoCollection.watch(any(Class.class))).thenThrow(exception);

            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(true));

            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted();

            // When
            mongoEventStore.write("1", serialize(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1")));

            // Then
            // Restart-recovery await: after the mocked failure the subscription model restarts the change stream, which
            // can take longer than a couple of seconds on a loaded CI machine. Awaitility short-circuits on success.
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
        }
    }

    @Nested
    @DisplayName("Restart resume position")
    class RestartResumePositionTest {

        /**
         * The existing restart tests fail the very first attempt to open a change stream, so the subscription has
         * read nothing and there is no position for a restart to continue from. This one lets the subscription read
         * an event first and then takes the change stream away, which is the case where restarting at the present
         * skips whatever was written during the outage.
         */
        @SuppressWarnings("unchecked")
        // Two waits, 5 seconds for the first delivery and 10 for the one after the restart, plus the model's
        // default retry backoff before it reconnects. 30 leaves headroom over that chain without letting a
        // subscription that never comes back sit here for a minute.
        @Timeout(value = 30, unit = SECONDS)
        @Test
        void continues_from_the_last_document_read_so_a_write_during_the_outage_still_arrives() {
            // Given a change stream whose cursor can be told to stop handing documents over and then to fail the
            // way a failover does. Withholding first is what makes the outage a window rather than an instant: an
            // event can be written while the subscription is provably not reading, which is the only way to tell a
            // restart that continues where it left off from one that reconnects at the present.
            AtomicBoolean withholdDocuments = new AtomicBoolean(false);
            AtomicBoolean failNextRead = new AtomicBoolean(false);
            MongoCollection<Document> realEventCollection = mongoTemplate.getDb().getCollection(eventCollectionName);

            MongoTemplate mongoTemplateSpy = spy(mongoTemplate);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection<Document> instrumentedCollection = (MongoCollection<Document>) mock(MongoCollection.class);
            // Only the first change stream is instrumented (ChangeStreamTask#initCursor calls getDb() per cursor),
            // so the restart runs through the real template and what it resumes from is the model's own decision
            // rather than something this test arranged.
            when(mongoTemplateSpy.getDb()).thenReturn(mongoDatabase).thenCallRealMethod();
            when(mongoDatabase.getCollection(eventCollectionName)).thenReturn(instrumentedCollection);
            when(instrumentedCollection.watch(any(Class.class)))
                    .thenAnswer(__ -> instrumentedChangeStream(realEventCollection.watch(Document.class), withholdDocuments, failNextRead));

            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, eventCollectionName, timeRepresentation);
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            NameDefined beforeTheOutage = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(beforeTheOutage));
            // Waited for, not assumed: the position the restart has to continue from is the one this delivery
            // leaves behind, so a test that raced ahead of it would be asserting something else.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

            // When the change stream stops reading, two events are written, and only then does the stream fail.
            // Two, because a change stream opened without a start position begins at the server's current
            // operation time and includes an operation stamped at exactly that time, so with a single write the
            // old restart-at-the-present behaviour delivered it anyway. The second write moves the server's
            // operation time past the first, which is what a restart at the present then skips.
            withholdDocuments.set(true);
            NameWasChanged firstDuringTheOutage = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            NameWasChanged lastDuringTheOutage = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name3");
            mongoEventStore.write("1", 1, serialize(firstDuringTheOutage));
            mongoEventStore.write("1", 2, serialize(lastDuringTheOutage));
            failNextRead.set(true);

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(state).extracting(CloudEvent::getId)
                            .as("the restarted subscription must continue from the event it had read, so both "
                                    + "events written while its change stream was down still arrive")
                            .containsExactly(beforeTheOutage.eventId(), firstDuringTheOutage.eventId(), lastDuringTheOutage.eventId()));
        }

        /**
         * Delegates every call to the real change stream, except that the cursor it hands out withholds documents
         * or fails on demand. Option calls are delegated too, and answer with this mock rather than the real
         * iterable they return, so the chain {@code ChangeStreamTask} builds ends at the instrumented cursor.
         */
        @SuppressWarnings("unchecked")
        private ChangeStreamIterable<Document> instrumentedChangeStream(ChangeStreamIterable<Document> real, AtomicBoolean withholdDocuments, AtomicBoolean failNextRead) {
            return mock(ChangeStreamIterable.class, invocation -> {
                if (invocation.getMethod().getName().equals("iterator")) {
                    return instrumentedCursor(real.iterator(), withholdDocuments, failNextRead);
                }
                Object answer = invocation.getMethod().invoke(real, invocation.getArguments());
                return answer == real ? invocation.getMock() : answer;
            });
        }

        @SuppressWarnings("unchecked")
        private MongoCursor<ChangeStreamDocument<Document>> instrumentedCursor(MongoCursor<ChangeStreamDocument<Document>> real, AtomicBoolean withholdDocuments, AtomicBoolean failNextRead) {
            return mock(MongoCursor.class, invocation -> {
                if (!invocation.getMethod().getName().equals("tryNext")) {
                    return invocation.getMethod().invoke(real, invocation.getArguments());
                }
                if (failNextRead.get()) {
                    throw new MongoSocketReadException("expected: simulated failover", new ServerAddress(), new IOException("Connection reset by peer"));
                }
                if (withholdDocuments.get()) {
                    // Nothing to read, as far as the container is concerned. Slept on rather than answered
                    // immediately, because its read loop calls tryNext() again as soon as this returns.
                    Thread.sleep(20);
                    return null;
                }
                return real.tryNext();
            });
        }
    }

    @Nested
    @DisplayName("Restart backoff")
    class RestartBackoffTest {

        @SuppressWarnings("unchecked")
        @Timeout(value = 20, unit = SECONDS)
        @Test
        void restarts_follow_bounded_retry_schedule_and_keep_thread_count_bounded_under_persistent_failure() {
            // Given
            MongoTemplate mongoTemplateSpy = spy(mongoTemplate);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection<Document> mongoCollection = (MongoCollection<Document>) mock(MongoCollection.class);

            List<BsonElement> elements = new ArrayList<>();
            elements.add(new BsonElement("code", new BsonInt32(11600)));
            elements.add(new BsonElement("codeName", new BsonString("InterruptedAtShutdown")));

            // Every attempt to open the change stream fails, simulating a persistent fault rather than a single
            // transient error.
            when(mongoTemplateSpy.getDb()).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection("events")).thenReturn(mongoCollection);
            when(mongoCollection.watch(any(Class.class))).thenThrow(new UncategorizedMongoDbException("expected", new MongoQueryException(new BsonDocument(elements), new ServerAddress())));

            Duration backoff = Duration.ofMillis(150);
            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig("events", TimeRepresentation.RFC_3339_STRING).retryStrategy(RetryStrategy.fixed(backoff)));

            String restartThreadNamePrefix = "spring-mongo-subscription-restart";
            AtomicInteger maxObservedRestartThreads = new AtomicInteger(countThreadsWithNamePrefix(restartThreadNamePrefix));

            // When
            subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> {
            });

            // Then
            // Sample the live thread count repeatedly while several restart cycles play out. A thread-per-attempt
            // implementation would keep creating new threads for every failed attempt; the shared restart executor
            // should keep the count bounded to (at most) one restart thread per subscription regardless of how many
            // attempts have been made.
            Duration observationWindow = backoff.multipliedBy(8);
            long deadline = System.currentTimeMillis() + observationWindow.toMillis();
            while (System.currentTimeMillis() < deadline) {
                maxObservedRestartThreads.accumulateAndGet(countThreadsWithNamePrefix(restartThreadNamePrefix), Math::max);
                sleep(20);
            }

            assertThat(maxObservedRestartThreads.get()).isLessThanOrEqualTo(1);

            // The retry schedule paces restarts roughly every "backoff" duration rather than restarting immediately
            // and repeatedly. With an 8x backoff observation window we expect well under twice as many attempts as
            // that would allow, never anywhere near an unbounded/immediate-restart count.
            long maxExpectedAttempts = observationWindow.dividedBy(backoff) * 2;
            verify(mongoCollection, atMost((int) maxExpectedAttempts)).watch(any(Class.class));
        }

        private int countThreadsWithNamePrefix(String prefix) {
            return (int) Thread.getAllStackTraces().keySet().stream()
                    .filter(Thread::isAlive)
                    .filter(thread -> thread.getName().startsWith(prefix))
                    .count();
        }
    }

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(TimeConversion.toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
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
