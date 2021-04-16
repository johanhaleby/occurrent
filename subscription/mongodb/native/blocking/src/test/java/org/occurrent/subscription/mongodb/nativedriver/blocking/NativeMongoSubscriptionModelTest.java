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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.condition.Condition;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.OccurrentSubscriptionFilter;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoJsonFilterSpecification;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import static org.occurrent.filter.Filter.data;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.functional.Not.not;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.FULL_DOCUMENT;
import static org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoBsonFilterSpecification.filter;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

@Testcontainers
@Timeout(15000)
public class NativeMongoSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:4.2.8").withReuse(true);

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl()));

    private MongoEventStore mongoEventStore;
    private NativeMongoSubscriptionModel subscriptionModel;
    private ObjectMapper objectMapper;
    private MongoClient mongoClient;
    private ExecutorService subscriptionExecutor;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        this.mongoClient = MongoClients.create(connectionString);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig config = new EventStoreConfig(timeRepresentation);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        MongoCollection<Document> eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
        mongoEventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), connectionString.getCollection(), config);
        subscriptionExecutor = Executors.newCachedThreadPool();
        subscriptionModel = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        subscriptionExecutor.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        mongoClient.close();
    }

    @Test
    void blocking_native_mongodb_subscription_throws_iae_when_subscription_already_exists() {
        // Given
        String subscriptionId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted();

        // When
        Throwable throwable = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, __ -> System.out.println("hello")).waitUntilStarted());
        
        // Then
        assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void blocking_native_mongodb_subscription_calls_listener_for_each_new_event() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void  blocking_native_mongodb_subscription_retries_on_failure() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        final AtomicInteger counter = new AtomicInteger(0);
        final CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(UUID.randomUUID().toString(), cloudEvent -> {
            int value = counter.incrementAndGet();
            if (value <= 4) {
                throw new IllegalArgumentException("expected");
            }
            state.add(cloudEvent);
        }).waitUntilStarted();
        NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2");
        NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name3");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined1));
        mongoEventStore.write("2", 0, serialize(nameDefined2));
        mongoEventStore.write("1", 1, serialize(nameWasChanged1));

        // Then
        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
    }

    @Test
    void blocking_native_mongodb_subscription_allows_cancelling_subscription() throws InterruptedException {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        String subscriberId = UUID.randomUUID().toString();
        subscriptionModel.subscribe(subscriberId, state::add).waitUntilStarted();
        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name1");
        NameWasChanged nameWasChanged = new NameWasChanged(UUID.randomUUID().toString(), now, "name2");

        // When
        mongoEventStore.write("1", 0, serialize(nameDefined));
        // The subscription is async so we need to wait for it
        await().atMost(FIVE_SECONDS).until(not(state::isEmpty));
        subscriptionModel.cancelSubscription(subscriberId);

        // Then
        mongoEventStore.write("1", 1, serialize(nameWasChanged));
        Thread.sleep(500);

        // Assert that no event has been consumed by subscriber
        assertThat(state).hasSize(1);
    }

    @Nested
    @DisplayName("SubscriptionFilter using BsonMongoDBFilterSpecification")
    class MongoBsonFilterSpecificationTest {

        @Test
        void using_bson_query_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, filter().type(Filters::eq, NameDefined.class.getName()), state::add).waitUntilStarted();
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
            ).waitUntilStarted();

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
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

            subscriptionModel.subscribe(subscriberId, filter(match(and(eq("fullDocument.id", nameDefined2.getEventId()), eq("fullDocument.type", NameDefined.class.getName())))), state::add).waitUntilStarted();

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
    @DisplayName("Lifecycle")
    class LifeCycleTest {

        @Test
        void native_mongodb_subscription_model_allows_cancelling_a_subscription() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name1");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            // The subscription is async so we need to wait for it
            await().atMost(ONE_SECOND).until(not(state::isEmpty));
            subscriptionModel.cancelSubscription(subscriptionId);

            // Then
            assertAll(
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                    () -> assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse(),
                    () -> assertThat(subscriptionModel.isRunning()).isTrue()
            );
        }

        @Test
        void native_mongodb_subscription_model_is_running_returns_false_when_subscription_is_not_started() {
            boolean running = subscriptionModel.isRunning(UUID.randomUUID().toString());

            assertThat(running).isFalse();
        }

        @Test
        void native_mongodb_subscription_model_is_running_returns_false_when_subscription_is_paused() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(5));;
            subscriptionModel.pauseSubscription(subscriptionId);

            // When
            boolean running = subscriptionModel.isRunning(subscriptionId);

            // Then
            assertThat(running).isFalse();
        }

        @Test
        void native_mongodb_subscription_model_is_running_returns_true_when_subscription_is_running() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(5));

            // When
            boolean running = subscriptionModel.isRunning(subscriptionId);

            // Then
            assertThat(running).isTrue();
        }

        @Test
        void native_mongodb_subscription_model_is_paused_returns_false_when_subscription_is_not_started() {
            boolean running = subscriptionModel.isPaused(UUID.randomUUID().toString());

            assertThat(running).isFalse();
        }

        @Test
        void native_mongodb_subscription_model_is_paused_returns_false_when_subscription_is_running() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(5));

            // When
            boolean paused = subscriptionModel.isPaused(subscriptionId);

            // Then
            assertThat(paused).isFalse();
        }

        @Test
        void native_mongodb_subscription_model_is_paused_returns_true_when_subscription_is_paused() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {}).waitUntilStarted(Duration.ofSeconds(5));
            subscriptionModel.pauseSubscription(subscriptionId);

            // When
            boolean paused = subscriptionModel.isPaused(subscriptionId);

            // Then
            assertThat(paused).isTrue();
        }

        @Test
        void native_mongodb_subscription_model_allows_stopping_and_starting_all_subscriptions() {
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
        void native_mongodb_subscription_model_allows_pausing_and_resuming_individual_subscriptions() throws InterruptedException {
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
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));

            await("subscription1 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription1State).extracting(CloudEvent::getId).containsExactly(nameDefined2.getEventId(), nameWasChanged1.getEventId()));
            await("subscription2 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription2State).extracting(CloudEvent::getId).containsExactly(nameDefined1.getEventId(), nameDefined2.getEventId(), nameWasChanged1.getEventId()));
        }
    }

    @Nested
    @DisplayName("SubscriptionFilter using JsonMongoDBFilterSpecification")
    class MongoJsonFilterSpecificationTest {

        @Test
        void using_json_query_for_type() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, MongoJsonFilterSpecification.filter("{ $match : { \"" + FULL_DOCUMENT + ".type\" : \"" + NameDefined.class.getName() + "\" } }"), state::add).waitUntilStarted();
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
            subscriptionModel.subscribe(subscriberId, OccurrentSubscriptionFilter.filter(type(NameDefined.class.getName())), state::add).waitUntilStarted();
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
        void using_occurrent_subscription_filter_for_data() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, OccurrentSubscriptionFilter.filter(data("name", Condition.eq("name3"))), state::add).waitUntilStarted();
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
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId).containsOnly(nameWasChanged1.getEventId());
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

            Filter filter = Filter.id(nameDefined2.getEventId()).and(type(NameDefined.class.getName()));
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
}