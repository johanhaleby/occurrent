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
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.condition.Condition;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoJsonFilterSpecification;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
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
@Timeout(15)
public class NativeMongoSubscriptionModelTest {

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion()
                    .withReuse(true)
                    .withReplicaSet();

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoEventStore mongoEventStore;
    private NativeMongoSubscriptionModel subscriptionModel;
    private ObjectMapper objectMapper;
    private MongoClient mongoClient;
    private ExecutorService subscriptionExecutor;
    private MongoDatabase database;
    private MongoCollection<Document> eventCollection;
    private TimeRepresentation timeRepresentation;

    @BeforeEach
    void create_mongo_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        this.mongoClient = MongoClients.create(connectionString);
        this.timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        EventStoreConfig config = new EventStoreConfig(timeRepresentation);
        this.database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        this.eventCollection = database.getCollection(requireNonNull(connectionString.getCollection()));
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
        assertThat(throwable).isExactlyInstanceOf(DuplicateSubscriptionIdException.class).hasMessage("Subscription " + subscriptionId + " is already defined.");
    }

    @Test
    void blocking_native_mongodb_subscription_refuses_a_start_position_it_cannot_parse_from_subscribe_itself() {
        // Given: a checkpoint whose string form contains "resumeToken" (steering MongoCommons.applyStartPosition
        // into its legacy string-parsing branch) but isn't valid BSON, so parsing it fails. Before subscribe made
        // this eager check, the same failure only happened later, inside newInternalSubscription on the
        // dispatcher thread: the retry wrapper around that thread caught it and re-threw it forever, so
        // waitUntilStarted() never returned and the caller was left holding a subscription that looked
        // registered but never delivered a single event.
        String subscriptionId = UUID.randomUUID().toString();
        StartAt unparsableStartAt = StartAt.checkpoint(new StringBasedCheckpoint("not-a-valid-resumeToken-document"));

        // When
        // A handler that does nothing, since subscribe is expected to throw before anything could reach it.
        Throwable throwable = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, unparsableStartAt, __ -> {
        }));

        // Then: refused synchronously by subscribe() itself, and nothing is left registered under the id
        assertThat(throwable).isExactlyInstanceOf(JsonParseException.class);
        assertAll(
                () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                () -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId)
        );
    }

    @Test
    void blocking_native_mongodb_subscription_delivers_events_when_batch_size_and_max_await_time_are_configured() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel configuredSubscriptionModel = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, executor,
                NativeMongoSubscriptionModelConfig.withConfig().batchSize(500).maxAwaitTime(Duration.ofMillis(500)));
        try {
            configuredSubscriptionModel.subscribe(UUID.randomUUID().toString(), state::add).waitUntilStarted();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));

            // Then
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(3));
        } finally {
            configuredSubscriptionModel.shutdown();
            executor.shutdown();
            ExecutorShutdown.shutdownSafely(executor, 10, TimeUnit.SECONDS);
        }
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
        void using_bson_query_dsl_composition() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(3), "name", "name3");
            NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(4), "name2", "name4");

            subscriptionModel.subscribe(subscriberId, filter().id(Filters::eq, nameDefined2.eventId()).type(Filters::eq, NameDefined.class.getName()), state::add
            ).waitUntilStarted();

            // When
            mongoEventStore.write("1", 0, serialize(nameDefined1));
            mongoEventStore.write("1", 1, serialize(nameWasChanged1));
            mongoEventStore.write("2", 0, serialize(nameDefined2));
            mongoEventStore.write("2", 1, serialize(nameWasChanged2));

            // Then
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
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

            subscriptionModel.subscribe(subscriberId, filter(match(and(eq("fullDocument.id", nameDefined2.eventId()), eq("fullDocument.type", NameDefined.class.getName())))), state::add).waitUntilStarted();

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
    @DisplayName("Lifecycle")
    class LifeCycleTest {

        @Test
        void native_mongodb_subscription_model_allows_cancelling_a_subscription() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");

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
        void native_mongodb_subscription_model_allows_pausing_and_resuming_individual_subscriptions() throws InterruptedException {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> subscription1State = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> subscription2State = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, subscription1State::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));
            subscriptionModel.subscribe(UUID.randomUUID().toString(), subscription2State::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined nameDefined1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now.plusSeconds(2), "name2", "name2");
            NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(10), "name", "name3");

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

            // nameDefined1 too, since it was written after subscription1 first opened and resuming starts from there
            await("subscription1 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription1State).extracting(CloudEvent::getId).containsExactly(nameDefined1.eventId(), nameDefined2.eventId(), nameWasChanged1.eventId()));
            await("subscription2 received all events").atMost(2, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscription2State).extracting(CloudEvent::getId).containsExactly(nameDefined1.eventId(), nameDefined2.eventId(), nameWasChanged1.eventId()));
        }

        @Test
        void a_subscription_paused_while_handling_its_first_event_delivers_what_was_written_while_paused_once_resumed() throws InterruptedException {
            // Given: a handler that holds on to the first event until released, so the subscription has not finished
            // handling anything when it is paused
            LocalDateTime now = LocalDateTime.now();
            NameDefined first = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            NameWasChanged writtenWhilePaused = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            AtomicBoolean firstCall = new AtomicBoolean(true);
            CountDownLatch handlingFirstEvent = new CountDownLatch(1);
            CountDownLatch releaseFirstEvent = new CountDownLatch(1);
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
                if (firstCall.getAndSet(false)) {
                    handlingFirstEvent.countDown();
                    try {
                        releaseFirstEvent.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                handled.add(cloudEvent);
            }).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(first));
            assertThat(handlingFirstEvent.await(10, SECONDS)).isTrue();

            // When: resumed before the handler returns, so the resume cannot start from the position recorded once
            // the first event is handled
            subscriptionModel.pauseSubscription(subscriptionId);
            mongoEventStore.write("1", 1, serialize(writtenWhilePaused));
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));
            releaseFirstEvent.countDown();

            // Then: the first event may be handed over twice, but nothing is skipped
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(handled).extracting(CloudEvent::getId).contains(first.eventId(), writtenWhilePaused.eventId()));
        }

        @Test
        void a_subscription_paused_before_handling_anything_delivers_what_was_written_while_paused_once_resumed() {
            // Given
            NameDefined writtenWhilePaused = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), handled::add).waitUntilStarted(Duration.ofSeconds(10));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            mongoEventStore.write("1", 0, serialize(writtenWhilePaused));
            subscriptionModel.resumeSubscription(subscriptionId).waitUntilStarted(Duration.ofSeconds(10));

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenWhilePaused.eventId()));
        }

        @Test
        void subscribe_returns_and_the_subscription_delivers_once_mongodb_can_be_reached() {
            // Given
            AtomicBoolean unreachable = new AtomicBoolean(true);
            AtomicInteger refusedOperationTimeRequests = new AtomicInteger();
            MongoDatabase databaseSpy = spy(database);
            doAnswer(invocation -> {
                if (unreachable.get()) {
                    refusedOperationTimeRequests.incrementAndGet();
                    throw new MongoTimeoutException("MongoDB cannot be reached");
                }
                return invocation.callRealMethod();
            }).when(databaseSpy).runCommand(any(Bson.class));
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(databaseSpy, eventCollection, timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
            try {
                CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();

                // When
                Throwable thrown = catchThrowable(() -> model.subscribe(UUID.randomUUID().toString(), StartAt.now(), handled::add));
                await().atMost(5, SECONDS).until(() -> refusedOperationTimeRequests.get() > 0);
                unreachable.set(false);

                // Then
                assertThat(thrown).isNull();
                await().atMost(10, SECONDS).until(() -> model.subscriptionIds().size() == 1);
                NameDefined writtenOnceReachable = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
                mongoEventStore.write("1", 0, serialize(writtenOnceReachable));
                await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).contains(writtenOnceReachable.eventId()));
            } finally {
                model.shutdown();
            }
        }

        @Test
        void a_subscription_made_while_the_model_is_stopped_delivers_nothing_until_started_and_then_each_event_once() {
            // Given
            subscriptionModel.stop();
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            boolean startedWhileStopped = subscriptionModel.subscribe(subscriptionId, handled::add).waitUntilStarted(Duration.ofMillis(500));
            NameDefined writtenWhileStopped = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenWhileStopped));
            await().during(ONE_SECOND).atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).isEmpty());
            boolean pausedWhileStopped = subscriptionModel.isPaused(subscriptionId);

            // When
            subscriptionModel.start();
            NameDefined writtenAfterStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2");
            mongoEventStore.write("2", 0, serialize(writtenAfterStart));

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).contains(writtenAfterStart.eventId()));
            await().during(ONE_SECOND).atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).containsExactly(writtenWhileStopped.eventId(), writtenAfterStart.eventId()));
            assertAll(
                    () -> assertThat(startedWhileStopped).isFalse(),
                    () -> assertThat(pausedWhileStopped).isTrue()
            );
        }

        @Test
        void the_subscription_made_while_the_model_is_stopped_answers_that_it_started_once_start_opens_its_change_stream() {
            // Given
            subscriptionModel.stop();
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            Subscription subscription = subscriptionModel.subscribe(UUID.randomUUID().toString(), handled::add);

            // When
            subscriptionModel.start();
            NameDefined writtenAfterStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenAfterStart));
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).contains(writtenAfterStart.eventId()));

            // Then
            assertThat(subscription.waitUntilStarted(Duration.ofSeconds(2))).isTrue();
        }

        @Test
        void start_resumes_the_subscriptions_that_stop_paused_through_resumeSubscription() {
            // Given a subclass that records what resumeSubscription(..) is called for
            CopyOnWriteArrayList<String> resumedThroughTheOverride = new CopyOnWriteArrayList<>();
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2)) {
                @Override
                public Subscription resumeSubscription(String subscriptionId) {
                    resumedThroughTheOverride.add(subscriptionId);
                    return super.resumeSubscription(subscriptionId);
                }
            };
            try {
                String subscriptionId = UUID.randomUUID().toString();
                model.subscribe(subscriptionId, __ -> {
                }).waitUntilStarted(Duration.ofSeconds(5));
                model.stop();

                // When
                model.start();

                // Then
                assertThat(resumedThroughTheOverride).containsExactly(subscriptionId);
                assertThat(model.isRunning(subscriptionId)).isTrue();
            } finally {
                model.shutdown();
            }
        }

        @Test
        void start_waits_on_the_subscription_that_resumeSubscription_returns() {
            // Given a subclass whose resumeSubscription(..) returns a subscription that records each wait on it
            CopyOnWriteArrayList<String> waitedOn = new CopyOnWriteArrayList<>();
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2)) {
                @Override
                public Subscription resumeSubscription(String subscriptionId) {
                    Subscription resumed = super.resumeSubscription(subscriptionId);
                    return new Subscription() {
                        @Override
                        public String id() {
                            return resumed.id();
                        }

                        @Override
                        public boolean waitUntilStarted(Duration timeout) {
                            waitedOn.add(resumed.id());
                            return resumed.waitUntilStarted(timeout);
                        }
                    };
                }
            };
            try {
                model.stop();
                String subscriptionId = UUID.randomUUID().toString();
                model.subscribe(subscriptionId, __ -> {
                });

                // When
                model.start();

                // Then
                assertThat(waitedOn).contains(subscriptionId);
            } finally {
                model.shutdown();
            }
        }

        @Test
        void start_resumes_every_other_subscription_when_a_resume_throws_and_then_throws_the_first_failure_with_the_rest_suppressed() {
            // Given a subclass whose first two resumes throw
            AtomicInteger resumes = new AtomicInteger();
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2)) {
                @Override
                public Subscription resumeSubscription(String subscriptionId) {
                    int resume = resumes.incrementAndGet();
                    if (resume <= 2) {
                        throw new IllegalStateException("resume " + resume + " failed");
                    }
                    return super.resumeSubscription(subscriptionId);
                }
            };
            try {
                model.stop();
                List<String> subscriptionIds = Stream.generate(() -> UUID.randomUUID().toString()).limit(4).toList();
                subscriptionIds.forEach(subscriptionId -> model.subscribe(subscriptionId, __ -> {
                }));

                // When
                Throwable thrown = catchThrowable(model::start);

                // Then
                assertAll(
                        () -> assertThat(thrown).hasMessage("resume 1 failed"),
                        () -> assertThat(thrown.getSuppressed()).extracting(Throwable::getMessage).containsExactly("resume 2 failed"),
                        () -> assertThat(subscriptionIds).filteredOn(model::isRunning).hasSize(2),
                        () -> assertThat(subscriptionIds).filteredOn(model::isPaused).hasSize(2)
                );
            } finally {
                model.shutdown();
            }
        }

        @Test
        void start_leaves_out_a_subscription_that_resuming_an_earlier_one_already_resumed() {
            // Given a subclass whose first resume also resumes every other paused subscription
            AtomicBoolean firstResume = new AtomicBoolean(true);
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2)) {
                @Override
                public Subscription resumeSubscription(String subscriptionId) {
                    if (firstResume.getAndSet(false)) {
                        subscriptionIds().stream().filter(this::isPaused).filter(other -> !other.equals(subscriptionId)).toList().forEach(super::resumeSubscription);
                    }
                    return super.resumeSubscription(subscriptionId);
                }
            };
            try {
                model.stop();
                List<String> subscriptionIds = Stream.generate(() -> UUID.randomUUID().toString()).limit(3).toList();
                subscriptionIds.forEach(subscriptionId -> model.subscribe(subscriptionId, __ -> {
                }));

                // When
                Throwable thrown = catchThrowable(model::start);

                // Then
                assertAll(
                        () -> assertThat(thrown).isNull(),
                        () -> assertThat(subscriptionIds).allMatch(model::isRunning)
                );
            } finally {
                model.shutdown();
            }
        }

        @Test
        void an_interrupted_wait_for_a_subscription_to_start_leaves_the_thread_interrupted() {
            // Given a subscription made while the model is stopped, so it does not start until the model does
            subscriptionModel.stop();
            Subscription notStarted = subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> {
            });
            Thread.currentThread().interrupt();

            // When
            Throwable thrown = catchThrowable(() -> notStarted.waitUntilStarted(Duration.ofSeconds(2)));

            // Then: Thread.interrupted() also clears the flag, so it isn't still set when the next test runs
            boolean stillInterrupted = Thread.interrupted();
            assertThat(stillInterrupted).isTrue();
            assertThat(thrown).hasCauseInstanceOf(InterruptedException.class);
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
    }

    @Nested
    @DisplayName("SubscriptionFilter using StreamSubscriptionFilter")
    class StreamSubscriptionFilterTest {

        @Test
        void using_occurrent_subscription_filter_for_data() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, StreamSubscriptionFilter.filter(data("name", Condition.eq("name3"))), state::add).waitUntilStarted();
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
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId).containsOnly(nameWasChanged1.eventId());
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

            Filter filter = Filter.id(nameDefined2.eventId()).and(type(NameDefined.class.getName()));
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
            subscriptionModel.subscribe(subscriberId, AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName())), state::add).waitUntilStarted();
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
        void using_occurrent_subscription_filter_for_data() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriberId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriberId, AgnosticSubscriptionFilter.filter(data("name", Condition.eq("name3"))), state::add).waitUntilStarted();
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
            await().atMost(FIVE_SECONDS).until(state::size, is(1));
            assertThat(state).extracting(CloudEvent::getId).containsOnly(nameWasChanged1.eventId());
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

            Filter filter = Filter.id(nameDefined2.eventId()).and(type(NameDefined.class.getName()));
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

    private List<CloudEvent> serialize(DomainEvent e) {
        return List.of(CloudEventBuilder.v1()
                .withId(e.eventId())
                .withSource(URI.create("http://name"))
                .withType(e.getClass().getName())
                .withTime(toLocalDateTime(e.timestamp()).atOffset(UTC))
                .withSubject(e.name())
                .withDataContentType("application/json")
                .withData(unchecked(objectMapper::writeValueAsBytes).apply(e))
                .build());
    }
}