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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.GlobalCheckpoint;
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
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoJsonFilterSpecification;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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
        void a_dynamic_start_position_is_evaluated_once_to_open_the_change_stream() {
            // Given
            AtomicInteger evaluations = new AtomicInteger();
            StartAt countsItsEvaluations = StartAt.dynamic(() -> {
                evaluations.incrementAndGet();
                return StartAt.now();
            });
            NameDefined written = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();

            // When
            subscriptionModel.subscribe(UUID.randomUUID().toString(), countsItsEvaluations, handled::add).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(written));

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(handled).extracting(CloudEvent::getId).containsExactly(written.eventId()));
            assertThat(evaluations).describedAs("evaluations of the start position").hasValue(1);
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
                AtomicReference<Subscription> subscription = new AtomicReference<>();
                Throwable thrown = catchThrowable(() -> subscription.set(model.subscribe(UUID.randomUUID().toString(), StartAt.now(), handled::add)));
                await().atMost(5, SECONDS).until(() -> refusedOperationTimeRequests.get() > 0);
                unreachable.set(false);

                // Then
                assertThat(thrown).isNull();
                assertThat(subscription.get().waitUntilStarted(Duration.ofSeconds(10))).isTrue();
                NameDefined writtenOnceReachable = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
                mongoEventStore.write("1", 0, serialize(writtenOnceReachable));
                await().atMost(10, SECONDS).untilAsserted(() -> assertThat(handled).extracting(CloudEvent::getId).contains(writtenOnceReachable.eventId()));
            } finally {
                model.shutdown();
            }
        }

        @Test
        void a_subscription_made_while_the_model_is_stopped_delivers_nothing_until_started_and_then_each_event_once() {
            // Given a stopped model over a database that counts MongoDB's replies
            AtomicInteger answered = new AtomicInteger();
            MongoDatabase databaseSpy = spy(database);
            doAnswer(invocation -> {
                Object reply = invocation.callRealMethod();
                answered.incrementAndGet();
                return reply;
            }).when(databaseSpy).runCommand(any(Bson.class));
            subscriptionModel.shutdown();
            subscriptionExecutor = Executors.newCachedThreadPool();
            subscriptionModel = new NativeMongoSubscriptionModel(databaseSpy, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
            subscriptionModel.stop();
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Subscription subscription = subscriptionModel.subscribe(subscriptionId, handled::add);
            // The subscription starts at the operation time MongoDB answers with, so an event written after the answer
            // is one it receives
            await().atMost(FIVE_SECONDS).until(() -> answered.get() == 1);
            boolean startedWhileStopped = subscription.waitUntilStarted(Duration.ofMillis(500));
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

    @Nested
    @DisplayName("Resume at a checkpoint the model cannot read")
    class ResumeAtACheckpointTheModelCannotReadTest {

        @Test
        void resuming_at_a_global_position_checkpoint_does_not_skip_what_was_written_while_paused() {
            assertWhatWasWrittenWhilePausedIsDeliveredWhenResumingAt(
                    liveStart -> StartAt.checkpoint(globalPositionCheckpoint(liveStart)));
        }

        @Test
        void resuming_at_a_time_based_checkpoint_does_not_skip_what_was_written_while_paused() {
            assertWhatWasWrittenWhilePausedIsDeliveredWhenResumingAt(
                    liveStart -> StartAt.checkpoint(timeBasedCheckpoint()));
        }

        @Test
        void resuming_at_a_dynamic_position_that_resolves_to_a_global_position_checkpoint_does_not_skip_what_was_written_while_paused() {
            assertWhatWasWrittenWhilePausedIsDeliveredWhenResumingAt(
                    liveStart -> StartAt.dynamic(() -> StartAt.checkpoint(globalPositionCheckpoint(liveStart))));
        }

        @Test
        void resuming_at_a_dynamic_position_that_resolves_to_a_time_based_checkpoint_does_not_skip_what_was_written_while_paused() {
            assertWhatWasWrittenWhilePausedIsDeliveredWhenResumingAt(
                    liveStart -> StartAt.dynamic(() -> StartAt.checkpoint(timeBasedCheckpoint())));
        }

        @Test
        void resuming_at_a_dynamic_position_that_resolves_to_a_readable_checkpoint_reopens_the_change_stream_there() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            NameDefined firstEvent = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(firstEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
            // Before the second event, so earlier than where the subscription has read to when it is paused below
            Checkpoint afterFirstEvent = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(state.get(0));

            NameWasChanged secondEvent = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            mongoEventStore.write("1", 1, serialize(secondEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            NameWasChanged thirdEvent = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name3");
            mongoEventStore.write("1", 2, serialize(thirdEvent));
            subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(() -> StartAt.checkpoint(afterFirstEvent))).waitUntilStarted(Duration.ofSeconds(10));

            // Then the second event arrives again, since the change stream reopened where the dynamic position
            // resolved to. The position the subscription had read to was past it and would have delivered only the third.
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(4));
            assertThat(state).extracting(CloudEvent::getId)
                    .containsExactly(firstEvent.eventId(), secondEvent.eventId(), secondEvent.eventId(), thirdEvent.eventId());
        }

        @Test
        void resuming_at_a_dynamic_position_that_resolves_to_a_global_position_checkpoint_while_the_present_is_being_recorded_does_not_skip_what_was_written_meanwhile() throws InterruptedException {
            assertWhatWasWrittenWhileThePresentIsBeingRecordedIsDeliveredWhenResumingAt(
                    liveStart -> StartAt.dynamic(() -> StartAt.checkpoint(globalPositionCheckpoint(liveStart))));
        }

        @Test
        void resuming_at_a_global_position_checkpoint_while_the_present_is_being_recorded_does_not_skip_what_was_written_meanwhile() throws InterruptedException {
            assertWhatWasWrittenWhileThePresentIsBeingRecordedIsDeliveredWhenResumingAt(
                    liveStart -> StartAt.checkpoint(globalPositionCheckpoint(liveStart)));
        }

        @Test
        void resuming_a_subscription_paused_before_its_dynamic_start_opened_at_a_dynamic_position_that_resolves_to_an_unreadable_checkpoint_does_not_skip_what_was_written_while_paused() throws Exception {
            assertWhatWasWrittenWhilePausedIsDeliveredWhenAPausedDynamicStartIsResumedAtAnUnreadableCheckpoint(() -> StartAt.now());
        }

        @Test
        void resuming_a_subscription_paused_before_its_dynamic_default_start_opened_at_a_dynamic_position_that_resolves_to_an_unreadable_checkpoint_does_not_skip_what_was_written_while_paused() throws Exception {
            assertWhatWasWrittenWhilePausedIsDeliveredWhenAPausedDynamicStartIsResumedAtAnUnreadableCheckpoint(() -> StartAt.subscriptionModelDefault());
        }

        private void assertWhatWasWrittenWhileThePresentIsBeingRecordedIsDeliveredWhenResumingAt(Function<Checkpoint, StartAt> repositionTo) throws InterruptedException {
            // Given
            HoldTheFirstPing firstPing = new HoldTheFirstPing();
            ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = null;
            try (MongoClient listeningClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(firstPing).build())) {
                MongoDatabase listeningDatabase = listeningClient.getDatabase(requireNonNull(connectionString.getDatabase()));
                model = new NativeMongoSubscriptionModel(listeningDatabase, listeningDatabase.getCollection(requireNonNull(connectionString.getCollection())), timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
                LocalDateTime now = LocalDateTime.now();
                CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
                String subscriptionId = UUID.randomUUID().toString();
                Checkpoint liveStart = requireNonNull(model.globalCheckpoint());
                model.subscribePaused(subscriptionId, null, StartAt.now(), state::add);
                assertThat(firstPing.held.await(5, SECONDS)).as("the reply to the ping that records the present is held").isTrue();

                // When
                NameDefined writtenWhileRecording = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
                mongoEventStore.write("1", 0, serialize(writtenWhileRecording));
                Subscription resumed = model.resumeSubscription(subscriptionId, repositionTo.apply(liveStart));
                firstPing.release();
                resumed.waitUntilStarted(Duration.ofSeconds(10));
                NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "sentinel");
                mongoEventStore.write("1", 1, serialize(sentinel));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(state).extracting(CloudEvent::getId).contains(sentinel.eventId()));

                // Then what was written after the present was read, and before the resume, is not lost. Duplicates are not asserted on.
                assertThat(state).extracting(CloudEvent::getId).contains(writtenWhileRecording.eventId());
            } finally {
                firstPing.release();
                if (model != null) {
                    model.shutdown();
                }
                executor.shutdown();
                ExecutorShutdown.shutdownSafely(executor, 10, TimeUnit.SECONDS);
            }
        }

        private void assertWhatWasWrittenWhilePausedIsDeliveredWhenAPausedDynamicStartIsResumedAtAnUnreadableCheckpoint(Supplier<StartAt> dynamicStart) throws Exception {
            // Given
            // One thread, so a task submitted after subscribePaused runs once the present at subscribe is recorded
            ExecutorService executor = Executors.newSingleThreadExecutor();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
            try {
                LocalDateTime now = LocalDateTime.now();
                CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
                String subscriptionId = UUID.randomUUID().toString();
                Checkpoint liveStart = requireNonNull(model.globalCheckpoint());
                model.subscribePaused(subscriptionId, null, StartAt.dynamic(dynamicStart), state::add);
                executor.submit(() -> {
                }).get(5, SECONDS);

                // When
                NameDefined duringPause = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
                mongoEventStore.write("1", 0, serialize(duringPause));
                model.resumeSubscription(subscriptionId, StartAt.dynamic(() -> StartAt.checkpoint(globalPositionCheckpoint(liveStart)))).waitUntilStarted(Duration.ofSeconds(10));
                NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "sentinel");
                mongoEventStore.write("1", 1, serialize(sentinel));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(state).extracting(CloudEvent::getId).contains(sentinel.eventId()));

                // Then what was written after the present at subscribe was recorded, while the subscription was paused, is not lost. Duplicates are not asserted on.
                assertThat(state).extracting(CloudEvent::getId).contains(duringPause.eventId());
            } finally {
                model.shutdown();
                executor.shutdown();
                ExecutorShutdown.shutdownSafely(executor, 10, TimeUnit.SECONDS);
            }
        }

        @Test
        void resuming_at_the_present_while_the_present_at_subscribe_is_being_recorded_does_not_deliver_what_was_written_before_the_resume() throws InterruptedException {
            assertWhatWasWrittenBeforeAResumeAtThePresentIsNotDelivered(subscribedAt -> StartAt.now());
        }

        @Test
        void resuming_at_the_very_start_the_subscription_was_made_with_while_the_present_at_subscribe_is_being_recorded_does_not_deliver_what_was_written_before_the_resume() throws InterruptedException {
            assertWhatWasWrittenBeforeAResumeAtThePresentIsNotDelivered(subscribedAt -> subscribedAt);
        }

        private void assertWhatWasWrittenBeforeAResumeAtThePresentIsNotDelivered(Function<StartAt, StartAt> repositionTo) throws InterruptedException {
            // Given
            HoldTheFirstPing firstPing = new HoldTheFirstPing();
            ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
            ExecutorService executor = Executors.newCachedThreadPool();
            NativeMongoSubscriptionModel model = null;
            try (MongoClient listeningClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(firstPing).build())) {
                MongoDatabase listeningDatabase = listeningClient.getDatabase(requireNonNull(connectionString.getDatabase()));
                model = new NativeMongoSubscriptionModel(listeningDatabase, listeningDatabase.getCollection(requireNonNull(connectionString.getCollection())), timeRepresentation, executor, RetryStrategy.exponentialBackoff(Duration.of(100, MILLIS), Duration.of(500, MILLIS), 2));
                LocalDateTime now = LocalDateTime.now();
                CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
                String subscriptionId = UUID.randomUUID().toString();
                StartAt subscribedAt = StartAt.now();
                model.subscribePaused(subscriptionId, null, subscribedAt, state::add);
                assertThat(firstPing.held.await(5, SECONDS)).as("the reply to the ping that records the present is held").isTrue();

                // When
                NameDefined backlog = new NameDefined(UUID.randomUUID().toString(), now, "name", "backlog");
                mongoEventStore.write("1", 0, serialize(backlog));
                Subscription resumed = model.resumeSubscription(subscriptionId, repositionTo.apply(subscribedAt));
                firstPing.release();
                resumed.waitUntilStarted(Duration.ofSeconds(10));
                NameWasChanged after = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "after");
                mongoEventStore.write("1", 1, serialize(after));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(state).extracting(CloudEvent::getId).contains(after.eventId()));

                // Then what was written before the resume at the present is not delivered, though the present at subscribe was read before it was written
                assertThat(state).extracting(CloudEvent::getId).as("the events delivered after a resume at the present").doesNotContain(backlog.eventId());
            } finally {
                firstPing.release();
                if (model != null) {
                    model.shutdown();
                }
                executor.shutdown();
                ExecutorShutdown.shutdownSafely(executor, 10, TimeUnit.SECONDS);
            }
        }

        private Checkpoint globalPositionCheckpoint(Checkpoint liveStart) {
            return GlobalCheckpoint.parse(new StringBasedCheckpoint(GlobalCheckpoint.of(5, liveStart, 0, 7).asString()));
        }

        private Checkpoint timeBasedCheckpoint() {
            return new StringBasedCheckpoint("2026-01-01T00:00:00Z");
        }

        private void assertWhatWasWrittenWhilePausedIsDeliveredWhenResumingAt(Function<Checkpoint, StartAt> repositionTo) {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint liveStart = requireNonNull(subscriptionModel.globalCheckpoint());
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), state::add).waitUntilStarted(Duration.ofSeconds(10));

            NameDefined firstEvent = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(firstEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            NameWasChanged duringPause = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            mongoEventStore.write("1", 1, serialize(duringPause));
            subscriptionModel.resumeSubscription(subscriptionId, repositionTo.apply(liveStart)).waitUntilStarted(Duration.ofSeconds(10));
            NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "sentinel");
            mongoEventStore.write("1", 2, serialize(sentinel));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(state).extracting(CloudEvent::getId).contains(sentinel.eventId()));

            // Then nothing written while the subscription was paused is lost. Duplicates are not asserted on.
            assertThat(state).extracting(CloudEvent::getId).contains(duringPause.eventId());
        }
    }

    @Nested
    @DisplayName("Subscribe at a catch-up position")
    class SubscribeAtACatchupPositionTest {

        @Test
        void subscribing_at_a_global_checkpoint_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(7), false);
        }

        @Test
        void subscribing_at_a_global_checkpoint_with_a_live_start_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(5, liveStart(), 0, 7), false);
        }

        @Test
        void subscribing_at_a_global_checkpoint_read_back_as_a_string_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(new StringBasedCheckpoint(GlobalCheckpoint.of(5, liveStart(), 0, 7).asString()), false);
        }

        @Test
        void subscribing_at_a_catchup_time_checkpoint_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(catchupTimeCheckpoint(), false);
        }

        @Test
        void subscribing_at_a_catchup_time_checkpoint_read_back_as_a_string_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(new StringBasedCheckpoint(catchupTimeCheckpoint().asString()), false);
        }

        @Test
        void subscribing_paused_at_a_global_checkpoint_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(7), true);
        }

        @Test
        void subscribing_paused_at_a_catchup_time_checkpoint_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(catchupTimeCheckpoint(), true);
        }

        @Test
        void subscribing_at_a_time_string_is_still_accepted() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();

            // When
            subscriptionModel.subscribe(subscriptionId, StartAt.checkpoint(new StringBasedCheckpoint("2026-01-01T00:00:00Z")), state::add).waitUntilStarted(Duration.ofSeconds(10));
            NameDefined written = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(written));

            // Then
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(state).extracting(CloudEvent::getId).contains(written.eventId()));
        }

        @Test
        void subscribing_at_a_global_checkpoint_of_zero_is_refused_and_leaves_the_id_free() {
            assertSubscribeIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(0), false);
        }

        @Test
        void subscribing_at_a_dynamic_position_answering_a_global_checkpoint_is_refused_and_leaves_the_id_free() {
            assertDynamicStartIsRefusedAndLeavesTheIdFree(GlobalCheckpoint.of(0));
        }

        @Test
        void subscribing_at_a_dynamic_position_answering_a_catchup_time_checkpoint_is_refused_and_leaves_the_id_free() {
            assertDynamicStartIsRefusedAndLeavesTheIdFree(catchupTimeCheckpoint());
        }

        @Test
        void waiting_without_a_timeout_on_a_dynamic_position_answering_a_global_checkpoint_throws_the_refusal_instead_of_hanging() {
            // Given
            Checkpoint checkpoint = GlobalCheckpoint.of(0);
            StartAt answered = StartAt.checkpoint(checkpoint);
            String subscriptionId = UUID.randomUUID().toString();
            Subscription started = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> answered), __ -> {
            });

            // When
            Throwable refusal = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> catchThrowable(started::waitUntilStarted));

            // Then
            assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                    .hasMessageContaining("\"" + checkpoint.asString() + "\"");
            assertThat(((UnsupportedStartAtException) refusal).startAt()).isSameAs(answered);
        }

        @Test
        void a_paused_subscription_whose_dynamic_position_answers_a_global_checkpoint_when_it_opens_throws_the_refusal_from_both_handles() {
            // Given
            CopyOnWriteArrayList<CloudEvent> refused = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint checkpoint = GlobalCheckpoint.of(0);
            StartAt answered = StartAt.checkpoint(checkpoint);
            Subscription paused = subscriptionModel.subscribePaused(subscriptionId, null, StartAt.dynamic(ctx -> answered), refused::add);

            // When
            Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);

            // Then both handles throw the one refusal
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse());
            Throwable fromPaused = catchThrowable(() -> paused.waitUntilStarted(Duration.ofMillis(200)));
            Throwable fromResumed = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofMillis(200)));
            assertThat(fromPaused).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                    .hasMessageContaining("\"" + checkpoint.asString() + "\"");
            assertAll(
                    () -> assertThat(((UnsupportedStartAtException) fromPaused).startAt()).isSameAs(answered),
                    () -> assertThat(fromResumed).isSameAs(fromPaused),
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                    () -> assertThat(refused).isEmpty()
            );
        }

        @Test
        void subscribing_at_a_dynamic_position_answering_a_live_checkpoint_delivers_the_events_after_it() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> first = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> second = new CopyOnWriteArrayList<>();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), first::add).waitUntilStarted(Duration.ofSeconds(10));
            NameDefined e1 = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(e1));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(first).extracting(CloudEvent::getId).contains(e1.eventId()));
            Checkpoint afterE1 = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(first.getFirst());
            NameWasChanged e2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            mongoEventStore.write("1", 1, serialize(e2));

            // When
            Subscription started = subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.dynamic(ctx -> StartAt.checkpoint(afterE1)), second::add);

            // Then it starts and delivers what was written after the checkpoint, and not what came before it
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(second).extracting(CloudEvent::getId).contains(e2.eventId()));
            assertThat(second).extracting(CloudEvent::getId).doesNotContain(e1.eventId());
        }

        @Test
        void a_refusal_of_a_run_a_resume_has_replaced_does_not_reach_the_handle_of_the_subscribe() throws Exception {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            CountDownLatch resumedRunMayOpen = new CountDownLatch(1);
            HoldsTheFirstRefusal holdsTheRefusal = new HoldsTheFirstRefusal();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            modelLogger.addAppender(holdsTheRefusal);
            try {
                Subscription first = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), state::add);
                assertThat(holdsTheRefusal.held.await(10, SECONDS)).isTrue();

                // When the run is paused and replaced by a resume while its refusal is held between the check and the failure
                subscriptionModel.pauseSubscription(subscriptionId);
                Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.dynamic(ctx -> {
                    try {
                        resumedRunMayOpen.await(20, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return StartAt.now();
                }));
                holdsTheRefusal.release.countDown();

                // Then the handle of the subscribe doesn't throw the refusal of the replaced run, and has not started yet
                Throwable thrown = catchThrowable(() -> first.waitUntilStarted(Duration.ofMillis(500)));
                assertThat(thrown).isNull();

                // And the resumed run opens, answering for both handles, and delivers
                resumedRunMayOpen.countDown();
                assertThat(first.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
                assertThat(resumed.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
                NameDefined written = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
                mongoEventStore.write("1", 0, serialize(written));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(state).extracting(CloudEvent::getId).contains(written.eventId()));
                assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
            } finally {
                holdsTheRefusal.release.countDown();
                resumedRunMayOpen.countDown();
                modelLogger.detachAppender(holdsTheRefusal);
            }
        }

        @Test
        void a_refusal_of_a_run_a_pause_has_closed_is_left_to_the_resume() throws Exception {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            HoldsTheFirstRefusal holdsTheRefusal = new HoldsTheFirstRefusal();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            modelLogger.addAppender(holdsTheRefusal);
            try {
                Subscription first = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), state::add);
                assertThat(holdsTheRefusal.held.await(10, SECONDS)).isTrue();

                // When the run is paused while its refusal is held, and the refusal is then released
                subscriptionModel.pauseSubscription(subscriptionId);
                holdsTheRefusal.release.countDown();

                // Then the handle of the subscribe doesn't throw the refusal, has not started, and the subscription is paused
                AtomicBoolean startedInTime = new AtomicBoolean(true);
                Throwable thrown = catchThrowable(() -> startedInTime.set(first.waitUntilStarted(Duration.ofMillis(500))));
                assertThat(thrown).isNull();
                assertThat(startedInTime).isFalse();
                assertThat(subscriptionModel.isPaused(subscriptionId)).isTrue();

                // And a resume at the present opens it, answering for both handles, and it delivers
                Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId, StartAt.now());
                assertThat(first.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
                assertThat(resumed.waitUntilStarted(Duration.ofSeconds(10))).isTrue();
                NameDefined written = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
                mongoEventStore.write("1", 0, serialize(written));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(state).extracting(CloudEvent::getId).contains(written.eventId()));
                assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
            } finally {
                holdsTheRefusal.release.countDown();
                modelLogger.detachAppender(holdsTheRefusal);
            }
        }

        @Test
        void a_resume_of_a_run_a_pause_has_closed_asks_the_position_again_and_is_refused() throws Exception {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> refused = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> later = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint checkpoint = GlobalCheckpoint.of(7);
            HoldsTheFirstRefusal holdsTheRefusal = new HoldsTheFirstRefusal();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            modelLogger.addAppender(holdsTheRefusal);
            try {
                Subscription first = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(checkpoint)), refused::add);
                assertThat(holdsTheRefusal.held.await(10, SECONDS)).isTrue();
                subscriptionModel.pauseSubscription(subscriptionId);
                holdsTheRefusal.release.countDown();

                // When the subscription is resumed without a position
                Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);

                // Then the position is asked again and answers the checkpoint again, so both handles throw that refusal
                Throwable fromFirst = catchThrowable(() -> first.waitUntilStarted(Duration.ofSeconds(10)));
                Throwable fromResumed = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
                assertThat(fromFirst).isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                        .hasMessageContaining("\"" + checkpoint.asString() + "\"");
                assertThat(fromResumed).isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                        .hasMessageContaining("\"" + checkpoint.asString() + "\"");

                // And nothing is registered under the id
                assertAll(
                        () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                        () -> assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse(),
                        () -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId)
                );

                // And the id can be used again, while the refused handler is never called
                subscriptionModel.subscribe(subscriptionId, StartAt.now(), later::add).waitUntilStarted(Duration.ofSeconds(10));
                NameDefined sentinel = new NameDefined(UUID.randomUUID().toString(), now, "name", "sentinel");
                mongoEventStore.write("1", 0, serialize(sentinel));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(later).extracting(CloudEvent::getId).contains(sentinel.eventId()));
                assertThat(refused).isEmpty();
            } finally {
                holdsTheRefusal.release.countDown();
                modelLogger.detachAppender(holdsTheRefusal);
            }
        }

        @Test
        void a_refusal_of_a_run_a_cancel_has_closed_does_not_reach_the_handle_of_the_subscribe() throws Exception {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> later = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            HoldsTheFirstRefusal holdsTheRefusal = new HoldsTheFirstRefusal();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            modelLogger.addAppender(holdsTheRefusal);
            try {
                Subscription first = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), __ -> {
                });
                assertThat(holdsTheRefusal.held.await(10, SECONDS)).isTrue();

                // When the run is cancelled while its refusal is held, and the refusal is then released
                subscriptionModel.cancelSubscription(subscriptionId);
                holdsTheRefusal.release.countDown();

                // Then the handle of the subscribe doesn't throw the refusal and has not started
                AtomicBoolean startedInTime = new AtomicBoolean(true);
                Throwable thrown = catchThrowable(() -> startedInTime.set(first.waitUntilStarted(Duration.ofMillis(500))));
                assertThat(thrown).isNull();
                assertThat(startedInTime).isFalse();

                // And nothing is registered under the id, which can be used again
                assertAll(
                        () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                        () -> assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse(),
                        () -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId)
                );
                subscriptionModel.subscribe(subscriptionId, StartAt.now(), later::add).waitUntilStarted(Duration.ofSeconds(10));
                NameDefined sentinel = new NameDefined(UUID.randomUUID().toString(), now, "name", "sentinel");
                mongoEventStore.write("1", 0, serialize(sentinel));
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(later).extracting(CloudEvent::getId).contains(sentinel.eventId()));
            } finally {
                holdsTheRefusal.release.countDown();
                modelLogger.detachAppender(holdsTheRefusal);
            }
        }

        @Test
        void a_resume_of_a_run_a_pause_closed_before_it_opened_is_refused_when_the_position_then_answers_a_global_checkpoint() throws Exception {
            // Given a live checkpoint with an event written after it, and a position that answers it only to the first run
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint afterE1 = checkpointAfter(new NameDefined(UUID.randomUUID().toString(), now, "name", "name1"));
            mongoEventStore.write("1", 1, serialize(new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2")));
            Checkpoint catchup = GlobalCheckpoint.of(7);
            AtomicInteger calls = new AtomicInteger();
            CountDownLatch asked = new CountDownLatch(1);
            CountDownLatch mayAnswer = new CountDownLatch(1);
            subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                if (calls.incrementAndGet() > 1) {
                    return StartAt.checkpoint(catchup);
                }
                asked.countDown();
                try {
                    mayAnswer.await(20, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return StartAt.checkpoint(afterE1);
            }), state::add);
            assertThat(asked.await(10, SECONDS)).isTrue();

            // And the run is paused while it is asked, so it ends without opening a change stream
            subscriptionModel.pauseSubscription(subscriptionId);
            mayAnswer.countDown();
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(activeSubscriptionThreads()).isZero());

            // When the subscription is resumed without a position
            Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);

            // Then the position answers the catch-up checkpoint, which refuses it instead of opening at the present
            Throwable refusal = catchThrowable(() -> resumed.waitUntilStarted(Duration.ofSeconds(10)));
            assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                    .hasMessageContaining("\"" + catchup.asString() + "\"");
            assertAll(
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                    () -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId),
                    () -> assertThat(state).isEmpty()
            );
        }

        @Test
        void a_dynamic_position_whose_first_answer_fails_to_open_and_whose_retry_answers_a_global_checkpoint_is_refused() {
            // Given a first answer that is not a resume token MongoDB accepts, and a retry that answers a catch-up checkpoint
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint catchup = GlobalCheckpoint.of(7);
            AtomicInteger calls = new AtomicInteger();
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));

            // When it subscribes
            Subscription started = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> calls.incrementAndGet() == 1
                    ? StartAt.checkpoint(new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("not-a-token"))))
                    : StartAt.checkpoint(catchup)), state::add);

            // Then no change stream has opened at either answer, so the second is refused
            Throwable refusal = catchThrowable(() -> started.waitUntilStarted(Duration.ofSeconds(10)));
            assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                    .hasMessageContaining("\"" + catchup.asString() + "\"");
            assertAll(
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                    () -> assertThat(state).isEmpty()
            );
        }

        @Test
        void an_action_that_throws_the_refusal_type_on_a_delivering_subscription_does_not_forget_the_subscription() throws Exception {
            assertSubscriptionIsKnownAfterTheActionThrows(new UnsupportedStartAtException(StartAt.now(), "thrown by the action"));
        }

        @Test
        void an_action_that_throws_another_exception_on_a_delivering_subscription_does_not_forget_the_subscription() throws Exception {
            assertSubscriptionIsKnownAfterTheActionThrows(new IllegalStateException("thrown by the action"));
        }

        @Test
        void start_throws_the_refusal_of_a_subscription_made_while_stopped_whose_position_answers_a_global_checkpoint() {
            // Given a stopped model whose resume holds its monitor until the run is gone
            String subscriptionId = UUID.randomUUID().toString();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none()) {
                @Override
                public synchronized Subscription resumeSubscription(String id) {
                    Subscription resumed = super.resumeSubscription(id);
                    await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(10, MILLIS)).until(() -> !isRunning(id));
                    return resumed;
                }
            };
            try {
                model.stop();
                model.subscribePaused(subscriptionId, null, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), __ -> {
                });

                // When it starts and resumes the subscription
                Throwable thrown = catchThrowable(() -> model.start(true));

                // Then the run was already gone when the resume returned, and start throws the refusal it recorded
                assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("subscription \"" + subscriptionId + "\"");
            } finally {
                model.shutdown();
            }
        }

        @Test
        void stop_does_not_throw_when_the_refusal_of_a_run_races_the_pause_of_it() throws Exception {
            // Given a refusal held between the check and the removal, and a model whose first pause releases it and
            // waits until the run is gone
            String subscriptionId = UUID.randomUUID().toString();
            HoldsTheFirstRefusal holdsTheRefusal = new HoldsTheFirstRefusal();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            AtomicBoolean firstPause = new AtomicBoolean(true);
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none()) {
                @Override
                public void pauseSubscription(String id) {
                    if (firstPause.getAndSet(false)) {
                        holdsTheRefusal.release.countDown();
                        await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(10, MILLIS)).until(() -> !isRunning(id));
                    }
                    super.pauseSubscription(id);
                }
            };
            modelLogger.addAppender(holdsTheRefusal);
            try {
                model.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), __ -> {
                });
                assertThat(holdsTheRefusal.held.await(10, SECONDS)).isTrue();

                // When the model is stopped
                Throwable thrown = catchThrowable(model::stop);

                // Then the pause of a run the refusal was about to remove doesn't fail
                assertThat(thrown).isNull();
            } finally {
                holdsTheRefusal.release.countDown();
                modelLogger.detachAppender(holdsTheRefusal);
                model.shutdown();
            }
        }

        @Test
        void start_throws_the_refusal_of_a_subscription_made_while_stopped_when_the_resume_waits_for_the_handle_to_start() throws Exception {
            // Given a stopped model whose resume waits for the subscription to start, while start holds the monitor
            String subscriptionId = UUID.randomUUID().toString();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none()) {
                @Override
                public Subscription resumeSubscription(String id) {
                    Subscription resumed = super.resumeSubscription(id);
                    resumed.waitUntilStarted();
                    return resumed;
                }
            };
            ExecutorService starter = Executors.newSingleThreadExecutor();
            try {
                model.stop();
                model.subscribePaused(subscriptionId, null, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), __ -> {
                });

                // When it starts and resumes the subscription, on a thread of its own so that a stall fails the assertion
                Future<Throwable> started = starter.submit(() -> catchThrowable(() -> model.start(true)));

                // Then start ends and throws the refusal
                assertThat(started).succeedsWithin(Duration.ofSeconds(5))
                        .asInstanceOf(InstanceOfAssertFactories.THROWABLE)
                        .isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("subscription \"" + subscriptionId + "\"");
            } finally {
                starter.shutdownNow();
                starter.awaitTermination(5, SECONDS);
                model.shutdown();
            }
        }

        @Test
        void waiting_for_a_refused_subscription_while_holding_the_monitor_of_the_model_throws_the_refusal() {
            // Given a thread that holds the monitor of the model
            String subscriptionId = UUID.randomUUID().toString();
            Throwable thrown;
            synchronized (subscriptionModel) {
                // When it subscribes at a position that answers a global checkpoint and waits for the subscription
                Subscription refused = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), __ -> {
                });
                thrown = catchThrowable(() -> refused.waitUntilStarted(Duration.ofSeconds(5)));
            }

            // Then the wait ends with the refusal before its bound, since the refusal needs no monitor
            assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"");
        }

        @Test
        void the_run_of_a_refused_subscription_ends_while_stop_holds_the_monitor_and_a_pause_waits_for_it() throws Exception {
            // Given a refusal held between the check and the removal, and a model whose first pause releases it and
            // waits for the run to end
            String subscriptionId = UUID.randomUUID().toString();
            HoldsTheFirstRefusal holdsTheRefusal = new HoldsTheFirstRefusal();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            AtomicBoolean firstPause = new AtomicBoolean(true);
            AtomicBoolean runEndedDuringThePause = new AtomicBoolean();
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none()) {
                @Override
                public void pauseSubscription(String id) {
                    super.pauseSubscription(id);
                    if (firstPause.getAndSet(false)) {
                        holdsTheRefusal.release.countDown();
                        runEndedDuringThePause.set(becomesTrueWithinFiveSeconds(() -> activeSubscriptionThreads() == 0));
                    }
                }
            };
            modelLogger.addAppender(holdsTheRefusal);
            try {
                model.subscribe(subscriptionId, StartAt.dynamic(ctx -> StartAt.checkpoint(GlobalCheckpoint.of(7))), __ -> {
                });
                assertThat(holdsTheRefusal.held.await(10, SECONDS)).isTrue();

                // When the model is stopped
                model.stop();

                // Then the refusal let go of the run while the pause waited for it
                assertThat(runEndedDuringThePause).isTrue();
            } finally {
                holdsTheRefusal.release.countDown();
                modelLogger.detachAppender(holdsTheRefusal);
                model.shutdown();
            }
        }

        @Test
        void start_throws_the_refusal_of_a_run_that_stops_restarting_after_start_found_it_running() {
            // Given a stopped model whose resume makes the first check of whether the run stopped restarting wait for the
            // position to answer and the refusal to end the run
            String subscriptionId = UUID.randomUUID().toString();
            CountDownLatch positionMayAnswer = new CountDownLatch(1);
            NativeMongoSubscriptionModel model = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none()) {
                @Override
                public synchronized Subscription resumeSubscription(String id) {
                    Subscription resumed = super.resumeSubscription(id);
                    replaceLatchOfTheRunOf(this, id, "stoppedRestartingLatch", own -> new RunsEndsBeforeTheFirstRead(own, positionMayAnswer));
                    return resumed;
                }
            };
            try {
                model.stop();
                model.subscribePaused(subscriptionId, null, StartAt.dynamic(ctx -> {
                    awaitUpToTenSeconds(positionMayAnswer);
                    return StartAt.checkpoint(GlobalCheckpoint.of(7));
                }), __ -> {
                });

                // When it starts and resumes the subscription
                Throwable thrown = catchThrowable(() -> model.start(true));

                // Then it throws the refusal, though the run stopped restarting only after start had found it running
                assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("subscription \"" + subscriptionId + "\"");
            } finally {
                model.shutdown();
            }
        }

        @Test
        void the_id_of_a_refused_run_is_free_when_its_waiters_are_released() throws Exception {
            // Given a run whose position waits to answer a global checkpoint, and a started latch that looks at the model
            // as the waiters of the run are released
            String subscriptionId = UUID.randomUUID().toString();
            CountDownLatch positionAsked = new CountDownLatch(1);
            CountDownLatch positionMayAnswer = new CountDownLatch(1);
            CompletableFuture<Boolean> runningAtTheRelease = new CompletableFuture<>();
            CompletableFuture<Throwable> subscribedAgainAtTheRelease = new CompletableFuture<>();
            Subscription refused = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                positionAsked.countDown();
                awaitUpToTenSeconds(positionMayAnswer);
                return StartAt.checkpoint(GlobalCheckpoint.of(7));
            }), __ -> {
            });
            assertThat(positionAsked.await(10, SECONDS)).isTrue();
            replaceLatchOfTheRunOf(subscriptionModel, subscriptionId, "startedLatch", own -> new LooksBeforeItReleasesTheWaiters(own, () -> {
                runningAtTheRelease.complete(subscriptionModel.isRunning(subscriptionId));
                subscribedAgainAtTheRelease.complete(catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
                })));
            }));

            // When the position answers and the refusal releases the waiters of the run
            positionMayAnswer.countDown();
            Throwable thrown = catchThrowable(() -> refused.waitUntilStarted(Duration.ofSeconds(5)));

            // Then the handle throws the refusal
            assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"");
            // And the run was no longer running as they were released, and the id could be subscribed to again
            assertThat(runningAtTheRelease).succeedsWithin(Duration.ofSeconds(5)).isEqualTo(false);
            assertThat(subscribedAgainAtTheRelease).succeedsWithin(Duration.ofSeconds(5)).isNull();
        }

        @Test
        void the_handle_of_a_subscribe_is_not_released_by_a_run_a_resume_replaced_until_the_refusal_has_freed_the_id() throws Exception {
            // Given a run whose position answers the present first, and a global checkpoint after
            String subscriptionId = UUID.randomUUID().toString();
            AtomicInteger answers = new AtomicInteger();
            CountDownLatch positionAsked = new CountDownLatch(1);
            CountDownLatch positionMayAnswer = new CountDownLatch(1);
            HoldsTheRemovalOfARefusedRun refusedRunsRemoval = new HoldsTheRemovalOfARefusedRun();
            replaceRunningSubscriptionsOf(subscriptionModel, refusedRunsRemoval);
            Subscription first = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                if (answers.incrementAndGet() == 1) {
                    positionAsked.countDown();
                    awaitUpToTenSeconds(positionMayAnswer);
                    return StartAt.now();
                }
                return StartAt.checkpoint(GlobalCheckpoint.of(0));
            }), __ -> {
            });
            // And the run is held as it has opened its change stream, before it reports that it started
            HoldsTheRunWhenItOpened holdsTheFirstRun = new HoldsTheRunWhenItOpened(Thread.currentThread());
            AtomicReference<CountDownLatch> startedLatchOfTheFirstRun = new AtomicReference<>();
            ExecutorService waiterExecutor = Executors.newSingleThreadExecutor();
            try {
                assertThat(positionAsked.await(10, SECONDS)).isTrue();
                replaceLockOfTheRunOf(subscriptionModel, subscriptionId, holdsTheFirstRun);
                // Kept before the pause, since the resume makes a latch of its own
                replaceLatchOfTheRunOf(subscriptionModel, subscriptionId, "startedLatch", own -> {
                    startedLatchOfTheFirstRun.set(own);
                    return own;
                });

                // And a waiter on the first handle that looks at the model when the handle lets it go
                record WhatTheWaiterSaw(Throwable thrown, boolean running, Throwable subscribedAgain) {
                }
                CompletableFuture<WhatTheWaiterSaw> waiter = CompletableFuture.supplyAsync(() -> {
                    Throwable thrown = catchThrowable(() -> first.waitUntilStarted(Duration.ofSeconds(60)));
                    boolean running = subscriptionModel.isRunning(subscriptionId);
                    Throwable subscribedAgain = catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
                    }));
                    return new WhatTheWaiterSaw(thrown, running, subscribedAgain);
                }, waiterExecutor);

                // When the subscription is paused and resumed after the first run opened, and the run of the resume is refused
                // while the id is still taken
                positionMayAnswer.countDown();
                assertThat(holdsTheFirstRun.held.await(10, SECONDS)).as("the first run opened its change stream").isTrue();
                subscriptionModel.pauseSubscription(subscriptionId);
                subscriptionModel.resumeSubscription(subscriptionId);
                assertThat(refusedRunsRemoval.held.await(10, SECONDS)).as("the refused run was recorded and held").isTrue();

                // And the first run then reports that it started
                holdsTheFirstRun.release.countDown();
                assertThat(startedLatchOfTheFirstRun.get().await(10, SECONDS)).as("the first run reported that it started").isTrue();

                // Then a waiter that is let go while the id is taken is let go within a second, so it is only given that long
                catchThrowable(() -> waiter.get(1, SECONDS));
                refusedRunsRemoval.release.countDown();

                // And once it is let go it throws the refusal with the id free, so it can be subscribed to again
                WhatTheWaiterSaw saw = waiter.get(10, SECONDS);
                assertThat(saw.thrown()).isExactlyInstanceOf(UnsupportedStartAtException.class);
                assertThat(saw.running()).as("running when the waiter was let go").isFalse();
                assertThat(saw.subscribedAgain()).as("thrown by subscribing again when the waiter was let go").isNull();
            } finally {
                positionMayAnswer.countDown();
                holdsTheFirstRun.release.countDown();
                refusedRunsRemoval.release.countDown();
                waiterExecutor.shutdownNow();
            }
        }

        @Test
        void a_resume_that_answered_it_started_ends_the_check_before_its_handle_is_released() throws Exception {
            // Given a paused subscription whose position answers a live checkpoint first and a global checkpoint after
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint live = liveStart();
            AtomicInteger answers = new AtomicInteger();
            CountDownLatch positionAsked = new CountDownLatch(1);
            CountDownLatch positionMayAnswer = new CountDownLatch(1);
            CountDownLatch held = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Subscription original = subscriptionModel.subscribePaused(subscriptionId, null, StartAt.dynamic(ctx -> {
                if (answers.incrementAndGet() == 1) {
                    positionAsked.countDown();
                    awaitUpToTenSeconds(positionMayAnswer);
                    return StartAt.checkpoint(live);
                }
                return StartAt.checkpoint(GlobalCheckpoint.of(0));
            }), __ -> {
            });
            try {
                // And a resume whose thread is held right after it released the handle of the resume
                Subscription resumed = subscriptionModel.resumeSubscription(subscriptionId);
                assertThat(positionAsked.await(10, SECONDS)).isTrue();
                replaceLatchOfTheRunOf(subscriptionModel, subscriptionId, "startedLatch", own -> new HoldsAfterReleasingTheStartedLatch(own, held, release));
                positionMayAnswer.countDown();
                assertThat(resumed.waitUntilStarted(Duration.ofSeconds(5))).isTrue();
                assertThat(held.await(10, SECONDS)).isTrue();

                // When the subscription is paused and resumed while the thread is held, and the second run has answered
                boolean[] secondRunStarted = new boolean[1];
                subscriptionModel.pauseSubscription(subscriptionId);
                Subscription resumedAgain = subscriptionModel.resumeSubscription(subscriptionId);
                Throwable thrownByTheSecondRun = catchThrowable(() -> secondRunStarted[0] = resumedAgain.waitUntilStarted(Duration.ofSeconds(5)));
                release.countDown();

                // Then the second run isn't refused, since a change stream has opened, and the original handle doesn't throw
                assertThat(thrownByTheSecondRun).isNull();
                assertThat(secondRunStarted[0]).isTrue();
                assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
                Throwable thrownByTheOriginal = catchThrowable(() -> original.waitUntilStarted(Duration.ofSeconds(5)));
                assertThat(thrownByTheOriginal).isNull();
            } finally {
                release.countDown();
            }
        }

        @Test
        void an_answer_that_throws_the_refusal_type_after_a_cursor_opened_restarts() {
            // Given a dynamic position that answers a live checkpoint, then throws the refusal type, then answers a live checkpoint
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint live = liveStart();
            AtomicInteger answers = new AtomicInteger();
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            Subscription started = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                if (answers.incrementAndGet() == 2) {
                    throw new UnsupportedStartAtException(StartAt.now(), "thrown by the answer");
                }
                return StartAt.checkpoint(live);
            }), delivered::add);
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();

            // When it is paused and resumed, so the position is asked again after a cursor has opened
            subscriptionModel.pauseSubscription(subscriptionId);
            subscriptionModel.resumeSubscription(subscriptionId);

            // Then the throw restarts the subscription like any other failure, and it delivers an event written afterwards
            NameDefined written = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> {
                assertThat(answers).hasValueGreaterThanOrEqualTo(3);
                assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
            });
            mongoEventStore.write("1", 0, serialize(written));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(delivered).extracting(CloudEvent::getId).contains(written.eventId()));
        }

        @Test
        void a_refusal_arriving_after_a_stream_opened_still_stops_the_subscription() {
            // Given a dynamic position that answers a live checkpoint, then throws the refusal of a catch-up's position, as the
            // check does, which stands in for a replaced run's late start landing between the check and the catch
            String subscriptionId = UUID.randomUUID().toString();
            Checkpoint live = liveStart();
            AtomicInteger answers = new AtomicInteger();
            Subscription started = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                if (answers.incrementAndGet() == 2) {
                    throw new UnsupportedStartAtException(StartAt.checkpoint(GlobalCheckpoint.of(0)), "thrown as the check does");
                }
                return StartAt.checkpoint(live);
            }), __ -> {
            });
            assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();

            // When it is paused and resumed, so the position is asked again
            subscriptionModel.pauseSubscription(subscriptionId);
            subscriptionModel.resumeSubscription(subscriptionId);

            // Then the run is gone, and the position isn't asked a third time
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse());
            assertThat(answers).hasValue(2);
        }

        @Test
        void a_refused_run_stays_known_until_its_failure_is_recorded() throws Exception {
            // Given a run whose position waits to answer a global checkpoint, and a started latch whose first read, made
            // while the failure of the run is recorded, holds the thread until released
            String subscriptionId = UUID.randomUUID().toString();
            CountDownLatch positionAsked = new CountDownLatch(1);
            CountDownLatch positionMayAnswer = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<HoldsTheFirstReadOfTheStartedLatch> holdsTheRead = new AtomicReference<>();
            Subscription refused = subscriptionModel.subscribe(subscriptionId, StartAt.dynamic(ctx -> {
                positionAsked.countDown();
                awaitUpToTenSeconds(positionMayAnswer);
                return StartAt.checkpoint(GlobalCheckpoint.of(7));
            }), __ -> {
            });
            try {
                assertThat(positionAsked.await(10, SECONDS)).isTrue();
                replaceLatchOfTheRunOf(subscriptionModel, subscriptionId, "startedLatch", own -> {
                    holdsTheRead.set(new HoldsTheFirstReadOfTheStartedLatch(own, release));
                    return holdsTheRead.get();
                });

                // When the refusal is recording the failure of the run
                positionMayAnswer.countDown();
                assertThat(holdsTheRead.get().reached.await(10, SECONDS)).isTrue();

                // Then the run is still known, so nobody reads it as gone before its failure can be read
                assertThat(subscriptionModel.isRunning(subscriptionId)).isTrue();
            } finally {
                release.countDown();
            }

            // And once the failure is recorded, the handle throws the refusal and the id is gone
            Throwable thrown = catchThrowable(() -> refused.waitUntilStarted(Duration.ofSeconds(5)));
            assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"");
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(10, MILLIS)).until(() -> !subscriptionModel.isRunning(subscriptionId));
            assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        }

        private void assertSubscribeIsRefusedAndLeavesTheIdFree(Checkpoint checkpoint, boolean paused) {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> refused = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> later = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            NameDefined writtenBefore = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBefore));
            StartAt startAt = StartAt.checkpoint(checkpoint);

            // When
            Throwable throwable = catchThrowable(() -> {
                if (paused) {
                    subscriptionModel.subscribePaused(subscriptionId, null, startAt, refused::add);
                } else {
                    subscriptionModel.subscribe(subscriptionId, startAt, refused::add);
                }
            });

            // Then it is refused by the call itself, naming the checkpoint, and nothing is registered under the id
            assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class).hasMessageContaining("\"" + checkpoint.asString() + "\"");
            assertAll(
                    () -> assertThat(((UnsupportedStartAtException) throwable).startAt()).isSameAs(startAt),
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse(),
                    () -> assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse(),
                    () -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId)
            );

            // And the id can be used again, while the refused handler is never called
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), later::add).waitUntilStarted(Duration.ofSeconds(10));
            NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "sentinel");
            mongoEventStore.write("1", 1, serialize(sentinel));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(later).extracting(CloudEvent::getId).contains(sentinel.eventId()));
            assertThat(refused).isEmpty();
        }

        private void assertDynamicStartIsRefusedAndLeavesTheIdFree(Checkpoint checkpoint) {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> refused = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> later = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            NameDefined writtenBefore = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBefore));
            StartAt answered = StartAt.checkpoint(checkpoint);
            StartAt startAt = StartAt.dynamic(ctx -> answered);
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
            ListAppender<ILoggingEvent> logged = new ListAppender<>();
            logged.start();
            modelLogger.addAppender(logged);
            try {
                // When
                AtomicReference<Subscription> started = new AtomicReference<>();
                Throwable thrownByTheCall = catchThrowable(() -> started.set(subscriptionModel.subscribe(subscriptionId, startAt, refused::add)));

                // Then the call returns, and the subscription is forgotten and its handle throws the refusal instead of starting
                assertThat(thrownByTheCall).isNull();
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse());
                Throwable refusal = catchThrowable(() -> started.get().waitUntilStarted(Duration.ofMillis(200)));
                assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                        .hasMessageContaining("\"" + checkpoint.asString() + "\"");
                assertAll(
                        () -> assertThat(((UnsupportedStartAtException) refusal).startAt()).isSameAs(answered),
                        () -> assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse(),
                        () -> assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId)
                );

                // And the refusal is logged, naming the id and the checkpoint
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(logged.list).anySatisfy(event -> {
                            assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                            assertThat(event.getThrowableProxy()).isInstanceOfSatisfying(ThrowableProxy.class, proxy -> {
                                assertThat(proxy.getThrowable()).isExactlyInstanceOf(UnsupportedStartAtException.class)
                                        .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                                        .hasMessageContaining("\"" + checkpoint.asString() + "\"");
                                assertThat(((UnsupportedStartAtException) proxy.getThrowable()).startAt()).isSameAs(answered);
                            });
                        }));
            } finally {
                modelLogger.detachAppender(logged);
            }

            // And the id can be used again, while the refused handler is never called
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), later::add).waitUntilStarted(Duration.ofSeconds(10));
            NameWasChanged sentinel = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "sentinel");
            mongoEventStore.write("1", 1, serialize(sentinel));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(later).extracting(CloudEvent::getId).contains(sentinel.eventId()));
            assertThat(refused).isEmpty();
        }

        private void assertSubscriptionIsKnownAfterTheActionThrows(RuntimeException thrownByTheAction) throws Exception {
            // Given a model that doesn't retry, and a delivering subscription whose action throws
            String subscriptionId = UUID.randomUUID().toString();
            CountDownLatch actionRan = new CountDownLatch(1);
            NativeMongoSubscriptionModel noRetries = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor, RetryStrategy.none());
            try {
                Subscription started = noRetries.subscribe(subscriptionId, StartAt.now(), __ -> {
                    actionRan.countDown();
                    throw thrownByTheAction;
                });
                assertThat(started.waitUntilStarted(Duration.ofSeconds(10))).isTrue();

                // When an event is written and the action throws
                mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
                assertThat(actionRan.await(10, SECONDS)).isTrue();
                // The executor thread ends with the exception of the action, which an await would otherwise throw
                await().atMost(FIVE_SECONDS).dontCatchUncaughtExceptions().with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                        assertThat(activeSubscriptionThreads()).isZero());

                // Then the subscription is still known, as for any exception an action throws
                assertThat(noRetries.subscriptionIds()).contains(subscriptionId);
            } finally {
                noRetries.shutdown();
            }
        }

        // Writes the event through a subscription at the present and returns the checkpoint of what it delivered
        private Checkpoint checkpointAfter(NameDefined written) {
            CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
            String helperId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(helperId, StartAt.now(), delivered::add).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(written));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(delivered).extracting(CloudEvent::getId).contains(written.eventId()));
            subscriptionModel.cancelSubscription(helperId);
            return CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(delivered.getFirst());
        }

        // Threads of the executor that run a subscription or its start, so zero once every run has ended
        private int activeSubscriptionThreads() {
            return ((ThreadPoolExecutor) subscriptionExecutor).getActiveCount();
        }

        // False when the condition still doesn't hold after the bound
        private static boolean becomesTrueWithinFiveSeconds(Callable<Boolean> condition) {
            try {
                await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(10, MILLIS)).until(condition);
                return true;
            } catch (ConditionTimeoutException bound) {
                return false;
            }
        }

        // The run of a subscription can't be reached from a public method, so one of its latches is replaced through the private fields
        private static void replaceLatchOfTheRunOf(NativeMongoSubscriptionModel model, String subscriptionId, String latchName, UnaryOperator<CountDownLatch> replacement) {
            try {
                Field subscriptionsField = NativeMongoSubscriptionModel.class.getDeclaredField("subscriptions");
                subscriptionsField.setAccessible(true);
                Object subscriptions = subscriptionsField.get(model);
                Field runningField = subscriptions.getClass().getDeclaredField("runningSubscriptions");
                runningField.setAccessible(true);
                Object run = ((Map<?, ?>) runningField.get(subscriptions)).get(subscriptionId);
                Field latchField = run.getClass().getDeclaredField(latchName);
                latchField.setAccessible(true);
                latchField.set(run, replacement.apply((CountDownLatch) latchField.get(run)));
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        // The map of the running subscriptions can't be reached from a public method, so it is replaced through the private fields
        @SuppressWarnings({"unchecked", "rawtypes"})
        private static void replaceRunningSubscriptionsOf(NativeMongoSubscriptionModel model, ConcurrentHashMap replacement) {
            try {
                Field subscriptionsField = NativeMongoSubscriptionModel.class.getDeclaredField("subscriptions");
                subscriptionsField.setAccessible(true);
                Object subscriptions = subscriptionsField.get(model);
                Field runningField = subscriptions.getClass().getDeclaredField("runningSubscriptions");
                runningField.setAccessible(true);
                replacement.putAll((Map) runningField.get(subscriptions));
                runningField.set(subscriptions, replacement);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        // The lock of a run is replaced together with the condition it hands out, through the private fields
        private static void replaceLockOfTheRunOf(NativeMongoSubscriptionModel model, String subscriptionId, ReentrantLock replacement) {
            try {
                Field subscriptionsField = NativeMongoSubscriptionModel.class.getDeclaredField("subscriptions");
                subscriptionsField.setAccessible(true);
                Object subscriptions = subscriptionsField.get(model);
                Field runningField = subscriptions.getClass().getDeclaredField("runningSubscriptions");
                runningField.setAccessible(true);
                Object run = ((Map<?, ?>) runningField.get(subscriptions)).get(subscriptionId);
                Field lockField = run.getClass().getDeclaredField("lock");
                lockField.setAccessible(true);
                lockField.set(run, replacement);
                Field stoppedDeliveringField = run.getClass().getDeclaredField("stoppedDelivering");
                stoppedDeliveringField.setAccessible(true);
                stoppedDeliveringField.set(run, replacement.newCondition());
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        private Checkpoint liveStart() {
            return requireNonNull(subscriptionModel.globalCheckpoint());
        }

        private CatchupTimeCheckpoint catchupTimeCheckpoint() {
            return CatchupTimeCheckpoint.of("2026-01-01T00:00:00Z", liveStart(), "2026-01-01T00:00:00Z");
        }
    }

    // Holds the thread that logs the first refusal of a start position until released
    private static final class HoldsTheFirstRefusal extends UnsynchronizedAppenderBase<ILoggingEvent> {
        private final AtomicBoolean first = new AtomicBoolean(true);
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private HoldsTheFirstRefusal() {
            start();
        }

        @Override
        protected void append(ILoggingEvent event) {
            if (event.getLevel() == Level.ERROR && event.getFormattedMessage().startsWith("Cannot open the change stream of subscription") && first.compareAndSet(true, false)) {
                held.countDown();
                try {
                    release.await(20, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // Replaces the latch whose count a run's start reads to learn whether the run stopped restarting. The first read lets
    // the position answer, and returns once the run has ended
    private static final class RunsEndsBeforeTheFirstRead extends CountDownLatch {
        private final CountDownLatch own;
        private final CountDownLatch positionMayAnswer;
        private final AtomicBoolean first = new AtomicBoolean(true);

        private RunsEndsBeforeTheFirstRead(CountDownLatch own, CountDownLatch positionMayAnswer) {
            super(1);
            this.own = own;
            this.positionMayAnswer = positionMayAnswer;
        }

        @Override
        public long getCount() {
            if (first.getAndSet(false)) {
                positionMayAnswer.countDown();
                Awaitility.await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(10, MILLIS)).until(() -> own.getCount() == 0);
            }
            return own.getCount();
        }

        @Override
        public void countDown() {
            own.countDown();
        }
    }

    // Replaces the latch of a run that its failure to start is read from and counted down on. The first read waits until
    // released, which holds the thread that is recording the failure of a refused run
    private static final class HoldsTheFirstReadOfTheStartedLatch extends CountDownLatch {
        private final CountDownLatch own;
        private final CountDownLatch release;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final AtomicBoolean first = new AtomicBoolean(true);

        private HoldsTheFirstReadOfTheStartedLatch(CountDownLatch own, CountDownLatch release) {
            super(1);
            this.own = own;
            this.release = release;
        }

        @Override
        public long getCount() {
            if (first.getAndSet(false)) {
                reached.countDown();
                awaitUpToTenSeconds(release);
            }
            return own.getCount();
        }

        @Override
        public void countDown() {
            own.countDown();
        }
    }

    // Replaces the latch of a run that its waiters are released by. Runs the look once, as the waiters are released and
    // before they are
    private static final class LooksBeforeItReleasesTheWaiters extends CountDownLatch {
        private final CountDownLatch own;
        private final Runnable look;
        private final AtomicBoolean first = new AtomicBoolean(true);

        private LooksBeforeItReleasesTheWaiters(CountDownLatch own, Runnable look) {
            super(1);
            this.own = own;
            this.look = look;
        }

        @Override
        public void countDown() {
            if (first.getAndSet(false)) {
                look.run();
            }
            own.countDown();
        }

        @Override
        public long getCount() {
            return own.getCount();
        }
    }

    // Replaces the lock of a run. The first thread other than the test's that lets go of it is held, which is the run
    // returning from having opened its change stream, before it reports that it started
    private static final class HoldsTheRunWhenItOpened extends ReentrantLock {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final Thread testThread;
        private final AtomicBoolean first = new AtomicBoolean(true);

        private HoldsTheRunWhenItOpened(Thread testThread) {
            this.testThread = testThread;
        }

        @Override
        public void unlock() {
            super.unlock();
            if (Thread.currentThread() != testThread && first.getAndSet(false)) {
                held.countDown();
                awaitUpToTenSeconds(release);
            }
        }
    }

    // Replaces the map of the running subscriptions. The thread that removes the run of a refused subscription is held
    // inside the update, once the failure is recorded and before the run is gone from the map
    private static final class HoldsTheRemovalOfARefusedRun extends ConcurrentHashMap<String, Object> {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean first = new AtomicBoolean(true);

        @Override
        public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
            return super.computeIfPresent(key, (id, current) -> {
                Object remapped = remappingFunction.apply(id, current);
                if (remapped == null && first.getAndSet(false)) {
                    held.countDown();
                    awaitUpToTenSeconds(release);
                }
                return remapped;
            });
        }
    }

    // Replaces the latch a resume releases its handle through. The thread that counts it down is held right after
    // that, before it records that the subscription started
    private static final class HoldsAfterReleasingTheStartedLatch extends CountDownLatch {
        private final CountDownLatch own;
        private final CountDownLatch held;
        private final CountDownLatch release;

        private HoldsAfterReleasingTheStartedLatch(CountDownLatch own, CountDownLatch held, CountDownLatch release) {
            super(1);
            this.own = own;
            this.held = held;
            this.release = release;
        }

        @Override
        public void countDown() {
            own.countDown();
            held.countDown();
            awaitUpToTenSeconds(release);
        }

        @Override
        public long getCount() {
            return own.getCount();
        }
    }

    private static void awaitUpToTenSeconds(CountDownLatch latch) {
        try {
            latch.await(10, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class HoldTheFirstPing implements CommandListener {
        final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicBoolean first = new AtomicBoolean(true);

        @Override
        public void commandSucceeded(CommandSucceededEvent event) {
            if ("ping".equals(event.getCommandName()) && first.getAndSet(false)) {
                held.countDown();
                try {
                    released.await(10, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void release() {
            released.countDown();
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