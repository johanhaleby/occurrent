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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
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
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.MongoFilterSpecification;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.occurrent.time.TimeConversion;
import org.slf4j.LoggerFactory;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.filter.Filter.all;
import static org.occurrent.filter.Filter.id;
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
                () -> assertThat(throwable).isExactlyInstanceOf(DuplicateSubscriptionIdException.class).hasMessage("Subscription " + subscriptionId + " is already defined."),
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
                () -> assertThat(throwable).isExactlyInstanceOf(DuplicateSubscriptionIdException.class).hasMessage("Subscription " + subscriptionId + " is already defined."),
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
        void an_interrupted_wait_for_a_subscription_to_start_leaves_the_thread_interrupted() {
            // Given
            Subscription notStarted = notAutoStarted.subscribe(UUID.randomUUID().toString(), __ -> {
            });
            Thread.currentThread().interrupt();

            // When
            Throwable thrown = catchThrowable(() -> notStarted.waitUntilStarted(Duration.ofSeconds(2)));

            // Then: Thread.interrupted() also clears the flag, so it isn't still set when the next test runs
            boolean stillInterrupted = Thread.interrupted();
            assertThat(stillInterrupted).isTrue();
            assertThat(thrown).hasCauseInstanceOf(InterruptedException.class);
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
        void a_subscription_at_now_registered_before_start_receives_what_was_written_before_the_model_starts() throws InterruptedException {
            // Given
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            notAutoStarted.subscribe(UUID.randomUUID().toString(), StartAt.now(), state::add);
            // The model asks MongoDB for the present on its executor once subscribe(..) has returned, so the write
            // waits for that answer
            Thread.sleep(1000);
            NameDefined writtenBeforeStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenBeforeStart));
            // A second write moves the server's operation time past the first, which a change stream that opens at
            // the present then skips
            mongoEventStore.write("3", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name3")));

            // When
            notAutoStarted.start(true);
            NameDefined writtenAfterStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2");
            mongoEventStore.write("2", 0, serialize(writtenAfterStart));

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(state).extracting(CloudEvent::getId).contains(writtenAfterStart.eventId()));
            assertThat(state).extracting(CloudEvent::getId).contains(writtenBeforeStart.eventId());
        }

        @Test
        void the_subscription_registered_before_start_answers_that_it_started_once_start_opens_its_change_stream() {
            // Given
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            Subscription subscription = notAutoStarted.subscribe(UUID.randomUUID().toString(), StartAt.now(), state::add);

            // When
            notAutoStarted.start(true);
            NameDefined writtenAfterStart = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenAfterStart));
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(state).extracting(CloudEvent::getId).contains(writtenAfterStart.eventId()));

            // Then
            assertThat(subscription.waitUntilStarted(Duration.ofSeconds(2))).isTrue();
        }

        @Test
        void a_subscription_registered_before_start_receives_each_event_once() {
            // Given
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            notAutoStarted.subscribe(UUID.randomUUID().toString(), StartAt.now(), state::add);

            // When
            notAutoStarted.start(true);
            NameDefined first = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            NameDefined second = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2");
            mongoEventStore.write("1", 0, serialize(first));
            mongoEventStore.write("2", 0, serialize(second));

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(state).extracting(CloudEvent::getId).contains(second.eventId()));
            await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(state).extracting(CloudEvent::getId).containsExactly(first.eventId(), second.eventId()));
        }

        @Test
        void a_subscription_registered_before_start_stays_paused_when_the_model_starts_without_resuming() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            notAutoStarted.subscribe(subscriptionId, StartAt.now(), state::add);

            // When
            notAutoStarted.start(false);
            NameDefined writtenWhilePaused = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenWhilePaused));

            // Then
            assertThat(notAutoStarted.isPaused(subscriptionId)).isTrue();
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(state).isEmpty());
        }

        @Test
        void resuming_one_subscription_before_start_leaves_the_others_registered_before_start_paused() {
            // Given
            CopyOnWriteArrayList<CloudEvent> resumedState = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<CloudEvent> otherState = new CopyOnWriteArrayList<>();
            notAutoStarted.subscribe("resumed", StartAt.now(), resumedState::add);
            notAutoStarted.subscribe("other", StartAt.now(), otherState::add);

            // When
            notAutoStarted.resumeSubscription("resumed").waitUntilStarted(Duration.ofSeconds(10));
            NameDefined writtenOnceResumed = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenOnceResumed));

            // Then
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(resumedState).extracting(CloudEvent::getId).contains(writtenOnceResumed.eventId()));
            assertThat(notAutoStarted.isPaused("other")).isTrue();
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(otherState).isEmpty());
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
        void pausing_subscriptions_one_after_the_other_does_not_wait_for_reads_that_return_nothing() throws InterruptedException {
            // Given: reads that wait on the server longer than a pause waits for an action
            List<String> subscriptionIds = subscribeFiveSubscriptionsWhoseReadsWaitThreeSeconds();

            // When
            long startedPausing = System.nanoTime();
            subscriptionIds.forEach(subscriptionModel::pauseSubscription);
            Duration pausing = Duration.ofNanos(System.nanoTime() - startedPausing);

            // Then
            assertThat(pausing).as("time to pause five subscriptions waiting on the server").isLessThan(Duration.ofSeconds(1));
        }

        @Test
        void stopping_the_model_does_not_wait_for_reads_that_return_nothing() throws InterruptedException {
            // Given: reads that wait on the server longer than a pause waits for an action
            List<String> subscriptionIds = subscribeFiveSubscriptionsWhoseReadsWaitThreeSeconds();

            // When
            long startedStopping = System.nanoTime();
            subscriptionModel.stop();
            Duration stopping = Duration.ofNanos(System.nanoTime() - startedStopping);

            // Then
            assertAll(
                    () -> assertThat(stopping).as("time to stop a model with five subscriptions waiting on the server").isLessThan(Duration.ofSeconds(1)),
                    () -> assertThat(subscriptionIds).as("paused").allMatch(subscriptionModel::isPaused)
            );
        }

        @Test
        void stopping_the_model_waits_for_the_actions_that_are_running_at_the_same_time_rather_than_one_after_the_other() throws InterruptedException {
            // Given: three subscriptions, each in an action that takes 800 ms, with more events waiting behind it
            List<CountDownLatch> handling = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                CountDownLatch handlingOne = new CountDownLatch(1);
                handling.add(handlingOne);
                subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), __ -> {
                    handlingOne.countDown();
                    try {
                        Thread.sleep(800);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).waitUntilStarted(Duration.ofSeconds(10));
            }
            for (int version = 0; version < 5; version++) {
                mongoEventStore.write("1", version, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name" + version)));
            }
            for (CountDownLatch handlingOne : handling) {
                assertThat(handlingOne.await(10, SECONDS)).isTrue();
            }

            // When
            long startedStopping = System.nanoTime();
            subscriptionModel.stop();
            Duration stopping = Duration.ofNanos(System.nanoTime() - startedStopping);

            // Then
            assertThat(stopping).as("time to stop three subscriptions whose actions take 800 ms").isLessThan(Duration.ofMillis(1200));
        }

        @Test
        void a_pause_while_the_action_runs_returns_once_the_action_has_returned_and_nothing_is_delivered_after_it() throws InterruptedException {
            // Given: an action that takes a while on the first of two events
            CountDownLatch handlingFirstEvent = new CountDownLatch(1);
            AtomicBoolean firstEventHandled = new AtomicBoolean();
            AtomicBoolean pauseReturned = new AtomicBoolean();
            CopyOnWriteArrayList<CloudEvent> deliveredAfterThePause = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), cloudEvent -> {
                if (pauseReturned.get()) {
                    deliveredAfterThePause.add(cloudEvent);
                } else if (handlingFirstEvent.getCount() == 1) {
                    handlingFirstEvent.countDown();
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    firstEventHandled.set(true);
                }
            }).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            mongoEventStore.write("2", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2")));
            assertThat(handlingFirstEvent.await(10, SECONDS)).isTrue();

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            boolean handledWhenThePauseReturned = firstEventHandled.get();
            pauseReturned.set(true);

            // Then
            assertThat(handledWhenThePauseReturned).as("the action had returned when the pause did").isTrue();
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(deliveredAfterThePause).isEmpty());
        }

        @Test
        void a_pause_from_inside_the_action_does_not_wait_for_the_action() throws InterruptedException {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            AtomicReference<Duration> pausing = new AtomicReference<>();
            CountDownLatch paused = new CountDownLatch(1);
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
                long startedPausing = System.nanoTime();
                subscriptionModel.pauseSubscription(subscriptionId);
                pausing.set(Duration.ofNanos(System.nanoTime() - startedPausing));
                paused.countDown();
            }).waitUntilStarted(Duration.ofSeconds(10));

            // When
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));

            // Then
            assertThat(paused.await(10, SECONDS)).isTrue();
            assertThat(pausing.get()).as("time to pause from inside the action").isLessThan(Duration.ofMillis(500));
        }

        private List<String> subscribeFiveSubscriptionsWhoseReadsWaitThreeSeconds() throws InterruptedException {
            subscriptionModel.shutdown();
            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, withConfig(eventCollectionName, timeRepresentation).maxAwaitTime(Duration.ofSeconds(3)));
            List<String> subscriptionIds = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                String subscriptionId = UUID.randomUUID().toString();
                subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
                }).waitUntilStarted(Duration.ofSeconds(10));
                subscriptionIds.add(subscriptionId);
            }
            // So every subscription is inside a read by the time it's paused
            Thread.sleep(500);
            return subscriptionIds;
        }

        @Test
        void a_wait_on_a_subscription_held_paused_on_a_running_model_ends_once_a_resume_opens_its_change_stream() {
            // Given
            String subscriptionId = UUID.randomUUID().toString();
            Subscription heldPaused = subscriptionModel.subscribePaused(subscriptionId, null, StartAt.now(), __ -> {
            });
            CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> heldPaused.waitUntilStarted(Duration.ofSeconds(30)));

            // When
            subscriptionModel.resumeSubscription(subscriptionId);

            // Then
            assertThat(waiting).as("a wait begun while the subscription was held paused").succeedsWithin(Duration.ofSeconds(10)).isEqualTo(true);
        }

        @Test
        void shutdown_lets_an_action_that_is_running_return_rather_than_interrupting_it() throws InterruptedException {
            // Given
            CountDownLatch handling = new CountDownLatch(1);
            AtomicBoolean interrupted = new AtomicBoolean();
            AtomicBoolean returned = new AtomicBoolean();
            subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), __ -> {
                handling.countDown();
                try {
                    Thread.sleep(500);
                    returned.set(true);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
            }).waitUntilStarted(Duration.ofSeconds(10));
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            assertThat(handling.await(10, SECONDS)).isTrue();

            // When
            subscriptionModel.shutdown();

            // Then
            assertAll(
                    () -> assertThat(interrupted).as("interrupted").isFalse(),
                    () -> assertThat(returned).as("returned before shutdown() did").isTrue()
            );
        }

        @Test
        void cancelling_a_paused_subscription_forgets_it_so_a_start_delivers_nothing_to_it_and_it_can_be_subscribed_again() throws InterruptedException {
            // Given
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), handled::add).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.pauseSubscription(subscriptionId);

            // When
            subscriptionModel.cancelSubscription(subscriptionId);
            subscriptionModel.stop();
            subscriptionModel.start(true);
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            Thread.sleep(1000);

            // Then
            assertAll(
                    () -> assertThat(subscriptionModel.isPaused(subscriptionId)).describedAs("is paused").isFalse(),
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).describedAs("is running").isFalse(),
                    () -> assertThat(handled).describedAs("delivered after the cancel").isEmpty(),
                    () -> assertThat(catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, StartAt.now(), handled::add))).describedAs("subscribing the id again").isNull()
            );
        }

        @Test
        void cancelling_a_subscription_made_while_the_model_is_stopped_forgets_it_so_a_start_delivers_nothing_to_it() throws InterruptedException {
            // Given
            CopyOnWriteArrayList<CloudEvent> handled = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.stop();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), handled::add);

            // When
            subscriptionModel.cancelSubscription(subscriptionId);
            subscriptionModel.start(true);
            Thread.sleep(1000);
            mongoEventStore.write("1", 0, serialize(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1")));
            Thread.sleep(1000);

            // Then
            assertAll(
                    () -> assertThat(subscriptionModel.isPaused(subscriptionId)).describedAs("is paused").isFalse(),
                    () -> assertThat(subscriptionModel.isRunning(subscriptionId)).describedAs("is running").isFalse(),
                    () -> assertThat(handled).describedAs("delivered after the cancel").isEmpty(),
                    () -> assertThat(catchThrowable(() -> subscriptionModel.subscribe(subscriptionId, StartAt.now(), handled::add))).describedAs("subscribing the id again").isNull()
            );
        }

    }

    @Nested
    @DisplayName("Resume at a given position")
    class ResumeAtAGivenPositionTest {

        @Test
        void resuming_at_a_given_position_reopens_the_change_stream_there() {
            // Given
            LocalDateTime now = LocalDateTime.now();
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, StartAt.now(), state::add).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            NameDefined firstEvent = new NameDefined(UUID.randomUUID().toString(), now, "name", "name1");
            mongoEventStore.write("1", 0, serialize(firstEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(1));
            // Captured so the model can be asked to reopen from here, a position earlier than the one it will have
            // tracked itself by the time it is paused below.
            Checkpoint afterFirstEvent = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(state.get(0));

            NameWasChanged secondEvent = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(1), "name", "name2");
            mongoEventStore.write("1", 1, serialize(secondEvent));
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(2));

            // When
            subscriptionModel.pauseSubscription(subscriptionId);
            NameWasChanged thirdEvent = new NameWasChanged(UUID.randomUUID().toString(), now.plusSeconds(2), "name", "name3");
            mongoEventStore.write("1", 2, serialize(thirdEvent));
            subscriptionModel.resumeSubscription(subscriptionId, StartAt.checkpoint(afterFirstEvent)).waitUntilStarted(Duration.of(10, ChronoUnit.SECONDS));

            // Then: the second and third events both arrive again, because the change stream reopened at the
            // explicit position rather than at the position the subscription itself had tracked (which was already
            // past the second event and would have delivered only the third).
            await().atMost(FIVE_SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() -> assertThat(state).hasSize(4));
            assertThat(state).extracting(CloudEvent::getId)
                    .containsExactly(firstEvent.eventId(), secondEvent.eventId(), secondEvent.eventId(), thirdEvent.eventId());
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
    @DisplayName("MongoDB cannot be reached")
    class MongoCannotBeReachedTest {

        private final AtomicBoolean unreachable = new AtomicBoolean();
        private final AtomicInteger refusedOperationTimeRequests = new AtomicInteger();
        private final AtomicReference<CountDownLatch> operationTimeRequestsWaitFor = new AtomicReference<>();
        private final CountDownLatch operationTimeRequestWaiting = new CountDownLatch(1);
        private MongoTemplate mongoTemplateSpy;

        @BeforeEach
        void control_the_request_for_the_operation_time() {
            mongoTemplateSpy = spy(mongoTemplate);
            doAnswer(invocation -> {
                CountDownLatch waitFor = operationTimeRequestsWaitFor.get();
                if (waitFor != null) {
                    operationTimeRequestWaiting.countDown();
                    waitFor.await();
                }
                if (unreachable.get()) {
                    refusedOperationTimeRequests.incrementAndGet();
                    throw new DataAccessResourceFailureException("MongoDB cannot be reached", new MongoTimeoutException("timed out"));
                }
                return invocation.callRealMethod();
            }).when(mongoTemplateSpy).executeCommand(any(Document.class));
            subscriptionModel.shutdown();
            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, eventCollectionName, timeRepresentation);
        }

        @Timeout(value = 30, unit = SECONDS)
        @Test
        void subscribe_returns_and_the_subscription_delivers_once_mongodb_can_be_reached() {
            // Given
            unreachable.set(true);
            CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
            AtomicReference<Subscription> subscription = new AtomicReference<>();

            // When
            Throwable thrown = catchThrowable(() -> subscription.set(subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), state::add)));
            await().atMost(FIVE_SECONDS).until(() -> refusedOperationTimeRequests.get() > 0);
            unreachable.set(false);

            // Then
            assertThat(thrown).isNull();
            assertThat(subscription.get().waitUntilStarted(Duration.ofSeconds(20))).isTrue();
            NameDefined writtenOnceReachable = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
            mongoEventStore.write("1", 0, serialize(writtenOnceReachable));
            await().atMost(10, SECONDS).untilAsserted(() -> assertThat(state).extracting(CloudEvent::getId).contains(writtenOnceReachable.eventId()));
        }

        @Timeout(value = 30, unit = SECONDS)
        @Test
        void start_returns_and_every_subscription_delivers_once_mongodb_can_be_reached() {
            // Given
            SpringMongoSubscriptionModel notAutoStarted = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig(eventCollectionName, timeRepresentation).autoStartup(false));
            try {
                unreachable.set(true);
                CopyOnWriteArrayList<CloudEvent> first = new CopyOnWriteArrayList<>();
                CopyOnWriteArrayList<CloudEvent> second = new CopyOnWriteArrayList<>();
                Throwable thrownBySubscribe = catchThrowable(() -> {
                    notAutoStarted.subscribe("first", StartAt.now(), first::add);
                    notAutoStarted.subscribe("second", StartAt.now(), second::add);
                });

                // When
                CompletableFuture<Void> start = CompletableFuture.runAsync(() -> notAutoStarted.start(true));
                await().atMost(FIVE_SECONDS).until(() -> refusedOperationTimeRequests.get() > 0 || start.isDone());
                unreachable.set(false);

                // Then
                assertThat(thrownBySubscribe).isNull();
                assertThat(start).succeedsWithin(Duration.ofSeconds(20));
                NameDefined writtenOnceReachable = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
                mongoEventStore.write("1", 0, serialize(writtenOnceReachable));
                await().atMost(10, SECONDS).untilAsserted(() -> assertAll(
                        () -> assertThat(first).extracting(CloudEvent::getId).contains(writtenOnceReachable.eventId()),
                        () -> assertThat(second).extracting(CloudEvent::getId).contains(writtenOnceReachable.eventId())));
            } finally {
                notAutoStarted.shutdown();
            }
        }

        @Timeout(value = 30, unit = SECONDS)
        @Test
        void a_request_for_the_operation_time_that_hangs_does_not_hold_up_pausing_or_cancelling_other_subscriptions() throws InterruptedException {
            // Given
            subscriptionModel.subscribe("paused", StartAt.now(), __ -> {
            }).waitUntilStarted(Duration.ofSeconds(10));
            subscriptionModel.subscribe("cancelled", StartAt.now(), __ -> {
            }).waitUntilStarted(Duration.ofSeconds(10));
            CountDownLatch release = new CountDownLatch(1);
            operationTimeRequestsWaitFor.set(release);
            try {
                CompletableFuture.runAsync(() -> subscriptionModel.subscribe("hanging", StartAt.now(), __ -> {
                }));
                assertThat(operationTimeRequestWaiting.await(10, SECONDS)).isTrue();

                // When
                CompletableFuture<Void> pauseAndCancel = CompletableFuture.runAsync(() -> {
                    subscriptionModel.pauseSubscription("paused");
                    subscriptionModel.cancelSubscription("cancelled");
                });

                // Then
                assertThat(pauseAndCancel).succeedsWithin(Duration.ofSeconds(2));
                assertAll(
                        () -> assertThat(subscriptionModel.isPaused("paused")).isTrue(),
                        () -> assertThat(subscriptionModel.isRunning("cancelled")).isFalse());
            } finally {
                release.countDown();
            }
        }

        @Timeout(value = 30, unit = SECONDS)
        @Test
        void start_returns_once_restarting_a_subscription_gives_up_and_a_pause_and_resume_starts_it_again() {
            // Given
            SpringMongoSubscriptionModel givesUpAfterTwoAttempts = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig(eventCollectionName, timeRepresentation)
                    .autoStartup(false).retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100)).maxAttempts(2)));
            try {
                unreachable.set(true);
                CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
                givesUpAfterTwoAttempts.subscribe("gives-up", StartAt.now(), state::add);

                // When
                CompletableFuture<Void> start = CompletableFuture.runAsync(() -> givesUpAfterTwoAttempts.start(true));

                // Then
                assertThat(start).succeedsWithin(Duration.ofSeconds(10));
                assertThat(givesUpAfterTwoAttempts.isRunning("gives-up")).isTrue();
                unreachable.set(false);
                givesUpAfterTwoAttempts.pauseSubscription("gives-up");
                assertThat(givesUpAfterTwoAttempts.resumeSubscription("gives-up").waitUntilStarted(Duration.ofSeconds(10))).isTrue();
                NameDefined writtenOnceResumed = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
                mongoEventStore.write("1", 0, serialize(writtenOnceResumed));
                await().atMost(10, SECONDS).untilAsserted(() -> assertThat(state).extracting(CloudEvent::getId).contains(writtenOnceResumed.eventId()));
            } finally {
                givesUpAfterTwoAttempts.shutdown();
            }
        }

        @Timeout(value = 30, unit = SECONDS)
        @Test
        void start_returns_when_a_give_up_from_before_a_pause_ends_after_the_subscription_is_resumed() throws InterruptedException {
            // Given
            SpringMongoSubscriptionModel givesUpAfterOneAttempt = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig(eventCollectionName, timeRepresentation)
                    .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(10)).maxAttempts(1)));
            HoldsTheFirstGiveUp holdsTheFirstGiveUp = new HoldsTheFirstGiveUp();
            ch.qos.logback.classic.Logger modelLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SpringMongoSubscriptionModel.class);
            modelLogger.addAppender(holdsTheFirstGiveUp);
            try {
                unreachable.set(true);
                givesUpAfterOneAttempt.subscribe("gives-up", StartAt.now(), __ -> {
                });
                assertThat(holdsTheFirstGiveUp.held.await(10, SECONDS)).isTrue();
                givesUpAfterOneAttempt.pauseSubscription("gives-up");

                // When
                CompletableFuture<Void> start = CompletableFuture.runAsync(() -> givesUpAfterOneAttempt.start(true));
                await().atMost(10, SECONDS).until(() -> givesUpAfterOneAttempt.isRunning("gives-up"));
                holdsTheFirstGiveUp.release.countDown();

                // Then
                assertThat(start).succeedsWithin(Duration.ofSeconds(10));
            } finally {
                holdsTheFirstGiveUp.release.countDown();
                modelLogger.detachAppender(holdsTheFirstGiveUp);
                givesUpAfterOneAttempt.shutdown();
            }
        }

        @Timeout(value = 60, unit = SECONDS)
        @Test
        void start_returns_and_the_subscription_delivers_after_pauses_and_resumes_race_restarts_that_give_up() throws InterruptedException {
            // Given
            SpringMongoSubscriptionModel givesUpAfterThreeAttempts = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig(eventCollectionName, timeRepresentation)
                    .retryStrategy(RetryStrategy.fixed(Duration.ofMillis(5)).maxAttempts(3)));
            long seed = System.nanoTime();
            Random random = new Random(seed);
            try {
                unreachable.set(true);
                CopyOnWriteArrayList<CloudEvent> state = new CopyOnWriteArrayList<>();
                givesUpAfterThreeAttempts.subscribe("racing", StartAt.now(), state::add);

                // When
                long end = System.nanoTime() + SECONDS.toNanos(5);
                while (System.nanoTime() < end) {
                    Thread.sleep(random.nextInt(20));
                    givesUpAfterThreeAttempts.pauseSubscription("racing");
                    if (random.nextBoolean()) {
                        givesUpAfterThreeAttempts.resumeSubscription("racing");
                    } else {
                        givesUpAfterThreeAttempts.resumeSubscription("racing", StartAt.now());
                    }
                    if (random.nextInt(10) == 0) {
                        givesUpAfterThreeAttempts.stop();
                        assertThat(CompletableFuture.runAsync(() -> givesUpAfterThreeAttempts.start(true))).describedAs("start with seed %d", seed).succeedsWithin(Duration.ofSeconds(10));
                    }
                }
                unreachable.set(false);
                givesUpAfterThreeAttempts.stop();

                // Then
                assertThat(CompletableFuture.runAsync(() -> givesUpAfterThreeAttempts.start(true))).describedAs("start with seed %d", seed).succeedsWithin(Duration.ofSeconds(10));
                assertThat(givesUpAfterThreeAttempts.isRunning("racing")).isTrue();
                NameDefined writtenOnceReachable = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
                mongoEventStore.write("1", 0, serialize(writtenOnceReachable));
                await().atMost(10, SECONDS).untilAsserted(() -> assertThat(state).describedAs("delivered with seed %d", seed).extracting(CloudEvent::getId).contains(writtenOnceReachable.eventId()));
            } finally {
                givesUpAfterThreeAttempts.shutdown();
            }
        }

    }

    // Holds the thread that logs giving up first until released
    private static final class HoldsTheFirstGiveUp extends UnsynchronizedAppenderBase<ILoggingEvent> {
        private final AtomicBoolean first = new AtomicBoolean(true);
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private HoldsTheFirstGiveUp() {
            start();
        }

        @Override
        protected void append(ILoggingEvent event) {
            if (event.getFormattedMessage().startsWith("Gave up asking MongoDB for its operation time") && first.compareAndSet(true, false)) {
                held.countDown();
                try {
                    release.await(20, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
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

        @SuppressWarnings("unchecked")
        @Timeout(value = 20, unit = SECONDS)
        @Test
        void start_returns_when_change_stream_history_is_lost_and_not_configured_to_restart() {
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

            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig("events", TimeRepresentation.RFC_3339_STRING).restartSubscriptionsOnChangeStreamHistoryLost(false).autoStartup(false));
            String subscriptionId = UUID.randomUUID().toString();
            subscriptionModel.subscribe(subscriptionId, __ -> {
            });

            // When
            CompletableFuture<Void> start = CompletableFuture.runAsync(() -> subscriptionModel.start(true));

            // Then
            assertThat(start).succeedsWithin(Duration.ofSeconds(10));
            await().atMost(FIVE_SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isRunning(subscriptionId)).isFalse());
            assertThat(subscriptionModel.isPaused(subscriptionId)).isFalse();
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
            AtomicBoolean readFailed = new AtomicBoolean(false);
            MongoCollection<Document> realEventCollection = mongoTemplate.getDb().getCollection(eventCollectionName);

            MongoTemplate mongoTemplateSpy = spy(mongoTemplate);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection<Document> instrumentedCollection = (MongoCollection<Document>) mock(MongoCollection.class);
            // Only the first change stream is instrumented (the model calls getDb() per change stream it opens),
            // so the restart runs through the real template and what it resumes from is the model's own decision
            // rather than something this test arranged.
            when(mongoTemplateSpy.getDb()).thenReturn(mongoDatabase).thenCallRealMethod();
            when(mongoDatabase.getCollection(eventCollectionName)).thenReturn(instrumentedCollection);
            when(instrumentedCollection.watch(any(Class.class)))
                    .thenAnswer(__ -> instrumentedChangeStream(realEventCollection.watch(Document.class), withholdDocuments, failNextRead, readFailed));

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
            // The outage has to be real for the rest of this test to mean anything. Without this the subscription
            // could be reading through the untouched change stream, delivering all three events for reasons that
            // have nothing to do with where a restart resumes, and the assertion below would still pass.
            await().during(ONE_SECOND).atMost(FIVE_SECONDS).untilAsserted(() ->
                    assertThat(state)
                            .as("nothing may reach the handler while the change stream is withholding documents")
                            .hasSize(1));
            failNextRead.set(true);

            // Then
            await().atMost(10, SECONDS).with().pollInterval(Duration.of(20, MILLIS)).untilAsserted(() ->
                    assertThat(state).extracting(CloudEvent::getId)
                            .as("the restarted subscription must continue from the event it had read, so both "
                                    + "events written while its change stream was down still arrive")
                            .containsExactly(beforeTheOutage.eventId(), firstDuringTheOutage.eventId(), lastDuringTheOutage.eventId()));
            assertThat(readFailed)
                    .as("the change stream this test instrumented must be the one the subscription was reading, "
                            + "or the outage never happened and the delivery above proves nothing")
                    .isTrue();
        }

        /**
         * Delegates every call to the real change stream, except that the cursor it hands out withholds documents
         * or fails on demand. Option calls are delegated too, and answer with this mock rather than the real
         * iterable they return, so the chain the model builds ends at the instrumented cursor.
         */
        @SuppressWarnings("unchecked")
        private ChangeStreamIterable<Document> instrumentedChangeStream(ChangeStreamIterable<Document> real, AtomicBoolean withholdDocuments, AtomicBoolean failNextRead, AtomicBoolean readFailed) {
            return mock(ChangeStreamIterable.class, invocation -> {
                if (invocation.getMethod().getName().equals("cursor")) {
                    return instrumentedCursor(real.cursor(), withholdDocuments, failNextRead, readFailed);
                }
                Object answer = invocation.getMethod().invoke(real, invocation.getArguments());
                return answer == real ? invocation.getMock() : answer;
            });
        }

        @SuppressWarnings("unchecked")
        private MongoChangeStreamCursor<ChangeStreamDocument<Document>> instrumentedCursor(MongoChangeStreamCursor<ChangeStreamDocument<Document>> real, AtomicBoolean withholdDocuments, AtomicBoolean failNextRead, AtomicBoolean readFailed) {
            // The position of what this cursor has handed over. The real cursor's own position moves past a document
            // that is dropped below, and the model takes the position of an empty read from the cursor.
            AtomicReference<BsonDocument> positionHandedOver = new AtomicReference<>(real.getResumeToken());
            return mock(MongoChangeStreamCursor.class, invocation -> {
                if (invocation.getMethod().getName().equals("getResumeToken")) {
                    return positionHandedOver.get();
                }
                if (!invocation.getMethod().getName().equals("tryNext")) {
                    return invocation.getMethod().invoke(real, invocation.getArguments());
                }
                if (failNextRead.get()) {
                    readFailed.set(true);
                    throw new MongoSocketReadException("expected: simulated failover", new ServerAddress(), new IOException("Connection reset by peer"));
                }
                if (withholdDocuments.get()) {
                    // Nothing to read, as far as the model is concerned. Slept on rather than answered
                    // immediately, because its read loop calls tryNext() again as soon as this returns.
                    Thread.sleep(20);
                    return null;
                }
                ChangeStreamDocument<Document> next = (ChangeStreamDocument<Document>) real.tryNext();
                if (next != null && withholdDocuments.get()) {
                    // Withholding started while this call was already waiting on the server. Handing the
                    // document over now would mean the outage never began. Dropping it costs nothing, since
                    // the restart re-reads from the tracked position rather than from this cursor.
                    return null;
                }
                positionHandedOver.set(real.getResumeToken());
                return next;
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
            // The threads a subscription is read and restarted on, named so they can be counted
            String restartThreadNamePrefix = "restart-backoff-test-" + UUID.randomUUID();
            ExecutorService executor = Executors.newCachedThreadPool(runnable -> new Thread(runnable, restartThreadNamePrefix));
            subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplateSpy, withConfig("events", TimeRepresentation.RFC_3339_STRING).retryStrategy(RetryStrategy.fixed(backoff)).executor(executor));

            AtomicInteger maxObservedRestartThreads = new AtomicInteger(countThreadsWithNamePrefix(restartThreadNamePrefix));

            // When
            subscriptionModel.subscribe(UUID.randomUUID().toString(), __ -> {
            });

            // Then
            // Sample the live thread count repeatedly while several restart cycles play out. A thread-per-attempt
            // implementation would keep creating new threads for every failed attempt. The model should use at most
            // one thread per subscription however many attempts have been made
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
            verify(mongoCollection, atLeast(2)).watch(any(Class.class));
            subscriptionModel.shutdown();
            executor.shutdownNow();
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
