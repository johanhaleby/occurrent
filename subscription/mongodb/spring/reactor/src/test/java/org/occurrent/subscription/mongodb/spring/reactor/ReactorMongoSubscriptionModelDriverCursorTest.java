/*
 * Copyright 2026 Johan Haleby
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.reactor.HistoryLostException;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * How a subscription with an id behaves when it reads through the change stream cursor of the MongoDB driver, and what it
 * falls back to when it can't. The subscription model reads with a client of its own, so the tests can see the commands
 * it sends and break them with a fail point.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelDriverCursorTest {

    private static final String SUBSCRIBER_APPLICATION_NAME = "driver-cursor-subscriber";
    private static final SubscriptionFilter NAME_DEFINED_ONLY = AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));
    private static final String QUIET_POSITIONS_NOT_READ_WARNING = "can't read the resume token";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ListAppender<ILoggingEvent> modelLog = new ListAppender<>();
    // The client that writes the events, which the fail points never touch
    private MongoClient mongoClient;
    // The client the subscription model reads with, and the only one the fail points break
    private MongoClient subscriberClient;
    private String databaseName;
    private String eventCollection;
    private CommandLog commands;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;

    @BeforeAll
    static void installDriverCursorFaults() {
        DriverCursorFaults.install();
    }

    @AfterAll
    static void uninstallDriverCursorFaults() {
        DriverCursorFaults.uninstall();
    }

    @BeforeEach
    void createClients() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivedrivercursor");
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        // A collection of its own for every test, since a subscription started at the present can also receive what
        // was written up to 16 seconds before it, which would otherwise include the previous test's events
        eventCollection = "events-" + UUID.randomUUID();
        commands = new CommandLog();
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(SUBSCRIBER_APPLICATION_NAME).addCommandListener(commands).build());
        ReactiveTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(mongoClient, databaseName), eventStoreConfig);
        subscriptionModel = modelOver(new ReactiveMongoTemplate(subscriberClient, databaseName), configuration());
        Logger logger = (Logger) LoggerFactory.getLogger(ReactorMongoSubscriptionModel.class);
        logger.setLevel(Level.INFO);
        modelLog.start();
        logger.addAppender(modelLog);
    }

    @AfterEach
    void shutdown() {
        ((Logger) LoggerFactory.getLogger(ReactorMongoSubscriptionModel.class)).detachAppender(modelLog);
        modelLog.stop();
        subscriptionModel.shutdown();
        FailPoint.off(mongoClient);
        DriverCursorFaults.reset();
        subscriberClient.close();
        mongoClient.close();
    }

    @Test
    void a_subscription_whose_change_stream_history_is_lost_starts_again_at_the_present_when_the_model_is_configured_to_restart_it() {
        // Given
        subscriptionModel.shutdown();
        subscriptionModel = modelOver(new ReactiveMongoTemplate(subscriberClient, databaseName), configuration().restartSubscriptionsOnChangeStreamHistoryLost(true));
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("history-lost", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        NameDefined first = nameDefined();
        write(first);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(first.eventId()));

        // When
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 286));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));
        CommandLog.Sent restarted = commands.changeStreamsOpened().getLast();
        assertThat(restarted.changeStreamStage().containsKey("startAfter")).as("the restarted change stream opens after the last event it read").isFalse();
        assertThat(restarted.changeStreamStage().containsKey("startAtOperationTime")).as("the restarted change stream opens at the present").isTrue();
        NameDefined second = nameDefined();
        write(second);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(first.eventId(), second.eventId()));
    }

    @Test
    void a_subscription_whose_change_stream_history_is_lost_ends_when_the_model_is_not_configured_to_restart_it() {
        // Given
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("history-lost", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        NameDefined first = nameDefined();
        write(first);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(first.eventId()));

        // When
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 286));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isRunning("history-lost")).as("subscription running").isFalse());
        write(nameDefined());
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
            assertThat(commands.changeStreamsOpened()).as("change streams opened").hasSize(1);
            assertThat(delivered).as("events delivered").containsExactly(first.eventId());
        });
    }

    @Test
    void a_quiet_position_handler_that_keeps_refusing_the_checkpoint_write_makes_the_model_read_again_from_the_quiet_position() {
        // Given
        CopyOnWriteArrayList<Checkpoint> handedOver = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(quietPosition -> {
            handedOver.add(quietPosition);
            return refusedCheckpointWrite(subscriptionId);
        }));
        waitUntilStarted(subscriptionModel.subscribe("refused", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));

        // When
        write(nameWasChanged());

        // Then
        await().atMost(30, SECONDS).untilAsserted(() -> {
            assertThat(handedOver).as("quiet positions handed over").hasSizeGreaterThan(1);
            assertThat(commands.changeStreamsOpened()).as("change streams opened").hasSizeGreaterThan(1);
        });
        assertThat(CommandLog.changeStreamField(commands.changeStreamsOpened().get(1), "startAfter")).as("position the second change stream opens after").isEqualTo(resumeTokenOf(handedOver.getFirst()));
    }

    @Test
    void a_quiet_position_handler_that_refuses_the_checkpoint_write_once_is_retried_and_the_subscription_keeps_delivering_events() {
        // Given
        AtomicInteger handedOver = new AtomicInteger();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(quietPosition -> handedOver.incrementAndGet() == 1 ? refusedCheckpointWrite(subscriptionId) : Mono.empty()));
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("refused-once", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        write(nameWasChanged());
        await().atMost(30, SECONDS).until(() -> handedOver.get() > 0);

        // When
        NameDefined matched = nameDefined();
        write(matched);

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).as("events delivered").containsExactly(matched.eventId()));
        assertThat(commands.changeStreamsOpened()).as("change streams opened").hasSizeGreaterThan(1);
    }

    @Test
    void a_quiet_position_handler_that_refuses_the_checkpoint_write_leaves_the_subscription_running() {
        // Given
        AtomicInteger handedOver = refusingCheckpointWrites();
        subscriptionModel.subscribe("refused", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty());
        write(nameWasChanged());

        // When
        await().atMost(30, SECONDS).until(() -> handedOver.get() > 0);

        // Then
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(subscriptionModel.isRunning("refused")).as("subscription running").isTrue());
    }

    @Test
    void a_subscription_whose_quiet_position_handler_refuses_the_checkpoint_write_can_be_paused() {
        // Given
        AtomicInteger handedOver = refusingCheckpointWrites();
        subscriptionModel.subscribe("refused", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty());
        write(nameWasChanged());
        await().atMost(30, SECONDS).until(() -> handedOver.get() > 0);

        // When
        assertThatCode(() -> subscriptionModel.pauseSubscription("refused")).doesNotThrowAnyException();

        // Then
        assertThat(subscriptionModel.isPaused("refused")).as("subscription paused").isTrue();
    }

    @Test
    void a_model_that_cannot_read_the_cursor_of_the_driver_delivers_events_through_the_change_stream_of_spring() {
        // Given
        subscriptionModel.shutdown();
        subscriptionModel = modelOver(templateWithoutBatchCursors(), configuration());
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        Subscription subscription = subscriptionModel.subscribe("fallback", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId())));
        waitUntilStarted(subscription);

        // When
        NameDefined matched = nameDefined();
        write(matched);

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(matched.eventId()));
        assertThat(subscriptionModel.readsQuietPositions()).as("reads quiet positions").isFalse();
    }

    @Test
    void a_dynamic_position_answering_a_catchup_position_is_refused_when_the_model_reads_through_the_change_stream_of_spring() {
        // Given a model that has fallen back to the change stream of Spring, and a dynamic position answering a global checkpoint
        useTheChangeStreamOfSpring();
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        GlobalCheckpoint answered = GlobalCheckpoint.of(0);

        // When it subscribes
        Subscription subscription = subscriptionModel.subscribe("refused", StartAt.dynamic(ctx -> StartAt.checkpoint(answered)), event -> Mono.fromRunnable(() -> delivered.add(event.getId())));

        // Then the subscription stops with the refusal and delivers nothing
        StepVerifier.create(subscription.waitUntilStarted())
                .expectErrorSatisfies(throwable -> assertThat(throwable).isExactlyInstanceOf(UnsupportedStartAtException.class)
                        .hasMessageContaining("\"" + answered.asString() + "\""))
                .verify(Duration.ofSeconds(10));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isRunning("refused")).isFalse());
        assertThat(delivered).isEmpty();
    }

    @Test
    void a_catchup_position_answered_after_a_first_answer_the_server_rejects_is_refused_when_the_model_reads_through_the_change_stream_of_spring() throws InterruptedException {
        // Given a model that has fallen back to the change stream of Spring, and a dynamic position whose first answer is a resume token the server rejects
        useTheChangeStreamOfSpring();
        AtomicInteger answers = new AtomicInteger();
        CountDownLatch askedAgain = new CountDownLatch(1);
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        GlobalCheckpoint catchup = GlobalCheckpoint.of(0);
        StartAt startAt = StartAt.dynamic(ctx -> {
            if (answers.incrementAndGet() == 1) {
                return StartAt.checkpoint(new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("not-a-token"))));
            }
            askedAgain.countDown();
            return StartAt.checkpoint(catchup);
        });

        // When it subscribes, so the retry asks the position again, and the answer is a catch-up's
        subscriptionModel.subscribe("not-accepted", startAt, event -> Mono.fromRunnable(() -> delivered.add(event.getId())));
        assertThat(askedAgain.await(10, SECONDS)).as("the position was asked again").isTrue();

        // Then the server never accepted the first answer, so the second is refused instead of opening at the present, and nothing is replayed
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isRunning("not-accepted")).isFalse());
        write(nameDefined());
        assertThat(delivered).isEmpty();
    }

    @Test
    void a_resume_before_the_change_stream_of_spring_read_a_document_opens_where_the_dynamic_position_first_answered() {
        // Given a model that has fallen back to the change stream of Spring, and a dynamic position that answers a live checkpoint first and a global checkpoint after
        useTheChangeStreamOfSpring();
        Checkpoint live = liveCheckpoint();
        AtomicInteger answers = new AtomicInteger();
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        StartAt startAt = StartAt.dynamic(ctx -> answers.incrementAndGet() == 1 ? StartAt.checkpoint(live) : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        waitUntilStarted(subscriptionModel.subscribe("resumed-before-a-read", startAt, event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));

        // When it is paused before it reads a document, an event is written, and it is resumed
        subscriptionModel.pauseSubscription("resumed-before-a-read");
        NameDefined written = nameDefined();
        write(written);
        Subscription resumed = subscriptionModel.resumeSubscription("resumed-before-a-read");

        // Then it opens at the live checkpoint without asking the position again, so it isn't refused and delivers the event
        waitUntilStarted(resumed);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(written.eventId()));
        assertThat(answers).hasValue(1);
    }

    @Test
    void a_first_answer_the_server_cannot_find_is_asked_again_when_the_model_reads_through_the_change_stream_of_spring() {
        // Given a model that has fallen back to the change stream of Spring, and a dynamic position whose first answer is a
        // token of another collection, which the server accepts as the stream opens and can't find once it reads an event
        useTheChangeStreamOfSpring();
        Checkpoint live = liveCheckpoint();
        Checkpoint ofAnotherCollection = tokenOfAnotherCollection();
        AtomicInteger answers = new AtomicInteger();
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        StartAt startAt = StartAt.dynamic(ctx -> answers.incrementAndGet() == 1 ? StartAt.checkpoint(ofAnotherCollection) : StartAt.checkpoint(live));

        // When it subscribes and an event is written
        waitUntilStarted(subscriptionModel.subscribe("not-found", startAt, event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        NameDefined written = nameDefined();
        write(written);

        // Then the position is asked again, the second answer opens the stream and the event is delivered
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(written.eventId()));
        assertThat(answers).hasValue(2);
        assertThat(subscriptionModel.isRunning("not-found")).isTrue();
    }

    @Test
    void a_first_answer_the_server_cannot_find_is_asked_again_after_a_pause_opened_the_change_stream_of_spring_at_it_again() {
        // Given a model that has fallen back to the change stream of Spring, and a dynamic position whose first answer is a
        // token of another collection, which the server accepts as the stream opens and can't find once it reads an event
        useTheChangeStreamOfSpring();
        Checkpoint live = liveCheckpoint();
        Checkpoint ofAnotherCollection = tokenOfAnotherCollection();
        AtomicInteger answers = new AtomicInteger();
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        StartAt startAt = StartAt.dynamic(ctx -> answers.incrementAndGet() == 1 ? StartAt.checkpoint(ofAnotherCollection) : StartAt.checkpoint(live));
        waitUntilStarted(subscriptionModel.subscribe("not-found-after-a-pause", startAt, event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));

        // When it is paused and resumed before it reads a document, so the stream opens at the token of the other collection again, and an event is written
        subscriptionModel.pauseSubscription("not-found-after-a-pause");
        waitUntilStarted(subscriptionModel.resumeSubscription("not-found-after-a-pause"));
        NameDefined written = nameDefined();
        write(written);

        // Then the position is asked again, the second answer opens the stream and the event is delivered
        await().atMost(15, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(written.eventId()));
        assertThat(answers).hasValue(2);
        assertThat(subscriptionModel.isRunning("not-found-after-a-pause")).isTrue();
    }

    @Test
    void a_run_that_was_paused_while_its_dynamic_position_answered_leaves_the_resumed_run_to_ask_again_when_the_server_cannot_find_its_answer() throws InterruptedException {
        // Given a model that has fallen back to the change stream of Spring, and a dynamic position whose first answer waits
        // until it is let go and is then a live checkpoint, whose second answer is a token of another collection, which the
        // server accepts as the stream opens and can't find once it reads an event, and whose answers after that are the live checkpoint
        useTheChangeStreamOfSpring();
        Checkpoint live = liveCheckpoint();
        Checkpoint ofAnotherCollection = tokenOfAnotherCollection();
        AtomicInteger answers = new AtomicInteger();
        CountDownLatch firstAnswerAsked = new CountDownLatch(1);
        CountDownLatch letFirstAnswerGo = new CountDownLatch(1);
        StartAt startAt = StartAt.dynamic(ctx -> {
            int answer = answers.incrementAndGet();
            if (answer == 1) {
                firstAnswerAsked.countDown();
                try {
                    letFirstAnswerGo.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            return answer == 2 ? StartAt.checkpoint(ofAnotherCollection) : StartAt.checkpoint(live);
        });
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch eventDelivered = new CountDownLatch(1);
        Thread subscriber = new Thread(() -> subscriptionModel.subscribe("paused-while-it-answered", startAt, event -> Mono.fromRunnable(() -> {
            delivered.add(event.getId());
            eventDelivered.countDown();
        })));
        subscriber.start();
        assertThat(firstAnswerAsked.await(10, SECONDS)).isTrue();

        // When it is paused and resumed while the first answer waits, so the resumed run opens at the token of the other
        // collection, the first answer is let go on the run the pause closed, and an event is written
        subscriptionModel.pauseSubscription("paused-while-it-answered");
        waitUntilStarted(subscriptionModel.resumeSubscription("paused-while-it-answered"));
        letFirstAnswerGo.countDown();
        subscriber.join(SECONDS.toMillis(10));
        NameDefined written = nameDefined();
        write(written);

        // Then the resumed run asks the position again once the server can't find the token, and the event is delivered.
        // The wait doesn't fail on its own, so the assertions say what went wrong
        eventDelivered.await(15, SECONDS);
        assertThat(answers).hasValue(3);
        assertThat(delivered).containsExactly(written.eventId());
    }

    @Test
    void the_flux_opened_again_before_it_read_a_document_opens_where_its_dynamic_position_first_answered() {
        // Given a dynamic position that answers a live checkpoint first and a global checkpoint after, and a first read of the change stream that fails
        Checkpoint live = liveCheckpoint();
        AtomicInteger answers = new AtomicInteger();
        StartAt startAt = StartAt.dynamic(ctx -> answers.incrementAndGet() == 1 ? StartAt.checkpoint(live) : StartAt.checkpoint(GlobalCheckpoint.of(0)));
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 2));
        NameDefined written = nameDefined();

        // When the flux is subscribed to, and an event is written once the first read has failed
        StepVerifier.create(subscriptionModel.subscribe(startAt))
                .then(() -> {
                    await().atMost(10, SECONDS).until(() -> !commands.failed("getMore").isEmpty());
                    write(written);
                })

                // Then it opens again at the live checkpoint, so it isn't refused and delivers the event
                .expectNextMatches(cloudEvent -> cloudEvent.getId().equals(written.eventId()))
                .thenCancel()
                .verify(Duration.ofSeconds(15));
        // And the position wasn't asked again
        assertThat(answers).hasValue(1);
    }

    @Test
    void the_flux_asks_its_dynamic_position_again_when_the_server_cannot_read_the_first_answer() {
        // Given a dynamic position whose first answer is a token the server can't read, and a live checkpoint after
        Checkpoint live = liveCheckpoint();
        AtomicInteger answers = new AtomicInteger();
        StartAt startAt = StartAt.dynamic(ctx -> answers.incrementAndGet() == 1
                ? StartAt.checkpoint(new MongoResumeTokenCheckpoint(new BsonDocument("_data", new BsonString("not-a-token"))))
                : StartAt.checkpoint(live));
        NameDefined written = nameDefined();

        // When the flux is subscribed to, and an event is written once the position has been asked again
        StepVerifier.create(subscriptionModel.subscribe(startAt))
                .then(() -> {
                    await().atMost(10, SECONDS).until(() -> answers.get() >= 2);
                    write(written);
                })

                // Then the second answer opens the stream and the event is delivered
                .expectNextMatches(cloudEvent -> cloudEvent.getId().equals(written.eventId()))
                .thenCancel()
                .verify(Duration.ofSeconds(15));
    }

    @Test
    void the_flux_asks_its_dynamic_position_again_when_the_server_cannot_read_the_first_answer_after_a_failed_read_opened_it_again() {
        // Given a dynamic position whose first answer is a token of another collection, which the server accepts as the stream opens and can't find once it reads an event, and a live checkpoint after, and a first read of the change stream that fails
        Checkpoint live = liveCheckpoint();
        Checkpoint ofAnotherCollection = tokenOfAnotherCollection();
        AtomicInteger answers = new AtomicInteger();
        StartAt startAt = StartAt.dynamic(ctx -> answers.incrementAndGet() == 1 ? StartAt.checkpoint(ofAnotherCollection) : StartAt.checkpoint(live));
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 2));
        NameDefined written = nameDefined();

        // When the flux is subscribed to, and an event is written once the first read has failed and the flux has opened again
        StepVerifier.create(subscriptionModel.subscribe(startAt))
                .then(() -> {
                    await().atMost(10, SECONDS).until(() -> !commands.failed("getMore").isEmpty());
                    write(written);
                })

                // Then the position is asked again once the server can't read the first answer, the second answer opens the stream and the event is delivered
                .expectNextMatches(cloudEvent -> cloudEvent.getId().equals(written.eventId()))
                .thenCancel()
                .verify(Duration.ofSeconds(15));
        assertThat(answers).hasValue(2);
    }

    @Test
    void the_flux_failing_on_history_loss_names_where_its_dynamic_position_opened_when_the_history_is_lost_after_a_failed_read_opened_it_again() {
        // Given a dynamic position answering a live checkpoint, and a first read of the change stream that fails
        Checkpoint live = liveCheckpoint();
        StartAt startAt = StartAt.dynamic(ctx -> StartAt.checkpoint(live));
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 2));

        // When the flux is subscribed to, and the server has lost the history once the first read has failed and the flux has opened again
        StepVerifier.create(subscriptionModel.subscribeFailingOnHistoryLoss(null, startAt))
                .then(() -> {
                    await().atMost(10, SECONDS).until(() -> !commands.failed("getMore").isEmpty());
                    FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 286));
                })

                // Then the flux fails with the position its dynamic answer opened at
                .expectErrorSatisfies(error -> assertThat(error).isInstanceOfSatisfying(HistoryLostException.class, lost -> assertThat(lost.lostFrom()).isEqualTo(live)))
                .verify(Duration.ofSeconds(15));
    }

    @Test
    void a_named_subscription_asks_its_history_loss_listener_where_its_dynamic_position_opened_when_the_history_is_lost_after_a_pause_opened_it_again() {
        // Given a model that has fallen back to the change stream of Spring, a listener that takes a subscription over, and a dynamic position answering a live checkpoint
        useTheChangeStreamOfSpring();
        Checkpoint live = liveCheckpoint();
        CopyOnWriteArrayList<String> asked = new CopyOnWriteArrayList<>();
        subscriptionModel.addHistoryLossListener((subscriptionId, lostFrom) -> {
            asked.add(subscriptionId + "@" + lostFrom.asString());
            return HistoryLossHandling.takeOver(() -> {
            });
        });
        StartAt startAt = StartAt.dynamic(ctx -> StartAt.checkpoint(live));
        waitUntilStarted(subscriptionModel.subscribe("history-lost-after-a-pause", startAt, event -> Mono.empty()));

        // When it is paused and resumed before it reads a document, and the server has lost the history
        subscriptionModel.pauseSubscription("history-lost-after-a-pause");
        waitUntilStarted(subscriptionModel.resumeSubscription("history-lost-after-a-pause"));
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", 286));

        // Then the listener is asked with the live checkpoint the resumed run opened at, and takes the subscription over
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(asked).containsExactly("history-lost-after-a-pause@" + live.asString()));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isPaused("history-lost-after-a-pause")).isTrue());
    }

    @Test
    void a_dynamic_position_answering_a_catchup_position_is_refused_when_the_model_falls_back_to_the_change_stream_of_spring_while_it_subscribes() {
        // Given a model whose driver cursor turns out unreadable on the first subscription, and a dynamic position that answers the present first and a global checkpoint when it is asked again
        subscriptionModel.shutdown();
        subscriptionModel = modelOver(templateWithoutBatchCursors(), configuration());
        AtomicInteger asked = new AtomicInteger();
        GlobalCheckpoint answered = GlobalCheckpoint.of(0);
        StartAt startAt = StartAt.dynamic(ctx -> asked.incrementAndGet() == 1 ? StartAt.now() : StartAt.checkpoint(answered));

        // When it subscribes
        Subscription subscription = subscriptionModel.subscribe("refused-after-falling-back", startAt, event -> Mono.empty());

        // Then the subscription stops with the refusal, after the handle has answered that it started, since the driver's cursor was asked to open before the position was asked again
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.isRunning("refused-after-falling-back")).isFalse());
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10)).block()).as("subscription started").isTrue();
        assertThat(asked).as("times the position was asked").hasValue(2);
        assertThat(modelLog.list).filteredOn(event -> event.getLevel() == Level.ERROR && event.getThrowableProxy() != null)
                .extracting(event -> event.getThrowableProxy().getClassName())
                .as("exceptions logged as errors").containsOnly(UnsupportedStartAtException.class.getName());
        assertThat(subscriptionModel.readsQuietPositions()).as("reads quiet positions").isFalse();
    }

    @Test
    void a_model_that_cannot_read_the_cursor_of_the_driver_warns_once_however_many_subscriptions_fall_back() {
        // Given
        subscriptionModel.shutdown();
        subscriptionModel = modelOver(templateWithoutBatchCursors(), configuration());
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("fallback-1", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        waitUntilStarted(subscriptionModel.subscribe("fallback-2", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));

        // When
        write(nameDefined());
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).hasSize(2));

        // Then
        assertThat(modelLog.list).filteredOn(event -> event.getLevel() == Level.WARN && event.getFormattedMessage().contains(QUIET_POSITIONS_NOT_READ_WARNING)).as("warnings that the resume token can't be read").hasSize(1);
    }

    @Test
    void a_model_whose_token_reads_start_failing_after_the_cursor_opened_warns_once_and_delivers_events_through_the_change_stream_of_spring() {
        // Given
        DriverCursorFaults.recordCursors = true;
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("token-reads-fail", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        await().atMost(10, SECONDS).until(() -> !DriverCursorFaults.cursors.isEmpty());

        // When
        DriverCursorFaults.failTokenReads = true;

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(tokenReadWarnings()).as("warnings that the resume token can't be read").hasSize(1));
        assertThat(subscriptionModel.readsQuietPositions()).as("reads quiet positions").isFalse();
        NameDefined matched = nameDefined();
        write(matched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(matched.eventId()));
        assertThat(tokenReadWarnings()).as("warnings that the resume token can't be read").hasSize(1);
    }

    @Test
    void a_model_whose_driver_cursor_is_opening_the_change_stream_again_keeps_reading_quiet_positions() throws ReflectiveOperationException {
        // Given
        DriverCursorFaults.recordCursors = true;
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        // Asked once before every wait of a second for a batch, so once per look at the token
        AtomicInteger looks = new AtomicInteger();
        subscriptionModel.addQuietPositionListener(subscriptionId -> {
            looks.incrementAndGet();
            return Mono.just(quietPosition -> Mono.fromRunnable(() -> quietPositions.add(quietPosition)));
        });
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("opening-again", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        write(nameWasChanged());
        await().atMost(30, SECONDS).until(() -> !quietPositions.isEmpty());
        // The driver empties this reference while it opens the change stream again, and puts a cursor back when it has
        Object driverCursor = DriverCursorFaults.cursors.getFirst();
        Field cursorOfChangeStream = driverCursor.getClass().getDeclaredField("wrapped");
        cursorOfChangeStream.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Object> reference = (AtomicReference<Object>) cursorOfChangeStream.get(driverCursor);
        Object commandCursor = reference.get();
        int looksBeforeTheWindow = looks.get();

        // When
        reference.set(null);
        // Four looks, so at least three token reads find the reference empty, or until the model stops reading quiet positions
        await().atMost(30, SECONDS).until(() -> looks.get() - looksBeforeTheWindow >= 4 || !subscriptionModel.readsQuietPositions());
        reference.set(commandCursor);

        // Then
        assertThat(tokenReadWarnings()).as("warnings that the resume token can't be read").isEmpty();
        assertThat(subscriptionModel.readsQuietPositions()).as("reads quiet positions").isTrue();
        assertThat(looks.get() - looksBeforeTheWindow).as("looks at the token while the cursor was empty").isGreaterThanOrEqualTo(3);
        int quietPositionsBeforeTheNextEvent = quietPositions.size();
        write(nameWasChanged());
        await().atMost(30, SECONDS).untilAsserted(() -> assertThat(quietPositions).as("quiet positions reported after the window").hasSizeGreaterThan(quietPositionsBeforeTheNextEvent));
        NameDefined matched = nameDefined();
        write(matched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(matched.eventId()));
    }

    private List<ILoggingEvent> tokenReadWarnings() {
        return modelLog.list.stream().filter(event -> event.getLevel() == Level.WARN && event.getFormattedMessage().contains(QUIET_POSITIONS_NOT_READ_WARNING)).toList();
    }

    // A quiet position handler that is called once per position the model finds, and refuses to write it
    private AtomicInteger refusingCheckpointWrites() {
        AtomicInteger handedOver = new AtomicInteger();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(quietPosition -> {
            handedOver.incrementAndGet();
            return refusedCheckpointWrite(subscriptionId);
        }));
        return handedOver;
    }

    private static Mono<Void> refusedCheckpointWrite(String subscriptionId) {
        return Mono.error(new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), CheckpointWriteCondition.any()));
    }

    private static BsonDocument resumeTokenOf(Checkpoint checkpoint) {
        return ((MongoResumeTokenCheckpoint) checkpoint).resumeToken;
    }

    private static ReactorMongoSubscriptionModelConfig configuration() {
        return ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS));
    }

    private ReactorMongoSubscriptionModel modelOver(ReactiveMongoTemplate template, ReactorMongoSubscriptionModelConfig config) {
        return new ReactorMongoSubscriptionModel(template, eventCollection, TimeRepresentation.RFC_3339_STRING, config);
    }

    // The model opens its change stream on the collection this template hands out, and a publisher that
    // isn't the driver's own has no cursor to read the resume token from
    private ReactiveMongoTemplate templateWithoutBatchCursors() {
        return new ReactiveMongoTemplate(subscriberClient, databaseName) {
            @Override
            public Mono<MongoCollection<Document>> getCollection(String collectionName) {
                return super.getCollection(collectionName).map(ReactorMongoSubscriptionModelDriverCursorTest::collectionWithoutBatchCursors);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> collectionWithoutBatchCursors(MongoCollection<Document> collection) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(), new Class<?>[]{MongoCollection.class}, delegatingTo(collection));
    }

    private static InvocationHandler delegatingTo(Object delegate) {
        return (proxy, method, args) -> {
            Object result;
            try {
                result = method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (result instanceof ChangeStreamPublisher<?> changeStream) {
                return Proxy.newProxyInstance(ChangeStreamPublisher.class.getClassLoader(), new Class<?>[]{ChangeStreamPublisher.class}, delegatingTo(changeStream));
            }
            return result;
        };
    }

    private Checkpoint liveCheckpoint() {
        return requireNonNull(subscriptionModel.globalCheckpoint().block(Duration.ofSeconds(10)));
    }

    // A token of a document inserted into another collection
    private Checkpoint tokenOfAnotherCollection() {
        MongoCollection<Document> other = mongoClient.getDatabase(databaseName).getCollection("other-" + UUID.randomUUID());
        BsonTimestamp before = requireNonNull(Mono.from(mongoClient.getDatabase(databaseName).runCommand(new Document("ping", 1))).block()).get("operationTime", BsonTimestamp.class);
        Mono.from(other.insertOne(new Document("marker", 1))).block();
        ChangeStreamDocument<Document> inserted = requireNonNull(Flux.from(other.watch().startAtOperationTime(before)).blockFirst(Duration.ofSeconds(10)));
        return new MongoResumeTokenCheckpoint(requireNonNull(inserted.getResumeToken()));
    }

    // The first subscription finds out that the driver's cursor can't be read, and the model reads through Spring's changeStream from then on
    private void useTheChangeStreamOfSpring() {
        subscriptionModel.shutdown();
        subscriptionModel = modelOver(templateWithoutBatchCursors(), configuration());
        waitUntilStarted(subscriptionModel.subscribe("first-to-fall-back", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.empty()));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(subscriptionModel.readsQuietPositions()).as("reads quiet positions").isFalse());
    }

    private static void waitUntilStarted(Subscription subscription) {
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10)).block()).as("subscription %s started", subscription.id()).isTrue();
    }

    private NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
    }

    private NameWasChanged nameWasChanged() {
        return new NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name2");
    }

    private void write(DomainEvent event) {
        mongoEventStore.write(UUID.randomUUID().toString(), 0, serialize(event)).block();
    }

    private Flux<CloudEvent> serialize(DomainEvent e) {
        return Flux.just(CloudEventBuilder.v1()
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
