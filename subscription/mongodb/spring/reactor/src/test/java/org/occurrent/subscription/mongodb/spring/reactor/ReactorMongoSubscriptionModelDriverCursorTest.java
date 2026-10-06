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
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
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
