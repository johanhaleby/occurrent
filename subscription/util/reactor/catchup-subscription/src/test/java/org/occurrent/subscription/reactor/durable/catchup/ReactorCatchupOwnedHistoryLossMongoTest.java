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

package org.occurrent.subscription.reactor.durable.catchup;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.BsonArray;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.CatchupCannotResumeException;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DcbSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.internal.CatchupCheckpoints;
import org.occurrent.subscription.internal.HistoryLossMessages;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A catch-up over the Mongo model answers a history loss that no replay can repair by failing, never by starting from
 * the present. The oplog answering 286 is made by a fail point, scoped to the client of the subscription model. Every
 * case runs with the model configured to restart lost subscriptions, which is Boot's default, unless it says otherwise.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupOwnedHistoryLossMongoTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String SUBSCRIBER_APPLICATION_NAME = "reactor-catchup-owned-history-loss-subscriber";
    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Tag TAG = Tag.parse("name:1");
    private static final int HISTORY_LOST = 286;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private enum Replay {STREAM, DCB}

    private final AtomicInteger historyLostReplies = new AtomicInteger();
    private final AtomicInteger changeStreamsOpened = new AtomicInteger();
    private final CopyOnWriteArrayList<Disposable> disposables = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
    private final List<Logger> catchupLoggers = List.of((Logger) LoggerFactory.getLogger(NamedCatchupSupport.class), (Logger) LoggerFactory.getLogger(PositionCatchupPipeline.class));
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private MongoClient writerClient;
    private MongoClient subscriberClient;
    private ReactorMongoEventStore eventStore;
    private ReactorMongoSubscriptionModel mongoModel;
    private ReactorCatchupSubscriptionModel catchup;
    private CloudEventConverter<DomainEvent> converter;
    private String database;

    @BeforeEach
    void createClients() {
        logged.start();
        catchupLoggers.forEach(logger -> logger.addAppender(logged));
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactorownedhistoryloss");
        database = requireNonNull(connectionString.getDatabase());
        writerClient = MongoClients.create(connectionString);
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).applicationName(SUBSCRIBER_APPLICATION_NAME)
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        BsonArray pipeline = event.getCommand().getArray("pipeline", new BsonArray());
                        if (event.getCommandName().equals("aggregate") && !pipeline.isEmpty() && pipeline.get(0).asDocument().containsKey("$changeStream")) {
                            changeStreamsOpened.incrementAndGet();
                        }
                    }

                    @Override
                    public void commandFailed(CommandFailedEvent event) {
                        if (event.getThrowable() instanceof MongoCommandException e && e.getErrorCode() == HISTORY_LOST) {
                            historyLostReplies.incrementAndGet();
                        }
                    }
                }).build());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(writerClient, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(writerClient, database), config);
        converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
    }

    @AfterEach
    void shutdown() {
        disposables.forEach(Disposable::dispose);
        Mono.from(writerClient.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"))).block(Duration.ofSeconds(10));
        if (catchup != null) {
            catchup.shutdown();
        }
        if (mongoModel != null) {
            mongoModel.shutdown();
        }
        subscriberClient.close();
        writerClient.close();
        catchupLoggers.forEach(logger -> logger.detachAppender(logged));
    }

    @Test
    void a_stream_flux_started_from_a_token_the_oplog_lost_fails_with_catchup_cannot_resume_before_it_reads_anything() {
        verifiesAFluxStartedFromALostTokenFailsBeforeItReadsAnything(Replay.STREAM);
    }

    @Test
    void a_dcb_flux_started_from_a_token_the_oplog_lost_fails_with_catchup_cannot_resume_before_it_reads_anything() {
        verifiesAFluxStartedFromALostTokenFailsBeforeItReadsAnything(Replay.DCB);
    }

    private void verifiesAFluxStartedFromALostTokenFailsBeforeItReadsAnything(Replay replay) {
        catchup = catchupOverAModelThatRestartsLostSubscriptions(true);
        Checkpoint token = requireNonNull(mongoModel.globalCheckpoint().block(Duration.ofSeconds(10)));
        write(replay, nameDefined());
        int openedBefore = changeStreamsOpened.get();
        failsTheNextChangeStreamOpen();

        catchup.subscribe(filter(replay), StartAt.checkpoint(token)).doOnNext(event -> received.add(event.getId())).subscribe(__ -> {
        }, failures::add);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(failures).hasSize(1));
        write(replay, nameDefined());
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(changeStreamsOpened.get() - openedBefore).as("change streams opened after the token was lost").isEqualTo(1));
        assertAll(
                () -> assertThat(failures.getFirst()).as("failure").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("Cannot start catch-up subscription").hasMessageContaining(token.asString()),
                () -> assertThat(received).as("delivered").isEmpty(),
                () -> assertThat(historyLostReplies).as("replies that said the oplog lost the history").hasValue(1));
    }

    @Test
    void a_stream_flux_started_from_now_that_reads_on_and_loses_the_history_fails_with_catchup_cannot_resume_and_logs_one_error() {
        verifiesAFluxReadingOnLosesTheHistory(Replay.STREAM, true, StartAt.now());
    }

    @Test
    void a_dcb_flux_started_from_now_that_reads_on_and_loses_the_history_fails_with_catchup_cannot_resume_and_logs_one_error() {
        verifiesAFluxReadingOnLosesTheHistory(Replay.DCB, true, StartAt.now());
    }

    @Test
    void a_stream_flux_started_from_now_that_reads_on_and_loses_the_history_fails_with_catchup_cannot_resume_and_not_with_the_raw_error_when_the_model_ends_lost_subscriptions() {
        verifiesAFluxReadingOnLosesTheHistory(Replay.STREAM, false, StartAt.now());
    }

    @Test
    void a_dcb_flux_started_from_now_that_reads_on_and_loses_the_history_fails_with_catchup_cannot_resume_and_not_with_the_raw_error_when_the_model_ends_lost_subscriptions() {
        verifiesAFluxReadingOnLosesTheHistory(Replay.DCB, false, StartAt.now());
    }

    @Test
    void a_stream_flux_started_from_a_token_the_oplog_has_that_reads_on_and_loses_the_history_fails_with_catchup_cannot_resume() {
        verifiesAFluxReadingOnLosesTheHistory(Replay.STREAM, true, null);
    }

    @Test
    void a_dcb_flux_started_from_a_token_the_oplog_has_that_reads_on_and_loses_the_history_fails_with_catchup_cannot_resume() {
        verifiesAFluxReadingOnLosesTheHistory(Replay.DCB, true, null);
    }

    // A null start is a token read from the oplog before the first event is written
    private void verifiesAFluxReadingOnLosesTheHistory(Replay replay, boolean restartLostSubscriptions, StartAt startAt) {
        catchup = catchupOverAModelThatRestartsLostSubscriptions(restartLostSubscriptions);
        StartAt start = startAt != null ? startAt : StartAt.checkpoint(requireNonNull(mongoModel.globalCheckpoint().block(Duration.ofSeconds(10))));
        disposables.add(catchup.subscribe(filter(replay), start).doOnNext(event -> received.add(event.getId())).subscribe(__ -> {
        }, failures::add));
        await().atMost(Duration.ofSeconds(10)).until(() -> changeStreamsOpened.get() >= 1);
        NameDefined first = nameDefined();
        write(replay, first);
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(received).containsExactly(first.eventId()));
        int openedBefore = changeStreamsOpened.get();

        FailPointCommands.failNextGetMore(writerClient, SUBSCRIBER_APPLICATION_NAME);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(failures).hasSize(1));
        write(replay, nameDefined());
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(changeStreamsOpened.get() - openedBefore).as("change streams opened after the loss, so none from the present").isZero());
        assertAll(
                () -> assertThat(failures.getFirst()).as("failure").isInstanceOf(CatchupCannotResumeException.class).hasMessageContaining("no longer has the history"),
                () -> assertThat(received).as("delivered").containsExactly(first.eventId()),
                () -> assertThat(historyLostReplies).as("replies that said the oplog lost the history").hasValue(1),
                () -> assertThat(errorsLogged()).as("errors logged").hasSize(1).allSatisfy(error -> assertThat(error).startsWith("The subscription model no longer has the history to resume an unnamed catch-up subscription from")));
    }

    @Test
    void a_stream_subscription_with_an_id_started_from_a_token_the_oplog_lost_is_paused_for_good_with_one_error_logged_and_nothing_delivered() {
        verifiesAnOwnedLiveStartFromALostTokenIsPausedForGood(Replay.STREAM);
    }

    @Test
    void a_dcb_subscription_with_an_id_started_from_a_token_the_oplog_lost_is_paused_for_good_with_one_error_logged_and_nothing_delivered() {
        verifiesAnOwnedLiveStartFromALostTokenIsPausedForGood(Replay.DCB);
    }

    private void verifiesAnOwnedLiveStartFromALostTokenIsPausedForGood(Replay replay) {
        catchup = catchupOverAModelThatRestartsLostSubscriptions(true);
        Checkpoint token = requireNonNull(mongoModel.globalCheckpoint().block(Duration.ofSeconds(10)));
        write(replay, nameDefined());
        int openedBefore = changeStreamsOpened.get();
        failsTheNextChangeStreamOpen();

        Subscription subscription = catchup.subscribe(SUBSCRIPTION_ID, filter(replay), StartAt.checkpoint(token), event -> Mono.fromRunnable(() -> received.add(event.getId())));

        assertThat(subscription).isNotNull();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(catchup.isPaused(SUBSCRIPTION_ID)).as("paused").isTrue());
        write(replay, nameDefined());
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(changeStreamsOpened.get() - openedBefore).as("change streams opened after the token was lost, so none from the present").isEqualTo(1));
        assertAll(
                () -> assertThat(catchup.isRunning(SUBSCRIPTION_ID)).as("running").isFalse(),
                () -> assertThatThrownBy(() -> catchup.resumeSubscription(SUBSCRIPTION_ID)).as("what a resume throws").isInstanceOf(CatchupCannotResumeException.class),
                () -> assertThat(received).as("delivered").isEmpty(),
                () -> assertThat(historyLostReplies).as("replies that said the oplog lost the history").hasValue(1),
                () -> assertThat(errorsLogged()).as("errors logged").containsExactly(terminalFailureLog(token)));
    }

    // The model routes a marked time to the replay branch, so it never reaches the straight to live branch that way. The
    // pipeline is asked for it directly, which is what a caller of the straight to live branch could pass it.
    @Test
    void a_marked_time_checkpoint_given_to_the_straight_to_live_branch_fails_the_flux_before_the_live_model_opens_anything() {
        mongoModel = mongoModel(true);
        Checkpoint markedTime = new StringBasedCheckpoint(CatchupCheckpoints.format("2026-10-01T12:00:00Z", "2026-10-01T11:00:00Z", new StringBasedCheckpoint("live-start")));

        Throwable failure = catchThrowable(() -> PositionCatchupPipeline.straightToLive(mongoModel, StreamSubscriptionFilter.filter(Filter.all()), StartAt.checkpoint(markedTime)).blockLast(Duration.ofSeconds(20)));

        assertAll(
                () -> assertThat(failure).as("failure").isInstanceOf(UnsupportedStartAtException.class),
                () -> assertThat(changeStreamsOpened.get()).as("change streams opened").isZero());
    }

    private ReactorCatchupSubscriptionModel catchupOverAModelThatRestartsLostSubscriptions(boolean restartLostSubscriptions) {
        return new ReactorCatchupSubscriptionModel(mongoModel(restartLostSubscriptions), eventStore, eventStore, null, null, 100, 1000);
    }

    private ReactorMongoSubscriptionModel mongoModel(boolean restartLostSubscriptions) {
        mongoModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, database), "events", TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)).restartSubscriptionsOnChangeStreamHistoryLost(restartLostSubscriptions));
        return mongoModel;
    }

    private void failsTheNextChangeStreamOpen() {
        FailPointCommands.failNext(writerClient, SUBSCRIBER_APPLICATION_NAME, "aggregate");
    }

    private List<String> errorsLogged() {
        synchronized (logged) {
            return logged.list.stream().filter(event -> event.getLevel().equals(Level.ERROR)).map(ILoggingEvent::getFormattedMessage).toList();
        }
    }

    private static String terminalFailureLog(Checkpoint lostFrom) {
        return MessageFormatter.arrayFormat(HistoryLossMessages.TERMINAL_FAILURE_LOG, new Object[]{SUBSCRIPTION_ID, lostFrom.asString(), lostFrom.asString()}).getMessage();
    }

    private static SubscriptionFilter filter(Replay replay) {
        return switch (replay) {
            case STREAM -> StreamSubscriptionFilter.filter(Filter.all());
            case DCB -> DcbSubscriptionFilter.filter(DcbCriteria.tags(TAG));
        };
    }

    private NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
    }

    private void write(Replay replay, DomainEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event));
        switch (replay) {
            case STREAM -> eventStore.write(UUID.randomUUID().toString(), WriteCondition.anyStreamVersion(), Flux.fromIterable(cloudEvents)).then().block(Duration.ofSeconds(10));
            case DCB -> eventStore.append(cloudEvents.stream().map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, Set.of(TAG))).toList()).then().block(Duration.ofSeconds(10));
        }
    }

    // The failCommand fail point, scoped to the client of the subscription model
    private static final class FailPointCommands {
        static void failNext(MongoClient admin, String applicationName, String command) {
            Mono.from(admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of(command)).append("errorCode", HISTORY_LOST).append("appName", applicationName)))).block(Duration.ofSeconds(10));
        }

        static void failNextGetMore(MongoClient admin, String applicationName) {
            failNext(admin, applicationName, "getMore");
        }
    }
}
