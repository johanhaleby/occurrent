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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.reactor.QuietPositionReportingSubscriptions.QuietPositionListener;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.filter.Filter.type;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * The position a subscription reports when a read returned no event for it, and where the subscription goes on from.
 * Every test writes events the subscription's filter doesn't match, since those are what move the change stream on
 * without an event to deliver. The subscription model reads with a client of its own, so the tests can see the commands
 * it sends and break them with a fail point.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelQuietPositionTest {

    private static final String SUBSCRIBER_APPLICATION_NAME = "quiet-position-subscriber";
    private static final SubscriptionFilter NAME_DEFINED_ONLY = AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CopyOnWriteArrayList<Disposable> disposables = new CopyOnWriteArrayList<>();
    // The client that writes the events, which the fail points never touch
    private MongoClient mongoClient;
    // The client the subscription model reads with, and the only one the fail points break
    private MongoClient subscriberClient;
    private String databaseName;
    private CommandLog commands;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createSubscriptionModel() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivequiet");
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        commands = new CommandLog();
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(SUBSCRIBER_APPLICATION_NAME).addCommandListener(commands).build());
        subscriptionModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, databaseName), "events", TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)));
        ReactiveTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(mongoClient, databaseName), eventStoreConfig);
    }

    @AfterEach
    void shutdown() {
        disposables.forEach(Disposable::dispose);
        subscriptionModel.shutdown();
        FailPoint.off(mongoClient);
        subscriberClient.close();
        mongoClient.close();
    }

    @Test
    void the_model_is_found_through_the_quiet_position_capability_lookup() {
        // When
        var capability = QuietPositionReportingSubscriptions.findIn(subscriptionModel);

        // Then
        assertThat(capability).containsSame(subscriptionModel);
    }

    @Test
    void a_subscription_that_matches_nothing_reports_a_position_after_the_events_it_did_not_match() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> subscriptionId.equals("quiet") ? Mono.just(collectingInto(quietPositions)) : Mono.empty());
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("quiet", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event))));
        NameDefined matched = nameDefined();
        write(matched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).containsExactly(matched.eventId()));
        Checkpoint positionOfLastEvent = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(delivered.getFirst());

        // When
        CopyOnWriteArrayList<CloudEvent> everyEvent = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("every-event", StartAt.now(), event -> Mono.fromRunnable(() -> everyEvent.add(event))));
        NameWasChanged notMatched = nameWasChanged();
        write(notMatched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(everyEvent).extracting(CloudEvent::getId).containsExactly(notMatched.eventId()));
        Checkpoint positionOfNotMatched = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(everyEvent.getFirst());
        await().atMost(30, SECONDS).untilAsserted(() -> assertThat(quietPositions).as("positions reported at or after the event that did not match").anyMatch(position -> isAtOrAfter(position, positionOfNotMatched)));
        Checkpoint quietPosition = quietPositions.stream().filter(position -> isAtOrAfter(position, positionOfNotMatched)).findFirst().orElseThrow();

        // Then
        assertThat(quietPosition.asString()).isNotEqualTo(positionOfLastEvent.asString());
        CopyOnWriteArrayList<CloudEvent> fromTheQuietPosition = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("from-the-quiet-position", StartAt.checkpoint(quietPosition), event -> Mono.fromRunnable(() -> fromTheQuietPosition.add(event))));
        NameDefined writtenLater = nameDefined();
        write(writtenLater);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(fromTheQuietPosition).extracting(CloudEvent::getId).contains(writtenLater.eventId()));
        // The position MongoDB sends with an empty batch can come before an event written at the same time as the
        // last one it read, so the event that did not match may be read again. The one that matched is not.
        assertThat(fromTheQuietPosition).as("read from the quiet position").extracting(CloudEvent::getId).doesNotContain(matched.eventId());
    }

    @Test
    void no_quiet_position_is_reported_while_the_action_runs_for_an_event() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(collectingInto(quietPositions)));
        CountDownLatch actionRunning = new CountDownLatch(1);
        Sinks.Empty<Void> finishAction = Sinks.empty();
        waitUntilStarted(subscriptionModel.subscribe("busy", NAME_DEFINED_ONLY, StartAt.now(), __ -> {
            actionRunning.countDown();
            return finishAction.asMono();
        }));
        write(nameDefined());
        assertThat(actionRunning.await(10, SECONDS)).isTrue();
        int reportedBeforeTheAction = quietPositions.size();

        // When
        write(nameWasChanged());

        // Then
        try {
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(quietPositions).hasSize(reportedBeforeTheAction));
        } finally {
            finishAction.tryEmitEmpty();
        }
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedBeforeTheAction);
    }

    @Test
    void a_paused_subscription_reports_no_quiet_position_and_a_resumed_one_does() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(collectingInto(quietPositions)));
        waitUntilStarted(subscriptionModel.subscribe("paused", StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> !quietPositions.isEmpty());

        // When
        subscriptionModel.pauseSubscription("paused");
        // A position that was being handed over when the subscription was paused
        settle();
        int reportedWhenPaused = quietPositions.size();

        // Then
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(quietPositions).hasSize(reportedWhenPaused));
        waitUntilStarted(subscriptionModel.resumeSubscription("paused"));
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedWhenPaused);
    }

    @Test
    void a_listener_that_is_removed_is_not_asked_again() {
        // Given
        AtomicInteger asked = new AtomicInteger();
        QuietPositionListener listener = subscriptionId -> {
            asked.incrementAndGet();
            return Mono.empty();
        };
        subscriptionModel.addQuietPositionListener(listener);
        waitUntilStarted(subscriptionModel.subscribe("asked", StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> asked.get() > 1);

        // When
        subscriptionModel.removeQuietPositionListener(listener);
        // The read that was under way when the listener was removed has already asked it
        settle();
        int askedWhenRemoved = asked.get();

        // Then
        await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(asked).hasValue(askedWhenRemoved));
    }

    @Test
    void pausing_and_resuming_a_quiet_subscription_opens_the_change_stream_at_the_quiet_position() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> subscriptionId.equals("quiet") ? Mono.just(collectingInto(quietPositions)) : Mono.empty());
        waitUntilStarted(subscriptionModel.subscribe("quiet", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> !quietPositions.isEmpty());
        Checkpoint positionBeforeTheEvents = quietPositions.getLast();
        // Reads the events the quiet subscription does not match, to know where they are. Its own change stream is the second one opened.
        CopyOnWriteArrayList<CloudEvent> everyEvent = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("every-event", StartAt.now(), event -> Mono.fromRunnable(() -> everyEvent.add(event))));
        write(nameWasChanged());
        write(nameWasChanged());
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(everyEvent).hasSize(2));
        subscriptionModel.cancelSubscription("every-event");
        Checkpoint positionOfLastNotMatched = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(everyEvent.getLast());
        await().atMost(30, SECONDS).untilAsserted(() -> assertThat(quietPositions).as("positions reported at or after the events that did not match").anyMatch(position -> isAtOrAfter(position, positionOfLastNotMatched)));

        // When
        subscriptionModel.pauseSubscription("quiet");
        // A position that was being handed over when the subscription was paused
        settle();
        Checkpoint quietPosition = quietPositions.getLast();
        waitUntilStarted(subscriptionModel.resumeSubscription("quiet"));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(3));
        CommandLog.Sent opened = commands.changeStreamsOpened().getFirst();
        CommandLog.Sent resumed = commands.changeStreamsOpened().getLast();
        assertThat(opened.changeStreamStage().containsKey("startAtOperationTime")).as("the first change stream opens at the operation time the subscription started at").isTrue();
        assertThat(resumed.changeStreamStage().containsKey("startAtOperationTime")).as("the resumed change stream opens at the operation time the subscription started at").isFalse();
        assertThat(CommandLog.changeStreamField(resumed, "startAfter")).as("the position the resumed change stream opens at").isEqualTo(resumeTokenOf(quietPosition));
        assertThat(resumeTokenOf(quietPosition)).as("the position reported after the events").isNotEqualTo(resumeTokenOf(positionBeforeTheEvents));
    }

    @Test
    void a_quiet_position_that_errors_makes_the_model_read_again_from_its_position_and_the_subscription_keeps_delivering() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        AtomicInteger handedOver = new AtomicInteger();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(quietPosition -> {
            quietPositions.add(quietPosition);
            return handedOver.incrementAndGet() == 1 ? Mono.error(new IllegalStateException("expected")) : Mono.empty();
        }));
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();

        // When
        subscriptionModel.subscribe("failing-hand-over", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event)));

        // Then: the change stream is opened again at the position that could not be handed over
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));
        assertThat(CommandLog.changeStreamField(commands.changeStreamsOpened().getLast(), "startAfter")).isEqualTo(resumeTokenOf(quietPositions.getFirst()));
        NameDefined matched = nameDefined();
        write(matched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).extracting(CloudEvent::getId).containsExactly(matched.eventId()));
    }

    @Test
    void pausing_cancels_a_running_quiet_position_and_the_resumed_subscription_reads_only_after_that_cancel() throws InterruptedException {
        // Given
        CopyOnWriteArrayList<String> signals = new CopyOnWriteArrayList<>();
        CountDownLatch cancelMayFinish = new CountDownLatch(1);
        subscriptionModel.addQuietPositionListener(subscriptionId -> {
            signals.add("asked before reading");
            return Mono.just(quietPosition -> Mono.<Void>never()
                    .doOnSubscribe(__ -> signals.add("quiet position being handled"))
                    .doOnCancel(() -> {
                        signals.add("cancel started");
                        awaitUninterruptibly(cancelMayFinish);
                        signals.add("cancel finished");
                    }));
        });
        waitUntilStarted(subscriptionModel.subscribe("slow-to-cancel", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));
        write(nameWasChanged());
        await().atMost(30, SECONDS).until(() -> signals.contains("quiet position being handled"));

        // When: the pause is slow, since it waits for the quiet position to be cancelled, so the subscription is resumed while it is still going on
        Thread pause = new Thread(() -> subscriptionModel.pauseSubscription("slow-to-cancel"));
        pause.start();
        try {
            await().atMost(10, SECONDS).until(() -> signals.contains("cancel started"));
            subscriptionModel.resumeSubscription("slow-to-cancel");

            // Then
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(signalsAfter(signals, "cancel started")).as("signals after the cancel started").isEmpty());
        } finally {
            cancelMayFinish.countDown();
            pause.join(10_000);
        }
        await().atMost(10, SECONDS).until(() -> signalsAfter(signals, "cancel finished").contains("asked before reading"));
        assertThat(signalsAfter(signals, "cancel started")).first().as("first signal after the cancel started").isEqualTo("cancel finished");
    }

    @Test
    void the_change_stream_cursor_of_the_driver_can_be_read_with_the_driver_of_this_build() {
        // When
        String unavailableBecause = DriverChangeStreamCursor.unavailableBecause();

        // Then
        assertThat(unavailableBecause).as("why the driver's change stream cursor can't be read").isNull();
    }

    @Test
    void the_driver_of_this_build_declares_the_fields_the_token_read_relies_on_final_or_volatile() throws ClassNotFoundException {
        // Given
        Class<?> commandCursor = Class.forName("com.mongodb.internal.operation.AsyncCommandCursor");

        // When
        String unavailableBecause = DriverChangeStreamCursor.unavailableBecause();
        String unsafeBecause = DriverChangeStreamCursor.unlessTokenIsReadSafelyThrough(commandCursor);

        // Then
        assertAll(
                () -> assertThat(unavailableBecause).as("why a field of the reactive wrapper or the change stream cursor of the driver can't be read safely from another thread").isNull(),
                () -> assertThat(unsafeBecause).as("why a token read through the driver's command cursor can see an older token than an earlier read").isNull()
        );
    }

    @Test
    void a_subscription_that_has_reported_a_quiet_position_still_reads_through_the_cursor_of_the_driver() {
        // Given
        CopyOnWriteArrayList<Checkpoint> quietPositions = new CopyOnWriteArrayList<>();
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(collectingInto(quietPositions)));
        waitUntilStarted(subscriptionModel.subscribe("through-the-cursor", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));
        write(nameWasChanged());

        // When: the subscription has reported a position, or has given up on reporting them
        await().atMost(30, SECONDS).until(() -> !quietPositions.isEmpty() || !subscriptionModel.readsQuietPositions());

        // Then
        assertThat(subscriptionModel.readsQuietPositions()).as("reads quiet positions through the cursor of the driver").isTrue();
        assertThat(quietPositions).as("quiet positions reported").isNotEmpty();
    }

    @Test
    void no_more_of_the_change_stream_is_read_while_the_action_runs_for_an_event() throws InterruptedException {
        // Given
        CountDownLatch actionRunning = new CountDownLatch(1);
        Sinks.Empty<Void> finishAction = Sinks.empty();
        waitUntilStarted(subscriptionModel.subscribe("busy-reader", NAME_DEFINED_ONLY, StartAt.now(), __ -> {
            actionRunning.countDown();
            return finishAction.asMono();
        }));
        write(nameDefined());
        assertThat(actionRunning.await(10, SECONDS)).isTrue();
        int getMoresSentWhenTheActionStarted = commands.named("getMore").size();

        // When
        write(nameWasChanged());

        // Then
        try {
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(commands.named("getMore")).as("getMore sent while the action runs").hasSize(getMoresSentWhenTheActionStarted));
        } finally {
            finishAction.tryEmitEmpty();
        }
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.named("getMore")).as("getMore sent once the action has completed").hasSizeGreaterThan(getMoresSentWhenTheActionStarted));
    }

    @Test
    void an_event_whose_action_has_not_completed_is_delivered_again_when_the_subscription_is_paused_and_resumed_after_other_events_were_written() {
        // Given
        CopyOnWriteArrayList<String> invokedFor = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("never-completes", NAME_DEFINED_ONLY, StartAt.now(), event -> {
            invokedFor.add(event.getId());
            return Mono.never();
        }));
        NameDefined matched = nameDefined();
        write(matched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(invokedFor).containsExactly(matched.eventId()));
        write(nameWasChanged());
        write(nameWasChanged());
        // Long enough for the change stream to have been read past the events that did not match, if it was read at all
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(invokedFor).hasSize(1));

        // When
        subscriptionModel.pauseSubscription("never-completes");
        subscriptionModel.resumeSubscription("never-completes");

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(invokedFor).as("events the action was invoked for").containsExactly(matched.eventId(), matched.eventId()));
    }

    @Test
    void a_subscription_started_under_the_id_of_a_cancelled_run_reads_nothing_until_every_earlier_run_for_the_id_has_ended() throws InterruptedException {
        // Given: a first run that is slow to cancel
        AtomicInteger asked = new AtomicInteger();
        subscriptionModel.addQuietPositionListener(subscriptionId -> {
            asked.incrementAndGet();
            return Mono.empty();
        });
        CountDownLatch actionRunning = new CountDownLatch(1);
        CountDownLatch cancelStarted = new CountDownLatch(1);
        CountDownLatch cancelMayFinish = new CountDownLatch(1);
        waitUntilStarted(subscriptionModel.subscribe("chained", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.<Void>never()
                .doOnSubscribe(subscription -> actionRunning.countDown())
                .doOnCancel(() -> {
                    cancelStarted.countDown();
                    awaitUninterruptibly(cancelMayFinish);
                })));
        write(nameDefined());
        assertThat(actionRunning.await(10, SECONDS)).isTrue();
        Thread pause = new Thread(() -> subscriptionModel.pauseSubscription("chained"));
        pause.start();
        Subscription third;
        CopyOnWriteArrayList<String> deliveredToTheThird = new CopyOnWriteArrayList<>();
        try {
            assertThat(cancelStarted.await(10, SECONDS)).isTrue();
            // The second run waits for the first, and is cancelled before it has read anything
            subscriptionModel.resumeSubscription("chained");
            subscriptionModel.cancelSubscription("chained");
            int askedWhenTheThirdWasStarted = asked.get();

            // When
            third = subscriptionModel.subscribe("chained", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> deliveredToTheThird.add(event.getId())));

            // Then
            assertThat(third.waitUntilStarted(Duration.ofSeconds(3)).block()).as("the third run started while the first run is being cancelled").isFalse();
            assertThat(commands.changeStreamsOpened()).as("change streams opened while the first run is being cancelled").hasSize(1);
            assertThat(asked).as("times the listener was asked while the first run is being cancelled").hasValue(askedWhenTheThirdWasStarted);
        } finally {
            cancelMayFinish.countDown();
            pause.join(10_000);
        }
        waitUntilStarted(third);
        NameDefined matched = nameDefined();
        write(matched);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(deliveredToTheThird).containsExactly(matched.eventId()));
    }

    @Test
    void pausing_a_subscription_kills_its_cursor_on_the_server_in_the_session_that_opened_it() {
        // Given
        waitUntilStarted(subscriptionModel.subscribe("killed", StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> commands.changeStreamsOpened().size() == 1 && commands.changeStreamsOpened().getFirst().reply() != null);
        CommandLog.Sent opened = commands.changeStreamsOpened().getFirst();

        // When
        subscriptionModel.pauseSubscription("killed");

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.named("killCursors")).as("killCursors sent for cursor %s", opened.cursorId()).anySatisfy(killCursors -> {
            assertThat(killCursors.command().getArray("cursors").stream().map(cursor -> cursor.asNumber().longValue())).containsExactly(opened.cursorId());
            assertThat(killCursors.lsid()).as("session of killCursors").isEqualTo(opened.lsid());
        }));
    }

    @Test
    void every_get_more_of_a_subscription_is_sent_in_the_session_of_the_aggregate_that_opened_the_cursor_also_while_other_sessions_are_busy() {
        // Given: 20 slow reads on the same client, each holding a session of its own
        write(nameDefined());
        MongoDatabase database = subscriberClient.getDatabase(databaseName);
        Document slowRead = new Document("find", "events").append("filter", new Document("$where", "sleep(100) || true"));
        disposables.add(Flux.range(0, 20)
                .flatMap(__ -> Mono.defer(() -> Mono.from(database.runCommand(slowRead))).repeat(60), 20)
                .subscribe(__ -> {
                }, __ -> {
                }));
        waitUntilStarted(subscriptionModel.subscribe("same-session", StartAt.now(), __ -> Mono.empty()));

        // When
        await().atMost(20, SECONDS).until(() -> commands.named("getMore").size() >= 5);

        // Then
        assertThat(commands.named("getMore")).allSatisfy(getMore -> assertThat(getMore.lsid()).as("session of getMore for cursor %s", getMore.command().get("getMore")).isEqualTo(sessionOfTheAggregateThatOpened(getMore)));
    }

    private BsonValue sessionOfTheAggregateThatOpened(CommandLog.Sent getMore) {
        long cursorId = getMore.command().getInt64("getMore").getValue();
        return commands.changeStreamsOpened().stream()
                .filter(aggregate -> aggregate.reply() != null && aggregate.cursorId() == cursorId)
                .findFirst()
                .map(CommandLog.Sent::lsid)
                .orElseThrow(() -> new AssertionError("No aggregate opened cursor " + cursorId));
    }

    // A resume token's data is the hex of an ordered key that starts with the operation time, so those characters tell which of two positions is later
    private static boolean isAtOrAfter(Checkpoint position, Checkpoint other) {
        return operationTimeOf(position).compareTo(operationTimeOf(other)) >= 0;
    }

    private static String operationTimeOf(Checkpoint checkpoint) {
        String data = resumeTokenOf(checkpoint).getString("_data").getValue();
        assertThat(data).as("data of resume token %s", checkpoint).startsWith("82");
        return data.substring(0, 18);
    }

    private static List<String> signalsAfter(List<String> signals, String signal) {
        List<String> copy = List.copyOf(signals);
        int index = copy.indexOf(signal);
        return index < 0 ? List.of() : copy.subList(index + 1, copy.size());
    }

    private static Function<Checkpoint, Mono<Void>> collectingInto(List<Checkpoint> quietPositions) {
        return quietPosition -> Mono.fromRunnable(() -> quietPositions.add(quietPosition));
    }

    private static BsonDocument resumeTokenOf(Checkpoint checkpoint) {
        return ((MongoResumeTokenCheckpoint) checkpoint).resumeToken;
    }

    private static void waitUntilStarted(Subscription subscription) {
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10)).block()).as("subscription %s started", subscription.id()).isTrue();
    }

    private static void settle() {
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
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
