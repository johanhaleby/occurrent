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
        // Positions reported once the event that did not match has been read
        int reportedBeforeTheRead = quietPositions.size();
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedBeforeTheRead + 2);
        Checkpoint quietPosition = quietPositions.getLast();

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
        subscriptionModel.addQuietPositionListener(subscriptionId -> Mono.just(collectingInto(quietPositions)));
        waitUntilStarted(subscriptionModel.subscribe("quiet", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> !quietPositions.isEmpty());
        Checkpoint positionBeforeTheEvents = quietPositions.getLast();
        write(nameWasChanged());
        write(nameWasChanged());
        int reportedBeforeTheEventsWereRead = quietPositions.size();
        await().atMost(10, SECONDS).until(() -> quietPositions.size() > reportedBeforeTheEventsWereRead + 2);

        // When
        subscriptionModel.pauseSubscription("quiet");
        // A position that was being handed over when the subscription was paused
        settle();
        Checkpoint quietPosition = quietPositions.getLast();
        waitUntilStarted(subscriptionModel.resumeSubscription("quiet"));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));
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
        waitUntilStarted(subscriptionModel.subscribe("slow-to-cancel", StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> signals.contains("quiet position being handled"));

        // When: the pause is slow, since it waits for the quiet position to be cancelled, so the subscription is resumed while it is still going on
        Thread pause = new Thread(() -> subscriptionModel.pauseSubscription("slow-to-cancel"));
        pause.start();
        try {
            await().atMost(10, SECONDS).until(() -> signals.contains("cancel started"));
            subscriptionModel.resumeSubscription("slow-to-cancel");

            // Then
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(signals).containsExactly("asked before reading", "quiet position being handled", "cancel started"));
        } finally {
            cancelMayFinish.countDown();
            pause.join(10_000);
        }
        await().atMost(10, SECONDS).until(() -> signals.size() > 4);
        assertThat(signals.subList(0, 5)).containsExactly("asked before reading", "quiet position being handled", "cancel started", "cancel finished", "asked before reading");
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

    @Test
    void a_plain_subscription_that_restarts_after_quiet_reads_opens_at_the_position_of_the_last_one() {
        // Given
        disposables.add(subscriptionModel.subscribe(NAME_DEFINED_ONLY, StartAt.now()).subscribe());
        await().atMost(10, SECONDS).until(() -> commands.named("getMore").stream().filter(getMore -> getMore.reply() != null).count() >= 2);

        // When
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("closeConnection", true));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));
        CommandLog.Sent restarted = commands.changeStreamsOpened().getLast();
        assertThat(restarted.changeStreamStage().containsKey("startAtOperationTime")).as("the restarted change stream opens at the operation time the subscription started at").isFalse();
        assertThat(CommandLog.changeStreamField(restarted, "startAfter")).as("the position the restarted change stream opens at").isEqualTo(postBatchResumeTokenOfTheLastReadBefore(restarted));
    }

    @Test
    void waiting_until_a_subscription_has_started_waits_for_the_change_stream_to_be_opened_on_the_server() {
        // Given
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("blockConnection", true).append("blockTimeMS", 2000));

        // When
        Subscription subscription = subscriptionModel.subscribe("blocked", __ -> Mono.empty());

        // Then
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(1)).block()).as("started while the server still holds the aggregate").isFalse();
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10)).block()).as("started once the server has answered the aggregate").isTrue();
    }

    private BsonValue sessionOfTheAggregateThatOpened(CommandLog.Sent getMore) {
        long cursorId = getMore.command().getInt64("getMore").getValue();
        return commands.changeStreamsOpened().stream()
                .filter(aggregate -> aggregate.reply() != null && aggregate.cursorId() == cursorId)
                .findFirst()
                .map(CommandLog.Sent::lsid)
                .orElseThrow(() -> new AssertionError("No aggregate opened cursor " + cursorId));
    }

    private BsonDocument postBatchResumeTokenOfTheLastReadBefore(CommandLog.Sent restarted) {
        BsonDocument postBatchResumeToken = null;
        for (CommandLog.Sent command : commands.all()) {
            if (command == restarted) {
                break;
            }
            if ((command.isChangeStream() || command.name().equals("getMore")) && command.reply() != null) {
                postBatchResumeToken = command.reply().getDocument("cursor").getDocument("postBatchResumeToken");
            }
        }
        return requireNonNull(postBatchResumeToken, "No read answered before the change stream was opened again");
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
