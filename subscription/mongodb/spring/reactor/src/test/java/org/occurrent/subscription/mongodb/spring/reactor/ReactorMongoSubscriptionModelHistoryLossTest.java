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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
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
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.HistoryLostException;
import org.occurrent.subscription.api.reactor.Subscription;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.functional.CheckedFunction.unchecked;
import static org.occurrent.time.TimeConversion.toLocalDateTime;

/**
 * MongoDB answers error 286 when the oplog no longer reaches back to the position a subscription opens at.
 * A fail point makes the subscription model client answer it once.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelHistoryLossTest {

    private static final String SUBSCRIBER_APPLICATION_NAME = "history-loss-subscriber";
    private static final int HISTORY_LOST = 286;

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ObjectMapper objectMapper = new ObjectMapper();
    // Writes the events and is never failed
    private MongoClient mongoClient;
    // Used by the subscription model and the only client the fail point fails
    private MongoClient subscriberClient;
    private String databaseName;
    private String eventCollection;
    private CommandLog commands;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createClients() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivehistoryloss");
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        eventCollection = "events-" + UUID.randomUUID();
        commands = new CommandLog();
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(SUBSCRIBER_APPLICATION_NAME).addCommandListener(commands).build());
        ReactiveTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName(eventCollection).transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(mongoClient, databaseName), eventStoreConfig);
    }

    @AfterEach
    void shutdown() {
        if (subscriptionModel != null) {
            subscriptionModel.shutdown();
        }
        FailPoint.off(mongoClient);
        subscriberClient.close();
        mongoClient.close();
    }

    @Test
    void a_flux_that_fails_if_its_start_is_lost_fails_with_the_start_when_the_model_restarts_lost_subscriptions() {
        verifiesTheFluxFailsWithTheStart(true);
    }

    @Test
    void a_flux_that_fails_if_its_start_is_lost_fails_with_the_start_when_the_model_ends_lost_subscriptions() {
        verifiesTheFluxFailsWithTheStart(false);
    }

    private void verifiesTheFluxFailsWithTheStart(boolean restartLostSubscriptions) {
        subscriptionModel = modelOver(restartLostSubscriptions);
        Checkpoint start = subscriptionModel.globalCheckpoint().block(Duration.ofSeconds(10));
        NameDefined written = nameDefined();
        write(written);

        StepVerifier.create(subscriptionModel.subscribeFailingIfStartLost(null, StartAt.checkpoint(start)).map(CloudEvent::getId).take(1))
                .expectNext(written.eventId())
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", HISTORY_LOST));
        // Bounded, since a model that restarts the Flux from the present never ends it
        StepVerifier.create(subscriptionModel.subscribeFailingIfStartLost(null, StartAt.checkpoint(start)).take(Duration.ofSeconds(3)))
                .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOfSatisfying(HistoryLostException.class,
                        lost -> assertThat(lost.lostFrom().asString()).isEqualTo(requireNonNull(start).asString())))
                .verify(Duration.ofSeconds(15));
    }

    @Test
    void a_flux_that_fails_on_history_loss_fails_with_the_start_when_the_oplog_lost_it_and_never_opens_a_second_change_stream_when_the_model_restarts_lost_subscriptions() {
        verifiesTheFluxThatFailsOnHistoryLossFailsWithTheStart(true);
    }

    @Test
    void a_flux_that_fails_on_history_loss_fails_with_the_start_when_the_oplog_lost_it_and_never_opens_a_second_change_stream_when_the_model_ends_lost_subscriptions() {
        verifiesTheFluxThatFailsOnHistoryLossFailsWithTheStart(false);
    }

    private void verifiesTheFluxThatFailsOnHistoryLossFailsWithTheStart(boolean restartLostSubscriptions) {
        subscriptionModel = modelOver(restartLostSubscriptions);
        Checkpoint start = requireNonNull(subscriptionModel.globalCheckpoint().block(Duration.ofSeconds(10)));
        NameDefined written = nameDefined();
        write(written);
        StepVerifier.create(subscriptionModel.subscribeFailingOnHistoryLoss(null, StartAt.checkpoint(start)).map(CloudEvent::getId).take(1))
                .expectNext(written.eventId())
                .expectComplete()
                .verify(Duration.ofSeconds(10));
        int openedBefore = commands.changeStreamsOpened().size();
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", HISTORY_LOST));

        StepVerifier.create(subscriptionModel.subscribeFailingOnHistoryLoss(null, StartAt.checkpoint(start)))
                .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOfSatisfying(HistoryLostException.class,
                        lost -> assertThat(lost.lostFrom().asString()).isEqualTo(start.asString())))
                .verify(Duration.ofSeconds(15));

        assertThat(commands.changeStreamsOpened()).as("change streams opened by the Flux that lost its start").hasSize(openedBefore + 1);
    }

    @Test
    void a_flux_that_fails_on_history_loss_fails_when_the_oplog_loses_the_history_after_it_read_and_does_not_start_again_from_the_present_when_the_model_restarts_lost_subscriptions() {
        verifiesTheFluxThatFailsOnHistoryLossFailsAfterReading(true);
    }

    @Test
    void a_flux_that_fails_on_history_loss_fails_when_the_oplog_loses_the_history_after_it_read_and_does_not_start_again_from_the_present_when_the_model_ends_lost_subscriptions() {
        verifiesTheFluxThatFailsOnHistoryLossFailsAfterReading(false);
    }

    private void verifiesTheFluxThatFailsOnHistoryLossFailsAfterReading(boolean restartLostSubscriptions) {
        subscriptionModel = modelOver(restartLostSubscriptions);
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribeFailingOnHistoryLoss(null, StartAt.now()).subscribe(event -> delivered.add(event.getId()), failures::add);
        await().atMost(10, SECONDS).until(() -> commands.changeStreamsOpened().size() == 1);
        NameDefined first = nameDefined();
        write(first);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(first.eventId()));

        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("errorCode", HISTORY_LOST));

        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(failures).hasSize(1));
        NameDefined afterTheLoss = nameDefined();
        write(afterTheLoss);
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(6)).untilAsserted(() -> {
            assertThat(failures.getFirst()).isInstanceOfSatisfying(HistoryLostException.class, lost -> assertThat(lost.lostFrom().asString()).as("position it had read to").isNotBlank());
            assertThat(commands.changeStreamsOpened()).as("change streams opened, so none from the present").hasSize(1);
            assertThat(delivered).as("delivered").containsExactly(first.eventId());
        });
    }

    @Test
    void a_flux_that_fails_on_history_loss_and_lost_its_history_before_it_read_anything_from_now_fails_with_the_position_it_opened_at() {
        subscriptionModel = modelOver(true);
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", HISTORY_LOST));

        StepVerifier.create(subscriptionModel.subscribeFailingOnHistoryLoss(null, StartAt.now()))
                .expectErrorSatisfies(throwable -> assertThat(throwable).isInstanceOfSatisfying(HistoryLostException.class, lost -> {
                    CommandLog.Sent opened = commands.changeStreamsOpened().getFirst();
                    assertThat(BsonDocument.parse(lost.lostFrom().asString()).getTimestamp("operationTime"))
                            .as("the operation time of the lost position, and the one the change stream opened at")
                            .isEqualTo(opened.changeStreamStage().getTimestamp("startAtOperationTime"));
                }))
                .verify(Duration.ofSeconds(15));

        assertThat(commands.changeStreamsOpened()).as("change streams opened").hasSize(1);
    }

    @Test
    void a_paused_subscription_resumed_at_a_position_continues_from_there_and_not_from_the_position_it_had_read_to() {
        subscriptionModel = modelOver(false);
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("sub", null, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        NameDefined first = nameDefined();
        write(first);
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(first.eventId()));
        subscriptionModel.pauseSubscription("sub");
        NameDefined skipped = nameDefined();
        write(skipped);
        Checkpoint afterSkipped = subscriptionModel.globalCheckpoint().block(Duration.ofSeconds(10));
        NameDefined resumed = nameDefined();
        write(resumed);

        subscriptionModel.resumeSubscription("sub", StartAt.checkpoint(afterSkipped));

        await().atMost(10, SECONDS).until(() -> delivered.contains(resumed.eventId()));
        assertThat(delivered).as("the event written while it was paused and before the position it resumed at is not delivered").containsExactly(first.eventId(), resumed.eventId());
        assertThat(subscriptionModel.isRunning("sub")).isTrue();
        assertThat(subscriptionModel.isPaused("sub")).isFalse();
    }

    @Test
    void a_listener_that_takes_a_subscription_over_when_its_start_is_lost_has_it_paused_and_its_task_run_instead_of_the_model_restarting_it() {
        verifiesTheListenerTakesTheSubscriptionOverWhenTheHistoryFromItsStartIsLost(StartAt::checkpoint);
    }

    @Test
    void a_listener_is_asked_about_the_position_a_dynamic_start_answered_when_the_history_from_it_is_lost() {
        verifiesTheListenerTakesTheSubscriptionOverWhenTheHistoryFromItsStartIsLost(start -> StartAt.dynamic(() -> StartAt.checkpoint(start)));
    }

    private void verifiesTheListenerTakesTheSubscriptionOverWhenTheHistoryFromItsStartIsLost(Function<Checkpoint, StartAt> startAtFrom) {
        subscriptionModel = modelOver(true);
        Checkpoint start = subscriptionModel.globalCheckpoint().block(Duration.ofSeconds(10));
        write(nameDefined());
        CopyOnWriteArrayList<String> asked = new CopyOnWriteArrayList<>();
        AtomicBoolean pausedWhenTheTaskRan = new AtomicBoolean();
        AtomicBoolean taskRan = new AtomicBoolean();
        subscriptionModel.addHistoryLossListener((subscriptionId, lostFrom) -> {
            asked.add(subscriptionId + "@" + lostFrom.asString());
            return HistoryLossHandling.takeOver(() -> {
                pausedWhenTheTaskRan.set(subscriptionModel.isPaused("sub"));
                taskRan.set(true);
            });
        });
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "aggregate", new Document("errorCode", HISTORY_LOST));

        subscriptionModel.subscribe("sub", null, startAtFrom.apply(requireNonNull(start)), event -> Mono.empty());

        // A model that restarts the subscription opens a second change stream instead
        await().atMost(10, SECONDS).until(() -> taskRan.get() || commands.changeStreamsOpened().size() > 1);
        assertThat(taskRan).as("the task of the listener ran").isTrue();
        assertThat(asked).containsExactly("sub@" + requireNonNull(start).asString());
        assertThat(pausedWhenTheTaskRan).as("paused when the task ran").isTrue();
        assertThat(subscriptionModel.isPaused("sub")).isTrue();
        assertThat(subscriptionModel.isRunning("sub")).isFalse();
        assertThat(commands.changeStreamsOpened()).as("the subscription is not read again").hasSize(1);
    }

    private ReactorMongoSubscriptionModel modelOver(boolean restartLostSubscriptions) {
        return new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, databaseName), eventCollection, TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)).restartSubscriptionsOnChangeStreamHistoryLost(restartLostSubscriptions));
    }

    private static void waitUntilStarted(Subscription subscription) {
        assertThat(subscription.waitUntilStarted(Duration.ofSeconds(10)).block()).as("subscription %s started", subscription.id()).isTrue();
    }

    private NameDefined nameDefined() {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "name1");
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
