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
package org.occurrent.subscription.blocking.durable.catchup;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModelConfig;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * A catch-up that is cancelled while the replay thread waits out a MongoDB outage inside the retry of canResumeFrom
 * stops that retry, and the warnings it logged for each failed attempt stop. The outage is a {@code failCommand} fail
 * point that closes the connection of every {@code aggregate} the live model sends.
 */
@Testcontainers
@Timeout(120)
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupCancelDuringOutageMongoTest {

    private static final String SUBSCRIPTION_APP = "catchup-cancel-during-outage-subscription";
    private static final String SUBSCRIPTION_ID = "subscription";

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final ListAppender<ILoggingEvent> modelLogged = new ListAppender<>();
    private final ListAppender<ILoggingEvent> catchupLogged = new ListAppender<>();
    private MongoClient client;
    private MongoClient subscriptionClient;
    private ExecutorService executor;
    private NativeMongoSubscriptionModel liveModel;
    private DurableSubscriptionModel durable;
    private CatchupSubscriptionModel catchup;

    @AfterEach
    void shutdown() {
        if (client != null) {
            failPointOff();
        }
        if (catchup != null) {
            catchup.shutdown();
        }
        if (durable != null) {
            durable.shutdown();
        }
        if (liveModel != null) {
            liveModel.shutdown();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        ((Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class)).detachAppender(modelLogged);
        ((Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class)).detachAppender(catchupLogged);
        if (subscriptionClient != null) {
            subscriptionClient.close();
        }
        if (client != null) {
            client.close();
        }
    }

    @Test
    void a_cancel_during_the_outage_ends_the_retry_of_canResumeFrom_and_the_warnings_stop() {
        // Given a catch-up whose replay has delivered its history and now asks whether the live start still holds
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        client = MongoClients.create(connectionString);
        subscriptionClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).applicationName(SUBSCRIPTION_APP).build());
        String database = requireNonNull(connectionString.getDatabase());
        SpringMongoEventStore eventStore = new SpringMongoEventStore(new MongoTemplate(client, database), new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(client, database)))
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM).withStreamPosition().build());
        CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();
        eventStore.write(UUID.randomUUID().toString(), WriteCondition.anyStreamVersion(),
                converter.toCloudEvents(List.of(new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", "event1"))));
        executor = Executors.newCachedThreadPool();
        liveModel = new NativeMongoSubscriptionModel(subscriptionClient.getDatabase(database), "events", TimeRepresentation.RFC_3339_STRING, executor,
                NativeMongoSubscriptionModelConfig.withConfig().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(100))));
        durable = new DurableSubscriptionModel(liveModel, new InMemoryCheckpointStorage());
        catchup = new CatchupSubscriptionModel(durable, eventStore, new CatchupSubscriptionModelConfig(100));
        captureLogs();
        closeTheConnectionOfEveryAggregateOfTheLiveModel();
        Subscription subscription = catchup.subscribe(SUBSCRIPTION_ID, StartAt.checkpoint(GlobalCheckpoint.of(0)), __ -> {
        });
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(attemptsLogged()).as("failed attempts logged while the outage lasts").isGreaterThanOrEqualTo(3));

        // When
        catchup.cancelSubscription(SUBSCRIPTION_ID);

        // Then the replay ends, and no attempt is logged after it
        boolean started = startedAccordingTo(subscription);
        int attemptsWhenTheReplayEnded = attemptsLogged();
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(attemptsLogged()).as("failed attempts logged after the replay ended").isEqualTo(attemptsWhenTheReplayEnded));
        assertThat(started).as("started").isFalse();
        assertThat(catchupLogged.list).as("lines logged at ERROR by the catch-up").noneMatch(event -> event.getLevel().isGreaterOrEqual(Level.ERROR));
    }

    private void captureLogs() {
        Logger modelLog = (Logger) LoggerFactory.getLogger(NativeMongoSubscriptionModel.class);
        modelLogged.start();
        modelLog.addAppender(modelLogged);
        Logger catchupLog = (Logger) LoggerFactory.getLogger(AbstractCatchupSubscriptionModel.class);
        catchupLogged.start();
        catchupLog.addAppender(catchupLogged);
    }

    private int attemptsLogged() {
        return (int) modelLogged.list.stream()
                .filter(event -> event.getLevel() == Level.WARN && event.getFormattedMessage().contains("can resume"))
                .count();
    }

    // A handle of a replay that ended on a failure either answers false or throws that failure, and neither is a start
    private static boolean startedAccordingTo(Subscription subscription) {
        try {
            return subscription.waitUntilStarted(Duration.ofSeconds(10));
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void closeTheConnectionOfEveryAggregateOfTheLiveModel() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                .append("data", new Document("failCommands", List.of("aggregate")).append("closeConnection", true).append("appName", SUBSCRIPTION_APP)));
    }

    private void failPointOff() {
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
    }
}
