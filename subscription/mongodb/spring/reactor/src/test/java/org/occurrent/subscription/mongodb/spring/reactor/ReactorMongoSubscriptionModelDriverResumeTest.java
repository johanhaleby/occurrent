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
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
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
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Subscription;
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

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

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
 * A network error on the connection a subscription's change stream is read over. The MongoDB driver opens the change
 * stream again by itself after such an error, so the subscription model has nothing to restart.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoSubscriptionModelDriverResumeTest {

    private static final String SUBSCRIBER_APPLICATION_NAME = "driver-resume-subscriber";
    private static final SubscriptionFilter NAME_DEFINED_ONLY = AgnosticSubscriptionFilter.filter(type(NameDefined.class.getName()));

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
    private CommandLog commands;
    private ReactorMongoEventStore mongoEventStore;
    private ReactorMongoSubscriptionModel subscriptionModel;

    @BeforeEach
    void createSubscriptionModel() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".reactivedriverresume");
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        commands = new CommandLog();
        subscriberClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(SUBSCRIBER_APPLICATION_NAME).addCommandListener(commands).build());
        subscriptionModel = new ReactorMongoSubscriptionModel(new ReactiveMongoTemplate(subscriberClient, databaseName), "events", TimeRepresentation.RFC_3339_STRING,
                ReactorMongoSubscriptionModelConfig.withConfig().backoff(Duration.of(20, MILLIS), Duration.of(200, MILLIS)));
        ReactiveTransactionManager transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder().eventStoreCollectionName("events").transactionConfig(transactionManager).timeRepresentation(TimeRepresentation.RFC_3339_STRING).build();
        mongoEventStore = new ReactorMongoEventStore(new ReactiveMongoTemplate(mongoClient, databaseName), eventStoreConfig);
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
        subscriberClient.close();
        mongoClient.close();
    }

    @Test
    void the_driver_opens_the_change_stream_again_after_the_connection_it_is_read_over_is_closed() {
        // Given
        waitUntilStarted(subscriptionModel.subscribe("resumed", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> commands.named("getMore").stream().anyMatch(getMore -> getMore.reply() != null));

        // When
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("closeConnection", true));

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));
        CommandLog.Sent opened = commands.changeStreamsOpened().getLast();
        assertThat(opened.changeStreamStage().containsKey("resumeAfter")).as("the change stream opens at the resume token the driver holds").isTrue();
    }

    @Test
    void the_subscription_model_does_not_restart_the_subscription_when_the_driver_opens_the_change_stream_again() {
        // Given
        waitUntilStarted(subscriptionModel.subscribe("resumed", NAME_DEFINED_ONLY, StartAt.now(), __ -> Mono.empty()));
        await().atMost(10, SECONDS).until(() -> commands.named("getMore").stream().anyMatch(getMore -> getMore.reply() != null));

        // When
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("closeConnection", true));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));

        // Then
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(modelLog.list).as("what the model logged about subscription resumed").noneMatch(event -> event.getFormattedMessage().contains("subscription resumed") && event.getFormattedMessage().contains("Will restart!")));
        assertThat(commands.changeStreamsOpened()).as("change streams opened").hasSize(2);
    }

    @Test
    void an_event_written_after_the_driver_opened_the_change_stream_again_is_delivered_exactly_once() {
        // Given
        CopyOnWriteArrayList<String> delivered = new CopyOnWriteArrayList<>();
        waitUntilStarted(subscriptionModel.subscribe("resumed", NAME_DEFINED_ONLY, StartAt.now(), event -> Mono.fromRunnable(() -> delivered.add(event.getId()))));
        await().atMost(10, SECONDS).until(() -> commands.named("getMore").stream().anyMatch(getMore -> getMore.reply() != null));
        FailPoint.failNext(mongoClient, SUBSCRIBER_APPLICATION_NAME, "getMore", new Document("closeConnection", true));
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(commands.changeStreamsOpened()).hasSize(2));
        await().atMost(10, SECONDS).until(() -> commands.changeStreamsOpened().getLast().reply() != null);

        // When
        NameDefined matched = nameDefined();
        write(matched);

        // Then
        await().atMost(10, SECONDS).untilAsserted(() -> assertThat(delivered).containsExactly(matched.eventId()));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(8)).untilAsserted(() -> assertThat(delivered).as("events delivered").containsExactly(matched.eventId()));
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
