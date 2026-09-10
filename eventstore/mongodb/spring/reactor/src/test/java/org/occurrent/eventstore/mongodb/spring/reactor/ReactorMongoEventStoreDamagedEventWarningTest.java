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


package org.occurrent.eventstore.mongodb.spring.reactor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoClient;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * The startup check for events that {@code updateEvent} damaged before 0.34.0. A damaged event is missing from every
 * position query, so this check is the only thing that tells anyone it is there, and
 * {@code requireRepairedEvents(true)} turns its warning into a refusal to start.
 * <p>
 * This store builds its startup work as a chain of {@code Mono}s, where an unsubscribed step does nothing at all and
 * fails silently rather than loudly, so the warning firing here is worth checking on its own and not only on the
 * blocking stores. The healthy case matters as much as the damaged one, since this warning stays in the store for as
 * long as anyone might still be upgrading across the defect.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDamagedEventWarningTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;
    private String databaseName;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger storeLogger;

    @BeforeEach
    void create_template_and_capture_the_store_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".damagedreactor");
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = com.mongodb.reactivestreams.client.MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));

        logAppender = new ListAppender<>();
        logAppender.start();
        storeLogger = (Logger) LoggerFactory.getLogger(ReactorMongoEventStore.class);
        storeLogger.addAppender(logAppender);
    }

    @org.junit.jupiter.api.AfterEach
    void close_mongo_client_and_release_the_log() {
        storeLogger.detachAppender(logAppender);
        logAppender.stop();
        mongoClient.close();
    }

    @Test
    void a_store_warns_when_it_starts_on_a_collection_holding_an_event_with_a_string_position() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        makePositionAString();

        newEventStore();

        assertThat(warnings())
                .as("a store must say so when it starts on damaged events, since nothing else will")
                .anySatisfy(message -> assertThat(message).contains("updateEvent damaged", "update-event-repair"));
    }

    @Test
    void a_store_says_nothing_when_every_position_is_a_number() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();

        logAppender.list.clear();
        newEventStore();

        assertThat(warnings())
                .as("a healthy store must not be warned about damage it does not have")
                .noneSatisfy(message -> assertThat(message).contains("updateEvent damaged"));
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_to_start_until_the_damage_is_gone() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        makePositionAString();

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("an operator who asked for this must not get a store that accepts a conditional append against a damaged event")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged")
                .hasMessageContaining("update-event-repair.md");

        makePositionANumberAgain();

        assertThatNoException()
                .as("the same setting must let a repaired store start, otherwise it refuses on the setting rather than on the damage")
                .isThrownBy(this::newStoreRequiringRepairedEvents);
    }

    private ReactorMongoEventStore newStoreRequiringRepairedEvents() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .requireRepairedEvents(true)
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    private void makePositionANumberAgain() {
        try (com.mongodb.client.MongoClient blockingClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl())) {
            MongoCollection<Document> events = blockingClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
            Document damaged = requireNonNull(events.find(
                    new Document(OccurrentCloudEventExtension.POSITION, new Document("$type", "string"))).first());
            long position = Long.parseLong(requireNonNull(damaged.getString(OccurrentCloudEventExtension.POSITION)));
            events.updateOne(new Document("_id", damaged.get("_id")),
                    new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, position)));
        }
    }

    private void makePositionAString() {
        try (com.mongodb.client.MongoClient blockingClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl())) {
            MongoCollection<Document> events = blockingClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
            Document stored = requireNonNull(events.find().first());
            long position = requireNonNull(stored.getLong(OccurrentCloudEventExtension.POSITION));
            events.updateOne(new Document("_id", stored.get("_id")),
                    new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, String.valueOf(position))));
        }
    }

    private List<String> warnings() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    private ReactorMongoEventStore newEventStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
