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


package org.occurrent.eventstore.mongodb.spring.blocking;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.function.UnaryOperator;

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
 * The healthy case matters as much as the damaged one. This warning stays in the store for as long as anyone might
 * still be upgrading across the defect, so a version that cried wolf would put a scary line in the log of every
 * store that was never damaged, on every startup, forever.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDamagedEventWarningTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String databaseName;
    private MongoTemplate mongoTemplate;
    private MongoTransactionManager transactionManager;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger storeLogger;

    @BeforeEach
    void create_template_and_capture_the_store_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".damaged");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        transactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));

        logAppender = new ListAppender<>();
        logAppender.start();
        storeLogger = (Logger) LoggerFactory.getLogger(SpringMongoEventStore.class);
        storeLogger.addAppender(logAppender);
    }

    @AfterEach
    void close_mongo_client_and_release_the_log() {
        storeLogger.detachAppender(logAppender);
        logAppender.stop();
        mongoClient.close();
    }

    @Test
    void a_store_warns_when_it_starts_on_a_collection_holding_an_event_with_a_string_position() {
        newEventStore().write("stream:1", List.of(event("Defined")));
        makePositionAString();

        newEventStore();

        assertThat(warnings())
                .as("a store must say so when it starts on damaged events, since nothing else will")
                .anySatisfy(message -> assertThat(message).contains("updateEvent damaged", "update-event-repair"));
    }

    @Test
    void a_store_says_nothing_when_every_position_is_a_number() {
        newEventStore().write("stream:1", List.of(event("Defined")));

        logAppender.list.clear();
        newEventStore();

        assertThat(warnings())
                .as("a healthy store must not be warned about damage it does not have")
                .noneSatisfy(message -> assertThat(message).contains("updateEvent damaged"));
    }

    @Test
    void a_store_that_turns_position_off_over_an_unpositioned_event_still_points_at_the_repair() {
        newEventStore().write("stream:1", List.of(event("Defined")));
        dropThePosition();

        logAppender.list.clear();
        newEventStoreWithPositionOnByDefault();

        assertThat(warnings())
                .as("turning position off skips both the damage check and the un-backfilled checks, so this is the only line the operator gets, and naming the backfill alone recommends the one remedy that cannot be undone")
                .anySatisfy(message -> assertThat(message).contains("position-backfill", "update-event-repair.md"));
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_to_start_until_the_damage_is_gone() {
        newEventStore().write("stream:1", List.of(event("Defined")));
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

    @Test
    void a_store_told_to_require_repaired_events_refuses_even_when_it_writes_no_position() {
        newEventStore().write("stream:1", List.of(event("Defined")));
        makePositionAString();

        assertThatThrownBy(() -> newEventStore(builder -> builder.withoutStreamPosition().requireRepairedEvents(true)))
                .as("withoutStreamPosition() skips the damage check, so a store that writes no position would start on damage the operator asked to be refused over")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    @Test
    void a_store_whose_position_is_turned_off_over_unpositioned_history_still_refuses_when_it_was_told_to() {
        newEventStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        makeTheNewestEventsPositionAString();
        // The resolver reads the oldest event only, and turns position off when that one has no position. That is
        // the store ADR 136 says reaches neither ordered check, so it is the one an operator hears nothing from.
        dropThePosition();

        assertThatThrownBy(() -> newEventStore(builder -> builder.requireRepairedEvents(true)))
                .as("this is the store that turns position off at startup, so without the damage check it is the one that says nothing at all")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    private void makeTheNewestEventsPositionAString() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        Document newest = requireNonNull(events.find().sort(new Document("_id", -1)).first());
        long position = requireNonNull(newest.getLong(OccurrentCloudEventExtension.POSITION));
        events.updateOne(new Document("_id", newest.get("_id")),
                new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, String.valueOf(position))));
    }

    private SpringMongoEventStore newStoreRequiringRepairedEvents() {
        return newEventStore(builder -> builder.withStreamPosition().requireRepairedEvents(true));
    }

    private void makePositionANumberAgain() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        Document damaged = requireNonNull(events.find(
                new Document(OccurrentCloudEventExtension.POSITION, new Document("$type", "string"))).first());
        long position = Long.parseLong(requireNonNull(damaged.getString(OccurrentCloudEventExtension.POSITION)));
        events.updateOne(new Document("_id", damaged.get("_id")),
                new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, position)));
    }

    private void makePositionAString() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        Document stored = requireNonNull(events.find().first());
        long position = requireNonNull(stored.getLong(OccurrentCloudEventExtension.POSITION));
        events.updateOne(new Document("_id", stored.get("_id")),
                new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, String.valueOf(position))));
    }

    private List<String> warnings() {
        return logAppender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    // Only the position of the oldest event matters, since that is the whole of the probe the resolver runs. One
    // event whose position updateEvent dropped is enough to put an otherwise healthy store on that path.
    private void dropThePosition() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        Document oldest = requireNonNull(events.find().sort(new Document("_id", 1)).first());
        events.updateOne(new Document("_id", oldest.get("_id")),
                new Document("$unset", new Document(OccurrentCloudEventExtension.POSITION, "")));
    }

    private SpringMongoEventStore newEventStore() {
        return newEventStore(EventStoreConfig.Builder::withStreamPosition);
    }

    // No withStreamPosition() call, so position is on only by default and the resolver is free to turn it off.
    private SpringMongoEventStore newEventStoreWithPositionOnByDefault() {
        return newEventStore(builder -> builder);
    }

    private SpringMongoEventStore newEventStore(UnaryOperator<EventStoreConfig.Builder> streamPosition) {
        EventStoreConfig config = streamPosition.apply(new EventStoreConfig.Builder()
                        .eventStoreCollectionName(EVENT_COLLECTION)
                        .transactionConfig(transactionManager)
                        .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                        .eventStoreCapabilities(STREAM))
                .build();
        return new SpringMongoEventStore(mongoTemplate, config);
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
