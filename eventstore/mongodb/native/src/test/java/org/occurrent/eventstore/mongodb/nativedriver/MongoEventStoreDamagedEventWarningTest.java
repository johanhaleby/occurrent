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


package org.occurrent.eventstore.mongodb.nativedriver;

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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * The startup warning about events that {@code updateEvent} damaged before 0.34.0. A damaged event is missing from
 * every position query, so the warning is the only thing that tells anyone it is there.
 * <p>
 * The healthy case matters as much as the damaged one. This warning stays in the store for as long as anyone might
 * still be upgrading across the defect, so a version that cried wolf would put a scary line in the log of every
 * store that was never damaged, on every startup, forever.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDamagedEventWarningTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String databaseName;
    private ListAppender<ILoggingEvent> logAppender;
    private Logger storeLogger;

    @BeforeEach
    void create_mongo_client_and_capture_the_store_log() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".damaged");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());

        logAppender = new ListAppender<>();
        logAppender.start();
        storeLogger = (Logger) LoggerFactory.getLogger(MongoEventStore.class);
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
    void a_store_that_refuses_to_start_over_unpositioned_events_still_warns_about_the_damage_first() {
        newEventStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        makePositionAString();
        // An update function returning an event built from scratch stored no position at all, so this event reaches
        // the unpositioned check looking like history that predates position. Backfilling it would give it a
        // position it never had, so the damage warning has to be out before that check refuses to start.
        dropPositionFromTheSecondEvent();

        logAppender.list.clear();
        assertThatThrownBy(this::newStoreRequiringBackfilledPosition)
                .as("the unpositioned check must still fail startup when it is configured to")
                .isInstanceOf(IllegalStateException.class);

        assertThat(warnings())
                .as("the damage warning must be logged before startup is refused, otherwise the only message an operator sees points at the position backfill")
                .anySatisfy(message -> assertThat(message).contains("updateEvent damaged", "update-event-repair"));
    }

    private MongoEventStore newStoreRequiringBackfilledPosition() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .requireBackfilledPosition(true)
                .build();
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private void dropPositionFromTheSecondEvent() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        Document withNumericPosition = requireNonNull(events.find(
                new Document(OccurrentCloudEventExtension.POSITION, new Document("$type", "long"))).first());
        events.updateOne(new Document("_id", withNumericPosition.get("_id")),
                new Document("$unset", new Document(OccurrentCloudEventExtension.POSITION, "")));
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

    private MongoEventStore newEventStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .withStreamPosition()
                .build();
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
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
