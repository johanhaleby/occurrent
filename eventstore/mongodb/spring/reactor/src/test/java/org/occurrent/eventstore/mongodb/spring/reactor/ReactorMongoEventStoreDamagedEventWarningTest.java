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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
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
    private static final Tag TAG = Tag.parse("name:1");

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

    @Test
    void a_store_told_to_require_repaired_events_refuses_a_dcb_event_that_lost_its_position_even_once_its_tag_index_is_rebuilt() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        // What the old write-back left when an update function returned a DCB event built from scratch. No string
        // position is involved, so only a check that looks at the tag index can see it.
        loseTheTagIndex();
        dropTheOldestEventsPosition();

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("a DCB event without its tag index is missing from the conflict query whatever its position is")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");

        // The state the repair writes for a POSITION_LOST event
        rebuildTheTagIndex();

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("a DCB event without a position is still missing from DCB reads and the conflict query, so the store must keep refusing")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_a_dcb_event_that_lost_its_tag_index_but_has_a_numeric_position() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        // An operator who sets a POSITION_ALREADY_TAKEN event's position by hand produces this, until the second
        // repair run rebuilds its tag index.
        loseTheTagIndex();

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("a numeric position does not make a DCB event without its tag index visible to the conflict query")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");

        rebuildTheTagIndex();

        assertThatNoException()
                .as("the same event with its tag index rebuilt is repaired, and that collection must start")
                .isThrownBy(this::newStoreRequiringRepairedEvents);
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_a_dcb_event_whose_position_is_null() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        loseTheTagIndex();
        rebuildTheTagIndex();
        setThePosition(null);

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("a null position keeps a DCB event out of DCB reads exactly as a missing one does")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_an_event_whose_position_is_null() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        setThePosition(null);

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("a null position is a field that holds something other than a number, which position reads skip")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_a_dcb_event_whose_position_is_above_the_counter() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        loseTheTagIndex();
        rebuildTheTagIndex();

        assertThatNoException()
                .as("a DCB event at the counter is one the store assigned")
                .isThrownBy(this::newStoreRequiringRepairedEvents);

        setThePosition(counter() + 1);

        assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                .as("DCB reads and reads in position order stop at the counter, so they skip a position above it")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_even_when_it_writes_no_position() {
        newEventStore().write("stream:1", Flux.just(event("Defined"))).block();
        makePositionAString();

        assertThatThrownBy(() -> newEventStore(builder -> builder.withoutStreamPosition().requireRepairedEvents(true)))
                .as("withoutStreamPosition() says the store wants no global position, not that the damage stopped mattering, so the refusal has to hold there too")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    @Test
    void a_store_whose_position_is_turned_off_over_unpositioned_history_still_refuses_when_it_was_told_to() {
        newEventStore().write("stream:1", Flux.just(event("Defined"), event("Renamed"))).block();
        makeTheNewestEventsPositionAString();
        // The resolver reads the oldest event only, and turns position off when that one has no position. That is
        // the store ADR 136 says reaches neither ordered check, so it is the one an operator hears nothing from.
        dropTheOldestEventsPosition();

        assertThatThrownBy(() -> newEventStore(builder -> builder.requireRepairedEvents(true)))
                .as("this store turns position off at startup and so runs neither ordered check, which makes it the one an operator hears nothing from unless the refusal reaches it")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updateEvent damaged");
    }

    private void makeTheNewestEventsPositionAString() {
        withEventCollection(events -> {
            Document newest = requireNonNull(events.find().sort(new Document("_id", -1)).first());
            long position = requireNonNull(newest.getLong(OccurrentCloudEventExtension.POSITION));
            events.updateOne(new Document("_id", newest.get("_id")),
                    new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, String.valueOf(position))));
        });
    }

    private void dropTheOldestEventsPosition() {
        withEventCollection(events -> {
            Document oldest = requireNonNull(events.find().sort(new Document("_id", 1)).first());
            events.updateOne(new Document("_id", oldest.get("_id")),
                    new Document("$unset", new Document(OccurrentCloudEventExtension.POSITION, "")));
        });
    }

    private void withEventCollection(java.util.function.Consumer<MongoCollection<Document>> work) {
        try (com.mongodb.client.MongoClient blockingClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl())) {
            work.accept(blockingClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION));
        }
    }

    private ReactorMongoEventStore newEventStore(java.util.function.UnaryOperator<EventStoreConfig.Builder> customize) {
        EventStoreConfig config = customize.apply(new EventStoreConfig.Builder()
                        .eventStoreCollectionName(EVENT_COLLECTION)
                        .transactionConfig(transactionManager)
                        .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                        .eventStoreCapabilities(STREAM))
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
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

    // A DCB event that updateEvent rewrote kept its dcbtags extension and lost the dcbTags index derived from it.
    private void loseTheTagIndex() {
        withEventCollection(events -> events.updateOne(new Document(),
                new Document("$set", new Document(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(List.of(TAG))))));
    }

    private void rebuildTheTagIndex() {
        withEventCollection(events -> events.updateOne(new Document(),
                new Document("$set", new Document(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, List.of(TAG.canonical())))));
    }

    private void setThePosition(@Nullable Object position) {
        withEventCollection(events -> events.updateOne(new Document(),
                new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, position))));
    }

    private long counter() {
        try (com.mongodb.client.MongoClient blockingClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl())) {
            Document counter = requireNonNull(blockingClient.getDatabase(databaseName)
                    .getCollection(DcbMarkerModel.positionCollectionName(EVENT_COLLECTION))
                    .find(new Document("_id", DcbMarkerModel.POSITION_DOCUMENT_ID)).first());
            // Spring increments by an int, so the counter is not always an int64
            return ((Number) requireNonNull(counter.get(DcbMarkerModel.COUNTER_POSITION))).longValue();
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
