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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.occurrent.testsupport.mongodb.StoredPositionShapes;
import org.occurrent.testsupport.mongodb.StoredPositionShapes.Kind;
import org.occurrent.testsupport.mongodb.StoredPositionShapes.Shape;
import org.occurrent.testsupport.mongodb.StoredCounterShapes;
import org.occurrent.testsupport.mongodb.StoredTagShapes;
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
    private static final Tag TAG = Tag.parse("name:1");

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
    void a_store_told_to_require_repaired_events_refuses_a_dcb_event_that_lost_its_position_even_once_its_tag_index_is_rebuilt() {
        newEventStore().write("stream:1", List.of(event("Defined")));
        // What the old write-back left when an update function returned a DCB event built from scratch. No string
        // position is involved, so only a check that looks at the tag index can see it.
        loseTheTagIndex();
        dropThePosition();

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
        newEventStore().write("stream:1", List.of(event("Defined")));
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

    @ParameterizedTest(name = "{0} on {1}")
    @MethodSource("org.occurrent.testsupport.mongodb.StoredPositionShapes#onADcbAndAPlainEvent")
    void a_store_told_to_require_repaired_events_starts_only_when_every_position_is_a_positive_integer_no_greater_than_the_counter(Shape shape, Kind kind) {
        newEventStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        assertThat(counter())
                .as("every case assumes this counter, so that a fraction below it is refused for its fraction alone")
                .isEqualTo(StoredPositionShapes.COUNTER);
        StoredPositionShapes.give(mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION), shape, kind, dcbFields());

        if (shape.startsOn(kind)) {
            assertThatNoException()
                    .as("a positive integer no greater than the counter is a position the store assigned, and a plain event without one predates position")
                    .isThrownBy(this::newStoreRequiringRepairedEvents);
        } else {
            assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                    .as("anything else is not a position the store assigned, and reads skip it, read it as another value or fail on it")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("updateEvent damaged");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("org.occurrent.testsupport.mongodb.StoredTagShapes#shapes")
    void a_store_told_to_require_repaired_events_starts_only_when_a_dcb_events_tag_index_holds_the_tags_it_lists(StoredTagShapes.Shape shape) {
        newEventStore().write("stream:1", List.of(event("Defined")));
        StoredTagShapes.give(mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION), shape);

        if (shape.starts()) {
            assertThatNoException()
                    .as("an index holding the tags dcbtags lists is what every append writes, and a plain event has neither")
                    .isThrownBy(this::newStoreRequiringRepairedEvents);
        } else {
            assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                    .as("DCB reads and the conflict query find an event by its index alone, so any other index hides it from them or shows it under the wrong tags")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("updateEvent damaged");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("org.occurrent.testsupport.mongodb.StoredCounterShapes#shapes")
    void a_store_told_to_require_repaired_events_starts_only_over_a_counter_a_writer_stores_and_no_position_above_it(StoredCounterShapes.Shape shape) {
        newEventStore().write("stream:1", List.of(event("Defined"), event("Renamed")));
        assertThat(counter()).isEqualTo(StoredPositionShapes.COUNTER);
        StoredCounterShapes.give(mongoClient.getDatabase(databaseName).getCollection(DcbMarkerModel.positionCollectionName(EVENT_COLLECTION)),
                DcbMarkerModel.POSITION_DOCUMENT_ID, DcbMarkerModel.COUNTER_POSITION, shape);

        if (shape.starts()) {
            assertThatNoException()
                    .as("every writer stores the counter as an int32 or int64, which $inc keeps exact, a missing one reads as zero, and DCB reads and reads in position order stop at it, so only such a counter covering every position is one the stores handle")
                    .isThrownBy(this::newStoreRequiringRepairedEvents);
        } else {
            assertThatThrownBy(this::newStoreRequiringRepairedEvents)
                    .as("every writer stores the counter as an int32 or int64, which $inc keeps exact, a missing one reads as zero, and DCB reads and reads in position order stop at it, so only such a counter covering every position is one the stores handle")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("updateEvent damaged")
                    .hasMessageContaining("position counter that is not an int32 or an int64 of 0 or more");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("org.occurrent.testsupport.mongodb.StoredCounterShapes#shapesOverUnpositionedEvents")
    void a_store_told_to_require_repaired_events_over_events_without_a_position_starts_only_over_a_counter_a_writer_stores(StoredCounterShapes.Shape shape) {
        newEventStore(builder -> builder.withoutStreamPosition().requireRepairedEvents(true)).write("stream:1", List.of(event("Defined"), event("Renamed")));
        StoredCounterShapes.give(mongoClient.getDatabase(databaseName).getCollection(DcbMarkerModel.positionCollectionName(EVENT_COLLECTION)),
                DcbMarkerModel.POSITION_DOCUMENT_ID, DcbMarkerModel.COUNTER_POSITION, shape);

        if (shape.starts()) {
            assertThatNoException()
                    .as("with no event positioned only the counter itself can be wrong, and a missing one reads as zero, so the store starts over no counter or an int32 or int64 at or above zero and refuses anything else")
                    .isThrownBy(() -> newEventStore(builder -> builder.withoutStreamPosition().requireRepairedEvents(true)));
        } else {
            assertThatThrownBy(() -> newEventStore(builder -> builder.withoutStreamPosition().requireRepairedEvents(true)))
                    .as("with no event positioned only the counter itself can be wrong, and a missing one reads as zero, so the store starts over no counter or an int32 or int64 at or above zero and refuses anything else")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("updateEvent damaged")
                    .hasMessageContaining("position counter that is not an int32 or an int64 of 0 or more");
        }
    }

    @Test
    void a_store_told_to_require_repaired_events_refuses_even_when_it_writes_no_position() {
        newEventStore().write("stream:1", List.of(event("Defined")));
        makePositionAString();

        assertThatThrownBy(() -> newEventStore(builder -> builder.withoutStreamPosition().requireRepairedEvents(true)))
                .as("withoutStreamPosition() says the store wants no global position, not that the damage stopped mattering, so the refusal has to hold there too")
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
                .as("this store turns position off at startup and so runs neither ordered check, which makes it the one an operator hears nothing from unless the refusal reaches it")
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

    // A DCB event that updateEvent rewrote kept its dcbtags extension and lost the dcbTags index derived from it.
    private void loseTheTagIndex() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        events.updateOne(new Document(), new Document("$set", new Document(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(List.of(TAG)))));
    }

    private void rebuildTheTagIndex() {
        MongoCollection<Document> events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        events.updateOne(new Document(), new Document("$set", new Document(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, List.of(TAG.canonical()))));
    }

    private static Document dcbFields() {
        return new Document(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(List.of(TAG)))
                .append(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, List.of(TAG.canonical()));
    }

    private long counter() {
        Document counter = requireNonNull(mongoClient.getDatabase(databaseName)
                .getCollection(DcbMarkerModel.positionCollectionName(EVENT_COLLECTION))
                .find(new Document("_id", DcbMarkerModel.POSITION_DOCUMENT_ID)).first());
        // Spring increments by an int, so the counter is not always an int64
        return ((Number) requireNonNull(counter.get(DcbMarkerModel.COUNTER_POSITION))).longValue();
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
