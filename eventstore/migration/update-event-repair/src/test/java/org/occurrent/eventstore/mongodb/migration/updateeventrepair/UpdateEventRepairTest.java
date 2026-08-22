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


package org.occurrent.eventstore.mongodb.migration.updateeventrepair;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * Verifies {@link UpdateEventRepair} against events damaged the way the pre-0.34.0 {@code updateEvent} damaged them.
 * <p>
 * The damage is produced by {@link #damageTheWayUpdateEventUsedTo}, which runs the write-back sequence the store ran
 * before PR 901 rather than writing a document that looks like what that sequence is assumed to produce. PR 901 added
 * four lines on top of that sequence, so the helper stays anchored to the real defect. It asserts what it produced
 * before any repair runs, so a later change to the mappers that stops producing the damage fails the test instead of
 * quietly making it prove nothing.
 * <p>
 * Where a test can check a consequence rather than a field it checks the consequence, because a damaged event's
 * whole problem is that it disappears from queries. Asserting only that {@code position} is a number again would
 * pass for a repair that left the event just as invisible.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class UpdateEventRepairTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private MongoDatabase database;
    private SpringMongoEventStore eventStore;

    @BeforeEach
    void create_store_and_raw_client() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".update-event-repair");
        String databaseName = requireNonNull(connectionString.getDatabase());
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(databaseName);

        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, databaseName);
        MongoTransactionManager transactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void a_dcb_read_finds_a_damaged_event_again_after_the_repair() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        assertThat(dcbEventIds(DcbCriteria.tags(Tag.parse("name:1"))))
                .as("a damaged event must be missing from a DCB read, otherwise this test proves nothing")
                .isEmpty();

        newRepair().run();

        assertThat(dcbEventIds(DcbCriteria.tags(Tag.parse("name:1"))))
                .as("the repaired event must be found by a DCB read again")
                .containsExactly("a");
    }

    @Test
    void a_conditional_append_conflicts_again_after_the_repair() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        DcbAppendCondition failIfNameTaken = DcbAppendCondition.failIfEventsMatch(DcbCriteria.tags(Tag.parse("name:1")));
        // The damaged event is missing from the conflict query, so the append that should have been refused succeeds.
        eventStore.append(List.of(taggedEvent("b", "Defined", "name:1")), failIfNameTaken);

        newRepair().run();

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent("c", "Defined", "name:1")), failIfNameTaken))
                .as("the repaired event must be visible to the conflict query behind a conditional append")
                .isInstanceOf(Exception.class);
    }

    @Test
    void a_position_ordered_read_finds_a_damaged_stream_event_again_after_the_repair() {
        eventStore.write("stream:1", List.of(event("a", "Defined")));
        eventStore.write("stream:1", List.of(event("b", "Renamed")));
        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        assertThat(dcbEventIds(DcbCriteria.all()))
                .as("a damaged stream event must be missing from a position ordered read")
                .doesNotContain("a");

        newRepair().run();

        assertThat(storedDocument("a").get(OccurrentCloudEventExtension.POSITION))
                .as("a repaired stream event's position must be a BSON int64 again")
                .isInstanceOf(Long.class);
    }

    @Test
    void a_repaired_event_matches_one_the_store_writes_fresh() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1", "other:2")));
        eventStore.append(List.of(taggedEvent("b", "Defined", "name:1", "other:2")));
        Document freshlyWritten = storedDocument("b");
        Object positionBeforeDamage = storedDocument("a").get(OccurrentCloudEventExtension.POSITION);

        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        newRepair().run();

        Document repaired = storedDocument("a");
        assertAll(
                () -> assertThat(repaired.get(OccurrentCloudEventExtension.POSITION))
                        .as("the repaired position must be the original value, as a BSON int64")
                        .isInstanceOf(Long.class)
                        .isEqualTo(positionBeforeDamage),
                () -> assertThat(repaired.get(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD))
                        .as("the repaired tag array must be what the store writes for the same tags")
                        .isInstanceOf(List.class),
                () -> assertThat(sortedTags(repaired))
                        .as("the repaired tag array must hold the same canonical tags a fresh write produces")
                        .isEqualTo(sortedTags(freshlyWritten)),
                () -> assertThat(repaired.getString("subject"))
                        .as("the repair must not undo the update itself")
                        .isEqualTo("rewritten")
        );
    }

    @Test
    void an_event_with_no_dcb_tags_is_repaired_to_an_empty_tag_array() {
        eventStore.append(List.of(taggedEvent("a", "Defined")));
        assertThat(storedDocument("a").getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class))
                .as("an untagged DCB append must store an empty tag array to begin with")
                .isEmpty();

        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        newRepair().run();

        assertThat(storedDocument("a").getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class))
                .as("an empty tag set must repair to an empty array, not to an array holding one empty string")
                .isEmpty();
    }

    @Test
    void a_second_run_changes_nothing() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        UpdateEventRepairResult first = newRepair().run();
        Document afterFirstRun = storedDocument("a");

        UpdateEventRepairResult second = newRepair().run();

        assertAll(
                () -> assertThat(first.eventsRepaired()).isEqualTo(1),
                () -> assertThat(second.eventsRepaired())
                        .as("a repair only touches events that still look damaged, so a second run repairs nothing")
                        .isZero(),
                () -> assertThat(storedDocument("a"))
                        .as("a second run must leave the document exactly as the first run left it")
                        .isEqualTo(afterFirstRun)
        );
    }

    @Test
    void report_counts_the_damage_without_changing_anything() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        eventStore.append(List.of(taggedEvent("b", "Defined", "name:2")));
        damageTheWayUpdateEventUsedTo("a", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());
        Document damaged = storedDocument("a");

        UpdateEventRepairReport report = newRepair().report();

        assertAll(
                () -> assertThat(report.eventsNeedingRepair()).isEqualTo(1),
                () -> assertThat(report.eventsWithLostPosition()).isZero(),
                () -> assertThat(storedDocument("a"))
                        .as("report must not write anything")
                        .isEqualTo(damaged)
        );
    }

    @Test
    void an_event_whose_position_was_lost_is_reported_and_never_given_an_invented_one() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        // An update function returning an event built from scratch carries none of the original's extensions, so the
        // pre-fix write-back stored no position at all for it.
        damageTheWayUpdateEventUsedTo("a", original -> taggedEvent("a", "Renamed", "name:1"));
        assertThat(storedDocument("a").containsKey(OccurrentCloudEventExtension.POSITION))
                .as("this damage must leave no position field, otherwise the test is not exercising the lost case")
                .isFalse();

        UpdateEventRepairResult result = newRepair().run();

        assertAll(
                () -> assertThat(result.unrecoverableEventCount()).isEqualTo(1),
                () -> assertThat(result.unrecoverableEvents())
                        .singleElement()
                        .extracting(UnrecoverableEvent::reason)
                        .isEqualTo(UnrecoverableEvent.Reason.POSITION_LOST),
                () -> assertThat(storedDocument("a").containsKey(OccurrentCloudEventExtension.POSITION))
                        .as("the repair must not invent a position for an event whose own one is gone")
                        .isFalse(),
                () -> assertThat(storedDocument("a").getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class))
                        .as("the tag array is still rebuildable from the dcbtags extension, so it is repaired")
                        .containsExactly("name:1")
        );
    }

    @Test
    void an_event_whose_position_is_already_held_by_another_event_is_reported_and_left_untouched() {
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        eventStore.append(List.of(taggedEvent("b", "Defined", "name:2")));
        damageTheWayUpdateEventUsedTo("b", original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        // An update function could forge any position it liked before 901, including one another event already holds.
        long positionOfA = ((Number) requireNonNull(storedDocument("a").get(OccurrentCloudEventExtension.POSITION))).longValue();
        events().updateOne(new Document("id", "b"), new Document("$set", new Document(OccurrentCloudEventExtension.POSITION, String.valueOf(positionOfA))));
        Document damaged = storedDocument("b");

        UpdateEventRepairResult result = newRepair().run();

        assertAll(
                () -> assertThat(result.unrecoverableEvents())
                        .singleElement()
                        .extracting(UnrecoverableEvent::reason)
                        .isEqualTo(UnrecoverableEvent.Reason.POSITION_ALREADY_TAKEN),
                () -> assertThat(storedDocument("b"))
                        .as("a rejected repair must leave the event exactly as it was found, tag array included")
                        .isEqualTo(damaged)
        );
    }

    @Test
    void a_killed_run_leaves_a_readable_collection_and_a_later_run_finishes_the_job() throws Exception {
        for (int i = 0; i < 4; i++) {
            String id = "event-" + i;
            eventStore.append(List.of(taggedEvent(id, "Defined", "name:" + i)));
            damageTheWayUpdateEventUsedTo(id, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());
        }
        assertThat(newRepair().report().eventsNeedingRepair()).isEqualTo(4);

        UpdateEventRepair throttledRepair = new UpdateEventRepair(database, EVENT_COLLECTION,
                UpdateEventRepairOptions.defaults().withBatchSize(1).withThrottleMillis(60_000));
        Thread runner = new Thread(throttledRepair::run);
        runner.start();
        waitUntilRepaired(1);
        runner.interrupt();
        runner.join(30_000);

        long repairedByTheKilledRun = 4 - newRepair().report().eventsNeedingRepair();
        assertAll(
                () -> assertThat(repairedByTheKilledRun)
                        .as("a killed run must keep the events it already repaired")
                        .isPositive(),
                () -> assertThat(repairedByTheKilledRun)
                        .as("a killed run must not have finished, otherwise this test does not exercise resuming")
                        .isLessThan(4),
                () -> assertThat(events().countDocuments())
                        .as("every event must still be readable after a killed run")
                        .isEqualTo(4)
        );

        UpdateEventRepairResult resumed = newRepair().run();

        assertAll(
                () -> assertThat(resumed.eventsRepaired()).isEqualTo(4 - repairedByTheKilledRun),
                () -> assertThat(newRepair().report().eventsNeedingRepair())
                        .as("the resumed run must leave nothing damaged behind")
                        .isZero(),
                () -> assertThat(dcbEventIds(DcbCriteria.all()))
                        .as("every repaired event must be visible to a DCB read again")
                        .hasSize(4)
        );
    }

    /**
     * Runs the write-back {@code updateEvent} ran before PR 901, so the document left behind carries the real defect
     * rather than an imitation of it. PR 901 added {@code preserveStreamIdentity}, {@code preservePosition},
     * {@code preserveTags} and {@code preservePositionAndDcbTags} on top of exactly this sequence.
     */
    private void damageTheWayUpdateEventUsedTo(String eventId, UnaryOperator<CloudEvent> updateFunction) {
        Document stored = storedDocument(eventId);
        CloudEvent current = DcbDocumentMapper.toCloudEvent(TimeRepresentation.RFC_3339_STRING, stored);
        CloudEvent updated = OccurrentCloudEventExtension.preserveAppendId(current, updateFunction.apply(current));

        Document damaged = OccurrentCloudEventMongoDocumentMapper.convertToDocument(
                TimeRepresentation.RFC_3339_STRING,
                stored.getString(OccurrentCloudEventExtension.STREAM_ID),
                requireNonNull(stored.getLong(OccurrentCloudEventExtension.STREAM_VERSION)),
                updated);
        damaged.put("_id", stored.get("_id"));
        events().replaceOne(new Document("_id", stored.get("_id")), damaged);

        Document afterDamage = storedDocument(eventId);
        assertThat(afterDamage.get(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD))
                .as("the pre-901 write-back must drop the indexed tag array, otherwise this helper no longer reproduces the defect")
                .isNull();
        Object position = afterDamage.get(OccurrentCloudEventExtension.POSITION);
        assertThat(position == null || position instanceof String)
                .as("the pre-901 write-back must leave position as a string or drop it, but it was %s", position)
                .isTrue();
    }

    private UpdateEventRepair newRepair() {
        return new UpdateEventRepair(database, EVENT_COLLECTION, UpdateEventRepairOptions.defaults());
    }

    private void waitUntilRepaired(int atLeast) throws InterruptedException {
        for (int attempt = 0; attempt < 200; attempt++) {
            if (4 - newRepair().report().eventsNeedingRepair() >= atLeast) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("The throttled repair never repaired an event, so there was nothing to interrupt");
    }

    private List<String> dcbEventIds(DcbCriteria criteria) {
        return eventStore.read(criteria, DcbReadOptions.fromBeginning()).events().stream().map(CloudEvent::getId).toList();
    }

    private static List<String> sortedTags(Document document) {
        List<String> tags = new ArrayList<>(document.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class));
        tags.sort(String::compareTo);
        return tags;
    }

    private MongoCollection<Document> events() {
        return database.getCollection(EVENT_COLLECTION);
    }

    private Document storedDocument(String eventId) {
        return requireNonNull(events().find(new Document("id", eventId).append("source", SOURCE.toString())).first());
    }

    private static CloudEvent taggedEvent(String id, String type, String... tags) {
        return DcbCloudEvents.withTags(event(id, type), Arrays.stream(tags).map(Tag::parse).toList());
    }

    private static CloudEvent event(String id, String type) {
        return CloudEventBuilder.v1()
                .withId(id)
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
