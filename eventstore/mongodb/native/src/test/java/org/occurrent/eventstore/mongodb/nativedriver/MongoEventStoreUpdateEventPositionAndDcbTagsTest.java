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
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

/**
 * {@code updateEvent} rebuilds the stored document through the stream-only
 * {@link org.occurrent.eventstore.mongodb.internal.OccurrentCloudEventMongoDocumentMapper#convertToDocument}, which
 * round-trips {@code position} through the general CloudEvent extension writer (no {@code Long} overload, so it
 * coerces to a string) and never writes the indexed {@code dcbTags} array at all (issue #876). The round trip this
 * verifies: reading a stored event and writing it back through {@code updateEvent} leaves {@code position},
 * {@code dcbTags}, {@code streamId} and {@code streamVersion} at their original values, on the stored document AND
 * on the CloudEvent {@code updateEvent} returns, for a plain stream event and for a DCB event alike (issues #876,
 * #904, #927). Every assertion here checks the returned event against the document read back in the same test,
 * not each independently against the original, since that was what let the position defect ship once already.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreUpdateEventPositionAndDcbTagsTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String databaseName;

    @BeforeEach
    void create_mongo_client() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".updateevent");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void updating_a_stream_event_keeps_the_stored_position_as_a_bson_int64() {
        MongoEventStore eventStore = newEventStore(STREAM);
        eventStore.write("stream:1", List.of(event("a", "Defined")));
        Document before = rawDocument("a");

        eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.get(OccurrentCloudEventExtension.POSITION))
                        .as("position must stay a BSON int64 after an update, not be coerced to a string")
                        .isInstanceOf(Long.class)
                        .isEqualTo(before.get(OccurrentCloudEventExtension.POSITION)),
                () -> assertThat(after.getString("subject")).isEqualTo("rewritten")
        );
    }

    @Test
    void updating_a_dcb_event_keeps_the_bson_int64_position_and_the_indexed_dcb_tags() {
        MongoEventStore eventStore = newEventStore(STREAM, DCB);
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));
        Document before = rawDocument("a");

        eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(original).withSubject("rewritten").build());

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.get(OccurrentCloudEventExtension.POSITION))
                        .as("position must stay a BSON int64 after an update, not be coerced to a string")
                        .isInstanceOf(Long.class)
                        .isEqualTo(before.get(OccurrentCloudEventExtension.POSITION)),
                () -> assertThat(after.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class))
                        .as("the indexed dcbTags array must survive an update, not be dropped")
                        .isEqualTo(before.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class)),
                () -> assertThat(after.getString("subject")).isEqualTo("rewritten")
        );
    }

    @Test
    void updating_a_dcb_event_with_a_fresh_replacement_event_keeps_the_original_dcb_tags_on_both_the_document_and_the_returned_event() {
        MongoEventStore eventStore = newEventStore(STREAM, DCB);
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));

        // A fresh event built from scratch, not derived from "original", carries none of the original's extensions
        // and, here, a different tag ("other:9" instead of "name:1"). Tags are store-owned the same way streamId,
        // streamVersion and the append id already are, so the update must not move the event across the
        // consistency boundary its original tags defined.
        CloudEvent updated = eventStore.updateEvent("a", SOURCE, original -> taggedEvent("a", "Renamed", "other:9")).orElseThrow();

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.getString("type")).isEqualTo("Renamed"),
                () -> assertReturnedEventMatchesStoredDocument(updated, after)
        );
    }

    @Test
    void updating_a_stream_event_with_a_fresh_replacement_event_keeps_the_original_stream_identity_on_both_the_document_and_the_returned_event() {
        MongoEventStore eventStore = newEventStore(STREAM);
        eventStore.write("stream:1", List.of(event("a", "Defined")));

        // A fresh event built from scratch carries no streamId or streamVersion of its own. Both are store-owned
        // the same way position, the append id and DCB tags already are, so the update must not lose them or let
        // an updater move the event to a stream it does not belong to.
        CloudEvent updated = eventStore.updateEvent("a", SOURCE, original -> event("a", "Renamed")).orElseThrow();

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.getString("type")).isEqualTo("Renamed"),
                () -> assertReturnedEventMatchesStoredDocument(updated, after)
        );
    }

    @Test
    void updating_an_event_with_a_forged_stream_identity_keeps_the_original() {
        MongoEventStore eventStore = newEventStore(STREAM);
        eventStore.write("stream:1", List.of(event("a", "Defined")));
        Document before = rawDocument("a");
        String originalStreamId = before.getString(OccurrentCloudEventExtension.STREAM_ID);
        long originalStreamVersion = before.getLong(OccurrentCloudEventExtension.STREAM_VERSION);

        // The updater forges a different streamId and streamVersion onto a fresh event. Both are store-owned, so
        // the update must not let an updater move the event into a stream, or a version, it does not belong to.
        CloudEvent updated = eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(event("a", "Renamed"))
                        .withExtension(OccurrentCloudEventExtension.STREAM_ID, "forged-stream")
                        .withExtension(OccurrentCloudEventExtension.STREAM_VERSION, 999L)
                        .build())
                .orElseThrow();

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.getString("type")).isEqualTo("Renamed"),
                () -> assertThat(after.getString(OccurrentCloudEventExtension.STREAM_ID)).isEqualTo(originalStreamId),
                () -> assertThat(after.getLong(OccurrentCloudEventExtension.STREAM_VERSION)).isEqualTo(originalStreamVersion),
                () -> assertReturnedEventMatchesStoredDocument(updated, after)
        );
    }

    @Test
    void updating_an_unpositioned_event_with_a_forged_position_leaves_it_unpositioned() {
        MongoEventStore eventStore = new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION,
                new EventStoreConfig.Builder()
                        .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                        .eventStoreCapabilities(STREAM)
                        .withoutStreamPosition()
                        .build());
        eventStore.write("stream:1", List.of(event("a", "Defined")));
        Document before = rawDocument("a");
        assertThat(before.containsKey(OccurrentCloudEventExtension.POSITION))
                .as("the original event must have no position to begin with")
                .isFalse();

        // Position is store-owned the same way streamId, streamVersion, the append id and DCB tags already are,
        // so an updater forging one onto an event that never had it must not let it through.
        CloudEvent updated = eventStore.updateEvent("a", SOURCE,
                original -> OccurrentCloudEventExtension.withPosition(CloudEventBuilder.v1(original).withSubject("rewritten").build(), 999)).orElseThrow();

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.getString("subject")).isEqualTo("rewritten"),
                () -> assertReturnedEventMatchesStoredDocument(updated, after)
        );
    }

    @Test
    void updating_a_stream_event_with_no_tags_and_a_forged_tag_keeps_it_tagless() {
        MongoEventStore eventStore = newEventStore(STREAM, DCB);
        eventStore.write("stream:1", List.of(event("a", "Defined")));
        Document before = rawDocument("a");
        assertThat(before.containsKey(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD))
                .as("the original event must carry no DCB tags to begin with")
                .isFalse();

        // DCB tags are store-owned the same way position, streamId, streamVersion and the append id already are,
        // so an updater forging one onto a plain stream event that never had any must not let it through. This is
        // preserveTags's other branch, CloudEventBuilder.v1(updated).withoutExtension(TAGS).build(), which the
        // fresh-replacement-event test above does not exercise since that original already carries tags.
        CloudEvent updated = eventStore.updateEvent("a", SOURCE,
                original -> taggedEvent("a", "Renamed", "forged:1")).orElseThrow();

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.getString("type")).isEqualTo("Renamed"),
                () -> assertReturnedEventMatchesStoredDocument(updated, after)
        );
    }

    @Test
    void a_second_update_keeps_the_bson_int64_position_and_the_indexed_dcb_tags_from_the_first_update() {
        MongoEventStore eventStore = newEventStore(STREAM, DCB);
        eventStore.append(List.of(taggedEvent("a", "Defined", "name:1")));

        eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(original).withSubject("first").build());
        Document afterFirst = rawDocument("a");

        eventStore.updateEvent("a", SOURCE, original -> CloudEventBuilder.v1(original).withSubject("second").build());
        Document afterSecond = rawDocument("a");

        assertAll(
                () -> assertThat(afterSecond.get(OccurrentCloudEventExtension.POSITION))
                        .as("position must still be a BSON int64 after a second update")
                        .isInstanceOf(Long.class)
                        .isEqualTo(afterFirst.get(OccurrentCloudEventExtension.POSITION)),
                () -> assertThat(afterSecond.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class))
                        .as("the indexed dcbTags array must still survive a second update")
                        .isEqualTo(afterFirst.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class)),
                () -> assertThat(afterSecond.getString("subject")).isEqualTo("second")
        );
    }

    private MongoEventStore newEventStore(EventStoreCapability first, EventStoreCapability... rest) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(first, rest)
                .build();
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private Document rawDocument(String id) {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(EVENT_COLLECTION);
        return requireNonNull(collection.find(new Document("id", id).append("source", SOURCE.toString())).first());
    }

    /**
     * Cross-checks the CloudEvent {@code updateEvent} returns against the document it actually wrote, rather than
     * each against the original independently. Checking them separately is what let the returned event's position
     * go unfixed even after the stored document was already correct.
     */
    private static void assertReturnedEventMatchesStoredDocument(CloudEvent returned, Document storedDocument) {
        assertAll(
                () -> assertThat(returned.getExtension(OccurrentCloudEventExtension.STREAM_ID))
                        .as("the returned event's streamId must match the stored document's")
                        .isEqualTo(storedDocument.getString(OccurrentCloudEventExtension.STREAM_ID)),
                () -> assertThat(OccurrentExtensionGetter.getStreamVersion(returned))
                        .as("the returned event's streamVersion must match the stored document's")
                        .isEqualTo(storedDocument.getLong(OccurrentCloudEventExtension.STREAM_VERSION)),
                () -> {
                    Object storedPosition = storedDocument.get(OccurrentCloudEventExtension.POSITION);
                    if (storedPosition == null) {
                        assertThat(returned.getExtension(OccurrentCloudEventExtension.POSITION))
                                .as("the returned event must have no position when the stored document has none")
                                .isNull();
                    } else {
                        assertThat(OccurrentCloudEventExtension.getPosition(returned))
                                .as("the returned event's position must match the stored document's")
                                .isEqualTo(((Number) storedPosition).longValue());
                    }
                },
                () -> {
                    List<String> storedTags = storedDocument.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class);
                    if (storedTags == null) {
                        assertThat(DcbCloudEvents.isDcbEvent(returned))
                                .as("the returned event must carry no DCB tags when the stored document has none")
                                .isFalse();
                    } else {
                        assertThat(DcbCloudEvents.getTags(returned).stream().map(Tag::canonical).sorted().toList())
                                .as("the returned event's tags must match the stored document's indexed dcbTags")
                                .isEqualTo(storedTags.stream().sorted().toList());
                    }
                }
        );
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
