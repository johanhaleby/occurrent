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
 * verifies: reading a stored event and writing it back through {@code updateEvent} leaves {@code position} at its
 * original BSON type and value, and leaves {@code dcbTags} at its original content, for a plain stream event and for
 * a DCB event alike.
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
        Document before = rawDocument("a");

        // A fresh event built from scratch, not derived from "original", carries none of the original's extensions
        // and, here, a different tag ("other:9" instead of "name:1"). Tags are store-owned the same way streamId,
        // streamVersion and the append id already are, so the update must not move the event across the
        // consistency boundary its original tags defined.
        CloudEvent updated = eventStore.updateEvent("a", SOURCE, original -> taggedEvent("a", "Renamed", "other:9")).orElseThrow();

        Document after = rawDocument("a");
        assertAll(
                () -> assertThat(after.get(OccurrentCloudEventExtension.POSITION))
                        .as("position must stay a BSON int64 after an update, not be coerced to a string")
                        .isInstanceOf(Long.class)
                        .isEqualTo(before.get(OccurrentCloudEventExtension.POSITION)),
                () -> assertThat(after.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class))
                        .as("the indexed dcbTags array must keep the original tags, not the replacement event's")
                        .isEqualTo(before.getList(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, String.class)),
                () -> assertThat(after.getString(DcbCloudEvents.TAGS))
                        .as("the dcbtags extension field on the document must match the indexed array, not the replacement event's tags")
                        .isEqualTo(before.getString(DcbCloudEvents.TAGS)),
                () -> assertThat(DcbCloudEvents.getTags(updated))
                        .as("the CloudEvent updateEvent returns must carry the original tags too, not the replacement event's")
                        .containsExactlyElementsOf(DcbCloudEvents.getTags(taggedEvent("a", "Defined", "name:1"))),
                () -> assertThat(after.getString("type")).isEqualTo("Renamed")
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
