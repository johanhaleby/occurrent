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
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbTagsIndexCheck;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;

/**
 * The lookup a store with {@code DCB} and without {@code STREAM} sends at startup to find a stream event that has a
 * position, run with {@code explain} so the number of index keys and documents it reads is asserted, not only what it
 * returns. The numbers are printed so they can be quoted.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DcbTagsIndexCheckBoundsTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoClient mongoClient;
    private String databaseName;
    private MongoCollection<Document> events;

    @BeforeEach
    void create_mongo_client() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbtagsindexbounds");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        events = mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION);
        events.drop();
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void the_lookup_reads_no_index_key_and_no_document_on_a_collection_holding_only_dcb_events() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), taggedEvent("Renamed", "name:1"), event("Untagged")));

        Explained lookup = explainLookup("only dcb events");

        assertThat(lookup.found()).as("a DCB event has a position, but its dcbTags is set, so it is not a stream event").isEmpty();
        assertThat(lookup.keysExamined()).as("the bounds start and end at the keys whose dcbTags is null, and a DCB event is indexed under its tags or under none").isZero();
        assertThat(lookup.docsExamined()).as("no key in range means no document to fetch").isZero();
    }

    @Test
    void the_lookup_reads_no_index_key_and_no_document_past_stream_events_whose_position_is_a_string() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), event("Untagged")));
        insertStreamEvent("stream:1", 1, "7");
        insertStreamEvent("stream:1", 2, "8");
        insertStreamEvent("stream:2", 1, "not-a-number");

        Explained lookup = explainLookup("stream events with a string position");

        assertThat(lookup.found()).as("a string position is the damage the old updateEvent left, and not a position a DCB read can use").isEmpty();
        assertThat(lookup.keysExamined()).as("a string sorts after every number, so the upper bound must stop before the first string for a damaged collection to read no keys").isZero();
        assertThat(lookup.docsExamined()).as("no key in range means no document to fetch").isZero();
    }

    @Test
    void the_lookup_reads_no_index_key_and_no_document_for_stream_events_that_have_no_position() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1")));
        insertStreamEvent("stream:1", 1, null);
        insertStreamEvent("stream:1", 2, null);

        Explained lookup = explainLookup("stream events without a position");

        assertThat(lookup.found()).as("a stream event without a position is not one a DCB read orders, and the sparse index does not hold it").isEmpty();
        assertThat(lookup.keysExamined()).as("a sparse (dcbTags, position) index has no key for a document with neither field").isZero();
        assertThat(lookup.docsExamined()).as("no key in range means no document to fetch").isZero();
    }

    @Test
    void the_lookup_finds_a_stream_event_that_has_a_numeric_position_among_damaged_and_unpositioned_ones() {
        newDcbStore().append(List.of(taggedEvent("Defined", "name:1"), event("Untagged")));
        insertStreamEvent("stream:1", 1, "7");
        insertStreamEvent("stream:2", 1, null);
        insertStreamEvent("stream:3", 1, 42L);

        Explained lookup = explainLookup("one stream event with a numeric position");

        assertThat(lookup.found())
                .as("this is the event the warning is about, and the upper bound must not cut it off together with the string positions")
                .hasSize(1)
                .allSatisfy(found -> assertThat(found.getString("streamid")).isEqualTo("stream:3"));
        assertThat(lookup.keysExamined()).as("the scan reads the one key in range and stops, since the limit is 1").isEqualTo(1);
        assertThat(lookup.docsExamined()).as("and fetches the one document behind it").isEqualTo(1);
    }

    private Explained explainLookup(String scenario) {
        Document stats = lookup().explain(ExplainVerbosity.EXECUTION_STATS).get("executionStats", Document.class);
        Explained explained = new Explained(lookup().into(new ArrayList<>()), ((Number) stats.get("totalKeysExamined")).longValue(), ((Number) stats.get("totalDocsExamined")).longValue());
        System.out.println("DCB_TAGS_INDEX_CHECK_EXPLAIN mongodb=" + serverVersion() + " scenario=\"" + scenario + "\" totalKeysExamined=" + explained.keysExamined()
                + " totalDocsExamined=" + explained.docsExamined() + " nReturned=" + stats.get("nReturned"));
        return explained;
    }

    private FindIterable<Document> lookup() {
        return events.find(DcbTagsIndexCheck.positionedStreamEvent())
                .hint(DcbTagsIndexCheck.hint())
                .min(DcbTagsIndexCheck.min())
                .max(DcbTagsIndexCheck.max())
                .limit(1)
                .projection(Projections.include("_id", "streamid"));
    }

    private String serverVersion() {
        return mongoClient.getDatabase("admin").runCommand(new Document("buildInfo", 1)).getString("version");
    }

    private void insertStreamEvent(String streamId, long streamVersion, Object position) {
        Document stored = new Document("id", UUID.randomUUID().toString())
                .append("source", SOURCE.toString())
                .append("type", "Defined")
                .append("streamid", streamId)
                .append("streamversion", streamVersion);
        if (position != null) {
            stored.append("position", position);
        }
        events.insertOne(stored);
    }

    private MongoEventStore newDcbStore() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(DCB)
                .build();
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private static CloudEvent taggedEvent(String type, String tag) {
        return DcbCloudEvents.withTags(event(type), List.of(Tag.parse(tag)));
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withData("{}".getBytes(UTF_8))
                .build();
    }

    private record Explained(List<Document> found, long keysExamined, long docsExamined) {
    }
}
