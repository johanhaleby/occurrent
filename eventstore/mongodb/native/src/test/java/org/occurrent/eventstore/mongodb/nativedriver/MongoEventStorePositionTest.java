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
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStorePositionTest {

    private static final URI SOURCE = URI.create("urn:test");
    private static final String EVENT_COLLECTION = "events";
    private static final String POSITION_INDEX = "position_1";

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".position"));

    private MongoClient mongoClient;
    private String databaseName;

    @BeforeEach
    void create_mongo_client() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".position");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void position_index_is_created_when_stream_position_is_enabled_for_a_stream_only_store() {
        newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());

        assertThat(indexNames()).contains(POSITION_INDEX);
    }

    @Test
    void position_index_is_not_created_for_a_stream_only_store_without_position() {
        newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build());

        assertThat(indexNames()).doesNotContain(POSITION_INDEX);
    }

    @Test
    void combining_dcb_with_an_explicit_stream_position_opt_out_fails_fast() {
        assertThatThrownBy(() -> eventStoreConfig(STREAM, DCB).withoutStreamPosition().build())
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot disable stream position when the DCB capability is enabled");
    }

    @Test
    void startup_guard_logs_a_warning_by_default_when_position_is_enabled_against_an_unbackfilled_collection() {
        // Seed a collection with a stream event written by a position-less store, mirroring an existing deployment
        // that has not yet run the position backfill migration.
        newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build())
                .write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("PreExistingEvent")));

        // Flipping stream position on against the pre-existing, unpositioned history must not throw by default (WARN).
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());

        // New writes still get a position; the guard only warns, it never blocks startup or new writes.
        eventStore.write("stream:2", WriteCondition.anyStreamVersion(), List.of(event("NewEvent")));
        CloudEvent newEvent = eventStore.read("stream:2").events().findFirst().orElseThrow();
        assertThat(newEvent.getExtensionNames()).contains(OccurrentCloudEventExtension.POSITION);
    }

    @Test
    void startup_guard_fails_fast_when_configured_to_require_backfilled_position() {
        newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build())
                .write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("PreExistingEvent")));

        assertThatThrownBy(() -> newEventStore(eventStoreConfig(STREAM).withStreamPosition().requireBackfilledPosition(true).build()))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("configured to require backfilled positions")
                .hasMessageContaining("doc/runbooks/position-backfill.md");
    }

    @Test
    void startup_guard_does_not_fire_when_all_existing_events_are_already_positioned() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("PositionedEvent")));
        CloudEvent written = eventStore.read("stream:1").events().findFirst().orElseThrow();
        long writtenPosition = OccurrentCloudEventExtension.getPosition(written);

        // Re-opening the store against the same, fully positioned collection must not fail even with a hard-fail guard,
        // and its high-watermark must still reflect the pre-existing write rather than resetting.
        MongoEventStore reopened = newEventStore(eventStoreConfig(STREAM).withStreamPosition().requireBackfilledPosition(true).build());
        assertThat(reopened.currentPosition()).isEqualTo(writtenPosition);
    }

    @Test
    void position_is_turned_off_on_an_existing_unpositioned_store_when_it_was_not_enabled_explicitly() {
        newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build())
                .write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("PreExistingEvent")));

        // Default (not explicit) position over a collection that already has unpositioned events turns itself off,
        // rather than building the position index over the whole collection at startup.
        MongoEventStore defaulted = newEventStore(eventStoreConfig(STREAM).build());
        assertThat(defaulted.writesPosition()).isFalse();
        assertThat(indexNames()).doesNotContain(POSITION_INDEX);
    }

    @Test
    void position_stays_on_by_default_for_an_empty_store() {
        MongoEventStore defaulted = newEventStore(eventStoreConfig(STREAM).build());
        assertThat(defaulted.writesPosition()).isTrue();
    }

    @Test
    void position_stays_on_by_default_once_the_store_has_positioned_events() {
        newEventStore(eventStoreConfig(STREAM).build())
                .write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("PositionedEvent")));

        // Re-opening a store whose events already have positions keeps position on.
        MongoEventStore reopened = newEventStore(eventStoreConfig(STREAM).build());
        assertThat(reopened.writesPosition()).isTrue();
    }

    private MongoEventStore newEventStore(EventStoreConfig config) {
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private List<String> indexNames() {
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(EVENT_COLLECTION);
        return collection.listIndexes(Document.class).map(index -> index.getString("name")).into(new ArrayList<>());
    }

    private EventStoreConfig.Builder eventStoreConfig(org.occurrent.eventstore.api.EventStoreCapability capability, org.occurrent.eventstore.api.EventStoreCapability... additionalCapabilities) {
        return new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(capability, additionalCapabilities);
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
