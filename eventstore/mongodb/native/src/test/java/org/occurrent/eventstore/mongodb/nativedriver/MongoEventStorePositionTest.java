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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    void stream_events_get_a_monotonic_position_shared_with_dcb_when_stream_position_is_enabled() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM, DCB).withStreamPosition().build());

        eventStore.append(List.of(taggedEvent("NameDefined", "name:1")));
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("StreamEvent1"), event("StreamEvent2")));
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        List<CloudEvent> nameEvents = eventStore.read(org.occurrent.eventstore.api.dcb.DcbQuery.tags("name:1")).events();
        List<CloudEvent> streamEvents = eventStore.read("stream:1").events().toList();
        assertThat(streamEvents).hasSize(2);

        long nameDefinedPosition = OccurrentCloudEventExtension.getPosition(nameEvents.get(0));
        long streamEvent1Position = OccurrentCloudEventExtension.getPosition(streamEvents.get(0));
        long streamEvent2Position = OccurrentCloudEventExtension.getPosition(streamEvents.get(1));
        long nameChangedPosition = OccurrentCloudEventExtension.getPosition(nameEvents.get(1));

        // Positions are shared with DCB: the two DCB appends bracket the stream write in the single global sequence.
        // A retried write may reserve (and abandon) an earlier block under contention, so positions can have gaps
        // (same as DCB, ADR 0021); only strict monotonic ordering across the interleaved writes is guaranteed.
        assertThat(nameDefinedPosition).isPositive();
        assertThat(streamEvent1Position).isGreaterThan(nameDefinedPosition);
        assertThat(streamEvent2Position).isGreaterThan(streamEvent1Position);
        assertThat(nameChangedPosition).isGreaterThan(streamEvent2Position);
        assertThat(eventStore.currentPosition()).isGreaterThanOrEqualTo(nameChangedPosition);
    }

    @Test
    void writing_a_single_stream_event_with_stream_position_enabled_still_carries_a_position() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("StreamEvent1")));

        CloudEvent written = eventStore.read("stream:1").events().findFirst().orElseThrow();
        long writtenPosition = OccurrentCloudEventExtension.getPosition(written);
        assertThat(writtenPosition).isPositive();
        assertThat(eventStore.currentPosition()).isEqualTo(writtenPosition);
    }

    @Test
    void stream_only_store_writes_no_position_when_opted_out() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("StreamEvent1")));

        CloudEvent written = eventStore.read("stream:1").events().findFirst().orElseThrow();
        assertThat(written.getExtensionNames()).doesNotContain(OccurrentCloudEventExtension.POSITION);
        assertThat(eventStore.writesPosition()).isFalse();
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
    void position_ordered_reader_returns_events_within_the_requested_range_in_position_order() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM, DCB).withStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("A")));
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("B")));
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("C")));
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("D")));

        // Read the actual positions rather than assuming a contiguous 1,2,3,4 sequence: a transaction retry under
        // contention reserves (and abandons) a position block, same as the DCB write path (ADR 0021), so gaps are
        // legal. B and C's own positions bound the range under test, whatever they happen to be.
        List<CloudEvent> allEvents = eventStore.read("stream:1").events().toList();
        long positionOfA = OccurrentCloudEventExtension.getPosition(allEvents.get(0));
        long positionOfB = OccurrentCloudEventExtension.getPosition(allEvents.get(1));
        long positionOfC = OccurrentCloudEventExtension.getPosition(allEvents.get(2));

        List<CloudEvent> events = eventStore.readInPositionOrder(Filter.all(), PositionRange.between(positionOfA, positionOfC)).toList();

        assertThat(events).extracting(CloudEvent::getType).containsExactly("B", "C");
        assertThat(events).extracting(OccurrentCloudEventExtension::getPosition).containsExactly(positionOfB, positionOfC);
    }

    @Test
    void position_ordered_reader_applies_the_supplied_filter() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("Included")));
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("Excluded")));

        List<CloudEvent> events = eventStore.readInPositionOrder(Filter.type("Included"), PositionRange.fromBeginning()).toList();

        assertThat(events).extracting(CloudEvent::getType).containsExactly("Included");
    }

    @Test
    void position_ordered_reader_clamps_to_the_current_high_watermark() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("A")));
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("B")));

        List<CloudEvent> events = eventStore.readInPositionOrder(Filter.all(), PositionRange.upToPosition(1_000_000)).toList();

        assertThat(events).extracting(CloudEvent::getType).containsExactly("A", "B");
    }

    @Test
    void opt_out_store_writes_no_stream_position() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("StreamEvent1")));

        CloudEvent written = eventStore.read("stream:1").events().findFirst().orElseThrow();
        assertThat(written.getExtensionNames()).doesNotContain(OccurrentCloudEventExtension.POSITION);
        assertThat(eventStore.writesPosition()).isFalse();
    }

    @Test
    void opt_out_store_rejects_current_position_with_a_clear_error() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build());

        assertThatThrownBy(eventStore::currentPosition)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not write a position");
    }

    @Test
    void opt_out_store_rejects_position_ordered_reads_with_a_clear_error() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build());

        assertThatThrownBy(() -> eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not write a position");
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
                .write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("PreExistingEvent")));

        // Flipping stream position on against the pre-existing, unpositioned history must not throw by default (WARN).
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());

        // New writes still get a position; the guard only warns, it never blocks startup or new writes.
        eventStore.write("stream:2", WriteCondition.anyStreamVersion(), Stream.of(event("NewEvent")));
        CloudEvent newEvent = eventStore.read("stream:2").events().findFirst().orElseThrow();
        assertThat(newEvent.getExtensionNames()).contains(OccurrentCloudEventExtension.POSITION);
    }

    @Test
    void startup_guard_fails_fast_when_configured_to_require_backfilled_position() {
        newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build())
                .write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("PreExistingEvent")));

        assertThatThrownBy(() -> newEventStore(eventStoreConfig(STREAM).withStreamPosition().requireBackfilledPosition(true).build()))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("position backfill migration");
    }

    @Test
    void startup_guard_does_not_fire_when_all_existing_events_are_already_positioned() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).withStreamPosition().build());
        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), Stream.of(event("PositionedEvent")));

        // Re-opening the store against the same, fully positioned collection must not fail even with a hard-fail guard.
        MongoEventStore reopened = newEventStore(eventStoreConfig(STREAM).withStreamPosition().requireBackfilledPosition(true).build());
        assertThat(reopened.currentPosition()).isEqualTo(1L);
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

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), Set.of(tags));
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
