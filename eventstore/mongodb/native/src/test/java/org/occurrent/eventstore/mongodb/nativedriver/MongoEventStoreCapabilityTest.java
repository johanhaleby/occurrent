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
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.Tag;
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
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreCapabilityTest {

    private static final String EVENT_COLLECTION = "events";
    private static final String CLOUD_EVENT_ID_SOURCE_INDEX = "id_1_source_1";
    private static final String STREAM_INDEX = "streamid_1_streamversion_1";
    private static final String POSITION_INDEX = "position_1";
    private static final String DCB_TAGS_INDEX = "dcbTags_1";
    private static final String TYPE_POSITION_INDEX = "type_1_position_1";
    private static final String DCB_TAGS_POSITION_INDEX = "dcbTags_1_position_1";

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
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".capabilities"));

    private MongoClient mongoClient;
    private String databaseName;

    @BeforeEach
    void create_mongo_client() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".capabilities");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void event_store_config_defaults_to_stream_capability() {
        EventStoreConfig config = eventStoreConfig(STREAM).build();
        EventStoreConfig defaultConfig = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();

        assertThat(defaultConfig.eventStoreCapabilities).containsExactly(STREAM);
        assertThat(config.eventStoreCapabilities).containsExactly(STREAM);
    }

    @Test
    void event_store_config_accepts_set_and_vararg_capabilities() {
        EventStoreConfig dcbOnly = eventStoreConfig(Set.of(DCB)).build();
        EventStoreConfig streamAndDcb = eventStoreConfig(STREAM, DCB).build();

        assertThat(dcbOnly.eventStoreCapabilities).containsExactly(DCB);
        assertThat(streamAndDcb.eventStoreCapabilities).containsExactlyInAnyOrder(STREAM, DCB);
    }

    @Test
    void event_store_config_rejects_empty_and_null_capabilities() {
        assertThatThrownBy(() -> eventStoreConfig(Set.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Event store capabilities cannot be empty");
        assertThatThrownBy(() -> eventStoreConfig((Set<EventStoreCapability>) null))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("Event store capabilities cannot be null");
        assertThatThrownBy(() -> eventStoreConfig(STREAM, (EventStoreCapability) null))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("Event store capability cannot be null");
    }

    @Test
    void stream_capability_without_position_initializes_only_stream_indexes() {
        newEventStore(eventStoreConfig(STREAM).withoutStreamPosition().build());

        assertThat(indexNames()).contains(STREAM_INDEX);
        assertThat(indexNames()).doesNotContain(POSITION_INDEX, DCB_TAGS_INDEX, TYPE_POSITION_INDEX, DCB_TAGS_POSITION_INDEX);
        assertThat(index(CLOUD_EVENT_ID_SOURCE_INDEX))
                .containsEntry("key", new Document("id", 1).append("source", 1))
                .containsEntry("unique", true);
        assertThat(index(STREAM_INDEX))
                .containsEntry("key", new Document("streamid", 1).append("streamversion", 1))
                .containsEntry("unique", true);
        assertThat(collectionExists(EVENT_COLLECTION + "_position")).isFalse();
        assertThat(collectionExists(EVENT_COLLECTION + "_dcb_checkpoints")).isFalse();
    }

    @Test
    void dcb_capability_initializes_only_dcb_indexes_and_support_collections() {
        newEventStore(eventStoreConfig(DCB).build());

        // A DCB-only store still creates the streamId+streamVersion compound index, since the DCB append path looks
        // up the current stream version per partition (currentStreamVersion). It is unique, identical to the STREAM
        // index: DCB-only writes assign sequential per-partition stream versions, and the only collision (two
        // disjoint DCB boundaries hashing to the same partition stream) is a retryable transient, not a duplicate.
        assertThat(indexNames()).contains(STREAM_INDEX, POSITION_INDEX, DCB_TAGS_INDEX, TYPE_POSITION_INDEX, DCB_TAGS_POSITION_INDEX);
        assertThat(index(CLOUD_EVENT_ID_SOURCE_INDEX))
                .containsEntry("key", new Document("id", 1).append("source", 1))
                .containsEntry("unique", true);
        assertThat(index(STREAM_INDEX))
                .containsEntry("key", new Document("streamid", 1).append("streamversion", 1))
                .containsEntry("unique", true);
        assertThat(index(POSITION_INDEX))
                .containsEntry("key", new Document("position", 1))
                .containsEntry("unique", true)
                .containsEntry("sparse", true);
        assertThat(index(DCB_TAGS_INDEX)).containsEntry("key", new Document("dcbTags", 1));
        // These compound indexes back type-only DCB reads and large tag-boundary DCB reads that would otherwise fall
        // back to a residual FETCH filter or an in-memory SORT over the position index (see MongoEventStore
        // initialization comments for the explain evidence).
        assertThat(index(TYPE_POSITION_INDEX))
                .containsEntry("key", new Document("type", 1).append("position", 1))
                .containsEntry("sparse", true);
        assertThat(index(DCB_TAGS_POSITION_INDEX))
                .containsEntry("key", new Document("dcbTags", 1).append("position", 1))
                .containsEntry("sparse", true);
        assertThat(collectionExists(EVENT_COLLECTION + "_position")).isTrue();
        assertThat(collectionExists(EVENT_COLLECTION + "_dcb_checkpoints")).isTrue();
    }

    @Test
    void an_operator_created_incompatible_stream_version_index_fails_startup_loudly() {
        // Simulate an operator manually creating a non-unique streamid+streamversion index out-of-band. Occurrent
        // requires this index to be unique, so constructing a store must fail loudly rather than silently run
        // without the uniqueness guarantee stream and DCB writes rely on.
        mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION)
                .createIndex(Indexes.compoundIndex(Indexes.ascending("streamid"), Indexes.ascending("streamversion")), new IndexOptions());
        assertThat(index(STREAM_INDEX)).doesNotContainKey("unique");

        assertThatThrownBy(() -> newEventStore(eventStoreConfig(STREAM, DCB).build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(STREAM_INDEX)
                .hasMessageContaining("unique");
        // The pre-existing non-unique index is never dropped or replaced automatically.
        assertThat(index(STREAM_INDEX)).doesNotContainKey("unique");
    }

    @Test
    void stream_and_dcb_capabilities_initialize_both_index_sets() {
        newEventStore(eventStoreConfig(STREAM, DCB).build());

        assertThat(indexNames()).contains(STREAM_INDEX, POSITION_INDEX, DCB_TAGS_INDEX, TYPE_POSITION_INDEX, DCB_TAGS_POSITION_INDEX);
    }




    @Test
    void dcb_only_events_still_have_occurrent_stream_metadata() {
        MongoEventStore dcbOnly = newEventStore(eventStoreConfig(DCB).build());
        dcbOnly.append(List.of(taggedEvent("NameDefined", "name:1"), taggedEvent("NameChanged", "name:1")));

        List<CloudEvent> events = dcbOnly.read(tags(Tag.parse("name:1"))).events();

        assertThat(events).hasSize(2);
        assertThat(events).extracting(OccurrentExtensionGetter::getStreamId).allSatisfy(streamId -> assertThat(streamId).startsWith("dcb:partition:"));
        assertThat(OccurrentExtensionGetter.getStreamId(events.get(0))).isEqualTo(OccurrentExtensionGetter.getStreamId(events.get(1)));
        assertThat(events).extracting(OccurrentExtensionGetter::getStreamVersion).containsExactly(1L, 2L);
    }

    @Test
    void dcb_events_are_readable_through_the_stream_api_by_their_derived_partition_stream() {
        MongoEventStore both = newEventStore(eventStoreConfig(STREAM, DCB).build());
        both.append(List.of(taggedEvent("NameDefined", "name:1")));
        both.append(List.of(taggedEvent("OrderPlaced", "order:1")));

        assertThat(both.read(tags(Tag.parse("name:1"))).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(both.read(tags(Tag.parse("order:1"))).events()).extracting(CloudEvent::getType).containsExactly("OrderPlaced");

        String nameStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags(Tag.parse("name:1"))).events().get(0));
        String orderStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags(Tag.parse("order:1"))).events().get(0));
        assertThat(nameStreamId).startsWith("dcb:partition:");
        assertThat(orderStreamId).startsWith("dcb:partition:");
        assertThat(both.read(nameStreamId).events()).extracting(CloudEvent::getType).contains("NameDefined");
        assertThat(both.read(orderStreamId).events()).extracting(CloudEvent::getType).contains("OrderPlaced");
    }

    @Test
    void write_rejects_a_dcb_tagged_event_written_through_the_plain_write_path() {
        MongoEventStore both = newEventStore(eventStoreConfig(STREAM, DCB).build());
        CloudEvent dcbTaggedEvent = taggedEvent("NameDefined", "name:1");

        assertThatThrownBy(() -> both.write("name:1", WriteCondition.anyStreamVersion(), List.of(dcbTaggedEvent)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead.");

        assertThat(both.query(Filter.capability(STREAM), 0, 10, SortBy.unsorted()).toList()).isEmpty();
        assertThat(both.query(Filter.capability(DCB), 0, 10, SortBy.unsorted()).toList()).isEmpty();
    }

    @Test
    void capability_filter_excludes_appended_dcb_event_from_stream_and_includes_it_in_dcb() {
        MongoEventStore both = newEventStore(eventStoreConfig(STREAM, DCB).build());
        both.write("name:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));
        both.append(List.of(taggedEvent("NameChanged", "name:1")));

        List<CloudEvent> streamCapabilityEvents = both.query(Filter.capability(STREAM), 0, 10, SortBy.unsorted()).toList();
        List<CloudEvent> dcbCapabilityEvents = both.query(Filter.capability(DCB), 0, 10, SortBy.unsorted()).toList();

        assertThat(streamCapabilityEvents).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(dcbCapabilityEvents).extracting(CloudEvent::getType).containsExactly("NameChanged");
    }

    @Test
    void capability_filter_matches_an_empty_tag_dcb_append_by_the_dcb_tags_array() {
        MongoEventStore both = newEventStore(eventStoreConfig(STREAM, DCB).build());
        both.write("name:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));
        both.append(List.of(event("SystemInitialized")));

        List<CloudEvent> streamCapabilityEvents = both.query(Filter.capability(STREAM), 0, 10, SortBy.unsorted()).toList();
        List<CloudEvent> dcbCapabilityEvents = both.query(Filter.capability(DCB), 0, 10, SortBy.unsorted()).toList();

        assertThat(streamCapabilityEvents).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(dcbCapabilityEvents).extracting(CloudEvent::getType).containsExactly("SystemInitialized");
    }

    private MongoEventStore newEventStore(EventStoreConfig config) {
        return new MongoEventStore(mongoClient, databaseName, EVENT_COLLECTION, config);
    }

    private List<String> indexNames() {
        return StreamSupport.stream(mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION).listIndexes(Document.class).spliterator(), false)
                .map(index -> index.getString("name"))
                .toList();
    }

    private Document index(String name) {
        return StreamSupport.stream(mongoClient.getDatabase(databaseName).getCollection(EVENT_COLLECTION).listIndexes(Document.class).spliterator(), false)
                .filter(index -> name.equals(index.getString("name")))
                .findFirst()
                .orElseThrow();
    }

    private boolean collectionExists(String collectionName) {
        for (String name : mongoClient.getDatabase(databaseName).listCollectionNames()) {
            if (name.equals(collectionName)) {
                return true;
            }
        }
        return false;
    }



    private EventStoreConfig.Builder eventStoreConfig(EventStoreCapability capability, EventStoreCapability... additionalCapabilities) {
        return eventStoreConfigBuilder().eventStoreCapabilities(capability, additionalCapabilities);
    }

    private EventStoreConfig.Builder eventStoreConfig(Set<EventStoreCapability> capabilities) {
        return eventStoreConfigBuilder().eventStoreCapabilities(capabilities);
    }

    private EventStoreConfig.Builder eventStoreConfigBuilder() {
        return new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING);
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), java.util.Arrays.stream(tags).map(Tag::parse).toList());
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:test"))
                .withType(type)
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
