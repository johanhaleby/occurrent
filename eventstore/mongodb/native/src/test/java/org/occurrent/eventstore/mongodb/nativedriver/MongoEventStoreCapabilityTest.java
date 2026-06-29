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
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
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
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
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
import static org.occurrent.eventstore.api.dcb.DcbQuery.tags;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreCapabilityTest {

    private static final String EVENT_COLLECTION = "events";
    private static final String CLOUD_EVENT_ID_SOURCE_INDEX = "id_1_source_1";
    private static final String STREAM_INDEX = "streamid_1_streamversion_1";
    private static final String DCB_POSITION_INDEX = "dcbposition_1";
    private static final String DCB_TAGS_INDEX = "dcbTags_1";

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
    void stream_capability_initializes_only_stream_indexes() {
        newEventStore(eventStoreConfig(STREAM).build());

        assertThat(indexNames()).contains(STREAM_INDEX);
        assertThat(indexNames()).doesNotContain(DCB_POSITION_INDEX, DCB_TAGS_INDEX);
        assertThat(index(CLOUD_EVENT_ID_SOURCE_INDEX))
                .containsEntry("key", new Document("id", 1).append("source", 1))
                .containsEntry("unique", true);
        assertThat(index(STREAM_INDEX))
                .containsEntry("key", new Document("streamid", 1).append("streamversion", 1))
                .containsEntry("unique", true);
        assertThat(collectionExists(EVENT_COLLECTION + "_dcb_position")).isFalse();
        assertThat(collectionExists(EVENT_COLLECTION + "_dcb_checkpoints")).isFalse();
    }

    @Test
    void dcb_capability_initializes_only_dcb_indexes_and_support_collections() {
        newEventStore(eventStoreConfig(DCB).build());

        assertThat(indexNames()).contains(DCB_POSITION_INDEX, DCB_TAGS_INDEX);
        assertThat(indexNames()).doesNotContain(STREAM_INDEX);
        assertThat(index(CLOUD_EVENT_ID_SOURCE_INDEX))
                .containsEntry("key", new Document("id", 1).append("source", 1))
                .containsEntry("unique", true);
        assertThat(index(DCB_POSITION_INDEX))
                .containsEntry("key", new Document("dcbposition", 1))
                .containsEntry("unique", true)
                .containsEntry("sparse", true);
        assertThat(index(DCB_TAGS_INDEX)).containsEntry("key", new Document("dcbTags", 1));
        assertThat(collectionExists(EVENT_COLLECTION + "_dcb_position")).isTrue();
        assertThat(collectionExists(EVENT_COLLECTION + "_dcb_checkpoints")).isTrue();
    }

    @Test
    void stream_and_dcb_capabilities_initialize_both_index_sets() {
        newEventStore(eventStoreConfig(STREAM, DCB).build());

        assertThat(indexNames()).contains(STREAM_INDEX, DCB_POSITION_INDEX, DCB_TAGS_INDEX);
    }

    @Test
    void dcb_operations_fail_without_dcb_capability() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM).build());

        assertUnsupportedDcbOperation(() -> eventStore.read(tags("name:1")));
        assertUnsupportedDcbOperation(() -> eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))));
        assertUnsupportedDcbOperation(() -> eventStore.append(List.of(taggedEvent("NameDefined", "name:1")), DcbAppendCondition.failIfEventsMatch(tags("name:1"))));
    }

    @Test
    void stream_operations_fail_without_stream_capability() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(DCB).build());

        assertUnsupportedStreamOperation(() -> eventStore.write("name:1", WriteCondition.anyStreamVersion(), java.util.stream.Stream.of(event("NameDefined"))));
        assertUnsupportedStreamOperation(() -> eventStore.read("name:1"));
        assertUnsupportedStreamOperation(() -> eventStore.read("name:1", 0, 10));
        assertUnsupportedStreamOperation(() -> eventStore.read("name:1", StreamReadFilter.type("NameDefined"), 0, 10));
        assertUnsupportedStreamOperation(() -> eventStore.exists("name:1"));
        assertUnsupportedStreamOperation(() -> eventStore.exists(Filter.all()));
        assertUnsupportedStreamOperation(() -> eventStore.deleteEventStream("name:1"));
        assertUnsupportedStreamOperation(() -> eventStore.deleteEvent("event:1", URI.create("urn:test")));
        assertUnsupportedStreamOperation(() -> eventStore.delete(Filter.all()));
        assertUnsupportedStreamOperation(() -> eventStore.updateEvent("event:1", URI.create("urn:test"), cloudEvent -> cloudEvent));
        assertUnsupportedStreamOperation(() -> eventStore.query(Filter.all(), 0, 10, SortBy.unsorted()));
        assertUnsupportedStreamOperation(() -> eventStore.count(Filter.all()));
    }

    @Test
    void both_stream_and_dcb_operations_work_when_both_capabilities_are_enabled() {
        MongoEventStore eventStore = newEventStore(eventStoreConfig(STREAM, DCB).build());

        eventStore.write("name:1", WriteCondition.anyStreamVersion(), java.util.stream.Stream.of(event("NameDefined")));
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThat(eventStore.read("name:1").events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(eventStore.read(tags("name:1")).events()).extracting(CloudEvent::getType).containsExactly("NameChanged");
    }

    @Test
    void dcb_only_events_still_have_occurrent_stream_metadata() {
        MongoEventStore dcbOnly = newEventStore(eventStoreConfig(DCB).build());
        dcbOnly.append(List.of(taggedEvent("NameDefined", "name:1"), taggedEvent("NameChanged", "name:1")));

        List<CloudEvent> events = dcbOnly.read(tags("name:1")).events();

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

        assertThat(both.read(tags("name:1")).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(both.read(tags("order:1")).events()).extracting(CloudEvent::getType).containsExactly("OrderPlaced");

        String nameStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags("name:1")).events().get(0));
        String orderStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags("order:1")).events().get(0));
        assertThat(nameStreamId).startsWith("dcb:partition:");
        assertThat(orderStreamId).startsWith("dcb:partition:");
        assertThat(both.read(nameStreamId).events()).extracting(CloudEvent::getType).contains("NameDefined");
        assertThat(both.read(orderStreamId).events()).extracting(CloudEvent::getType).contains("OrderPlaced");
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

    private static void assertUnsupportedStreamOperation(ThrowingCallable operation) {
        assertThatThrownBy(operation)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("STREAM capability is not enabled for this MongoEventStore");
    }

    private static void assertUnsupportedDcbOperation(ThrowingCallable operation) {
        assertThatThrownBy(operation)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("DCB capability is not enabled for this MongoEventStore");
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
        return DcbCloudEvents.withTags(event(type), Set.of(tags));
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
