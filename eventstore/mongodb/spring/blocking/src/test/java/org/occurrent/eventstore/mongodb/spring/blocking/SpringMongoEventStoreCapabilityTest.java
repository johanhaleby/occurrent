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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreCapabilityTest {

    private static final String EVENT_COLLECTION = "events";
    private static final String CLOUD_EVENT_ID_SOURCE_INDEX = "id_1_source_1";
    private static final String STREAM_INDEX = "streamid_1_streamversion_1";
    private static final String POSITION_INDEX = "position_1";
    private static final String DCB_TAGS_INDEX = "dcbTags_1";
    private static final String TYPE_POSITION_INDEX = "type_1_position_1";
    private static final String DCB_TAGS_POSITION_INDEX = "dcbTags_1_position_1";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.droppingTheDatabaseIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoTemplate mongoTemplate;
    private MongoTransactionManager mongoTransactionManager;

    @BeforeEach
    void create_mongo_template() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".capabilities");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
    }

    @Test
    void event_store_config_defaults_to_stream_capability() {
        EventStoreConfig config = eventStoreConfig(STREAM).build();
        EventStoreConfig defaultConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(mongoTransactionManager)
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
        new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM).withoutStreamPosition().build());

        assertThat(indexNames()).contains(STREAM_INDEX);
        assertThat(indexNames()).doesNotContain(POSITION_INDEX, DCB_TAGS_INDEX, TYPE_POSITION_INDEX, DCB_TAGS_POSITION_INDEX);
        assertThat(index(CLOUD_EVENT_ID_SOURCE_INDEX))
                .containsEntry("key", new Document("id", 1).append("source", 1))
                .containsEntry("unique", true);
        assertThat(index(STREAM_INDEX))
                .containsEntry("key", new Document("streamid", 1).append("streamversion", 1))
                .containsEntry("unique", true);
        assertThat(mongoTemplate.collectionExists(EVENT_COLLECTION + "_position")).isFalse();
        assertThat(mongoTemplate.collectionExists(EVENT_COLLECTION + "_dcb_checkpoints")).isFalse();
    }

    @Test
    void dcb_capability_initializes_only_dcb_indexes_and_support_collections() {
        new SpringMongoEventStore(mongoTemplate, eventStoreConfig(DCB).build());

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
        // back to a residual FETCH filter or an in-memory SORT over the position index (see initializeEventStore's
        // comments for the explain evidence).
        assertThat(index(TYPE_POSITION_INDEX))
                .containsEntry("key", new Document("type", 1).append("position", 1))
                .containsEntry("sparse", true);
        assertThat(index(DCB_TAGS_POSITION_INDEX))
                .containsEntry("key", new Document("dcbTags", 1).append("position", 1))
                .containsEntry("sparse", true);
        assertThat(mongoTemplate.collectionExists(EVENT_COLLECTION + "_position")).isTrue();
        assertThat(mongoTemplate.collectionExists(EVENT_COLLECTION + "_dcb_checkpoints")).isTrue();
    }

    @Test
    void stream_and_dcb_capabilities_initialize_both_index_sets() {
        new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM, DCB).build());

        assertThat(indexNames()).contains(STREAM_INDEX, POSITION_INDEX, DCB_TAGS_INDEX, TYPE_POSITION_INDEX, DCB_TAGS_POSITION_INDEX);
    }




    @Test
    void stream_to_stream_and_dcb_preserves_stream_reads_and_enables_dcb_for_new_events() {
        SpringMongoEventStore streamOnly = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM).build());
        streamOnly.write("name:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));

        SpringMongoEventStore both = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM, DCB).build());
        both.append(List.of(taggedEvent("NameChanged", "name:1")));

        assertThat(both.read("name:1").events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(both.read(tags(Tag.parse("name:1"))).events()).extracting(CloudEvent::getType).containsExactly("NameChanged");
    }

    @Test
    void dcb_to_stream_and_dcb_preserves_dcb_reads_and_enables_stream_reads_of_dcb_events() {
        SpringMongoEventStore dcbOnly = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(DCB).build());
        dcbOnly.append(List.of(taggedEvent("NameDefined", "name:1")));
        dcbOnly.append(List.of(taggedEvent("OrderPlaced", "order:1")));

        SpringMongoEventStore both = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM, DCB).build());

        assertThat(both.read(tags(Tag.parse("name:1"))).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(both.read(tags(Tag.parse("order:1"))).events()).extracting(CloudEvent::getType).containsExactly("OrderPlaced");

        // DCB-written events are still stored as normal Occurrent stream events, readable via the stream API by the
        // storage stream id the store derived for them from the events' DCB tags.
        String nameStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags(Tag.parse("name:1"))).events().get(0));
        String orderStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags(Tag.parse("order:1"))).events().get(0));
        assertThat(nameStreamId).startsWith("dcb:partition:");
        assertThat(orderStreamId).startsWith("dcb:partition:");
        assertThat(both.read(nameStreamId).events()).extracting(CloudEvent::getType).contains("NameDefined");
        assertThat(both.read(orderStreamId).events()).extracting(CloudEvent::getType).contains("OrderPlaced");
    }

    @Test
    void dcb_only_events_still_have_occurrent_stream_metadata() {
        SpringMongoEventStore dcbOnly = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(DCB).build());
        dcbOnly.append(List.of(taggedEvent("NameDefined", "name:1"), taggedEvent("NameChanged", "name:1")));

        List<CloudEvent> events = dcbOnly.read(tags(Tag.parse("name:1"))).events();

        assertThat(events).hasSize(2);
        // Appended together, so both events share the same derived partition stream, in order.
        assertThat(events).extracting(OccurrentExtensionGetter::getStreamId).allSatisfy(streamId -> assertThat(streamId).startsWith("dcb:partition:"));
        assertThat(OccurrentExtensionGetter.getStreamId(events.get(0))).isEqualTo(OccurrentExtensionGetter.getStreamId(events.get(1)));
        assertThat(events).extracting(OccurrentExtensionGetter::getStreamVersion).containsExactly(1L, 2L);
    }

    @Test
    void stream_and_dcb_to_stream_preserves_stream_reads_for_stream_and_dcb_written_events() {
        SpringMongoEventStore both = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM, DCB).build());
        both.write("name:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));
        both.append(List.of(taggedEvent("NameChanged", "name:1")));
        String dcbStreamId = OccurrentExtensionGetter.getStreamId(both.read(tags(Tag.parse("name:1"))).events().get(0));

        SpringMongoEventStore streamOnly = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM).build());

        assertThat(streamOnly.read("name:1").events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
        assertThat(dcbStreamId).startsWith("dcb:partition:");
        assertThat(streamOnly.read(dcbStreamId).events()).extracting(CloudEvent::getType).contains("NameChanged");
        assertThatThrownBy(() -> streamOnly.read(tags(Tag.parse("name:1"))))
                .isExactlyInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void write_rejects_a_dcb_tagged_event_written_through_the_plain_write_path() {
        SpringMongoEventStore both = new SpringMongoEventStore(mongoTemplate, eventStoreConfig(STREAM, DCB).build());
        CloudEvent dcbTaggedEvent = taggedEvent("NameDefined", "name:1");

        assertThatThrownBy(() -> both.write("name:1", WriteCondition.anyStreamVersion(), List.of(dcbTaggedEvent)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead.");
    }

    private List<String> indexNames() {
        return StreamSupport.stream(mongoTemplate.getCollection(EVENT_COLLECTION).listIndexes(Document.class).spliterator(), false)
                .map(index -> index.getString("name"))
                .toList();
    }

    private Document index(String name) {
        return StreamSupport.stream(mongoTemplate.getCollection(EVENT_COLLECTION).listIndexes(Document.class).spliterator(), false)
                .filter(index -> name.equals(index.getString("name")))
                .findFirst()
                .orElseThrow();
    }



    private EventStoreConfig.Builder eventStoreConfig(EventStoreCapability capability, EventStoreCapability... additionalCapabilities) {
        return eventStoreConfigBuilder().eventStoreCapabilities(capability, additionalCapabilities);
    }

    private EventStoreConfig.Builder eventStoreConfig(Set<EventStoreCapability> capabilities) {
        return eventStoreConfigBuilder().eventStoreCapabilities(capabilities);
    }

    private EventStoreConfig.Builder eventStoreConfigBuilder() {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(mongoTransactionManager)
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
