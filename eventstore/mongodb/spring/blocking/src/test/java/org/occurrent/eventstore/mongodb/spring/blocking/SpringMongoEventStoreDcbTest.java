/*
 * Copyright 2020 Johan Haleby
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.*;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

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
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb"));

    private SpringMongoEventStore eventStore;

    @BeforeEach
    void create_mongo_spring_blocking_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb");
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoTemplate mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Test
    void dcb_writes_are_visible_to_normal_event_store_queries() {
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));

        assertThat(eventStore.all(SortBy.natural(SortBy.SortDirection.ASCENDING)))
                .extracting(CloudEvent::getType)
                .containsExactly("NameDefined");
        CloudEvent dcbEvent = eventStore.read(tagsAllOf("name:1")).events().get(0);
        assertThat(DcbCloudEvents.getTags(dcbEvent)).containsExactly("name:1");
        assertThat(dcbEvent.getExtension(DcbCloudEvents.POSITION)).isEqualTo(1L);
    }

    @Test
    void reads_events_matching_type_or_all_tags_after_sequence_position() {
        eventStore.append("dcb:partition:0", List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1", "tenant:1"),
                taggedEvent("OrderPlaced", "order:1")));

        DcbEventStream eventStream = eventStore.read(
                fromItems(List.of(
                        DcbQueryItem.types(List.of("OrderPlaced")),
                        DcbQueryItem.tagsAllOf(List.of("name:1", "tenant:1")))),
                DcbReadOptions.afterSequencePosition(1));

        assertThat(eventStream.events())
                .extracting(CloudEvent::getType)
                .containsExactly("NameChanged", "OrderPlaced");
        assertThat(eventStream.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void rejects_append_when_matching_event_exists_after_condition_position() {
        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")));
        DcbEventStream readModel = eventStore.read(tagsAllOf("name:1"));

        eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:1")));

        assertThatThrownBy(() -> eventStore.append(
                "dcb:partition:0",
                List.of(taggedEvent("NameChanged", "name:1")),
                failIfEventsMatch(tagsAllOf("name:1"), readModel.lastSequencePosition())))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);
    }

    @Test
    void rolls_back_position_reservation_when_duplicate_cloud_event_fails_insert() {
        CloudEvent cloudEvent = taggedEvent("NameDefined", "name:1");
        eventStore.append("dcb:partition:0", List.of(cloudEvent));

        assertThatThrownBy(() -> eventStore.append("dcb:partition:0", List.of(cloudEvent)))
                .isExactlyInstanceOf(DuplicateCloudEventException.class);

        DcbAppendResult appendResult = eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:1")));
        assertThat(appendResult.firstSequencePosition()).isEqualTo(2);
        assertThat(appendResult.lastSequencePosition()).isEqualTo(2);
    }

    @Test
    void same_stale_append_condition_rejects_second_append_without_advancing_position() {
        DcbEventStream readModel = eventStore.read(tagsAllOf("name:1"));
        DcbAppendCondition appendCondition = failIfEventsMatch(tagsAllOf("name:1"), readModel.lastSequencePosition());

        DcbAppendResult firstAppend = eventStore.append("dcb:partition:0", List.of(taggedEvent("NameDefined", "name:1")), appendCondition);
        assertThat(firstAppend).isEqualTo(new DcbAppendResult(1, 1, 1));

        assertThatThrownBy(() -> eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:1")), appendCondition))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

        DcbAppendResult nextAppend = eventStore.append("dcb:partition:0", List.of(taggedEvent("NameChanged", "name:2")));
        assertThat(nextAppend).isEqualTo(new DcbAppendResult(2, 2, 1));
    }

    @Test
    void does_not_inspect_payload_when_matching_tags() {
        CloudEvent cloudEvent = DcbCloudEvents.withTags(CloudEventBuilder.v1(event("NameDefined"))
                .withDataContentType("application/json")
                .withData("{\"tags\":[\"name:1\"]}".getBytes(UTF_8))
                .build(), Set.of("name:2"));

        eventStore.append("dcb:partition:0", List.of(cloudEvent));

        assertThat(eventStore.read(tagsAllOf("name:1")).events()).isEmpty();
        assertThat(eventStore.read(tagsAllOf("name:2")).events()).hasSize(1);
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), Set.of(tags));
    }

    private static CloudEvent event(String type) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
    }
}
