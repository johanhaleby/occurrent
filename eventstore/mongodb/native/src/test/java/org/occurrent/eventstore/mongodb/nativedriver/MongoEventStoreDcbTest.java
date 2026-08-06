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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * The tests remaining in this class document the position gap a failed append leaves behind (ADR 84), which is a
 * real per-store variation the shared DCB conformance suites ({@link org.occurrent.tck.eventstore.blocking.DcbEventStoreConformance},
 * {@link org.occurrent.tck.eventstore.blocking.DcbStreamInteropConformance}) deliberately decline to assert either
 * way. Every other DCB behavior this store has is asserted once, by those shared suites, in
 * {@link MongoEventStoreDcbConformanceTest} and {@link MongoEventStoreDcbStreamInteropConformanceTest}.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private MongoEventStore eventStore;
    private MongoClient mongoClient;

    @BeforeEach
    void create_mongo_native_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb");
        mongoClient = MongoClients.create(connectionString);
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new MongoEventStore(mongoClient, requireNonNull(connectionString.getDatabase()), "events", config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void abandons_reserved_position_block_when_duplicate_cloud_event_fails_insert() {
        CloudEvent cloudEvent = taggedEvent("NameDefined", "name:1");
        eventStore.append(List.of(cloudEvent));

        assertThatThrownBy(() -> eventStore.append(List.of(cloudEvent)))
                .isExactlyInstanceOf(DuplicateCloudEventException.class);

        // Positions are reserved outside the transaction (ADR 0021), so the failed append abandons its reserved block
        // and position has a gap. The next successful append lands after the abandoned block.
        DcbAppendResult appendResult = eventStore.append(List.of(taggedEvent("NameChanged", "name:1")));
        assertThat(appendResult.firstSequencePosition()).isEqualTo(3);
        assertThat(appendResult.lastSequencePosition()).isEqualTo(3);
    }

    @Test
    void same_stale_append_condition_rejects_second_append_and_abandons_its_position_block() {
        DcbEventStream readModel = eventStore.read(tags(Tag.parse("name:1")));
        DcbAppendCondition appendCondition = failIfEventsMatch(tags(Tag.parse("name:1")), readModel.consistencyToken());

        DcbAppendResult firstAppend = eventStore.append(List.of(taggedEvent("NameDefined", "name:1")), appendCondition);
        assertThat(firstAppend).isEqualTo(new DcbAppendResult(1, 1, 1));

        assertThatThrownBy(() -> eventStore.append(List.of(taggedEvent("NameChanged", "name:1")), appendCondition))
                .isExactlyInstanceOf(DcbAppendConditionNotFulfilledException.class);

        // The condition-failed append abandons its reserved position block (ADR 0021), so position has a gap and the
        // next successful append lands at position 3 rather than 2.
        DcbAppendResult nextAppend = eventStore.append(List.of(taggedEvent("NameChanged", "name:2")));
        assertThat(nextAppend).isEqualTo(new DcbAppendResult(3, 3, 1));
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), java.util.Arrays.stream(tags).map(Tag::parse).toList());
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
