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
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.WriteCondition;
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
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStorePositionTest {

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
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".stream_position"));

    private MongoTemplate mongoTemplate;
    private MongoTransactionManager mongoTransactionManager;

    @BeforeEach
    void create_mongo_template() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".stream_position");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
    }

    @Test
    void the_position_index_exists_when_stream_position_is_enabled_on_a_stream_only_store() {
        new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withStreamPosition().build());

        assertThat(indexNames()).contains(POSITION_INDEX);
        assertThat(index(POSITION_INDEX))
                .containsEntry("key", new Document("position", 1))
                .containsEntry("unique", true)
                .containsEntry("sparse", true);
    }

    @Test
    void the_position_index_does_not_exist_when_stream_position_is_opted_out_on_a_stream_only_store() {
        SpringMongoEventStore eventStore = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withoutStreamPosition().build());

        eventStore.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));

        assertThat(indexNames()).doesNotContain(POSITION_INDEX);
    }

    @Test
    void combining_stream_position_opt_out_with_dcb_fails_fast_at_build_time() {
        assertThatThrownBy(() -> configBuilder(STREAM, DCB).withoutStreamPosition().build())
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot disable stream position when the DCB capability is enabled; a combined store must position everything.");
    }

    @Test
    void startup_logs_a_warning_but_does_not_fail_when_unpositioned_events_exist_and_backfill_is_not_required() {
        SpringMongoEventStore optedOut = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withoutStreamPosition().build());
        optedOut.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));

        // Re-opening the same collection with stream position now enabled must not fail (default is WARN, not fail),
        // even though the existing event lacks a position.
        SpringMongoEventStore positioned = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withStreamPosition().build());
        assertThat(positioned.writesPosition()).isTrue();
    }

    @Test
    void position_is_turned_off_on_an_existing_unpositioned_store_when_it_was_not_enabled_explicitly() {
        SpringMongoEventStore optedOut = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withoutStreamPosition().build());
        optedOut.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));

        // Default (not explicit) position over a collection that already has unpositioned events turns itself off,
        // rather than building the position index over the whole collection at startup.
        SpringMongoEventStore defaulted = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).build());
        assertThat(defaulted.writesPosition()).isFalse();
        assertThat(indexNames()).doesNotContain("position_1");
    }

    @Test
    void position_stays_on_by_default_for_an_empty_store() {
        SpringMongoEventStore defaulted = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).build());
        assertThat(defaulted.writesPosition()).isTrue();
    }

    @Test
    void position_stays_on_by_default_once_the_store_has_positioned_events() {
        SpringMongoEventStore first = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).build());
        first.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));

        // Re-opening a store whose events already have positions keeps position on.
        SpringMongoEventStore reopened = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).build());
        assertThat(reopened.writesPosition()).isTrue();
    }

    @Test
    void startup_fails_hard_when_unpositioned_events_exist_and_backfill_is_required() {
        SpringMongoEventStore optedOut = new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withoutStreamPosition().build());
        optedOut.write("stream:1", WriteCondition.anyStreamVersion(), List.of(event("NameDefined")));

        assertThatThrownBy(() -> new SpringMongoEventStore(mongoTemplate, configBuilder(STREAM).withStreamPosition().requireBackfilledPosition(true).build()))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageContaining("configured to require backfilled positions")
                .hasMessageContaining("doc/runbooks/position-backfill.md");
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

    private EventStoreConfig.Builder configBuilder(org.occurrent.eventstore.api.EventStoreCapability capability, org.occurrent.eventstore.api.EventStoreCapability... additionalCapabilities) {
        return new EventStoreConfig.Builder()
                .eventStoreCollectionName(EVENT_COLLECTION)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(capability, additionalCapabilities);
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
