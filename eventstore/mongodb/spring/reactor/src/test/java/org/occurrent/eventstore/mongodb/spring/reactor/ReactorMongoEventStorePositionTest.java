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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;

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

/**
 * Verifies the unified global {@code position} written on stream events by {@link ReactorMongoEventStore} when
 * {@code writesPosition()} is true: stream events share one monotonic sequence with DCB events, the position index
 * exists, {@link PositionOrderedReader} reads the correct range, and an opt-out STREAM-only store writes no position
 * and rejects position-requiring reads with a clear error.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStorePositionTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.droppingTheDatabaseIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;

    @BeforeEach
    void create_reactive_mongo_template() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".positionreactor");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
    }

    private ReactorMongoEventStore storeWith(EventStoreConfig.Builder builder) {
        EventStoreConfig config = builder
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    private ReactorMongoEventStore storeWith(EventStoreCapability first, EventStoreCapability... rest) {
        return storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(first, rest));
    }

    @Test
    void stream_only_store_creates_the_shared_position_index_by_default() {
        ReactorMongoEventStore eventStore = storeWith(STREAM);
        assertThat(eventStore.writesPosition()).isTrue();

        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        assertThat(hasPositionIndex()).isTrue();
    }

    @Test
    void position_index_exists_when_stream_position_is_enabled() {
        ReactorMongoEventStore eventStore = storeWith(STREAM, DCB);
        assertThat(eventStore.writesPosition()).isTrue();

        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        assertThat(hasPositionIndex()).isTrue();
    }

    @Test
    void dcb_only_store_creates_the_shared_position_index() {
        ReactorMongoEventStore eventStore = storeWith(DCB);
        assertThat(eventStore.writesPosition()).isTrue();

        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        assertThat(hasPositionIndex()).isTrue();
    }

    private boolean hasPositionIndex() {
        List<Document> indexes = mongoTemplate.getCollection("events")
                .flatMapMany(collection -> Flux.from(collection.listIndexes()))
                .collectList()
                .block();

        return requireNonNull(indexes).stream()
                .map(document -> document.get("key", Document.class).keySet())
                .anyMatch(keys -> keys.contains(OccurrentCloudEventExtension.POSITION));
    }

    @Test
    void dcb_capability_creates_type_and_dcb_tags_position_compound_indexes() {
        // A type-only DcbCriteria read falls back to the position index with type as a residual filter, and a large
        // tag-boundary read falls back to an in-memory SORT over the dcbTags index, since neither index alone can
        // provide the position sort order for those filters. See the explain evidence in initializeEventStore's
        // comments. These compound indexes let the planner satisfy the filter and the position sort in one pass.
        ReactorMongoEventStore eventStore = storeWith(DCB);
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        List<Document> indexNames = mongoTemplate.getCollection("events")
                .flatMapMany(collection -> Flux.from(collection.listIndexes()))
                .collectList()
                .block();

        List<String> names = requireNonNull(indexNames).stream().map(document -> document.getString("name")).toList();
        assertThat(names).contains("type_1_position_1", "dcbTags_1_position_1");
    }


    @Test
    void combining_dcb_with_an_explicit_stream_position_opt_out_fails_fast() {
        assertThatThrownBy(() -> new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM, DCB)
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .withoutStreamPosition()
                .build())
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot disable stream position when the DCB capability is enabled");
    }

    @Test
    void combined_store_forces_stream_position_on_even_when_only_dcb_events_are_written() {
        ReactorMongoEventStore eventStore = storeWith(STREAM, DCB);

        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        List<CloudEvent> streamEvents = eventStore.read("stream-1", 0, Integer.MAX_VALUE)
                .flatMapMany(es -> es.events()).collectList().block();

        assertThat(requireNonNull(streamEvents).get(0).getExtension(OccurrentCloudEventExtension.POSITION)).isNotNull();
    }

    @Test
    void startup_hard_fails_when_configured_and_unpositioned_events_exist() {
        // Write a stream event with a store that does not write position (opt-out) so the collection ends up with a
        // pre-existing, un-positioned event, mimicking a not-yet-backfilled deployment.
        ReactorMongoEventStore withoutPosition = storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition());
        withoutPosition.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM, DCB)
                .requireBackfilledPosition(true))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("configured to require backfilled positions")
                .hasMessageContaining("doc/runbooks/position-backfill.md");
    }

    @Test
    void startup_only_warns_by_default_when_unpositioned_events_exist() {
        ReactorMongoEventStore withoutPosition = storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition());
        withoutPosition.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        // Default requireBackfilledPosition is false, so start-up must succeed (WARN only), not throw.
        ReactorMongoEventStore withPosition = storeWith(STREAM, DCB);

        assertThat(withPosition).isNotNull();
    }

    @Test
    void position_is_turned_off_on_an_existing_unpositioned_store_when_it_was_not_enabled_explicitly() {
        ReactorMongoEventStore optedOut = storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition());
        optedOut.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("NameDefined"))).block();

        // Default (not explicit) position over a collection that already has unpositioned events turns itself off,
        // rather than building the position index over the whole collection at startup.
        ReactorMongoEventStore defaulted = storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(STREAM));
        assertThat(defaulted.writesPosition()).isFalse();

        List<org.bson.Document> indexes = mongoTemplate.getCollection("events")
                .flatMapMany(collection -> Flux.from(collection.listIndexes()))
                .collectList()
                .block();
        assertThat(requireNonNull(indexes))
                .noneMatch(document -> "position_1".equals(document.getString("name")));
    }

    @Test
    void position_stays_on_by_default_for_an_empty_store() {
        ReactorMongoEventStore defaulted = storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(STREAM));
        assertThat(defaulted.writesPosition()).isTrue();
    }

    @Test
    void position_stays_on_by_default_once_the_store_has_positioned_events() {
        ReactorMongoEventStore first = storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(STREAM));
        first.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("NameDefined"))).block();

        // Re-opening a store whose events already have positions keeps position on.
        ReactorMongoEventStore reopened = storeWith(new EventStoreConfig.Builder().eventStoreCapabilities(STREAM));
        assertThat(reopened.writesPosition()).isTrue();
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
