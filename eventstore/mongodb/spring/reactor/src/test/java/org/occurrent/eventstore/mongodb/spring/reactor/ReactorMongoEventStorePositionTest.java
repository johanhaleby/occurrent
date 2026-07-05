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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
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
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27017:27017");
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".positionreactor"));

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
    void stream_events_get_a_monotonic_position_shared_with_dcb_events_when_both_capabilities_enabled() {
        ReactorMongoEventStore eventStore = storeWith(STREAM, DCB);

        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();
        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1"))).block();

        List<CloudEvent> streamEvents = eventStore.read("stream-1", 0, Integer.MAX_VALUE)
                .flatMapMany(es -> es.events()).collectList().block();

        assertThat(requireNonNull(streamEvents)).hasSize(1);
        assertThat(streamEvents.get(0).getExtension(OccurrentCloudEventExtension.POSITION)).isEqualTo(2L);
        assertThat(eventStore.currentPosition().block()).isEqualTo(3L);
    }

    // There is currently no way to opt a STREAM-only store IN to position, so today the only way to get a STREAM
    // store where writesPosition() is true is to combine it with DCB, which forces stream position on
    // (writesPosition() = DCB present || (STREAM present && streamPositionEnabled)).
    // These tests use a combined STREAM+DCB store to exercise the position-enabled behavior on stream-written events.

    @Test
    void position_index_exists_when_stream_position_is_enabled() {
        ReactorMongoEventStore eventStore = storeWith(STREAM, DCB);
        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        List<org.bson.Document> indexes = mongoTemplate.getCollection("events")
                .flatMapMany(collection -> reactor.core.publisher.Flux.from(collection.listIndexes()))
                .collectList()
                .block();

        assertThat(requireNonNull(indexes))
                .extracting(document -> document.get("key", org.bson.Document.class).keySet())
                .anyMatch(keys -> keys.contains(OccurrentCloudEventExtension.POSITION));
    }

    @Test
    void position_ordered_reader_returns_events_within_the_requested_range() {
        ReactorMongoEventStore eventStore = storeWith(STREAM, DCB);
        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("A"), event("B"))).block();
        eventStore.write("stream-2", WriteCondition.anyStreamVersion(), Flux.just(event("C"), event("D"))).block();

        List<CloudEvent> events = eventStore.readInPositionOrder(Filter.all(), PositionRange.between(1, 3)).collectList().block();

        assertThat(requireNonNull(events)).extracting(CloudEvent::getType).containsExactly("B", "C");
    }

    @Test
    void position_ordered_reader_clamps_to_the_high_watermark_at_read_time() {
        ReactorMongoEventStore eventStore = storeWith(STREAM, DCB);
        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("A"), event("B"))).block();

        List<CloudEvent> events = eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()).collectList().block();

        assertThat(requireNonNull(events)).extracting(CloudEvent::getType).containsExactly("A", "B");
    }

    @Test
    void opt_out_stream_only_store_writes_no_position_on_stream_events() {
        ReactorMongoEventStore eventStore = storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition());

        eventStore.write("stream-1", WriteCondition.anyStreamVersion(), Flux.just(event("SomethingHappened"))).block();

        List<CloudEvent> streamEvents = eventStore.read("stream-1", 0, Integer.MAX_VALUE)
                .flatMapMany(es -> es.events()).collectList().block();

        assertThat(requireNonNull(streamEvents)).hasSize(1);
        assertThat(streamEvents.get(0).getExtension(OccurrentCloudEventExtension.POSITION)).isNull();
    }

    @Test
    void opt_out_stream_only_store_rejects_current_position_with_a_clear_error() {
        ReactorMongoEventStore eventStore = storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition());

        StepVerifier.create(eventStore.currentPosition())
                .expectErrorSatisfies(throwable -> assertThat(throwable)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContaining("does not write a position"))
                .verify();
    }

    @Test
    void opt_out_stream_only_store_rejects_position_ordered_reads_with_a_clear_error() {
        ReactorMongoEventStore eventStore = storeWith(new EventStoreConfig.Builder()
                .eventStoreCapabilities(STREAM)
                .withoutStreamPosition());

        StepVerifier.create(eventStore.readInPositionOrder(Filter.all(), PositionRange.fromBeginning()))
                .expectErrorSatisfies(throwable -> assertThat(throwable)
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContaining("does not write a position"))
                .verify();
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
                .hasMessageContaining("position-backfill");
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
