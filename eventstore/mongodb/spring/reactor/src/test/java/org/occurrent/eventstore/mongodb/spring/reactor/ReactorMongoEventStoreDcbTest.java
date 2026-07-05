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
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.*;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbTest {

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
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbreactor"));

    private ReactorMongoEventStore eventStore;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;

    @BeforeEach
    void create_reactive_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbreactor");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        eventStore = storeWith(STREAM, DCB);
    }

    private ReactorMongoEventStore storeWith(EventStoreCapability first, EventStoreCapability... rest) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(first, rest)
                .build();
        return new ReactorMongoEventStore(mongoTemplate, config);
    }

    @Test
    void dcb_write_is_readable_by_tag_and_carries_position() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        DcbEventStream stream = requireNonNull(eventStore.read(tags(Tag.parse("name:1"))).block());
        assertAll(
                () -> assertThat(stream.events()).extracting(CloudEvent::getType).containsExactly("NameDefined"),
                () -> assertThat(DcbCloudEvents.getTags(stream.events().get(0))).containsExactly(Tag.parse("name:1")),
                () -> assertThat(stream.events().get(0).getExtension(OccurrentCloudEventExtension.POSITION)).isEqualTo(1L),
                () -> assertThat(stream.lastSequencePosition()).isEqualTo(1)
        );
    }

    @Test
    void reads_events_matching_type_or_all_tags_after_sequence_position() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameChanged", "name:1", "tenant:1"),
                taggedEvent("OrderPlaced", "order:1"))).block();

        DcbEventStream stream = requireNonNull(eventStore.read(
                anyOf(List.of(types(List.of("OrderPlaced")), tags(List.of(Tag.parse("name:1"), Tag.parse("tenant:1"))))),
                DcbReadOptions.afterPosition(1)).block());

        assertAll(
                () -> assertThat(stream.events()).extracting(CloudEvent::getType).containsExactly("NameChanged", "OrderPlaced"),
                () -> assertThat(stream.lastSequencePosition()).isEqualTo(3)
        );
    }

    @Test
    void reads_tagged_events_except_excluded_types() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("NameSnapshot", "name:1"),
                taggedEvent("OrderPlaced", "order:1"))).block();

        DcbEventStream stream = eventStore.read(tags(List.of(Tag.parse("name:1"))).excludingTypes(List.of("NameSnapshot"))).block();

        assertThat(requireNonNull(stream).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
    }

    @Test
    void conditional_append_with_stale_token_is_rejected() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();
        DcbEventStream readModel = eventStore.read(tags(Tag.parse("name:1"))).block();

        // Another append on the same boundary advances the marker.
        eventStore.append(List.of(taggedEvent("NameChanged", "name:1"))).block();

        StepVerifier.create(eventStore.append(
                        List.of(taggedEvent("NameChanged", "name:1")),
                        failIfEventsMatch(tags(Tag.parse("name:1")), requireNonNull(readModel).consistencyToken())))
                .expectError(DcbAppendConditionNotFulfilledException.class)
                .verify();
    }

    @Test
    void conditional_append_with_fresh_token_succeeds() {
        DcbEventStream readModel = eventStore.read(tags(Tag.parse("name:1"))).block();

        DcbAppendResult result = eventStore.append(
                List.of(taggedEvent("NameDefined", "name:1")),
                failIfEventsMatch(tags(Tag.parse("name:1")), requireNonNull(readModel).consistencyToken())).block();

        assertThat(requireNonNull(result).firstSequencePosition()).isEqualTo(1);
    }

    @Test
    void no_token_guard_rejects_when_a_matching_event_exists() {
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        StepVerifier.create(eventStore.append(
                        List.of(taggedEvent("NameChanged", "name:1")),
                        failIfEventsMatch(tags(Tag.parse("name:1")))))
                .expectError(DcbAppendConditionNotFulfilledException.class)
                .verify();
    }

    @Test
    void unconditional_append_makes_a_concurrent_conditional_append_on_the_same_tag_fail() {
        // Read the boundary while empty: token is 0.
        DcbEventStream readModel = eventStore.read(tags(Tag.parse("name:1"))).block();

        // An unconditional append must still bump the tag marker, so the stale-token conditional append below is rejected.
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        StepVerifier.create(eventStore.append(
                        List.of(taggedEvent("NameChanged", "name:1")),
                        failIfEventsMatch(tags(Tag.parse("name:1")), requireNonNull(readModel).consistencyToken())))
                .expectError(DcbAppendConditionNotFulfilledException.class)
                .verify();
    }

    @Test
    void exists_and_count_match_the_query() {
        eventStore.append(List.of(
                taggedEvent("NameDefined", "name:1"),
                taggedEvent("OrderPlaced", "order:1"))).block();

        assertAll(
                () -> assertThat(eventStore.exists(tags(Tag.parse("name:1"))).block()).isTrue(),
                () -> assertThat(eventStore.exists(tags(Tag.parse("missing:1"))).block()).isFalse(),
                () -> assertThat(eventStore.count(types(List.of("NameDefined", "OrderPlaced"))).block()).isEqualTo(2L)
        );
    }

    @Test
    void concurrent_appends_to_disjoint_boundaries_both_succeed() {
        Long appended = Flux.range(0, 8)
                .parallel(8)
                .runOn(Schedulers.boundedElastic())
                .flatMap(i -> eventStore.append(List.of(taggedEvent("Defined", "entity:" + i))))
                .sequential()
                .count()
                .block();

        assertAll(
                () -> assertThat(appended).isEqualTo(8L),
                () -> assertThat(eventStore.count(all()).block()).isEqualTo(8L)
        );
    }

    @Test
    void dcb_methods_are_rejected_when_only_stream_is_enabled() {
        ReactorMongoEventStore streamOnly = storeWith(STREAM);
        StepVerifier.create(streamOnly.append(List.of(taggedEvent("NameDefined", "name:1"))))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void stream_methods_are_rejected_when_only_dcb_is_enabled() {
        ReactorMongoEventStore dcbOnly = storeWith(DCB);
        StepVerifier.create(dcbOnly.exists("some-stream"))
                .expectError(UnsupportedOperationException.class)
                .verify();
    }

    @Test
    void empty_capability_set_is_rejected() {
        assertThat(catchThrowable(() -> new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(Set.of())
                .build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void same_boundary_serialization_under_contention() throws Exception {
        int threadCount = 8;
        int iterations = 3;

        for (int i = 0; i < iterations; i++) {
            Tag tag = Tag.of("contention", String.valueOf(i));
            DcbConsistencyToken boundaryToken = requireNonNull(eventStore.read(tags(tag)).block()).consistencyToken();
            DcbAppendCondition condition = failIfEventsMatch(tags(tag), boundaryToken);

            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger condFailCount = new AtomicInteger(0);
            AtomicInteger unexpectedFailCount = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    barrier.await();
                    try {
                        eventStore.append(List.of(taggedEvent("SomeEvent", tag.canonical())), condition).block();
                        successCount.incrementAndGet();
                    } catch (DcbAppendConditionNotFulfilledException e) {
                        condFailCount.incrementAndGet();
                    } catch (Exception e) {
                        unexpectedFailCount.incrementAndGet();
                    }
                    return null;
                }));
            }

            pool.shutdown();
            for (Future<?> f : futures) {
                f.get();
            }

            int iteration = i;
            assertAll(
                    () -> assertThat(successCount.get()).as("iteration %d: exactly one append wins (tag=%s)", iteration, tag).isEqualTo(1),
                    () -> assertThat(unexpectedFailCount.get()).as("iteration %d: no unexpected failures, transient errors must be retried internally", iteration).isZero(),
                    () -> assertThat(condFailCount.get()).as("iteration %d: the other %d appends fail the condition", iteration, threadCount - 1).isEqualTo(threadCount - 1)
            );
        }
    }

    @Test
    void dcb_all_only_matches_dcb_written_events_not_stream_written_events() {
        eventStore.write("stream:1", Flux.just(event("OrderPlaced"))).block();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        DcbEventStream stream = eventStore.read(all()).block();

        assertThat(requireNonNull(stream).events()).extracting(CloudEvent::getType).containsExactly("NameDefined");
    }

    @Test
    void dcb_type_only_criterion_does_not_match_a_stream_written_event_of_that_type() {
        eventStore.write("stream:1", Flux.just(event("OrderPlaced"))).block();
        eventStore.append(List.of(taggedEvent("NameDefined", "name:1"))).block();

        DcbEventStream stream = eventStore.read(types(List.of("OrderPlaced"))).block();

        assertThat(requireNonNull(stream).events()).isEmpty();
    }

    @Test
    void exists_and_count_are_false_and_zero_for_a_store_with_only_stream_written_events() {
        eventStore.write("stream:1", Flux.just(event("OrderPlaced"))).block();

        assertAll(
                () -> assertThat(eventStore.exists(all()).block()).isFalse(),
                () -> assertThat(eventStore.count(all()).block()).isZero()
        );
    }

    @Test
    void dcb_match_all_query_is_index_backed_on_dcb_tags_field() {
        eventStore.append(List.of(taggedEvent("SeedType", "explain:tag"))).block();

        // Bias the planner the same way ADR 49's spike did: many more position-only stream
        // documents than dcbTags-carrying ones, so the position index is only cheaper if the
        // planner ignores dcbTags selectivity. Without this skew, a single-document collection
        // ties and the planner may pick either index, defeating the point of the assertion below.
        List<org.bson.Document> streamOnlyDocuments = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            streamOnlyDocuments.add(new org.bson.Document("id", "explain-stream-only-" + i)
                    .append("source", SOURCE.toString())
                    .append("streamid", "explain-stream-only-" + i)
                    .append("streamversion", 0L)
                    .append("position", 1_000 + i));
        }
        requireNonNull(mongoTemplate.getCollection("events")
                .flatMap(collection -> reactor.core.publisher.Mono.from(collection.insertMany(streamOnlyDocuments)))
                .block());

        // Mirrors what toDcbMongoQuery(...) now builds for DcbCriteria.all(): the position window ANDed with an
        // existence check on dcbTags, so a MatchAll/type-only read can never match a stream-written event (which has
        // no dcbTags field) and does so via an index rather than a collection scan.
        org.bson.Document matchAllQuery = new org.bson.Document("$and", List.of(
                new org.bson.Document("position", new org.bson.Document("$gt", 0).append("$lte", 1_000_000)),
                new org.bson.Document("dcbTags", new org.bson.Document("$exists", true))
        ));

        org.bson.Document explain = requireNonNull(mongoTemplate.getCollection("events")
                .flatMap(collection -> reactor.core.publisher.Mono.from(collection.find(matchAllQuery).explain(com.mongodb.ExplainVerbosity.QUERY_PLANNER)))
                .block());

        assertThat(extractWinningPlanStage(explain))
                .as("MatchAll DCB read should be index-backed via the sparse dcbTags index, not a COLLSCAN. Full explain: %s", explain.toJson())
                .isEqualTo("IXSCAN");
        assertThat(extractWinningIndexName(explain))
                .as("The winning plan should use the dcbTags index specifically. Full explain: %s", explain.toJson())
                .isEqualTo("dcbTags_1");
    }

    private static String extractWinningPlanStage(org.bson.Document explainDoc) {
        org.bson.Document queryPlanner = explainDoc.get("queryPlanner", org.bson.Document.class);
        if (queryPlanner == null) {
            return "UNKNOWN";
        }
        org.bson.Document plan = queryPlanner.get("winningPlan", org.bson.Document.class);
        int depth = 0;
        while (plan != null && depth++ < 20) {
            String stage = plan.getString("stage");
            if ("IXSCAN".equals(stage) || "COLLSCAN".equals(stage) || "COUNT_SCAN".equals(stage)) {
                return stage;
            }
            org.bson.Document inputStage = plan.get("inputStage", org.bson.Document.class);
            if (inputStage == null) {
                List<?> inputStages = plan.getList("inputStages", org.bson.Document.class);
                if (inputStages != null && !inputStages.isEmpty()) {
                    plan = (org.bson.Document) inputStages.get(0);
                    continue;
                }
                return stage == null ? "UNKNOWN" : stage;
            }
            plan = inputStage;
        }
        return "UNKNOWN";
    }

    private static String extractWinningIndexName(org.bson.Document explainDoc) {
        org.bson.Document queryPlanner = explainDoc.get("queryPlanner", org.bson.Document.class);
        org.bson.Document plan = queryPlanner == null ? null : queryPlanner.get("winningPlan", org.bson.Document.class);
        int depth = 0;
        while (plan != null && depth++ < 20) {
            if ("IXSCAN".equals(plan.getString("stage"))) {
                return plan.getString("indexName");
            }
            org.bson.Document inputStage = plan.get("inputStage", org.bson.Document.class);
            if (inputStage == null) {
                List<?> inputStages = plan.getList("inputStages", org.bson.Document.class);
                plan = inputStages != null && !inputStages.isEmpty() ? (org.bson.Document) inputStages.get(0) : null;
            } else {
                plan = inputStage;
            }
        }
        return null;
    }

    @Test
    void appending_an_event_with_a_duplicate_id_and_source_fails_fast_without_retrying() {
        CloudEvent event = taggedEvent("NameDefined", "name:1");
        eventStore.append(List.of(event)).block();

        // The same id and source again is a duplicate CloudEvent, a business error, not a transient conflict or the
        // cold-start marker race. It must surface as DuplicateCloudEventException and must not be fed into the append
        // backoff, so it fails fast instead of running the full 15-attempt retry before giving up.
        long start = System.nanoTime();
        StepVerifier.create(eventStore.append(List.of(event)))
                .expectError(DuplicateCloudEventException.class)
                .verify(Duration.ofSeconds(10));
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();
        assertThat(elapsedMillis).as("a duplicate CloudEvent must not be retried, so it fails well before the multi-second backoff").isLessThan(3000L);
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
