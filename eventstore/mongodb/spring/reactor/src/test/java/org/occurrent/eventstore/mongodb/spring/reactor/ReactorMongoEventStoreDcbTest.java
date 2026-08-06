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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionalOperator;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.all;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreDcbTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private ReactorMongoEventStore eventStore;
    private ReactiveMongoTemplate mongoTemplate;
    private ReactiveMongoTransactionManager transactionManager;
    private MongoClient mongoClient;
    private String databaseName;

    @BeforeEach
    void create_reactive_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcbreactor");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        mongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        eventStore = storeWith(STREAM, DCB);
    }

    @AfterEach
    void close_mongo_client() {
        // One client per test, shared by every store this class builds, so closing it here keeps the suite from
        // accumulating connections and threads.
        mongoClient.close();
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

    @Test
    void stream_write_rejects_a_dcb_tagged_event_reactively() {
        CloudEvent dcbTaggedEvent = taggedEvent("NameDefined", "name:1");

        StepVerifier.create(eventStore.write("name:1", Flux.just(dcbTaggedEvent)))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isExactlyInstanceOf(IllegalArgumentException.class)
                        .hasMessage("A DCB-tagged event cannot be written through the stream write(...) API, use the DCB append(...) API instead."))
                .verify();

        assertThat(requireNonNull(eventStore.read("name:1").block()).eventList().block()).isEmpty();
    }

    private static CloudEvent taggedEvent(String type, String... tags) {
        return DcbCloudEvents.withTags(event(type), java.util.Arrays.stream(tags).map(Tag::parse).toList());
    }

    // ---------------------------------------------------------------------------
    // Transaction ownership (ADR 0074), the reactive half of the rule.
    //
    // Rather than race threads and hope for a conflict, these force one: the first insert fails with a retryable
    // duplicate key, and the two tests differ only in who owns the transaction. Counting inserts shows whether the
    // store retried, which is the behaviour under test.
    // ---------------------------------------------------------------------------
    @Test
    void append_is_retried_when_the_store_owns_the_transaction() {
        AtomicInteger inserts = new AtomicInteger();
        ReactorMongoEventStore store = storeThatFailsFirstInsert(inserts);

        store.append(List.of(taggedEvent("NameDefined", "owned:1"))).block();

        assertThat(inserts.get())
                .as("Owning the transaction, the store must retry the duplicate and commit on the second attempt")
                .isEqualTo(2);
    }

    @Test
    void append_is_not_retried_when_a_caller_owns_the_transaction() {
        AtomicInteger inserts = new AtomicInteger();
        ReactorMongoEventStore store = storeThatFailsFirstInsert(inserts);
        TransactionalOperator outerTransaction = TransactionalOperator.create(transactionManager);

        Throwable failure = catchThrowable(() -> store.append(List.of(taggedEvent("NameDefined", "notowned:1")))
                .as(outerTransaction::transactional)
                .block());

        assertAll(
                () -> assertThat(failure).as("The conflict must reach the caller rather than being retried away").isNotNull(),
                () -> assertThat(inserts.get())
                        .as("Joining someone else's transaction, the store must run the body once: a conflict has aborted that transaction and no further attempt could commit")
                        .isEqualTo(1)
        );
    }

    /**
     * A store whose first event insert fails with a duplicate key, which the store treats as retryable, and which
     * counts how many inserts it was asked for.
     */
    private ReactorMongoEventStore storeThatFailsFirstInsert(AtomicInteger inserts) {
        // Reuse the client and database this test class already connected to, so the failing template and the
        // caller-owned transaction genuinely share one database.
        ReactiveMongoTemplate failingTemplate = new ReactiveMongoTemplate(mongoClient, databaseName) {
            @Override
            public <T> Flux<T> insert(java.util.Collection<? extends T> batchToSave, String collectionName) {
                if ("events".equals(collectionName) && inserts.incrementAndGet() == 1) {
                    return Flux.error(new DuplicateKeyException("Simulated partition stream-version collision"));
                }
                return super.insert(batchToSave, collectionName);
            }
        };
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events")
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        return new ReactorMongoEventStore(failingTemplate, config);
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
