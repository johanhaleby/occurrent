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
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tags;
import static org.occurrent.eventstore.api.dcb.DcbQuery.types;

/**
 * Adversarial concurrency tests for the native driver DCB write path (ADR 0021).
 * <p>
 * Each scenario drives real threads to a barrier so appends race in earnest. Assertions focus on safety invariants:
 * write-skew is NEVER permitted (never two concurrent conflicting successes), and
 * DcbAppendConditionNotFulfilledException is the only loser exception (transient errors must be retried internally).
 */
@Testcontainers
@Timeout(180)
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreDcbConcurrencyTest {

    private static final URI SOURCE = URI.create("urn:test:concurrency");
    private static final int ITERATIONS = 50;

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
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(
            new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_concurrency"));

    private MongoEventStore eventStore;
    private MongoClient mongoClient;
    private String databaseName;
    private static final String COLLECTION = "events";

    @BeforeEach
    void create_mongo_native_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_concurrency");
        mongoClient = MongoClients.create(connectionString);
        databaseName = requireNonNull(connectionString.getDatabase());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new MongoEventStore(mongoClient, databaseName, COLLECTION, config);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void type_vs_tag_write_skew_is_prevented() throws Exception {
        AtomicLong streamCounter = new AtomicLong(0);
        MongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:stream:" + streamCounter.getAndIncrement());

        int successes = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            String type = "TypeX_" + i;
            String tag = "tag-x-" + i;

            DcbEventStream boundaryA = isolatedStore.read(types(type));
            DcbEventStream boundaryB = isolatedStore.read(tags(tag));
            DcbConsistencyToken tokenA = boundaryA.consistencyToken();
            DcbConsistencyToken tokenB = boundaryB.consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(types(type), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(tags(tag), tokenB);

            CloudEvent eventA = taggedEvent(type, tag);
            CloudEvent eventB = taggedEvent(type, tag);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(eventA), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(eventB), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            pool.shutdown();
            boolean aSucceeded = futA.get();
            boolean bSucceeded = futB.get();

            int iterSuccesses = (aSucceeded ? 1 : 0) + (bSucceeded ? 1 : 0);

            assertThat(iterSuccesses)
                    .as("Iteration %d: both appends succeeded (type-vs-tag write skew)", i)
                    .isLessThanOrEqualTo(1);
            assertThat(iterSuccesses)
                    .as("Iteration %d: neither append succeeded", i)
                    .isGreaterThanOrEqualTo(1);

            if (aSucceeded || bSucceeded) successes++;
        }

        assertThat(successes).isEqualTo(ITERATIONS);
    }

    @Test
    void tag_vs_tag_write_skew_is_prevented() throws Exception {
        AtomicLong streamCounter = new AtomicLong(0);
        MongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:stream:" + streamCounter.getAndIncrement());

        for (int i = 0; i < ITERATIONS; i++) {
            String sharedTag = "shared-tag-" + i;
            String extraTagA = "extra-a-" + i;

            DcbConsistencyToken tokenA = isolatedStore.read(tags(sharedTag)).consistencyToken();
            DcbConsistencyToken tokenB = isolatedStore.read(tags(sharedTag, extraTagA)).consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(tags(sharedTag), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(tags(sharedTag, extraTagA), tokenB);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(taggedEvent("SomeType", sharedTag, extraTagA)), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(taggedEvent("SomeType", sharedTag, extraTagA)), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            pool.shutdown();
            boolean aSucceeded = futA.get();
            boolean bSucceeded = futB.get();

            int iterSuccesses = (aSucceeded ? 1 : 0) + (bSucceeded ? 1 : 0);

            assertThat(iterSuccesses)
                    .as("Iteration %d: both tag-vs-tag appends succeeded (write skew)", i)
                    .isLessThanOrEqualTo(1);
            assertThat(iterSuccesses)
                    .as("Iteration %d: neither tag-vs-tag append succeeded", i)
                    .isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void type_vs_type_write_skew_is_prevented() throws Exception {
        AtomicLong streamCounter = new AtomicLong(0);
        MongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:stream:" + streamCounter.getAndIncrement());

        for (int i = 0; i < ITERATIONS; i++) {
            String sharedType = "SharedType_" + i;
            String tag = "tt-tag-" + i;

            DcbConsistencyToken tokenA = isolatedStore.read(types(sharedType)).consistencyToken();
            DcbConsistencyToken tokenB = isolatedStore.read(types(sharedType)).consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(types(sharedType), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(types(sharedType), tokenB);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(taggedEvent(sharedType, tag)), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(taggedEvent(sharedType, tag)), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            pool.shutdown();
            boolean aSucceeded = futA.get();
            boolean bSucceeded = futB.get();

            int iterSuccesses = (aSucceeded ? 1 : 0) + (bSucceeded ? 1 : 0);

            assertThat(iterSuccesses)
                    .as("Iteration %d: both type-vs-type appends succeeded (write skew)", i)
                    .isLessThanOrEqualTo(1);
            assertThat(iterSuccesses)
                    .as("Iteration %d: neither type-vs-type append succeeded", i)
                    .isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void same_boundary_serialization_under_contention() throws Exception {
        int threadCount = 8;

        for (int i = 0; i < ITERATIONS; i++) {
            String tag = "contention-" + i;

            DcbConsistencyToken boundaryToken = eventStore.read(tags(tag)).consistencyToken();
            DcbAppendCondition condition = failIfEventsMatch(tags(tag), boundaryToken);

            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger condFailCount = new AtomicInteger(0);
            AtomicInteger unexpectedFailCount = new AtomicInteger(0);
            List<Future<Void>> futures = new ArrayList<>();

            final int iteration = i;
            for (int t = 0; t < threadCount; t++) {
                final int threadIdx = t;
                futures.add(pool.submit(() -> {
                    barrier.await();
                    try {
                        eventStore.append(List.of(taggedEvent("SomeEvent", tag)), condition);
                        successCount.incrementAndGet();
                    } catch (DcbAppendConditionNotFulfilledException e) {
                        condFailCount.incrementAndGet();
                    } catch (Exception e) {
                        unexpectedFailCount.incrementAndGet();
                        System.err.println("Unexpected exception in iteration " + iteration + " thread " + threadIdx + ": " + e);
                    }
                    return null;
                }));
            }

            pool.shutdown();
            for (Future<Void> f : futures) {
                f.get();
            }

            assertThat(successCount.get())
                    .as("Iteration %d: expected exactly one success under contention (tag=%s)", i, tag)
                    .isEqualTo(1);
            assertThat(unexpectedFailCount.get())
                    .as("Iteration %d: unexpected (non-DcbAppendConditionNotFulfilledException) failures must be zero (transient errors must be retried internally)", i)
                    .isZero();
            assertThat(condFailCount.get())
                    .as("Iteration %d: expected %d DcbAppendConditionNotFulfilledException failures", i, threadCount - 1)
                    .isEqualTo(threadCount - 1);
        }
    }

    @Test
    void disjoint_boundaries_do_not_falsely_serialize() throws Exception {
        int threadCount = 8;

        MongoEventStore disjointStore = buildEventStoreWithStreamIdGenerator(
                tags -> "disjoint:stream:" + String.join(",", new java.util.TreeSet<>(tags)));

        for (int i = 0; i < ITERATIONS; i++) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "disjoint-iter" + i + "-t" + t;
                final String distinctType = "DisjointEvent-iter" + i + "-t" + t;
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = disjointStore.read(tags(distinctTag)).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tags(distinctTag), token);
                    barrier.await();
                    try {
                        disjointStore.append(List.of(taggedEvent(distinctType, distinctTag)), cond);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        failCount.incrementAndGet();
                        System.err.println("Unexpected failure for disjoint tag " + distinctTag + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
                    }
                    return null;
                }));
            }

            pool.shutdown();
            for (Future<Void> f : futures) {
                f.get();
            }

            assertThat(successCount.get())
                    .as("Iteration %d: all %d disjoint-boundary appends should succeed (no false conflicts)", i, threadCount)
                    .isEqualTo(threadCount);
            assertThat(failCount.get())
                    .as("Iteration %d: no disjoint-boundary append should fail", i)
                    .isZero();
        }
    }

    @Test
    void disjoint_boundaries_sharing_a_partition_stream_are_retried_to_success() throws Exception {
        int threadCount = 6;
        MongoEventStore sharedStreamStore = buildEventStoreWithStreamIdGenerator(tags -> "shared:partition:stream");

        for (int i = 0; i < ITERATIONS; i++) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            List<Throwable> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "shared-iter" + i + "-t" + t;
                final String distinctType = "SharedStreamEvent-iter" + i + "-t" + t;
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = sharedStreamStore.read(tags(distinctTag)).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tags(distinctTag), token);
                    barrier.await();
                    try {
                        sharedStreamStore.append(List.of(taggedEvent(distinctType, distinctTag)), cond);
                        successCount.incrementAndGet();
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                    return null;
                }));
            }

            pool.shutdown();
            for (Future<Void> f : futures) {
                f.get();
            }

            assertThat(failures)
                    .as("Iteration %d: disjoint boundaries on a shared stream must all be retried to success, not fail", i)
                    .isEmpty();
            assertThat(successCount.get())
                    .as("Iteration %d: all %d appends should succeed", i, threadCount)
                    .isEqualTo(threadCount);
        }
    }

    @Test
    void multi_marker_boundary_serialization_under_contention() throws Exception {
        int threadCount = 8;

        for (int i = 0; i < ITERATIONS; i++) {
            String tag1 = "mm-t1-" + i;
            String tag2 = "mm-t2-" + i;

            DcbQuery multiMarkerQuery = tags(tag1, tag2);
            DcbConsistencyToken boundaryToken = eventStore.read(multiMarkerQuery).consistencyToken();
            DcbAppendCondition condition = failIfEventsMatch(multiMarkerQuery, boundaryToken);

            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger condFailCount = new AtomicInteger(0);
            AtomicReference<Throwable> firstUnexpected = new AtomicReference<>();
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    barrier.await();
                    try {
                        eventStore.read(multiMarkerQuery).consistencyToken();
                        eventStore.append(List.of(taggedEvent("MultiMarkerEvent", tag1, tag2)), condition);
                        successCount.incrementAndGet();
                    } catch (DcbAppendConditionNotFulfilledException e) {
                        condFailCount.incrementAndGet();
                    } catch (Exception e) {
                        firstUnexpected.compareAndSet(null, e);
                    }
                    return null;
                }));
            }

            pool.shutdown();
            for (Future<Void> f : futures) {
                f.get();
            }

            Throwable unexpected = firstUnexpected.get();
            if (unexpected != null) {
                throw new AssertionError("Iteration " + i + ": unexpected (non-DcbAppendConditionNotFulfilledException) failure under multi-marker contention", unexpected);
            }

            assertThat(successCount.get())
                    .as("Iteration %d: expected exactly one success under multi-marker contention (tags=%s,%s)", i, tag1, tag2)
                    .isEqualTo(1);
            assertThat(condFailCount.get())
                    .as("Iteration %d: expected %d DcbAppendConditionNotFulfilledException failures", i, threadCount - 1)
                    .isEqualTo(threadCount - 1);
        }
    }

    @Test
    void tokenless_conditional_append_prevents_double_commit() throws Exception {
        AtomicLong streamCounter = new AtomicLong(0);
        MongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:tokenless:stream:" + streamCounter.getAndIncrement());

        int successes = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            String tag = "tokenless-" + i;

            DcbAppendCondition condA = failIfEventsMatch(tags(tag));
            DcbAppendCondition condB = failIfEventsMatch(tags(tag));

            CloudEvent eventA = taggedEvent("TokenlessEvent", tag);
            CloudEvent eventB = taggedEvent("TokenlessEvent", tag);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(eventA), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await();
                try {
                    isolatedStore.append(List.of(eventB), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            pool.shutdown();
            boolean aSucceeded = futA.get();
            boolean bSucceeded = futB.get();

            int iterSuccesses = (aSucceeded ? 1 : 0) + (bSucceeded ? 1 : 0);

            assertThat(iterSuccesses)
                    .as("Iteration %d: both tokenless conditional appends succeeded (double-commit)", i)
                    .isLessThanOrEqualTo(1);
            assertThat(iterSuccesses)
                    .as("Iteration %d: neither tokenless conditional append succeeded", i)
                    .isGreaterThanOrEqualTo(1);

            if (aSucceeded || bSucceeded) successes++;
        }

        assertThat(successes).isEqualTo(ITERATIONS);
    }

    @Test
    void dcb_queries_are_index_backed() {
        eventStore.append(List.of(
                taggedEvent("SeedType", "explain-tag"),
                taggedEvent("SeedType", "explain-tag"),
                taggedEvent("SeedType", "explain-tag")));

        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(COLLECTION);

        Document tagReadQuery = new Document("$and", List.of(
                new Document("dcbposition", new Document("$gt", 0).append("$lte", 1000000)),
                new Document("$or", List.of(
                        new Document("dcbTags", new Document("$all", List.of("explain-tag")))
                ))
        ));
        Document tagReadExplain = collection.find(tagReadQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(tagReadExplain))
                .as("Tag read query should use IXSCAN, not COLLSCAN or unrecognized stage. Full explain: %s", tagReadExplain.toJson())
                .isEqualTo("IXSCAN");

        Document typeReadQuery = new Document("$and", List.of(
                new Document("dcbposition", new Document("$gt", 0).append("$lte", 1000000)),
                new Document("$or", List.of(
                        new Document("type", new Document("$in", List.of("SeedType")))
                ))
        ));
        Document typeReadExplain = collection.find(typeReadQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(typeReadExplain))
                .as("Type read query should use IXSCAN (dcbposition index), not COLLSCAN or unrecognized stage. Full explain: %s", typeReadExplain.toJson())
                .isEqualTo("IXSCAN");

        Document existenceQuery = new Document("$and", List.of(
                new Document("dcbposition", new Document("$gt", 0).append("$lte", Long.MAX_VALUE)),
                new Document("$or", List.of(
                        new Document("dcbTags", new Document("$all", List.of("explain-tag")))
                ))
        ));
        Document existenceExplain = collection.find(existenceQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(existenceExplain))
                .as("Existence/conflict check query should use IXSCAN, not COLLSCAN or unrecognized stage. Full explain: %s", existenceExplain.toJson())
                .isEqualTo("IXSCAN");
    }

    private MongoEventStore buildEventStoreWithStreamIdGenerator(DcbStreamIdGenerator streamIdGenerator) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .dcbStreamIdGenerator(streamIdGenerator)
                .build();
        return new MongoEventStore(mongoClient, databaseName, COLLECTION, config);
    }

    private static String extractWinningPlanStage(Document explainDoc) {
        Document queryPlanner = explainDoc.get("queryPlanner", Document.class);
        if (queryPlanner == null) {
            return "UNKNOWN";
        }
        Document winningPlan = queryPlanner.get("winningPlan", Document.class);
        if (winningPlan == null) {
            return "UNKNOWN";
        }
        Document plan = winningPlan;
        int depth = 0;
        while (plan != null && depth++ < 20) {
            String stage = plan.getString("stage");
            if ("IXSCAN".equals(stage) || "COLLSCAN".equals(stage) || "COUNT_SCAN".equals(stage)) {
                return stage;
            }
            Document input = plan.get("inputStage", Document.class);
            if (input != null) {
                plan = input;
            } else {
                @SuppressWarnings("unchecked")
                List<Document> inputStages = (List<Document>) plan.get("inputStages");
                if (inputStages != null && !inputStages.isEmpty()) {
                    plan = inputStages.get(0);
                } else {
                    return stage != null ? stage : "UNKNOWN";
                }
            }
        }
        return "UNKNOWN";
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
