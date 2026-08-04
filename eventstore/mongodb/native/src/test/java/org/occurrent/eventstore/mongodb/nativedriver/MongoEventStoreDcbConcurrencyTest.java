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
import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.dcb.*;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.types;

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
    // Generous relative to a single (possibly internally-retried) append against real MongoDB, but far below the
    // class-level @Timeout(180), so a wedged worker is reported by the specific bounded wait, not the class timeout.
    private static final long BARRIER_TIMEOUT_SECONDS = 10;
    private static final long FUTURE_TIMEOUT_SECONDS = 20;

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

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
            String tag = "tagx:" + i;

            DcbEventStream boundaryA = isolatedStore.read(types(type));
            DcbEventStream boundaryB = isolatedStore.read(tags(Tag.parse(tag)));
            DcbConsistencyToken tokenA = boundaryA.consistencyToken();
            DcbConsistencyToken tokenB = boundaryB.consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(types(type), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(tags(Tag.parse(tag)), tokenB);

            CloudEvent eventA = taggedEvent(type, tag);
            CloudEvent eventB = taggedEvent(type, tag);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(eventA), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(eventB), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            boolean[] results = awaitBothThenShutdownNow(pool, futA, futB);
            boolean aSucceeded = results[0];
            boolean bSucceeded = results[1];

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
            String sharedTag = "sharedtag:" + i;
            String extraTagA = "extraa:" + i;

            DcbConsistencyToken tokenA = isolatedStore.read(tags(Tag.parse(sharedTag))).consistencyToken();
            DcbConsistencyToken tokenB = isolatedStore.read(tags(Tag.parse(sharedTag), Tag.parse(extraTagA))).consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(tags(Tag.parse(sharedTag)), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(tags(Tag.parse(sharedTag), Tag.parse(extraTagA)), tokenB);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(taggedEvent("SomeType", sharedTag, extraTagA)), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(taggedEvent("SomeType", sharedTag, extraTagA)), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            boolean[] results = awaitBothThenShutdownNow(pool, futA, futB);
            boolean aSucceeded = results[0];
            boolean bSucceeded = results[1];

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
            String tag = "tttag:" + i;

            DcbConsistencyToken tokenA = isolatedStore.read(types(sharedType)).consistencyToken();
            DcbConsistencyToken tokenB = isolatedStore.read(types(sharedType)).consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(types(sharedType), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(types(sharedType), tokenB);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(taggedEvent(sharedType, tag)), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(taggedEvent(sharedType, tag)), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            boolean[] results = awaitBothThenShutdownNow(pool, futA, futB);
            boolean aSucceeded = results[0];
            boolean bSucceeded = results[1];

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
            String tag = "contention:" + i;

            DcbConsistencyToken boundaryToken = eventStore.read(tags(Tag.parse(tag))).consistencyToken();
            DcbAppendCondition condition = failIfEventsMatch(tags(Tag.parse(tag)), boundaryToken);

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
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

            awaitAllThenShutdownNow(pool, futures);

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
                tags -> "disjoint:stream:" + new TreeSet<>(tags).stream().map(Tag::canonical).collect(java.util.stream.Collectors.joining(",")));

        for (int i = 0; i < ITERATIONS; i++) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "disjoint:iter" + i + "t" + t;
                final String distinctType = "DisjointEvent-iter" + i + "-t" + t;
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = disjointStore.read(tags(Tag.parse(distinctTag))).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tags(Tag.parse(distinctTag)), token);
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

            awaitAllThenShutdownNow(pool, futures);

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
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "shared:iter" + i + "t" + t;
                final String distinctType = "SharedStreamEvent-iter" + i + "-t" + t;
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = sharedStreamStore.read(tags(Tag.parse(distinctTag))).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tags(Tag.parse(distinctTag)), token);
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    try {
                        sharedStreamStore.append(List.of(taggedEvent(distinctType, distinctTag)), cond);
                        successCount.incrementAndGet();
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                    return null;
                }));
            }

            awaitAllThenShutdownNow(pool, futures);

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
            String tag1 = "mmt1:" + i;
            String tag2 = "mmt2:" + i;

            DcbCriteria multiMarkerQuery = tags(Tag.parse(tag1), Tag.parse(tag2));
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
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

            awaitAllThenShutdownNow(pool, futures);

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
            String tag = "tokenless:" + i;

            DcbAppendCondition condA = failIfEventsMatch(tags(Tag.parse(tag)));
            DcbAppendCondition condB = failIfEventsMatch(tags(Tag.parse(tag)));

            CloudEvent eventA = taggedEvent("TokenlessEvent", tag);
            CloudEvent eventB = taggedEvent("TokenlessEvent", tag);

            CyclicBarrier barrier = new CyclicBarrier(2);
            ExecutorService pool = Executors.newFixedThreadPool(2);

            Future<Boolean> futA = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(eventA), condA);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            Future<Boolean> futB = pool.submit(() -> {
                barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    isolatedStore.append(List.of(eventB), condB);
                    return true;
                } catch (DcbAppendConditionNotFulfilledException e) {
                    return false;
                }
            });

            boolean[] results = awaitBothThenShutdownNow(pool, futA, futB);
            boolean aSucceeded = results[0];
            boolean bSucceeded = results[1];

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
                taggedEvent("SeedType", "explain:tag"),
                taggedEvent("SeedType", "explain:tag"),
                taggedEvent("SeedType", "explain:tag")));

        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(COLLECTION);

        Document tagReadQuery = new Document("$and", List.of(
                new Document("position", new Document("$gt", 0).append("$lte", 1000000)),
                new Document("$or", List.of(
                        new Document("dcbTags", new Document("$all", List.of("explain:tag")))
                ))
        ));
        Document tagReadExplain = collection.find(tagReadQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(tagReadExplain))
                .as("Tag read query should use IXSCAN, not COLLSCAN or unrecognized stage. Full explain: %s", tagReadExplain.toJson())
                .isEqualTo("IXSCAN");

        Document typeReadQuery = new Document("$and", List.of(
                new Document("position", new Document("$gt", 0).append("$lte", 1000000)),
                new Document("$or", List.of(
                        new Document("type", new Document("$in", List.of("SeedType")))
                ))
        ));
        Document typeReadExplain = collection.find(typeReadQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(typeReadExplain))
                .as("Type read query should use IXSCAN (position index), not COLLSCAN or unrecognized stage. Full explain: %s", typeReadExplain.toJson())
                .isEqualTo("IXSCAN");

        Document existenceQuery = new Document("$and", List.of(
                new Document("position", new Document("$gt", 0).append("$lte", Long.MAX_VALUE)),
                new Document("$or", List.of(
                        new Document("dcbTags", new Document("$all", List.of("explain:tag")))
                ))
        ));
        Document existenceExplain = collection.find(existenceQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(existenceExplain))
                .as("Existence/conflict check query should use IXSCAN, not COLLSCAN or unrecognized stage. Full explain: %s", existenceExplain.toJson())
                .isEqualTo("IXSCAN");
    }

    @Test
    void dcb_append_current_stream_version_lookup_is_index_backed_on_a_dcb_only_store() {
        // A DCB-only store (no STREAM capability), so the compound index under test is the non-unique variant DCB
        // gets, not the unique variant STREAM creates.
        String dcbOnlyCollectionName = "dcb_only_" + COLLECTION;
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(DCB)
                .build();
        MongoEventStore dcbOnlyStore = new MongoEventStore(mongoClient, databaseName, dcbOnlyCollectionName, config);
        dcbOnlyStore.append(List.of(taggedEvent("SeedType", "streamversion:tag")));

        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(dcbOnlyCollectionName);

        // Mirrors the query MongoEventStore.currentStreamVersion runs inside the DCB append transaction: find by
        // streamId, sorted by streamVersion descending, limited to one. Before the streamId+streamVersion index was
        // created for DCB-only stores this was a COLLSCAN+SORT that grows with store size.
        String dcbPartitionStreamId = dcbOnlyStore.read(DcbCriteria.tags(org.occurrent.eventstore.api.dcb.Tag.parse("streamversion:tag"))).events().stream()
                .findFirst()
                .map(org.occurrent.cloudevents.OccurrentExtensionGetter::getStreamId)
                .orElseThrow();
        Document streamVersionLookupExplain = collection.find(new Document("streamid", dcbPartitionStreamId))
                .sort(new Document("streamversion", -1))
                .limit(1)
                .explain(ExplainVerbosity.EXECUTION_STATS);
        assertThat(extractWinningPlanStage(streamVersionLookupExplain))
                .as("DCB append's currentStreamVersion lookup should use IXSCAN, not COLLSCAN. Full explain: %s", streamVersionLookupExplain.toJson())
                .isEqualTo("IXSCAN");
    }

    @Test
    void dcb_match_all_query_is_index_backed_on_dcb_tags_field() {
        eventStore.append(List.of(taggedEvent("SeedType", "explain:tag")));

        MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(COLLECTION);

        // Bias the planner the same way ADR 49's spike did: many more position-only stream
        // documents than dcbTags-carrying ones, so the position index is only cheaper if the
        // planner ignores dcbTags selectivity. Without this skew, a single-document collection
        // ties and the planner may pick either index, defeating the point of the assertion below.
        List<Document> streamOnlyDocuments = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            streamOnlyDocuments.add(new Document("id", "explain-stream-only-" + i)
                    .append("source", SOURCE.toString())
                    .append("streamid", "explain-stream-only-" + i)
                    .append("streamversion", 0L)
                    .append("position", 1_000 + i));
        }
        collection.insertMany(streamOnlyDocuments);

        // Mirrors what toDcbBsonQuery(...) now builds for DcbCriteria.all(): the position window ANDed with an
        // existence check on dcbTags, so a MatchAll/type-only read can never match a stream-written event (which has
        // no dcbTags field) and does so via an index rather than a collection scan.
        Document matchAllQuery = new Document("$and", List.of(
                new Document("position", new Document("$gt", 0).append("$lte", 1_000_000)),
                new Document("dcbTags", new Document("$exists", true))
        ));
        Document matchAllExplain = collection.find(matchAllQuery).explain(ExplainVerbosity.QUERY_PLANNER);
        assertThat(extractWinningPlanStage(matchAllExplain))
                .as("MatchAll DCB read should be index-backed via the sparse dcbTags index, not a COLLSCAN. Full explain: %s", matchAllExplain.toJson())
                .isEqualTo("IXSCAN");
        assertThat(extractWinningIndexName(matchAllExplain))
                .as("The winning plan should use the dcbTags index specifically. Full explain: %s", matchAllExplain.toJson())
                .isEqualTo("dcbTags_1");
    }

    @Test
    void single_partition_disjoint_boundaries_are_retried_to_success() throws Exception {
        // A single partition forces every DCB append into one stream, so any two concurrent appends to disjoint
        // boundaries collide on the next stream version and one loses on the unique streamid+streamversion index. That
        // loser must be retried to success, not surfaced as a duplicate CloudEvent failure. On MongoDB 4.2 the
        // collision surfaces as a transient write conflict that the transaction retries on its own, so this passes with
        // or without the fix and stands as a regression guard for the intent.
        int threadCount = 12;
        int iterations = 40;
        MongoEventStore singlePartitionStore = buildEventStoreWithStreamIdGenerator(
                new PartitionedDcbStreamIdGenerator(1, "dcb:partition:"));

        for (int i = 0; i < iterations; i++) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "single:iter" + i + "t" + t;
                final String distinctType = "SinglePartitionEvent-iter" + i + "-t" + t;
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = singlePartitionStore.read(tags(Tag.parse(distinctTag))).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tags(Tag.parse(distinctTag)), token);
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    try {
                        singlePartitionStore.append(List.of(taggedEvent(distinctType, distinctTag)), cond);
                        successCount.incrementAndGet();
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                    return null;
                }));
            }

            awaitAllThenShutdownNow(pool, futures);

            assertThat(failures)
                    .as("Iteration %d: disjoint boundaries in a single partition must all be retried to success, not fail with a duplicate CloudEvent error", i)
                    .isEmpty();
            assertThat(successCount.get())
                    .as("Iteration %d: all %d single-partition appends should succeed", i, threadCount)
                    .isEqualTo(threadCount);
        }
    }

    @Test
    void single_partition_concurrent_disjoint_boundaries_all_commit_and_remain_readable() throws Exception {
        // Stronger, observable-contract sibling of single_partition_disjoint_boundaries_are_retried_to_success and the
        // synthetic non_transient_stream_version_duplicate_is_retried_to_success below. Those assert only that no append
        // throws (real race) or that one injected E11000 is retried (synthetic). This one drives the REAL race on the
        // configured MongoDB version and asserts the full contract PR #297 promises: with a single storage partition
        // every DCB append shares one (streamid, streamversion) sequence, so concurrent appends to disjoint boundaries
        // deterministically collide on the next stream version and one loses on the unique streamid+streamversion index.
        // The loser must be retried to success and NEVER surface a misleading DuplicateCloudEventException. Beyond "no
        // throw", this verifies persistence: every append commits, the total committed event count equals the number of
        // successful appends (no lost writes hidden behind a swallowed retry), and every disjoint boundary's single
        // event is present and readable afterwards.
        //
        // Observed on MongoDB 8.0 (mongo:${test.mongo.version}): the contract holds. The partition stream-version
        // collision is absorbed internally and never leaks a spurious duplicate or append failure to the caller.
        // Whether 8.0 surfaces the collision as a transient WriteConflict that withTransaction retries or a
        // non-transient E11000 that the #297 exception-translation fix retries is not distinguishable from the caller's
        // side here; either way the observable contract asserted below is upheld.
        int threadCount = 12;
        int iterations = 40;
        MongoEventStore singlePartitionStore = buildEventStoreWithStreamIdGenerator(
                new PartitionedDcbStreamIdGenerator(1, "dcb:readable:partition:"));

        List<String> allTags = new CopyOnWriteArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger duplicateCloudEventFailures = new AtomicInteger(0);
        List<Throwable> unexpectedFailures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < iterations; i++) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "readable:iter" + i + "t" + t;
                final String distinctType = "ReadableEvent-iter" + i + "-t" + t;
                allTags.add(distinctTag);
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = singlePartitionStore.read(tags(Tag.parse(distinctTag))).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tags(Tag.parse(distinctTag)), token);
                    barrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    try {
                        singlePartitionStore.append(List.of(taggedEvent(distinctType, distinctTag)), cond);
                        successCount.incrementAndGet();
                    } catch (DuplicateCloudEventException e) {
                        duplicateCloudEventFailures.incrementAndGet();
                    } catch (Throwable e) {
                        unexpectedFailures.add(e);
                    }
                    return null;
                }));
            }

            awaitAllThenShutdownNow(pool, futures);
        }

        int expectedAppends = threadCount * iterations;

        assertThat(duplicateCloudEventFailures.get())
                .as("No concurrent DCB append to a disjoint boundary may fail with a misleading DuplicateCloudEventException")
                .isZero();
        assertThat(unexpectedFailures)
                .as("No concurrent single-partition DCB append may fail; the stream-version collision loser must be retried to success")
                .isEmpty();
        assertThat(successCount.get())
                .as("Every one of the %d concurrent appends must eventually commit", expectedAppends)
                .isEqualTo(expectedAppends);
        assertThat(singlePartitionStore.count(DcbCriteria.all()))
                .as("Total committed events must equal the number of successful appends (no lost writes under collision retries)")
                .isEqualTo(expectedAppends);
        for (String tag : allTags) {
            assertThat(singlePartitionStore.read(tags(Tag.parse(tag))).events())
                    .as("Disjoint boundary %s must have exactly its one committed event readable", tag)
                    .hasSize(1);
        }
    }

    @Test
    void non_transient_stream_version_duplicate_is_retried_to_success() {
        // Deterministic, MongoDB-version-independent gate. The first insertMany in the DCB append path is made to throw
        // a non-transient duplicate-key error on the streamid+streamversion index, exactly the post-commit E11000 a
        // partition stream-version collision produces on the MongoDB versions and configurations that return it rather
        // than a transient write conflict. Without the fix that error is mistranslated to a non-retryable duplicate
        // CloudEvent and the append fails. With the fix it is retried and the append commits on the next attempt, which
        // does not throw.
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> realEventCollection = database.getCollection(COLLECTION);
        MongoCollection<Document> failingOnceCollection = collectionThatFailsFirstInsertManyWithStreamVersionDuplicate(realEventCollection);

        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .dcbStreamIdGenerator(new PartitionedDcbStreamIdGenerator(1, "dcb:partition:"))
                .build();
        MongoEventStore store = new MongoEventStore(mongoClient, database, failingOnceCollection, config);

        String tag = "injected:streamversion:collision";
        store.append(List.of(taggedEvent("InjectedEvent", tag)), failIfEventsMatch(tags(Tag.parse(tag))));

        assertThat(store.exists(types("InjectedEvent")))
                .as("The append must be retried past the injected stream-version duplicate and commit its event")
                .isTrue();
    }

    /**
     * Joins both futures before returning or throwing, so a throw from one cannot leave the other still writing to
     * MongoDB in the background. The pool is interrupted via {@code shutdownNow()} only after both joins have been
     * attempted, so a wedged worker is stopped rather than left running.
     */
    private static boolean[] awaitBothThenShutdownNow(ExecutorService pool, Future<Boolean> futA, Future<Boolean> futB) throws Exception {
        Boolean resultA = null;
        Boolean resultB = null;
        Exception failureA = null;
        Exception failureB = null;
        try {
            resultA = getUnwrapped(futA);
        } catch (Exception e) {
            failureA = e;
        }
        try {
            resultB = getUnwrapped(futB);
        } catch (Exception e) {
            failureB = e;
        }
        pool.shutdownNow();

        if (failureA != null) {
            throw failureA;
        }
        if (failureB != null) {
            throw failureB;
        }
        return new boolean[]{resultA, resultB};
    }

    /**
     * Joins every future before returning or throwing, so one thread's failure cannot leave the others still writing
     * to MongoDB in the background. The pool is interrupted via {@code shutdownNow()} only after every join has been
     * attempted.
     */
    private static void awaitAllThenShutdownNow(ExecutorService pool, List<Future<Void>> futures) throws Exception {
        Exception firstFailure = null;
        for (Future<Void> future : futures) {
            try {
                getUnwrapped(future);
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        pool.shutdownNow();

        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private static <T> T getUnwrapped(Future<T> future) throws Exception {
        try {
            return future.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof Exception ? (Exception) cause : e;
        }
    }

    private MongoEventStore buildEventStoreWithStreamIdGenerator(DcbStreamIdGenerator streamIdGenerator) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .dcbStreamIdGenerator(streamIdGenerator)
                .build();
        return new MongoEventStore(mongoClient, databaseName, COLLECTION, config);
    }

    /**
     * Wraps a real event collection so the first {@code insertMany(...)} call throws a non-transient duplicate-key
     * error on the streamid+streamversion index and every other call and method delegates to the real collection.
     */
    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> collectionThatFailsFirstInsertManyWithStreamVersionDuplicate(MongoCollection<Document> realCollection) {
        AtomicInteger insertManyCalls = new AtomicInteger(0);
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    if ("insertMany".equals(method.getName()) && insertManyCalls.getAndIncrement() == 0) {
                        throw streamVersionDuplicateKeyException();
                    }
                    try {
                        return method.invoke(realCollection, args);
                    } catch (InvocationTargetException invocationTargetException) {
                        // Unwrap so the code under test observes the real collection's exception directly, not
                        // wrapped in InvocationTargetException, which would break its duplicate/transient classification.
                        throw invocationTargetException.getCause();
                    }
                });
    }

    private static MongoBulkWriteException streamVersionDuplicateKeyException() {
        BulkWriteError error = new BulkWriteError(11000,
                "E11000 duplicate key error collection: test.events index: streamid_1_streamversion_1 dup key: { streamid: \"dcb:partition:\", streamversion: 1 }",
                new BsonDocument(), 0);
        BulkWriteResult writeResult = BulkWriteResult.acknowledged(0, 0, 0, 0, List.of(), List.of());
        // Empty error labels so the exception is NOT a transient transaction error, matching a post-commit E11000.
        return new MongoBulkWriteException(writeResult, List.of(error), null, new ServerAddress("localhost", 27017), Set.of());
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

    private static String extractWinningIndexName(Document explainDoc) {
        Document queryPlanner = explainDoc.get("queryPlanner", Document.class);
        Document plan = queryPlanner == null ? null : queryPlanner.get("winningPlan", Document.class);
        int depth = 0;
        while (plan != null && depth++ < 20) {
            if ("IXSCAN".equals(plan.getString("stage"))) {
                return plan.getString("indexName");
            }
            Document input = plan.get("inputStage", Document.class);
            if (input != null) {
                plan = input;
            } else {
                @SuppressWarnings("unchecked")
                List<Document> inputStages = (List<Document>) plan.get("inputStages");
                plan = inputStages != null && !inputStages.isEmpty() ? inputStages.get(0) : null;
            }
        }
        return null;
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
