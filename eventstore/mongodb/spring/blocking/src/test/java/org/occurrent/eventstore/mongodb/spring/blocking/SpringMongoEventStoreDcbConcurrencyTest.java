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
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
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
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbQuery.tagsAllOf;
import static org.occurrent.eventstore.api.dcb.DcbQuery.types;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.DCB;
import static org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStoreCapability.STREAM;

/**
 * Adversarial concurrency tests for the DCB write path (ADR 0021).
 * <p>
 * Each scenario drives real threads to a barrier so appends race in earnest.
 * We repeat each scenario many times (fresh data per iteration via FlushMongoDBExtension
 * is not practical per-iteration here; instead we use unique per-iteration tags/types so
 * each iteration is logically isolated on a fresh empty boundary).
 * <p>
 * Assertions focus on safety invariants:
 * - write-skew is NEVER permitted (never two concurrent conflicting successes)
 * - DcbAppendConditionNotFulfilledException is the only loser exception (transient errors must be retried internally)
 */
@Testcontainers
// Bound each scenario so a deadlock or broken barrier fails the test instead of hanging the whole Maven build on the
// blocking Future.get() / barrier.await() calls. Generous because each scenario runs many threaded iterations.
@Timeout(180)
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoEventStoreDcbConcurrencyTest {

    private static final URI SOURCE = URI.create("urn:test:concurrency");
    private static final int ITERATIONS = 50;

    @Container
    private static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version"))
                .withReplicaSet();
        List<String> ports = new ArrayList<>();
        ports.add("27018:27017");   // distinct port from the DCB functional test to allow parallel suite runs
        mongoDBContainer.withReuse(true).setPortBindings(ports);
    }

    @RegisterExtension
    FlushMongoDBExtension flushMongoDBExtension = new FlushMongoDBExtension(
            new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_concurrency"));

    private SpringMongoEventStore eventStore;
    private MongoTemplate mongoTemplate;
    private static final String COLLECTION = "events";

    @BeforeEach
    void create_mongo_spring_blocking_event_store() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_concurrency");
        MongoClient mongoClient = MongoClients.create(connectionString);
        mongoTemplate = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(
                new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build();
        eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    // ---------------------------------------------------------------------------
    // Scenario 1: type-vs-tag write skew is prevented
    //
    // Two commands both see an empty boundary at position 0 (fresh per iteration via unique
    // iteration suffix).  Command A appends {type:X_i, tag:t_i} with condition types("X_i").
    // Command B appends {type:X_i, tag:t_i} with condition tagsAllOf("t_i").
    // The appended event satisfies BOTH conditions, so whichever commits first invalidates
    // the other's condition.  The marker keys written by A include "type:X_i"; the marker
    // keys written by B include "tag:t_i".  The event marker keys for A's append also include
    // "tag:t_i" (from the event), and the event marker keys for B's append include "type:X_i".
    // So A and B share the "type:X_i" marker (B's event contributes it) and the "tag:t_i" marker
    // (A's event contributes it), ensuring a write-write conflict forces serialization.
    //
    // Expected invariant: exactly one succeeds every iteration, never both.
    // ---------------------------------------------------------------------------
    @Test
    void type_vs_tag_write_skew_is_prevented() throws Exception {
        // Use a per-call counter so each append call receives a DISTINCT storage stream.
        // This removes stream_version as a serializer: only shared markers can serialize the two
        // racing appends. If enforceAppendCondition wrote no markers, both would sometimes commit.
        AtomicLong streamCounter = new AtomicLong(0);
        SpringMongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:stream:" + streamCounter.getAndIncrement());

        int successes = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            String type = "TypeX_" + i;
            String tag = "tag-x-" + i;

            // Both commands read the (empty) boundary at position 0
            DcbEventStream boundaryA = isolatedStore.read(types(type));
            DcbEventStream boundaryB = isolatedStore.read(tagsAllOf(tag));
            DcbConsistencyToken tokenA = boundaryA.consistencyToken();
            DcbConsistencyToken tokenB = boundaryB.consistencyToken();

            DcbAppendCondition condA = failIfEventsMatch(types(type), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(tagsAllOf(tag), tokenB);

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

            // Core safety invariant: both must NEVER succeed simultaneously (marker is the sole serializer)
            assertThat(iterSuccesses)
                    .as("Iteration %d: both appends succeeded (type-vs-tag write skew)", i)
                    .isLessThanOrEqualTo(1);

            // At least one must succeed (one may win when both see empty boundary)
            assertThat(iterSuccesses)
                    .as("Iteration %d: neither append succeeded", i)
                    .isGreaterThanOrEqualTo(1);

            if (aSucceeded || bSucceeded) successes++;
        }

        // Across all iterations: always exactly one winner per iteration
        assertThat(successes).isEqualTo(ITERATIONS);
    }

    // ---------------------------------------------------------------------------
    // Scenario 2a: tag-vs-tag write skew is prevented
    // Two overlapping tag queries, shared tag "shared-tag-N".
    // ---------------------------------------------------------------------------
    @Test
    void tag_vs_tag_write_skew_is_prevented() throws Exception {
        // Distinct storage stream per append call — stream_version cannot serialize; markers are the sole guard.
        AtomicLong streamCounter = new AtomicLong(0);
        SpringMongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:stream:" + streamCounter.getAndIncrement());

        for (int i = 0; i < ITERATIONS; i++) {
            String sharedTag = "shared-tag-" + i;
            String extraTagA = "extra-a-" + i;

            // Both read empty boundary
            DcbConsistencyToken tokenA = isolatedStore.read(tagsAllOf(sharedTag)).consistencyToken();
            DcbConsistencyToken tokenB = isolatedStore.read(tagsAllOf(sharedTag, extraTagA)).consistencyToken();

            // Event matches BOTH conditions: has sharedTag and extraTagA
            // condA: tagsAllOf(sharedTag) — matched by the event
            // condB: tagsAllOf(sharedTag, extraTagA) — also matched if event carries both
            DcbAppendCondition condA = failIfEventsMatch(tagsAllOf(sharedTag), tokenA);
            DcbAppendCondition condB = failIfEventsMatch(tagsAllOf(sharedTag, extraTagA), tokenB);

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

    // ---------------------------------------------------------------------------
    // Scenario 2b: type-vs-type write skew is prevented
    // Both commands use types() queries on the same type; their events carry that type.
    // ---------------------------------------------------------------------------
    @Test
    void type_vs_type_write_skew_is_prevented() throws Exception {
        // Distinct storage stream per append call — stream_version cannot serialize; markers are the sole guard.
        AtomicLong streamCounter = new AtomicLong(0);
        SpringMongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
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

    // ---------------------------------------------------------------------------
    // Scenario 3: same-boundary serialization under contention (N=8 threads)
    //
    // 8 threads all read the same boundary and concurrently append.  Exactly one
    // wins; the rest must throw DcbAppendConditionNotFulfilledException.
    // No raw MongoException / transient error must escape — the retry loop must absorb those.
    // ---------------------------------------------------------------------------
    @Test
    void same_boundary_serialization_under_contention() throws Exception {
        int threadCount = 8;

        for (int i = 0; i < ITERATIONS; i++) {
            String tag = "contention-" + i;

            // All threads read the boundary at the same position
            DcbConsistencyToken boundaryToken = eventStore.read(tagsAllOf(tag)).consistencyToken();
            DcbAppendCondition condition = failIfEventsMatch(tagsAllOf(tag), boundaryToken);

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

    // ---------------------------------------------------------------------------
    // Scenario 4: disjoint boundaries do not falsely serialize
    //
    // N threads each appends to a DISTINCT tag boundary (tag:iter-N-thread-T) with a
    // distinct event type, so neither the tag marker nor the type marker is shared.
    // All should succeed — no false conflicts, no leaked transient errors.
    //
    // Configuration note: we use a boundary-tag-keyed stream ID generator (one stream per
    // unique tag-set) so all threads map to distinct MongoDB streams. With the default 64-
    // partition hash, two distinct boundaries may collide to the same partition stream, which
    // causes a WriteConflict (a separate implementation concern). This test is scoped to
    // proving query-scoped isolation, not partition placement, so we eliminate the variable.
    // The cold-start counter race (concurrent first upserts of the position document) is handled by
    // reserveDcbPositions itself (it retries the duplicate key), so this test deliberately does NOT pre-warm and
    // exercises the concurrent first append directly.
    // ---------------------------------------------------------------------------
    @Test
    void disjoint_boundaries_do_not_falsely_serialize() throws Exception {
        int threadCount = 8;

        // Build a dedicated event store whose stream generator maps each unique tag-set to its
        // own stream, so no two threads share a stream and no WriteConflict can arise at the
        // stream-version level.
        SpringMongoEventStore disjointStore = buildEventStoreWithStreamIdGenerator(
                tags -> "disjoint:stream:" + String.join(",", new java.util.TreeSet<>(tags)));

        for (int i = 0; i < ITERATIONS; i++) {
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);

            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failCount = new AtomicInteger(0);
            List<Future<Void>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                final String distinctTag = "disjoint-iter" + i + "-t" + t;
                // Use a distinct type per thread so the type marker does not create false conflicts.
                // The goal is to prove query-scoped isolation; shared-type conflicts are a separate, expected behavior.
                final String distinctType = "DisjointEvent-iter" + i + "-t" + t;
                futures.add(pool.submit(() -> {
                    DcbConsistencyToken token = disjointStore.read(tagsAllOf(distinctTag)).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tagsAllOf(distinctTag), token);
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

    // ---------------------------------------------------------------------------
    // Disjoint boundaries that collide on the same storage partition stream must still all succeed. The collision
    // causes a transient stream_version WriteConflict, which must be retried (not surfaced as a
    // WriteConditionNotFulfilledException or a leaked transient error). This pins the DCB-path conflict translation.
    // ---------------------------------------------------------------------------
    @Test
    void disjoint_boundaries_sharing_a_partition_stream_are_retried_to_success() throws Exception {
        int threadCount = 6;
        // Force every append onto ONE storage stream, so semantically-disjoint boundaries collide on stream_version.
        SpringMongoEventStore sharedStreamStore = buildEventStoreWithStreamIdGenerator(tags -> "shared:partition:stream");

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
                    DcbConsistencyToken token = sharedStreamStore.read(tagsAllOf(distinctTag)).consistencyToken();
                    DcbAppendCondition cond = failIfEventsMatch(tagsAllOf(distinctTag), token);
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

    private SpringMongoEventStore buildEventStoreWithStreamIdGenerator(DcbStreamIdGenerator streamIdGenerator) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".dcb_concurrency");
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoTemplate template = new MongoTemplate(mongoClient, requireNonNull(connectionString.getDatabase()));
        MongoTransactionManager txManager = new MongoTransactionManager(
                new SimpleMongoClientDatabaseFactory(mongoClient, requireNonNull(connectionString.getDatabase())));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(txManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .dcbStreamIdGenerator(streamIdGenerator)
                .build();
        return new SpringMongoEventStore(template, config);
    }

    // ---------------------------------------------------------------------------
    // Scenario 5: multi-marker boundary serialization under contention
    //
    // Coverage note: the single-marker tests above (tagsAllOf with one tag, or types) exercise the
    // common code path but do not exercise multi-marker token capture — where the consistency token
    // must aggregate ALL markers in the query (the sum of their versions), not just one. This test
    // closes that gap and acts as a regression guard for the single-read token capture logic.
    //
    // N threads share one multi-marker boundary token (tagsAllOf("t1_i","t2_i"), two markers) and
    // concurrently append an event tagged with BOTH markers using failIfEventsMatch(query, token).
    // Exactly one must win; the rest must throw DcbAppendConditionNotFulfilledException. The shared
    // upfront token is what makes "exactly one" deterministic: an in-worker capture would let a late
    // reader observe a fresher token and legitimately also succeed. Each worker additionally re-reads
    // the token after the barrier so the single $in capture (ADR 31) runs under read/append contention.
    // ---------------------------------------------------------------------------
    @Test
    void multi_marker_boundary_serialization_under_contention() throws Exception {
        int threadCount = 8;

        for (int i = 0; i < ITERATIONS; i++) {
            String tag1 = "mm-t1-" + i;
            String tag2 = "mm-t2-" + i;

            // Multi-marker query: both tag1 and tag2 must match — exercises the two-marker token capture path
            DcbQuery multiMarkerQuery = tagsAllOf(tag1, tag2);
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
                        // Re-read the multi-marker token concurrently with the appends so the single $in capture runs under contention
                        eventStore.read(multiMarkerQuery).consistencyToken();
                        // Event carries both markers so it satisfies the multi-marker query
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

    // ---------------------------------------------------------------------------
    // Scenario 6: no-token conditional append prevents double-commit (Finding 4 coverage gap)
    //
    // DcbAppendCondition.failIfEventsMatch(query) — the single-arg form — omits the consistency
    // token entirely, meaning the condition check scans the full event history for the given query.
    // Two concurrent threads both call failIfEventsMatch(query) (no token) on the same scoped
    // boundary and both try to append an event that satisfies that query. Exactly one must succeed;
    // the other must fail with DcbAppendConditionNotFulfilledException.
    // ---------------------------------------------------------------------------
    @Test
    void tokenless_conditional_append_prevents_double_commit() throws Exception {
        // Distinct storage stream per append call — stream_version cannot serialize; markers are the sole guard.
        AtomicLong streamCounter = new AtomicLong(0);
        SpringMongoEventStore isolatedStore = buildEventStoreWithStreamIdGenerator(
                tags -> "isolated:tokenless:stream:" + streamCounter.getAndIncrement());

        int successes = 0;

        for (int i = 0; i < ITERATIONS; i++) {
            String tag = "tokenless-" + i;

            // Both conditions use the single-arg (no-token) form: fail if ANY existing event matches
            DcbAppendCondition condA = failIfEventsMatch(tagsAllOf(tag));
            DcbAppendCondition condB = failIfEventsMatch(tagsAllOf(tag));

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

            // Core safety invariant: both must NEVER succeed simultaneously
            assertThat(iterSuccesses)
                    .as("Iteration %d: both tokenless conditional appends succeeded (double-commit)", i)
                    .isLessThanOrEqualTo(1);

            // At least one must succeed (boundary is empty at the start of each iteration)
            assertThat(iterSuccesses)
                    .as("Iteration %d: neither tokenless conditional append succeeded", i)
                    .isGreaterThanOrEqualTo(1);

            if (aSucceeded || bSucceeded) successes++;
        }

        // Across all iterations: always exactly one winner per iteration
        assertThat(successes).isEqualTo(ITERATIONS);
    }

    // ---------------------------------------------------------------------------
    // Scenario 7 (was 5): explain() index verification
    //
    // With a seeded collection, run explain() on the DCB existence/conflict query shape
    // and the read query shape and assert the winning plan does NOT use COLLSCAN.
    // The DCB query uses dcbposition (range) and dcbTags (all), both indexed.
    // ---------------------------------------------------------------------------
    @Test
    void dcb_queries_are_index_backed() {
        // Seed the collection with a few events so the query planner sees a non-empty collection
        eventStore.append(List.of(
                taggedEvent("SeedType", "explain-tag"),
                taggedEvent("SeedType", "explain-tag"),
                taggedEvent("SeedType", "explain-tag")));

        MongoCollection<Document> collection = mongoTemplate.getCollection(COLLECTION);

        // Explain the tag-based read query shape:
        // { dcbposition: { $gt: 0, $lte: <high> }, dcbTags: { $all: ["explain-tag"] } }
        Document tagReadQuery = new Document("$and", List.of(
                new Document("dcbposition", new Document("$gt", 0).append("$lte", 1000000)),
                new Document("$or", List.of(
                        new Document("dcbTags", new Document("$all", List.of("explain-tag")))
                ))
        ));
        Document tagReadExplain = collection.find(tagReadQuery).explain(ExplainVerbosity.QUERY_PLANNER);

        String tagReadPlanStage = extractWinningPlanStage(tagReadExplain);
        assertThat(tagReadPlanStage)
                .as("Tag read query should use IXSCAN, not COLLSCAN or unrecognized stage. Full explain: %s", tagReadExplain.toJson())
                .isEqualTo("IXSCAN");

        // Explain the type-based read query shape:
        // { dcbposition: { $gt: 0, $lte: <high> }, type: { $in: ["SeedType"] } }
        Document typeReadQuery = new Document("$and", List.of(
                new Document("dcbposition", new Document("$gt", 0).append("$lte", 1000000)),
                new Document("$or", List.of(
                        new Document("type", new Document("$in", List.of("SeedType")))
                ))
        ));
        Document typeReadExplain = collection.find(typeReadQuery).explain(ExplainVerbosity.QUERY_PLANNER);

        String typeReadPlanStage = extractWinningPlanStage(typeReadExplain);
        assertThat(typeReadPlanStage)
                .as("Type read query should use IXSCAN (dcbposition index), not COLLSCAN or unrecognized stage. Full explain: %s", typeReadExplain.toJson())
                .isEqualTo("IXSCAN");

        // Explain the existence/conflict check query shape (used in enforceAppendCondition):
        // { dcbposition: { $gt: <afterPos>, $lte: Long.MAX_VALUE }, dcbTags: { $all: ["explain-tag"] } }
        Document existenceQuery = new Document("$and", List.of(
                new Document("dcbposition", new Document("$gt", 0).append("$lte", Long.MAX_VALUE)),
                new Document("$or", List.of(
                        new Document("dcbTags", new Document("$all", List.of("explain-tag")))
                ))
        ));
        Document existenceExplain = collection.find(existenceQuery).explain(ExplainVerbosity.QUERY_PLANNER);

        String existencePlanStage = extractWinningPlanStage(existenceExplain);
        assertThat(existencePlanStage)
                .as("Existence/conflict check query should use IXSCAN, not COLLSCAN or unrecognized stage. Full explain: %s", existenceExplain.toJson())
                .isEqualTo("IXSCAN");
    }

    /**
     * Extracts the stage name of the winning plan from an explain() output document.
     * Walks the {@code queryPlanner.winningPlan} tree, following {@code inputStage} or
     * {@code inputStages} links until it finds a leaf or the top-level stage of the
     * first access pattern (IXSCAN, COLLSCAN, etc.).
     */
    private static String extractWinningPlanStage(Document explainDoc) {
        Document queryPlanner = explainDoc.get("queryPlanner", Document.class);
        if (queryPlanner == null) {
            // Sharded explain wraps in parsedQuery/queryPlanner per shard — not expected here
            return "UNKNOWN";
        }
        Document winningPlan = queryPlanner.get("winningPlan", Document.class);
        if (winningPlan == null) {
            return "UNKNOWN";
        }
        // Descend through FETCH → IXSCAN (or directly to COLLSCAN)
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
                // Multi-plan: take the first inputStages entry
                @SuppressWarnings("unchecked")
                List<Document> inputStages = (List<Document>) plan.get("inputStages");
                if (inputStages != null && !inputStages.isEmpty()) {
                    plan = inputStages.get(0);
                } else {
                    // Return whatever stage we have at this level
                    return stage != null ? stage : "UNKNOWN";
                }
            }
        }
        return "UNKNOWN";
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

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
