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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.internal.DcbMarkerVersionsSupport;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster.Hold;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.tag;
import static org.occurrent.tck.eventstore.blocking.DcbConformanceEvents.taggedEventWithId;

// Probe for #1479, reports what a plain read and a causally consistent read see while a matching event is prepared on the events' shard
class MongoEventStoreDcbPlainReadThroughAMongosProbeTest {

    private static final String ENROLLED = "StudentEnrolledInCourse";
    private static final String COURSE_C1 = "course:c1";
    private static final DcbCriteria ENROLLMENTS_IN_C1 = DcbCriteria.type(ENROLLED).tags(tag(COURSE_C1));
    private static final String COURSE_C1_KEY = DcbMarkerVersionsSupport.tagKey(tag(COURSE_C1));
    private static final long BLOCKED_AFTER_SECONDS = 3;

    private static TwoShardMongoCluster cluster;
    private static MongoClient client;

    private String database;
    private MongoEventStore eventStore;

    @BeforeAll
    static void start_the_cluster() {
        cluster = TwoShardMongoCluster.start();
        client = MongoClients.create(cluster.getConnectionString());
    }

    @AfterAll
    static void stop_the_cluster() {
        if (client != null) {
            client.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @BeforeEach
    void create_the_event_store_with_its_markers_split_across_the_shards() {
        database = cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD);
        eventStore = new MongoEventStore(client, database, "events", new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM, DCB)
                .build());
        cluster.shardCheckpointCollection(database, "events_dcb_checkpoints");
    }

    private record Observation(Future<Result> future, String readConcern) {
    }

    private record Result(String markerDocument, BsonTimestamp operationTime, long eventsFound, long finishedAtNanos) {
    }

    @RepeatedTest(3)
    void a_causally_consistent_read_of_the_events_while_a_matching_event_is_prepared() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        ExecutorService readers = Executors.newFixedThreadPool(4);
        StringBuilder report = new StringBuilder("PROBE1479");
        Future<?> matching = null;
        try {
            long releasedAt;
            Observation local;
            Observation majority;
            DcbEventStream plain;
            try (Hold hold = cluster.holdCommitsOnSecondShard()) {
                matching = executor.submit(() -> eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1))));
                awaitMarkerOnTheFirstShard();
                plain = CompletableFuture.supplyAsync(() -> eventStore.read(ENROLLMENTS_IN_C1)).get(10, TimeUnit.SECONDS);
                report.append(" step1 markerVersion=").append(plain.consistencyToken().markers().get(COURSE_C1_KEY).version())
                        .append(" events=").append(plain.events().size());

                local = new Observation(readers.submit(() -> causalRead(ReadConcern.LOCAL)), "local");
                majority = new Observation(readers.submit(() -> causalRead(ReadConcern.MAJORITY)), "majority");
                Observation sessionlessLocal = new Observation(readers.submit(() -> sessionlessRead(ReadConcern.LOCAL)), "sessionless-local");
                Observation sessionlessMajority = new Observation(readers.submit(() -> sessionlessRead(ReadConcern.MAJORITY)), "sessionless-majority");
                report.append(" control-a sessionlessLocal=").append(heldOutcome(sessionlessLocal)).append(" sessionlessMajority=").append(heldOutcome(sessionlessMajority));
                report.append(" step3 local=").append(heldOutcome(local)).append(" majority=").append(heldOutcome(majority));
                releasedAt = System.nanoTime();
                hold.close();
            }
            report.append(" release local=").append(afterRelease(local, releasedAt)).append(" majority=").append(afterRelease(majority, releasedAt));
            try {
                matching.get(30, TimeUnit.SECONDS);
                report.append(" appendOutcome=committed");
            } catch (Exception e) {
                report.append(" appendOutcome=").append(e);
            }
            report.append(" finalPlainRead.events=").append(eventStore.read(ENROLLMENTS_IN_C1).events().size());
            assertThat(plain.events()).as(report.toString()).isEmpty();
        } finally {
            System.out.println(report);
            executor.shutdownNow();
            readers.shutdownNow();
        }
    }

    private Result sessionlessRead(ReadConcern readConcern) {
        long found = client.getDatabase(database).getCollection("events").withReadConcern(readConcern).countDocuments(new Document("id", "matching"));
        return new Result("n/a", null, found, System.nanoTime());
    }

    @RepeatedTest(3)
    void a_causally_consistent_read_of_an_event_that_has_committed_with_no_hold() {
        eventStore.append(List.of(taggedEventWithId("matching", ENROLLED, COURSE_C1)));
        for (ReadConcern readConcern : List.of(ReadConcern.LOCAL, ReadConcern.MAJORITY)) {
            long start = System.nanoTime();
            Result result = causalRead(readConcern);
            System.out.println("PROBE1479 control-b " + readConcern.getLevel() + " eventsFound=" + result.eventsFound() + " millis=" + TimeUnit.NANOSECONDS.toMillis(result.finishedAtNanos() - start));
        }
    }

    // The marker read sets the session's operation time, the events find then carries it as afterClusterTime
    private Result causalRead(ReadConcern readConcern) {
        try (ClientSession session = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build())) {
            Document marker = client.getDatabase(database).getCollection("events_dcb_checkpoints").withReadConcern(readConcern)
                    .find(session, new Document("_id", "marker:" + COURSE_C1_KEY)).first();
            BsonTimestamp operationTime = session.getOperationTime();
            long found = client.getDatabase(database).getCollection("events").withReadConcern(readConcern)
                    .countDocuments(session, new Document("id", "matching"));
            String markerDocument = marker == null ? "none" : marker.toJson();
            return new Result(markerDocument, operationTime, found, System.nanoTime());
        }
    }

    private static String heldOutcome(Observation observation) throws Exception {
        try {
            return "returned-while-held" + observation.future().get(BLOCKED_AFTER_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return "blocked(" + BLOCKED_AFTER_SECONDS + "s)";
        }
    }

    private static String afterRelease(Observation observation, long releasedAt) throws Exception {
        Result result = observation.future().get(60, TimeUnit.SECONDS);
        return result + " millisAfterRelease=" + TimeUnit.NANOSECONDS.toMillis(result.finishedAtNanos() - releasedAt);
    }

    // The first shard committed its half of the transaction once the marker is there, while the second shard holds its own
    private void awaitMarkerOnTheFirstShard() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (client.getDatabase(database).getCollection("events_dcb_checkpoints").find(new Document("_id", "marker:" + COURSE_C1_KEY)).first() == null) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("The marker didn't show up on the first shard within 30 seconds");
            }
            Thread.sleep(50);
        }
    }
}
