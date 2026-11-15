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

package org.occurrent.subscription.mongodb.nativedriver.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.internal.MongoCommons;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Starts subscriptions through a mongos, which reports a start position whose history is gone with the first reply of
 * the change stream rather than as the change stream opens. The cluster has one shard that holds the events, with a
 * one megabyte oplog, so a test can make that oplog forget a position. No shard of
 * {@code TwoShardMongoCluster} has a small oplog.
 */
@Timeout(600)
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoSubscriptionModelThroughMongosTest {

    private static final String SHARD = "rs2";
    private static final int MONGOS_PORT = 27017;
    private static final int SHARD_PORT = 27020;
    private static final Duration MAX_AWAIT_TIME = Duration.ofSeconds(10);

    private static MongoDBContainer container;
    private static String hostname;

    private String databaseName;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private ExecutorService executor;
    private NativeMongoSubscriptionModel subscriptionModel;

    @BeforeAll
    static void start_cluster() {
        container = new MongoDBContainer(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
                .withSharding()
                .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");
        try {
            container.start();
            hostname = exec("sh", "-c", "hostname").getStdout().trim();
            exec("sh", "-c", "mkdir -p /tmp/" + SHARD + " && mongod --bind_ip_all --shardsvr --replSet " + SHARD + " --port " + SHARD_PORT
                    + " --dbpath /tmp/" + SHARD + " --fork --logpath /tmp/" + SHARD + ".log --wiredTigerCacheSizeGB 0.25 --oplogSize 1");
            mongosh(SHARD_PORT, "rs.initiate({_id: '" + SHARD + "', members: [{_id: 0, host: '" + hostname + ":" + SHARD_PORT + "'}]});");
            awaitShardPrimary();
            mongosh(MONGOS_PORT, "assert(sh.addShard('" + SHARD + "/" + hostname + ":" + SHARD_PORT + "').ok === 1);");
        } catch (RuntimeException | Error e) {
            container.stop();
            throw e;
        }
    }

    @AfterAll
    static void stop_cluster() {
        if (container != null) {
            container.stop();
        }
    }

    @BeforeEach
    void create_instances() {
        databaseName = "mongos_" + UUID.randomUUID().toString().replace("-", "");
        // The events are on the one-megabyte-oplog shard
        mongosh(MONGOS_PORT, "assert(sh.enableSharding('" + databaseName + "', '" + SHARD + "').ok === 1);");
        mongoClient = MongoClients.create(new ConnectionString(container.getConnectionString()));
        database = mongoClient.getDatabase(databaseName);
        database.createCollection("events");
        executor = Executors.newCachedThreadPool();
        subscriptionModel = new NativeMongoSubscriptionModel(database, "events", TimeRepresentation.RFC_3339_STRING, executor,
                NativeMongoSubscriptionModelConfig.withConfig().restartSubscriptionsOnChangeStreamHistoryLost(false).maxAwaitTime(MAX_AWAIT_TIME));
    }

    @AfterEach
    void shutdown() {
        subscriptionModel.shutdown();
        executor.shutdownNow();
        mongoClient.close();
    }

    @Test
    void a_timed_wait_for_a_start_at_a_position_whose_history_is_gone_throws_the_refusal_and_frees_the_id() {
        assertRefusedFromTheWait(subscription -> subscription.waitUntilStarted(Duration.ofSeconds(60)));
    }

    @Test
    void an_untimed_wait_for_a_start_at_a_position_whose_history_is_gone_throws_the_refusal_and_frees_the_id() {
        assertRefusedFromTheWait(Subscription::waitUntilStarted);
    }

    @Test
    void a_start_at_now_does_not_wait_for_the_first_reply_of_the_change_stream() {
        // Given a quiet collection, so the first reply comes only when maxAwaitTime has passed
        // When
        long startedAt = System.nanoTime();
        boolean started = subscriptionModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(5));
        Duration waited = Duration.ofNanos(System.nanoTime() - startedAt);

        // Then
        assertThat(started).isTrue();
        assertThat(waited).isLessThan(MAX_AWAIT_TIME.dividedBy(2));
    }

    private void assertRefusedFromTheWait(Consumer<Subscription> wait) {
        // Given a checkpoint the oplog of the shard no longer reaches back to
        MongoOperationTimeCheckpoint lost = (MongoOperationTimeCheckpoint) requireNonNull(subscriptionModel.globalCheckpoint());
        dropHistoryFrom(lost.operationTime);
        String subscriptionId = UUID.randomUUID().toString();

        // When
        Subscription subscription = subscriptionModel.subscribe(subscriptionId, StartAt.checkpoint(lost), __ -> {
        });
        Throwable thrown = catchThrowable(() -> assertTimeoutPreemptively(Duration.ofSeconds(60), () -> wait.accept(subscription)));

        // Then
        assertThat(thrown).isExactlyInstanceOf(UnsupportedStartAtException.class)
                .hasMessageStartingWith("There was not enough oplog to resume subscription \"" + subscriptionId + "\"")
                .isInstanceOfSatisfying(UnsupportedStartAtException.class, refusal -> assertThat(refusal.startAt())
                        .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, refused -> assertThat(refused.checkpoint).isEqualTo(lost)))
                .rootCause().isInstanceOfSatisfying(MongoException.class, e -> assertThat(e.getCode()).isEqualTo(MongoCommons.CHANGE_STREAM_HISTORY_LOST_ERROR_CODE));
        assertThat(subscriptionModel.subscriptionIds()).doesNotContain(subscriptionId);
        assertThat(subscriptionModel.subscribe(subscriptionId, StartAt.now(), __ -> {
        }).waitUntilStarted(Duration.ofSeconds(10))).as("the id is free to subscribe with again").isTrue();
    }

    // Writes ten megabytes into the shard through the mongos, then a small document every hundred milliseconds, until
    // the oldest entry of the shard's oplog is newer than the operation time, as ChangeStreamHistory does for a replica set
    private void dropHistoryFrom(BsonTimestamp operationTime) {
        mongosh(MONGOS_PORT, "const filler = db.getSiblingDB('" + databaseName + "').getCollection('filler');"
                + "const oplog = connect('mongodb://localhost:" + SHARD_PORT + "/local').getCollection('oplog.rs');"
                + "const lostT = " + operationTime.getTime() + ", lostI = " + operationTime.getInc() + ";"
                + "const reaches = () => { const ts = oplog.find().sort({$natural: 1}).limit(1).next().ts; return ts.t < lostT || (ts.t === lostT && ts.i <= lostI); };"
                + "const deadline = Date.now() + 180000; let batches = 0;"
                + "while (reaches()) {"
                + "  if (Date.now() > deadline) { throw new Error('The oplog still reaches back to the operation time after 3 minutes'); }"
                + "  if (batches < 5) { filler.insertMany(Array.from({length: 20}, () => ({filler: 'x'.repeat(100000)}))); batches++; } else { filler.insertOne({filler: 'x'}); }"
                + "  sleep(100);"
                + "}"
                + "filler.drop();");
    }

    private static void awaitShardPrimary() {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        try {
            while (System.nanoTime() < deadline) {
                ExecResult result = container.execInContainer("mongosh", "--quiet", "--port", String.valueOf(SHARD_PORT), "--eval", "db.hello().isWritablePrimary");
                if (result.getExitCode() == 0 && result.getStdout().trim().equals("true")) {
                    return;
                }
                Thread.sleep(500);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the shard", e);
        }
        throw new IllegalStateException("The shard had no primary after 60 seconds");
    }

    private static void mongosh(int port, String script) {
        exec("mongosh", "--quiet", "--port", String.valueOf(port), "--eval", "const assert = (c) => { if (!c) { throw new Error('assertion failed'); } };" + script);
    }

    private static ExecResult exec(String... command) {
        try {
            ExecResult result = container.execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(String.join(" ", command) + " failed with exit code " + result.getExitCode() + ": " + result.getStdout() + result.getStderr());
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while running " + String.join(" ", command), e);
        }
    }
}
