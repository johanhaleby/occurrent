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

package org.occurrent.testsupport.mongodb;

import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.mongodb.MongoDBContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.UUID;

/**
 * A sharded MongoDB cluster with two shards in one container, so a test can hold a transaction's commit on one shard
 * while the other shard has already committed.
 * <p>
 * The first shard is the one {@code withSharding()} starts. The second is a single-node replica set started in the same
 * container. {@link #shardCheckpointCollection(String, String)} puts a collection's ids below {@link #SPLIT_POINT} on
 * the first shard and the rest on the second.
 */
public final class TwoShardMongoCluster implements AutoCloseable {

    /**
     * The name of the shard that {@code MongoDBContainer.withSharding()} starts.
     */
    public static final String FIRST_SHARD = "shardsvr-rs";

    /**
     * The name of the shard that this class adds, and the one {@link #holdCommitsOnSecondShard()} holds.
     */
    public static final String SECOND_SHARD = "rs2";

    /**
     * The id at which {@link #shardCheckpointCollection(String, String)} splits a collection. Lower ids sit on the first
     * shard, and this id and higher ones on the second.
     */
    public static final String SPLIT_POINT = "marker:tag:m";

    private static final int MONGOS_PORT = 27017;
    private static final int SECOND_SHARD_PORT = 27020;
    private static final String COMMIT_FAIL_POINT = "hangBeforeCommitingTxn";

    // A shell helper every call goes through, so an answer that isn't ok throws instead of being printed and ignored
    private static final String OK = "const ok = (r) => { if (!r || r.ok !== 1) { throw new Error('not ok: ' + JSON.stringify(r)); } return r; };";

    private final MongoDBContainer container;
    private final String hostname;

    private TwoShardMongoCluster(MongoDBContainer container, String hostname) {
        this.container = container;
        this.hostname = hostname;
    }

    /**
     * Starts the container, starts the second shard in it and adds that shard to the cluster.
     */
    public static TwoShardMongoCluster start() {
        MongoDBContainer container = new MongoDBContainer(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
                .withSharding()
                .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");
        try {
            container.start();
            TwoShardMongoCluster cluster = new TwoShardMongoCluster(container, shell(container, "hostname").trim());
            cluster.addSecondShard();
            return cluster;
        } catch (RuntimeException | Error e) {
            container.stop();
            throw e;
        }
    }

    /**
     * The connection string of the mongos, with no database.
     */
    public String getConnectionString() {
        return container.getConnectionString();
    }

    /**
     * Creates a database with a unique name whose primary shard is the given one, so the collections a store creates
     * there unsharded, such as its events, live on that shard. Returns the name.
     */
    public String createDatabase(String primaryShard) {
        String database = "sharded_" + UUID.randomUUID().toString().replace("-", "");
        mongosh(MONGOS_PORT, "ok(sh.enableSharding('" + database + "', '" + primaryShard + "'));");
        return database;
    }

    /**
     * Shards the collection on {@code _id} and splits it at {@link #SPLIT_POINT}, so ids below it are on the
     * {@link #FIRST_SHARD first shard} and the rest on the {@link #SECOND_SHARD second}. A store's DCB markers are in the
     * collection named {@code <event collection>_dcb_checkpoints}.
     */
    public void shardCheckpointCollection(String database, String collection) {
        String namespace = database + "." + collection;
        mongosh(MONGOS_PORT, "ok(sh.shardCollection('" + namespace + "', {_id: 1}));"
                + "ok(sh.splitAt('" + namespace + "', {_id: '" + SPLIT_POINT + "'}));"
                + "ok(sh.moveChunk('" + namespace + "', {_id: 'marker:tag:zzz'}, '" + SECOND_SHARD + "'));"
                + "ok(sh.moveChunk('" + namespace + "', {_id: 'marker:tag:aaa'}, '" + FIRST_SHARD + "'));");
    }

    /**
     * Makes every transaction that commits on the second shard wait before it commits there, until
     * {@link #releaseCommitsOnSecondShard()}.
     */
    public void holdCommitsOnSecondShard() {
        setCommitFailPoint("alwaysOn");
    }

    /**
     * Lets the commits that {@link #holdCommitsOnSecondShard()} holds go through.
     */
    public void releaseCommitsOnSecondShard() {
        setCommitFailPoint("off");
    }

    private void setCommitFailPoint(String mode) {
        mongosh(SECOND_SHARD_PORT, "ok(db.adminCommand({configureFailPoint: '" + COMMIT_FAIL_POINT + "', mode: '" + mode + "'}));");
    }

    private void addSecondShard() {
        shell(container, "mkdir -p /tmp/" + SECOND_SHARD + " && mongod --bind_ip_all --shardsvr --replSet " + SECOND_SHARD + " --port " + SECOND_SHARD_PORT
                + " --dbpath /tmp/" + SECOND_SHARD + " --fork --logpath /tmp/" + SECOND_SHARD + ".log --wiredTigerCacheSizeGB 0.25 --setParameter enableTestCommands=1");
        mongosh(SECOND_SHARD_PORT, "ok(rs.initiate({_id: '" + SECOND_SHARD + "', members: [{_id: 0, host: '" + hostname + ":" + SECOND_SHARD_PORT + "'}]}));");
        awaitSecondShardPrimary();
        mongosh(MONGOS_PORT, "ok(sh.addShard('" + SECOND_SHARD + "/" + hostname + ":" + SECOND_SHARD_PORT + "'));");
    }

    private void awaitSecondShardPrimary() {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        try {
            while (System.nanoTime() < deadline) {
                ExecResult result = exec("mongosh", "--quiet", "--port", String.valueOf(SECOND_SHARD_PORT), "--eval", "db.hello().isWritablePrimary");
                if (result.getExitCode() == 0 && result.getStdout().trim().equals("true")) {
                    return;
                }
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the second shard", e);
        }
        throw new IllegalStateException("The second shard had no primary after 60 seconds");
    }

    private void mongosh(int port, String script) {
        ExecResult result = exec("mongosh", "--quiet", "--port", String.valueOf(port), "--eval", OK + script);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("mongosh on port " + port + " failed with exit code " + result.getExitCode() + " for [" + script + "]: " + result.getStdout() + result.getStderr());
        }
    }

    private static String shell(MongoDBContainer container, String command) {
        ExecResult result = exec(container, "sh", "-c", command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("[" + command + "] failed with exit code " + result.getExitCode() + ": " + result.getStdout() + result.getStderr());
        }
        return result.getStdout();
    }

    private ExecResult exec(String... command) {
        return exec(container, command);
    }

    private static ExecResult exec(MongoDBContainer container, String... command) {
        try {
            return container.execInContainer(command);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while running " + String.join(" ", command), e);
        }
    }

    /**
     * Stops the container, and with it both shards.
     */
    @Override
    public void close() {
        container.stop();
    }
}
