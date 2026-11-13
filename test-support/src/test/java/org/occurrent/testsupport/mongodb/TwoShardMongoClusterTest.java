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

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster.Hold;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.testsupport.mongodb.TwoShardMongoCluster.FIRST_SHARD;
import static org.occurrent.testsupport.mongodb.TwoShardMongoCluster.SECOND_SHARD;

@DisplayName("Two shard MongoDB cluster")
@DisplayNameGeneration(ReplaceUnderscores.class)
class TwoShardMongoClusterTest {

    private static final String CHECKPOINTS = "events_dcb_checkpoints";
    private static final String ID_ON_FIRST_SHARD = "marker:tag:a:x";
    private static final String ID_ON_SECOND_SHARD = "marker:tag:z:y";

    private static TwoShardMongoCluster cluster;
    private static MongoClient mongo;

    @BeforeAll
    static void start_the_cluster() {
        cluster = TwoShardMongoCluster.start();
        mongo = MongoClients.create(cluster.getConnectionString());
    }

    @AfterAll
    static void stop_the_cluster() {
        if (mongo != null) {
            mongo.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void places_checkpoint_markers_on_the_shard_their_id_belongs_to_when_the_database_primary_is_the_second_shard() {
        String database = cluster.createDatabase(SECOND_SHARD);

        cluster.shardCheckpointCollection(database, CHECKPOINTS);

        assertMarkersAreSplitAcrossTheShards(database);
    }

    @Test
    void places_checkpoint_markers_on_the_shard_their_id_belongs_to_when_the_database_primary_is_the_first_shard() {
        String database = cluster.createDatabase(FIRST_SHARD);

        cluster.shardCheckpointCollection(database, CHECKPOINTS);

        assertMarkersAreSplitAcrossTheShards(database);
    }

    @Test
    void puts_an_unsharded_collection_on_the_primary_shard_of_its_database() {
        String database = cluster.createDatabase(SECOND_SHARD);
        MongoCollection<Document> events = mongo.getDatabase(database).getCollection("events");
        events.insertOne(new Document("_id", "event-1"));

        assertThat(shardsOwning(database, "events", "event-1")).containsExactly(SECOND_SHARD);
    }

    @Test
    void places_checkpoint_markers_on_the_shard_their_id_belongs_to_when_the_collection_name_has_a_quote_and_a_backslash() {
        String database = cluster.createDatabase(SECOND_SHARD);
        String collection = "events'archive\\x_dcb_checkpoints";

        cluster.shardCheckpointCollection(database, collection);

        assertMarkersAreSplitAcrossTheShards(database, collection);
    }

    @Test
    void holds_a_commit_on_the_second_shard_until_the_commits_are_released() throws Exception {
        String database = cluster.createDatabase(SECOND_SHARD);
        MongoDatabase db = mongo.getDatabase(database);
        db.createCollection("events");
        MongoCollection<Document> events = db.getCollection("events");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ClientSession session = mongo.startSession()) {
            Future<?> commit = null;
            try (Hold ignored = cluster.holdCommitsOnSecondShard()) {
                session.startTransaction();
                events.insertOne(session, new Document("_id", "held"));
                commit = executor.submit(session::commitTransaction);
                Future<?> held = commit;

                assertThatThrownBy(() -> held.get(2, TimeUnit.SECONDS))
                        .as("the commit on the second shard should still be held after 2 seconds")
                        .isInstanceOf(TimeoutException.class);

                cluster.releaseCommitsOnSecondShard();

                commit.get(30, TimeUnit.SECONDS);
            } finally {
                // The hold is released by now, so the commit can finish before the session closes
                awaitSettled(commit);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(events.find(new Document("_id", "held")).first()).isNotNull();
    }

    @Test
    void holds_the_second_shards_half_of_a_cross_shard_commit_while_the_first_shard_commits() throws Exception {
        String database = cluster.createDatabase(SECOND_SHARD);
        MongoDatabase db = mongo.getDatabase(database);
        cluster.shardCheckpointCollection(database, CHECKPOINTS);
        db.createCollection("events");
        MongoCollection<Document> checkpoints = db.getCollection(CHECKPOINTS);
        MongoCollection<Document> events = db.getCollection("events");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ClientSession session = mongo.startSession()) {
            Future<?> commit = null;
            try (Hold ignored = cluster.holdCommitsOnSecondShard()) {
                session.startTransaction();
                checkpoints.insertOne(session, new Document("_id", ID_ON_FIRST_SHARD));
                events.insertOne(session, new Document("_id", "event-1"));
                commit = executor.submit(session::commitTransaction);
                Future<?> held = commit;

                assertThatThrownBy(() -> held.get(2, TimeUnit.SECONDS))
                        .as("the cross shard commit should still be held after 2 seconds")
                        .isInstanceOf(TimeoutException.class);
                assertThat(events.find(new Document("_id", "event-1")).first())
                        .as("the event on the second shard while its commit is held").isNull();
                assertThat(visibleWithinTenSeconds(checkpoints, ID_ON_FIRST_SHARD))
                        .as("the marker on the first shard, read outside the transaction, while the second shard's commit is held").isTrue();

                cluster.releaseCommitsOnSecondShard();

                commit.get(30, TimeUnit.SECONDS);
            } finally {
                awaitSettled(commit);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(checkpoints.find(new Document("_id", ID_ON_FIRST_SHARD)).first()).as("marker after the commit").isNotNull();
        assertThat(events.find(new Document("_id", "event-1")).first()).as("event after the commit").isNotNull();
    }

    // On a failing path the hold is already released when this runs, so a commit still in flight finishes
    private static void awaitSettled(Future<?> commit) {
        if (commit == null) {
            return;
        }
        try {
            commit.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException ignored) {
            // The test reports its own failure, and this only keeps the session open until the commit is over
        }
    }

    private static boolean visibleWithinTenSeconds(MongoCollection<Document> collection, String id) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (collection.find(new Document("_id", id)).first() == null) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(100);
        }
        return true;
    }

    private void assertMarkersAreSplitAcrossTheShards(String database) {
        assertMarkersAreSplitAcrossTheShards(database, CHECKPOINTS);
    }

    private void assertMarkersAreSplitAcrossTheShards(String database, String collection) {
        MongoCollection<Document> checkpoints = mongo.getDatabase(database).getCollection(collection);
        checkpoints.insertOne(new Document("_id", ID_ON_FIRST_SHARD));
        checkpoints.insertOne(new Document("_id", ID_ON_SECOND_SHARD));

        assertThat(shardsOwning(database, collection, ID_ON_FIRST_SHARD))
                .as("shards owning %s", ID_ON_FIRST_SHARD).containsExactly(FIRST_SHARD);
        assertThat(shardsOwning(database, collection, ID_ON_SECOND_SHARD))
                .as("shards owning %s", ID_ON_SECOND_SHARD).containsExactly(SECOND_SHARD);
    }

    // The mongos reports in the explain which shards it would route a find by _id to
    private Set<String> shardsOwning(String database, String collection, String id) {
        Document explain = mongo.getDatabase(database).runCommand(new Document("explain",
                new Document("find", collection).append("filter", new Document("_id", id)))
                .append("verbosity", "queryPlanner"));
        Set<String> shardNames = new LinkedHashSet<>();
        collectShardNames(explain, shardNames);
        return shardNames;
    }

    private static void collectShardNames(Object node, Set<String> shardNames) {
        if (node instanceof Map<?, ?> map) {
            map.forEach((key, value) -> {
                if ("shardName".equals(key) && value instanceof String name) {
                    shardNames.add(name);
                } else {
                    collectShardNames(value, shardNames);
                }
            });
        } else if (node instanceof Collection<?> values) {
            values.forEach(value -> collectShardNames(value, shardNames));
        }
    }
}
