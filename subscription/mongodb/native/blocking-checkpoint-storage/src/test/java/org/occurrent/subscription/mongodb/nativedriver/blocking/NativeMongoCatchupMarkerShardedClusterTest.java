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
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The catch-up marker through a mongos in front of two shards. On the collection sharded on {@code {_id: 1}} a
 * checkpoint id below {@link TwoShardMongoCluster#SPLIT_POINT} is on the first shard and every generation document on
 * the second, since an embedded {@code _id} sorts after every string.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerShardedClusterTest {

    private static final String RANGED = "ranged_checkpoints";
    private static final String HASHED = "hashed_checkpoints";
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    private static final RecordingCommandListener commands = new RecordingCommandListener();
    private static TwoShardMongoCluster cluster;
    private static MongoClient client;
    private static String database;

    @BeforeAll
    static void start_the_cluster_and_shard_both_checkpoint_collections() {
        cluster = TwoShardMongoCluster.start();
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(cluster.getConnectionString()))
                .writeConcern(WriteConcern.MAJORITY)
                .addCommandListener(commands)
                .build());
        database = cluster.createDatabase(TwoShardMongoCluster.FIRST_SHARD);
        cluster.shardCheckpointCollection(database, RANGED);
        client.getDatabase("admin").runCommand(new Document("shardCollection", database + "." + HASHED).append("key", new Document("_id", "hashed")));
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

    @ParameterizedTest
    @ValueSource(strings = {RANGED, HASHED})
    void a_late_save_after_a_forget_reads_absent_and_a_save_with_the_new_generation_reads_present(String collection) {
        // Given
        String subscriptionId = "a-" + UUID.randomUUID();
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(client.getDatabase(database).getCollection(collection).withReadPreference(ReadPreference.secondaryPreferred()));
        long before = storage.readCatchupMarker(subscriptionId).generation();
        storage.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        CatchupMarker saved = storage.readCatchupMarker(subscriptionId);

        // When
        storage.forgetCatchupMarker(subscriptionId);
        storage.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        commands.clear();
        CatchupMarker lateSaved = storage.readCatchupMarker(subscriptionId);
        List<Command> finds = commands.commands("find", collection);
        storage.saveCatchupMarker(subscriptionId, MARKER, lateSaved.generation(), CheckpointWriteCondition.any());
        CatchupMarker savedAfterForget = storage.readCatchupMarker(subscriptionId);

        // Then
        assertAll(
                () -> assertThat(saved.isPresent()).as("present after the save").isTrue(),
                () -> assertThat(lateSaved.isPresent()).as("present after the late save").isFalse(),
                () -> assertThat(lateSaved.generation()).as("generation after the forget").isNotEqualTo(before),
                () -> assertThat(savedAfterForget.isPresent()).as("present after a save with the new generation").isTrue(),
                () -> assertThat(finds).extracting(Command::readConcernLevel).as("%s", finds).containsExactly("majority", "linearizable"),
                () -> assertThat(finds).extracting(NativeMongoCatchupMarkerShardedClusterTest::readPreferenceMode).as("%s", finds).containsOnly("primary")
        );
    }

    // A mongos gets no $readPreference for a primary read
    private static String readPreferenceMode(Command command) {
        BsonDocument readPreference = command.document().getDocument("$readPreference", null);
        return readPreference == null ? "primary" : readPreference.getString("mode").getValue();
    }
}
