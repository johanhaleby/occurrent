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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerNoJournalTest {

    private static final String COLLECTION = "checkpoints";
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    // A mongod started with --nojournal refuses every write that asks for j: true. The option is gone from 6.1, so the
    // image is pinned to a version that still has it rather than taken from the default version.
    @Container
    private static final GenericContainer<?> withoutJournaling = new GenericContainer<>("mongo:4.4.5")
            .withCommand("--nojournal")
            .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1")
            .withExposedPorts(27017)
            .waitingFor(Wait.forLogMessage(".*[Ww]aiting for connections.*", 1));

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private NativeMongoCheckpointStorage storage;

    @BeforeEach
    void open_a_store_that_does_not_retry() {
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://" + withoutJournaling.getHost() + ":" + withoutJournaling.getMappedPort(27017)))
                .addCommandListener(commands)
                .build());
        String database = "nojournal_" + UUID.randomUUID().toString().replace("-", "");
        storage = new NativeMongoCheckpointStorage(mongoClient.getDatabase(database).getCollection(COLLECTION), RetryStrategy.none());
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void forget_leaves_the_marker_absent_and_asks_for_journal_only_until_the_server_refuses_it() {
        // Given
        long before = storage.readCatchupMarker(subscriptionId).generation();
        storage.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        commands.clear();

        // When
        Throwable firstForget = catchThrowable(() -> storage.forgetCatchupMarker(subscriptionId));
        CatchupMarker forgotten = storage.readCatchupMarker(subscriptionId);
        Throwable secondForget = catchThrowable(() -> storage.forgetCatchupMarker(subscriptionId));
        CatchupMarker forgottenAgain = storage.readCatchupMarker(subscriptionId);

        // Then
        List<Command> forgetWrites = commands.commands().stream()
                .filter(command -> COLLECTION.equals(command.collection()) && (command.name().equals("update") || command.name().equals("delete")))
                .toList();
        List<Boolean> askedForJournal = forgetWrites.stream().map(write -> write.document().getDocument("writeConcern", new BsonDocument()).containsKey("j")).toList();
        assertAll(
                () -> assertThat(firstForget).as("first forget").isNull(),
                () -> assertThat(secondForget).as("second forget").isNull(),
                () -> assertThat(forgetWrites).extracting(Command::name).as("%s", forgetWrites).containsExactly("update", "update", "delete", "update", "delete"),
                () -> assertThat(askedForJournal).as("asked for j in each write, in order, %s", forgetWrites).containsExactly(true, false, false, false, false),
                () -> assertThat(forgetWrites.get(0).document().getDocument("writeConcern").get("j")).as("j in the first write").isEqualTo(BsonBoolean.TRUE),
                () -> assertThat(forgetWrites.get(0).reply()).as("the server refused the first write, so it has no success reply, %s", forgetWrites.get(0)).isNull(),
                () -> assertThat(forgotten.isPresent()).as("present after the forget").isFalse(),
                () -> assertThat(forgotten.generation()).as("generation after the forget").isNotEqualTo(before),
                () -> assertThat(forgottenAgain.isPresent()).as("present after the second forget").isFalse(),
                () -> assertThat(forgottenAgain.generation()).as("generation after the second forget").isNotIn(before, forgotten.generation())
        );
    }
}
