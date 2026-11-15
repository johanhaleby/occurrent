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
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerForgetFailureTest {

    private static final String COLLECTION = "checkpoints";
    private static final String APPLICATION_NAME = "marker-forget-failure";
    private static final int BAD_VALUE = 2;
    private static final int WRITE_CONCERN_FAILED = 64;
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    // enableTestCommands lets the server accept configureFailPoint. withCommand replaces the whole command line, so
    // the replica set name is repeated
    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion()
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private NativeMongoCheckpointStorage storage;

    @BeforeEach
    void open_a_store_that_does_not_retry_and_save_a_marker() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("nativemarkerforgetfailure"));
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applicationName(APPLICATION_NAME)
                .writeConcern(WriteConcern.MAJORITY)
                .addCommandListener(commands)
                .build());
        storage = new NativeMongoCheckpointStorage(mongoClient.getDatabase(requireNonNull(connectionString.getDatabase())).getCollection(COLLECTION), RetryStrategy.none());
        storage.saveCatchupMarker(subscriptionId, MARKER, storage.readCatchupMarker(subscriptionId).generation(), CheckpointWriteCondition.any());
    }

    @AfterEach
    void turn_off_the_fail_point_and_close_mongo_client() {
        configureFailPoint(new Document("mode", "off"));
        mongoClient.close();
    }

    @Test
    void forget_throws_and_keeps_the_marker_when_raising_the_generation_fails() {
        // Given
        failNextUpdate(new Document("errorCode", BAD_VALUE));
        commands.clear();

        // When
        Throwable thrown = catchThrowable(() -> storage.forgetCatchupMarker(subscriptionId));

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(MongoException.class),
                () -> assertThat(commands.commands("delete", COLLECTION)).as("deletes").isEmpty(),
                () -> assertThat(storage.read(subscriptionId)).as("plain read").isNotNull()
        );
    }

    @Test
    void forget_throws_and_keeps_the_marker_when_raising_the_generation_passes_its_write_timeout() {
        // Given
        failNextUpdate(new Document("writeConcernError", new Document("code", WRITE_CONCERN_FAILED)
                .append("errmsg", "waiting for replication timed out")
                .append("errInfo", new Document("wtimeout", true))));
        commands.clear();

        // When
        Throwable thrown = catchThrowable(() -> storage.forgetCatchupMarker(subscriptionId));

        // Then
        assertAll(
                () -> assertThat(thrown).isInstanceOf(MongoException.class),
                () -> assertThat(commands.commands("delete", COLLECTION)).as("deletes").isEmpty(),
                () -> assertThat(storage.read(subscriptionId)).as("plain read").isNotNull()
        );
    }

    private void failNextUpdate(Document failure) {
        Document data = new Document("failCommands", List.of("update")).append("appName", APPLICATION_NAME);
        data.putAll(failure);
        configureFailPoint(new Document("mode", new Document("times", 1)).append("data", data));
    }

    private void configureFailPoint(Document configuration) {
        Document command = new Document("configureFailPoint", "failCommand");
        command.putAll(configuration);
        mongoClient.getDatabase("admin").runCommand(command);
    }
}
