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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoCatchupMarkerStandaloneTest {

    private static final String COLLECTION = "checkpoints";
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    // Not a replica set member, so the server refuses a linearizable read
    @Container
    private static final MongoDBContainer standalone = new MongoDBContainer(ReplicaSetReadyMongoDBContainer.withDefaultVersion().getDockerImageName())
            .withEnv("GLIBC_TUNABLES", "glibc.pthread.rseq=1");

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private SpringMongoCheckpointStorage storage;

    @BeforeEach
    void open_a_store_that_does_not_retry() {
        mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(standalone.getConnectionString()))
                .addCommandListener(commands)
                .build());
        String database = "standalone_" + UUID.randomUUID().toString().replace("-", "");
        storage = new SpringMongoCheckpointStorage(new MongoTemplate(mongoClient, database), COLLECTION, RetryStrategy.none());
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void reads_the_generation_at_majority_after_the_server_refuses_the_first_linearizable_read() {
        // When
        long before = storage.readCatchupMarker(subscriptionId).generation();
        storage.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        CatchupMarker saved = storage.readCatchupMarker(subscriptionId);
        storage.forgetCatchupMarker(subscriptionId);
        CatchupMarker forgotten = storage.readCatchupMarker(subscriptionId);
        storage.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        CatchupMarker lateSaved = storage.readCatchupMarker(subscriptionId);
        storage.saveCatchupMarker(subscriptionId, MARKER, forgotten.generation(), CheckpointWriteCondition.any());
        CatchupMarker savedAfterForget = storage.readCatchupMarker(subscriptionId);

        // Then
        List<Command> linearizableReads = commands.commands("find", COLLECTION).stream().filter(find -> "linearizable".equals(find.readConcernLevel())).toList();
        assertAll(
                () -> assertThat(linearizableReads).as("%s", linearizableReads).hasSize(1),
                () -> assertThat(linearizableReads.get(0).reply()).as("the reply to the linearizable read").isNull(),
                () -> assertThat(before).as("generation before the forget").isZero(),
                () -> assertThat(saved.isPresent()).as("present after the save").isTrue(),
                () -> assertThat(forgotten.isPresent()).as("present after the forget").isFalse(),
                () -> assertThat(forgotten.generation()).as("generation after the forget").isNotEqualTo(before),
                () -> assertThat(lateSaved.isPresent()).as("present after the late save").isFalse(),
                () -> assertThat(savedAfterForget.isPresent()).as("present after a save with the new generation").isTrue()
        );
    }
}
