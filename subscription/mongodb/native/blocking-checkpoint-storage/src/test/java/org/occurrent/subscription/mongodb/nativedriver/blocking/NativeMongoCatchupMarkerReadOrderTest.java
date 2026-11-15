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
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.CatchupMarker;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerReadOrderTest {

    private static final String COLLECTION = "checkpoints";
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    // Runs once, on the reading thread, right after the first find of a read has returned
    private final AtomicReference<Runnable> afterFirstFind = new AtomicReference<>();
    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient readingClient;
    private MongoClient writingClient;
    private NativeMongoCheckpointStorage reading;
    private NativeMongoCheckpointStorage writing;

    @BeforeEach
    void open_a_reading_store_and_a_writing_store_on_two_clients() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("nativemarkerreadorder"));
        String database = requireNonNull(connectionString.getDatabase());
        CommandListener runBetweenTheReads = new CommandListener() {
            @Override
            public void commandSucceeded(CommandSucceededEvent event) {
                if (event.getCommandName().equals("find")) {
                    Runnable between = afterFirstFind.getAndSet(null);
                    if (between != null) {
                        between.run();
                    }
                }
            }
        };
        readingClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.MAJORITY).addCommandListener(runBetweenTheReads).build());
        writingClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.MAJORITY).build());
        reading = new NativeMongoCheckpointStorage(readingClient.getDatabase(database).getCollection(COLLECTION));
        writing = new NativeMongoCheckpointStorage(writingClient.getDatabase(database).getCollection(COLLECTION));
    }

    @AfterEach
    void close_mongo_clients() {
        readingClient.close();
        writingClient.close();
    }

    @Test
    void read_reports_absent_when_a_forget_and_a_late_save_land_between_its_two_finds() {
        // Given
        long before = reading.readCatchupMarker(subscriptionId).generation();
        reading.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        afterFirstFind.set(() -> {
            writing.forgetCatchupMarker(subscriptionId);
            writing.saveCatchupMarker(subscriptionId, MARKER, before, CheckpointWriteCondition.any());
        });

        // When
        CatchupMarker marker = reading.readCatchupMarker(subscriptionId);

        // Then
        assertAll(
                () -> assertThat(afterFirstFind.get()).as("the forget and the late save ran between the two finds").isNull(),
                () -> assertThat(marker.isPresent()).as("present").isFalse(),
                () -> assertThat(marker.generation()).as("generation").isNotEqualTo(before)
        );
    }
}
