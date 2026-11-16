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
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.jspecify.annotations.NullMarked;
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
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerIfAbsentTest {

    private static final String COLLECTION = "checkpoints";
    private static final Checkpoint MARKER = new StringBasedCheckpoint("marker");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private final String subscriptionId = UUID.randomUUID().toString();
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @BeforeEach
    void open_a_collection_that_writes_at_majority() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("nativemarkerifabsent"));
        mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).writeConcern(WriteConcern.MAJORITY).build());
        collection = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase())).getCollection(COLLECTION);
    }

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void an_if_absent_marker_save_on_an_empty_id_returns_true_and_stores_the_marker_with_the_generation() {
        // Given
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(collection);
        long generation = storage.readCatchupMarker(subscriptionId).generation();

        // When
        boolean written = storage.saveCatchupMarker(subscriptionId, MARKER, generation, CheckpointWriteCondition.ifAbsent());

        // Then
        CatchupMarker marker = storage.readCatchupMarker(subscriptionId);
        assertAll(
                () -> assertThat(written).as("written").isTrue(),
                () -> assertThat(marker.isPresent()).as("present").isTrue(),
                () -> assertThat(marker.generation()).as("generation").isEqualTo(generation)
        );
    }

    @Test
    void an_if_absent_marker_save_whose_first_reply_is_lost_returns_true_when_the_retry_finds_its_own_marker() {
        // Given, a first attempt that writes the marker and then fails, so the call retries
        AtomicInteger attempts = new AtomicInteger();
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(collection, RetryStrategy.fixed(1L).maxAttempts(3)) {
            @NullMarked
            @Override
            Document persistConditionalCheckpointDocument(String subscriptionId, Document newCheckpointDocument, CheckpointWriteCondition condition) {
                Document afterDocument = super.persistConditionalCheckpointDocument(subscriptionId, newCheckpointDocument, condition);
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("Reply lost after the write");
                }
                return afterDocument;
            }
        };
        long generation = storage.readCatchupMarker(subscriptionId).generation();

        // When
        boolean written = storage.saveCatchupMarker(subscriptionId, MARKER, generation, CheckpointWriteCondition.ifAbsent());

        // Then
        CatchupMarker marker = storage.readCatchupMarker(subscriptionId);
        assertAll(
                () -> assertThat(attempts).as("attempts that reached Mongo").hasValue(2),
                () -> assertThat(written).as("written").isTrue(),
                () -> assertThat(marker.isPresent()).as("present").isTrue(),
                () -> assertThat(marker.generation()).as("generation").isEqualTo(generation)
        );
    }

    @Test
    void an_if_absent_marker_save_returns_false_over_a_marker_with_the_same_value_and_an_older_stamp() {
        // Given, a marker that a forget removed and a late write of the same value put back with the old generation
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(collection);
        long olderGeneration = storage.readCatchupMarker(subscriptionId).generation();
        storage.saveCatchupMarker(subscriptionId, MARKER, olderGeneration, CheckpointWriteCondition.any());
        storage.forgetCatchupMarker(subscriptionId);
        storage.saveCatchupMarker(subscriptionId, MARKER, olderGeneration, CheckpointWriteCondition.any());
        long currentGeneration = storage.readCatchupMarker(subscriptionId).generation();

        // When
        boolean written = storage.saveCatchupMarker(subscriptionId, MARKER, currentGeneration, CheckpointWriteCondition.ifAbsent());

        // Then
        CatchupMarker marker = storage.readCatchupMarker(subscriptionId);
        assertAll(
                () -> assertThat(currentGeneration).as("generation after the forget").isNotEqualTo(olderGeneration),
                () -> assertThat(written).as("written").isFalse(),
                () -> assertThat(marker.isPresent()).as("present").isFalse(),
                () -> assertThat(marker.generation()).as("generation").isEqualTo(currentGeneration)
        );
    }
}
