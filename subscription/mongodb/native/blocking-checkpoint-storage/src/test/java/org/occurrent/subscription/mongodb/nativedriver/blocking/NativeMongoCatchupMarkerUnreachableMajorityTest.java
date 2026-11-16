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

import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.testsupport.mongodb.LaggingSecondaryReplicaSet;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Runs against a client whose operation timeout is 0, which makes the driver drop the write timeout of a forget and the
 * time limit of the generation read, on a set where no majority answers. Without a bound of its own the store waits forever.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class NativeMongoCatchupMarkerUnreachableMajorityTest {

    private static final Duration BOUND_WELL_ABOVE_THE_TEN_SECOND_LIMITS = Duration.ofSeconds(30);

    private static LaggingSecondaryReplicaSet replicaSet;
    private static MongoClient mongoClient;
    private static MongoClient clientWithoutOperationTimeout;
    private static NativeMongoCheckpointStorage storage;

    @BeforeAll
    static void start_a_replica_set_whose_secondaries_have_stopped_replicating_and_a_store_on_a_client_with_an_infinite_operation_timeout() {
        replicaSet = LaggingSecondaryReplicaSet.start();
        mongoClient = MongoClients.create(replicaSet.clientSettings().timeout(0, TimeUnit.SECONDS).build());
        storage = new NativeMongoCheckpointStorage(mongoClient.getDatabase("markerunreachablemajority").getCollection("checkpoints", Document.class), RetryStrategy.retry().maxAttempts(1));
        // A majority read of a collection that only an unacknowledged write has created waits for that write, so the collection exists first
        mongoClient.getDatabase("markerunreachablemajority").createCollection("checkpoints");
        clientWithoutOperationTimeout = MongoClients.create(replicaSet.clientSettings().build());
        replicaSet.stopReplicatingToSecondaries();
    }

    @AfterAll
    static void stop_the_replica_set() {
        clientWithoutOperationTimeout.close();
        mongoClient.close();
        replicaSet.close();
    }

    @Test
    void forget_fails_within_a_bound_when_no_majority_answers() {
        assertTimeoutPreemptively(BOUND_WELL_ABOVE_THE_TEN_SECOND_LIMITS, () ->
                assertThatThrownBy(() -> storage.forgetCatchupMarker(UUID.randomUUID().toString())).isInstanceOf(MongoOperationTimeoutException.class));
    }

    @Test
    void read_fails_within_a_bound_when_no_majority_answers() {
        assertTimeoutPreemptively(BOUND_WELL_ABOVE_THE_TEN_SECOND_LIMITS, () ->
                assertThatThrownBy(() -> storage.readCatchupMarker(UUID.randomUUID().toString())).isInstanceOf(MongoOperationTimeoutException.class));
    }

    @Test
    void read_of_a_collection_that_no_majority_saw_created_fails_within_a_bound_on_a_client_with_an_infinite_operation_timeout() {
        // Given
        NativeMongoCheckpointStorage unseeded = storageOnACollectionCreatedWithoutAMajority(mongoClient, "unseededinfinite");

        // When / Then
        assertTimeoutPreemptively(BOUND_WELL_ABOVE_THE_TEN_SECOND_LIMITS, () ->
                assertThatThrownBy(() -> unseeded.readCatchupMarker(UUID.randomUUID().toString())).isInstanceOf(MongoOperationTimeoutException.class));
    }

    @Test
    void read_of_a_collection_that_no_majority_saw_created_fails_within_a_bound_on_a_client_without_an_operation_timeout() {
        // Given
        NativeMongoCheckpointStorage unseeded = storageOnACollectionCreatedWithoutAMajority(clientWithoutOperationTimeout, "unseededdefault");

        // When / Then
        assertTimeoutPreemptively(BOUND_WELL_ABOVE_THE_TEN_SECOND_LIMITS, () ->
                assertThatThrownBy(() -> unseeded.readCatchupMarker(UUID.randomUUID().toString())).isInstanceOf(MongoExecutionTimeoutException.class));
    }

    // Written at w:1 after replication stopped, so only the primary knows the collection
    private static NativeMongoCheckpointStorage storageOnACollectionCreatedWithoutAMajority(MongoClient client, String collectionName) {
        MongoCollection<Document> collection = client.getDatabase("markerunreachablemajority").getCollection(collectionName, Document.class);
        collection.withWriteConcern(WriteConcern.W1).insertOne(new Document("_id", "created-without-a-majority"));
        return new NativeMongoCheckpointStorage(collection, RetryStrategy.retry().maxAttempts(1));
    }
}
