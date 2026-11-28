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
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * The write version {@link DurableSubscriptionModel} stores with a checkpoint already stored, over
 * {@link NativeMongoCheckpointStorage}, which compares two operation times when it resolves a first position race and
 * must not do that for a version.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStoredCheckpointWriteVersionMongoTest {

    private static final String DATABASE = "durablestoredcheckpointwriteversion";
    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    @BeforeAll
    static void connect() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl(DATABASE));
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Test
    void a_later_operation_time_a_stale_node_stored_after_this_node_read_an_earlier_one_stays_stored_and_gets_this_nodes_version() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollection.getNamespace().getCollectionName(), RFC_3339_STRING, executor);
        // Holds the delivery of an event at the later position, whose checkpointing would replace what is asserted on
        CountDownLatch assertionsRead = new CountDownLatch(1);
        try {
            Checkpoint earlier = requireNonNull(nativeModel.globalCheckpoint());
            eventStore.write("stream", List.of(event("moves-the-clock-on", "SomethingHappened")));
            Checkpoint later = requireNonNull(nativeModel.globalCheckpoint());
            assertThat(later.asString()).as("test setup, the stale node's position is later").isNotEqualTo(earlier.asString());
            StaleNodeSavesBeforeTheVersionWrite storage = new StaleNodeSavesBeforeTheVersionWrite(checkpointCollection(), later);
            storage.save(SUBSCRIPTION_ID, earlier);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            durable.subscribe(SUBSCRIPTION_ID, __ -> holdUntilReleased(assertionsRead)).waitUntilStarted();

            assertAll(
                    () -> assertThat(storage.staleNodeSaved).as("test setup, the stale node saved before the version write").isTrue(),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(later.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L)
            );
        } finally {
            assertionsRead.countDown();
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    private static void holdUntilReleased(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The delivery was never let continue.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while the delivery was held.", e);
        }
    }

    private static MongoCollection<Document> checkpointCollection() {
        return database.getCollection("checkpoints-" + UUID.randomUUID());
    }

    /**
     * Lets a stale node save {@code staleNodesPosition} right before this node writes its version, whichever call it
     * writes it with, once.
     */
    private static final class StaleNodeSavesBeforeTheVersionWrite extends NativeMongoCheckpointStorage {
        private final Checkpoint staleNodesPosition;
        volatile boolean staleNodeSaved;
        private final AtomicBoolean saving = new AtomicBoolean();

        StaleNodeSavesBeforeTheVersionWrite(MongoCollection<Document> checkpointCollection, Checkpoint staleNodesPosition) {
            super(checkpointCollection);
            this.staleNodesPosition = staleNodesPosition;
        }

        @Override
        public Optional<Checkpoint> storeWriteVersion(String subscriptionId, long writeVersion) {
            staleNodeSavesFirst(subscriptionId);
            return super.storeWriteVersion(subscriptionId, writeVersion);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            staleNodeSavesFirst(subscriptionId);
            return super.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }

        private void staleNodeSavesFirst(String subscriptionId) {
            if (saving.compareAndSet(false, true)) {
                save(subscriptionId, staleNodesPosition);
                staleNodeSaved = true;
            }
        }
    }
}
