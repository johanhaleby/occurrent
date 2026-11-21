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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * The write version {@link DurableSubscriptionModel} stores with the first position it records, over
 * {@link NativeMongoCheckpointStorage}, whose {@code ifAbsent()} write stores no version.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionWriteVersionMongoTest {

    private static final String DATABASE = "durablefirstpositionwriteversion";
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
    void a_first_position_recorded_by_default_stores_the_version_and_refuses_a_write_with_an_older_one() {
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, "events-" + UUID.randomUUID(), RFC_3339_STRING, executor);
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(database.getCollection("checkpoints-" + UUID.randomUUID()));
        try {
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted();
            String firstPosition = storage.read(SUBSCRIPTION_ID).asString();

            assertAll(
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThatThrownBy(() -> storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("stale-node-position"), CheckpointWriteCondition.notOlderThan(1)))
                            .as("a node with an older version writing over it")
                            .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(firstPosition)
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_lost_race_that_resolves_to_this_nodes_earlier_position_stores_the_version_and_refuses_a_write_with_an_older_one() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            Checkpoint earlier = nativeModel.globalCheckpoint();
            eventStore.write("stream", List.of(event("moves-the-clock-on", "SomethingHappened")));
            Checkpoint later = requireNonNull(nativeModel.superGlobalCheckpoint());
            nativeModel.answers = earlier;
            assertThat(later.asString()).as("test setup, the other node's position is later").isNotEqualTo(earlier.asString());
            NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(database.getCollection("checkpoints-" + UUID.randomUUID())) {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    if (condition instanceof CheckpointWriteCondition.IfAbsent) {
                        super.save(subscriptionId, later, condition);
                    }
                    return super.save(subscriptionId, checkpoint, condition);
                }
            };
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted();

            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(earlier.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThatThrownBy(() -> storage.save(SUBSCRIPTION_ID, later, CheckpointWriteCondition.notOlderThan(1)))
                            .as("a node with an older version writing over it")
                            .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class)
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_node_whose_earlier_position_was_stored_in_between_starts_from_that_position_and_not_from_the_later_one_it_captured() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            Checkpoint earlier = nativeModel.globalCheckpoint();
            eventStore.write("stream", List.of(event("moves-the-clock-on", "SomethingHappened")));
            Checkpoint later = requireNonNull(nativeModel.superGlobalCheckpoint());
            nativeModel.answers = later;
            assertThat(later.asString()).as("test setup, this node's position is later").isNotEqualTo(earlier.asString());
            NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(database.getCollection("checkpoints-" + UUID.randomUUID())) {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
                    if (condition instanceof CheckpointWriteCondition.IfAbsent) {
                        super.resolveFirstCheckpointRace(subscriptionId, earlier);
                        super.save(subscriptionId, earlier, CheckpointWriteCondition.notOlderThan(1));
                    }
                    return saved;
                }
            };
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));
            CountDownLatch delivered = new CountDownLatch(1);

            durable.subscribe(SUBSCRIPTION_ID, __ -> delivered.countDown()).waitUntilStarted();

            assertAll(
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(earlier.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThat(delivered.await(5, TimeUnit.SECONDS)).as("the event between the two positions is delivered").isTrue()
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    /**
     * Answers {@link #answers} as the global checkpoint once it is set, the position this node computed before the
     * other node's later one was stored.
     */
    private static final class EarlierPositionModel extends NativeMongoSubscriptionModel {
        @Nullable
        volatile Checkpoint answers;

        EarlierPositionModel(String eventCollectionName, ExecutorService executor) {
            super(database, eventCollectionName, RFC_3339_STRING, executor);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            Checkpoint fixed = answers;
            return fixed != null ? fixed : super.globalCheckpoint();
        }

        @Nullable Checkpoint superGlobalCheckpoint() {
            return super.globalCheckpoint();
        }
    }
}
