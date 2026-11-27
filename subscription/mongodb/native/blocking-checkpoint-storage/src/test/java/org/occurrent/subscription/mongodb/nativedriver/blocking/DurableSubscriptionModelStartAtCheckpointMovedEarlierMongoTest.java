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
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * A subscribe at {@code StartAt.checkpoint(S)}, where S is the checkpoint stored, stores this node's write version with
 * it. Another node's first-position race can move the stored checkpoint earlier to E in between this node's read and
 * that write. The storage then keeps E, stores this node's version with it and removes the mark that refuses this
 * node's saves, so this node must start at E. Starting at S would deliver nothing from E up to S, and this node's first
 * save would move storage past what the other node replays.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStartAtCheckpointMovedEarlierMongoTest {

    private static final String DATABASE = "durablestartatcheckpointmovedearlier";
    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final long THIS_NODES_VERSION = 2;

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
    void a_start_at_the_stored_checkpoint_starts_at_the_earlier_checkpoint_another_node_stored_after_the_read_and_delivers_the_event_between_the_two() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollection.getNamespace().getCollectionName(), RFC_3339_STRING, executor);
        try {
            Checkpoint earlier = requireNonNull(nativeModel.globalCheckpoint());
            eventStore.write("stream", List.of(event("between-the-two", "SomethingHappened")));
            Checkpoint stored = requireNonNull(nativeModel.globalCheckpoint());
            assertThat(stored.asString()).as("test setup, the stored checkpoint is later").isNotEqualTo(earlier.asString());
            ReadHookStorage storage = new ReadHookStorage(database.getCollection("checkpoints-" + UUID.randomUUID()), stored,
                    other -> other.resolveFirstCheckpointRace(SUBSCRIPTION_ID, earlier));
            storage.save(SUBSCRIPTION_ID, stored);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, new DurableSubscriptionModelConfig(__ -> false), id -> OptionalLong.of(THIS_NODES_VERSION));
            List<String> delivered = new CopyOnWriteArrayList<>();

            durable.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(stored), cloudEvent -> delivered.add(cloudEvent.getId())).waitUntilStarted();
            @Nullable String deliveredWithinTimeout = awaitDeliveryOf("between-the-two", delivered);

            assertAll(
                    () -> assertThat(storage.otherNodeHasMovedTheCheckpoint.get()).as("test setup, another node moved the stored checkpoint in between the read and the write").isTrue(),
                    () -> assertThat(deliveredWithinTimeout).as("the event between the earlier and the stored checkpoint, delivered to this node").isEqualTo("between-the-two"),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(earlier.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(THIS_NODES_VERSION)
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    private static @Nullable String awaitDeliveryOf(String eventId, List<String> delivered) {
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> delivered.contains(eventId));
            return eventId;
        } catch (ConditionTimeoutException e) {
            return null;
        }
    }

    /**
     * Lets another node store its own checkpoint right after this node has read the checkpoint it starts at, and only
     * after that one read.
     */
    private static final class ReadHookStorage extends NativeMongoCheckpointStorage {
        private final String readOnce;
        private final Consumer<NativeMongoCheckpointStorage> otherNodeWrites;
        final AtomicBoolean otherNodeHasMovedTheCheckpoint = new AtomicBoolean();

        ReadHookStorage(MongoCollection<Document> checkpointCollection, Checkpoint readOnce, Consumer<NativeMongoCheckpointStorage> otherNodeWrites) {
            super(checkpointCollection);
            this.readOnce = readOnce.asString();
            this.otherNodeWrites = otherNodeWrites;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint read = super.read(subscriptionId);
            if (read != null && read.asString().equals(readOnce) && otherNodeHasMovedTheCheckpoint.compareAndSet(false, true)) {
                otherNodeWrites.accept(this);
            }
            return read;
        }
    }
}
