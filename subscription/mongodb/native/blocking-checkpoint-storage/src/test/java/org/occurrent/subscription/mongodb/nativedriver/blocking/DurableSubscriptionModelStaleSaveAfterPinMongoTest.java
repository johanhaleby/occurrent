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
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.internal.StartPositionPin;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
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
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * A node that has lost its lease must not be able to store the checkpoint of a later event over the position the
 * lease holder starts from. The position here is the one the Spring Boot starter pins, which moves an operation time
 * stored by another node to an earlier one and keeps the version stored with it, so the stored version is the older
 * node's until the new holder subscribes and stores its own.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStaleSaveAfterPinMongoTest {

    private static final String DATABASE = "durablestalesaveafterpin";
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
    void a_node_with_an_older_version_cannot_store_the_checkpoint_of_a_later_event_over_the_pinned_position_so_the_next_holder_gets_the_event_between() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        NativeMongoCheckpointStorage storage = new NativeMongoCheckpointStorage(database.getCollection("checkpoints-" + UUID.randomUUID()));
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nextHoldersModel = new NativeMongoSubscriptionModel(database, eventCollection.getNamespace().getCollectionName(), RFC_3339_STRING, executor);
        try {
            eventStore.write("stream", List.of(event("moves-the-clock-on", "SomethingHappened")));
            Checkpoint pinnedPosition = requireNonNull(nextHoldersModel.globalCheckpoint());
            CloudEvent between = event("between-the-pinned-position-and-the-later-event", "SomethingHappened");
            CloudEvent later = event("later-event", "SomethingHappened");
            Checkpoint checkpointOfTheLaterEvent;
            Checkpoint firstPositionOfTheOldNode;
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = eventCollection.watch().cursor()) {
                eventStore.write("stream", List.of(between));
                firstPositionOfTheOldNode = requireNonNull(nextHoldersModel.globalCheckpoint());
                eventStore.write("stream", List.of(later));
                cursor.next();
                cursor.next();
                checkpointOfTheLaterEvent = new MongoResumeTokenCheckpoint(requireNonNull(cursor.getResumeToken()));
            }
            assertThat(firstPositionOfTheOldNode.asString()).as("test setup, the old node's first position is later than the pinned one").isNotEqualTo(pinnedPosition.asString());

            DurableSubscriptionModel oldNode = new DurableSubscriptionModel(new PositionOnlyFeed(firstPositionOfTheOldNode), storage, id -> OptionalLong.of(1));
            oldNode.subscribe(SUBSCRIPTION_ID, __ -> {
            });
            StartPositionPin.pinOrSaveUnconditionally(storage, SUBSCRIPTION_ID, true, pinnedPosition);
            assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("test setup, the pin moved the stored position to the earlier one").isEqualTo(pinnedPosition.asString());
            assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("test setup, the pin kept the old node's version").hasValue(1L);

            DurableSubscriptionModel newHolder = new DurableSubscriptionModel(new PositionOnlyFeed(firstPositionOfTheOldNode), storage, id -> OptionalLong.of(2));
            newHolder.subscribe(SUBSCRIPTION_ID, __ -> {
            });
            OptionalLong versionAfterTheNewHolderSubscribed = storage.writeVersion(SUBSCRIPTION_ID);

            Throwable savedByTheOldNode = catchThrowable(() -> storage.save(SUBSCRIPTION_ID, checkpointOfTheLaterEvent, CheckpointWriteCondition.notOlderThan(1)));
            newHolder.stop();

            List<String> delivered = new CopyOnWriteArrayList<>();
            DurableSubscriptionModel nextHolder = new DurableSubscriptionModel(nextHoldersModel, storage, id -> OptionalLong.of(3));
            nextHolder.subscribe(SUBSCRIPTION_ID, cloudEvent -> delivered.add(cloudEvent.getId())).waitUntilStarted();
            CloudEvent sentinel = event("sentinel", "SomethingHappened");
            eventStore.write("stream", List.of(sentinel));
            await().atMost(10, SECONDS).until(() -> delivered.contains(sentinel.getId()));

            assertAll(
                    () -> assertThat(versionAfterTheNewHolderSubscribed).as("version stored once the new holder subscribed").hasValue(2L),
                    () -> assertThat(savedByTheOldNode).as("save of the later event by the node with the older version")
                            .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                    () -> assertThat(delivered).as("events the next holder was delivered").contains(between.getId())
            );
        } finally {
            nextHoldersModel.shutdown();
            executor.shutdown();
        }
    }

    /**
     * Answers a fixed global checkpoint and starts no feed, so a node using it stores what it stores on subscribe and
     * never delivers an event, which is a node that stops before its first save.
     */
    private static final class PositionOnlyFeed implements CheckpointAwareSubscriptionModel {
        private final Checkpoint globalCheckpoint;

        PositionOnlyFeed(Checkpoint globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }

        @Override
        public Checkpoint globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return true;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }
    }
}
