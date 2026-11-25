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
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
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
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
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
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, later, CheckpointWriteCondition.ifAbsent()));
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
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, earlier, CheckpointWriteCondition.notOlderThan(1)));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));
            CountDownLatch delivered = new CountDownLatch(1);

            durable.subscribe(SUBSCRIPTION_ID, __ -> delivered.countDown()).waitUntilStarted();

            assertAll(
                    () -> assertThat(storage.answered.get()).as("the position this node starts from")
                            .isNotNull().extracting(Checkpoint::asString).isEqualTo(earlier.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThat(delivered.await(5, TimeUnit.SECONDS)).as("the event between the two positions is delivered").isTrue()
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_checkpoint_for_a_delivered_event_another_node_stored_with_an_older_version_refuses_the_subscribe_and_is_left_as_it_was() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, deliveredEvent, CheckpointWriteCondition.notOlderThan(1)));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }));

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class,
                            e -> assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue(deliveredEvent.asString())),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(1L),
                    () -> assertThat(storage.answered.get()).as("the checkpoint the resolution answered").isNull(),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from").isEmpty()
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_checkpoint_for_a_delivered_event_another_node_stored_with_no_version_refuses_the_subscribe_and_is_left_as_it_was() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, deliveredEvent));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }));

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class,
                            e -> assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue(deliveredEvent.asString())),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from").isEmpty()
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_checkpoint_for_a_delivered_event_another_node_stored_with_a_newer_version_is_started_from_and_nothing_is_written() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, deliveredEvent, CheckpointWriteCondition.notOlderThan(3)));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted());

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                    () -> assertThat(storage.refused.get()).as("the write of this node, older than the stored version, was refused")
                            .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(3L),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from")
                            .isNotEmpty().containsOnly(deliveredEvent.asString())
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void the_very_checkpoint_for_a_delivered_event_this_node_read_stored_by_another_node_is_started_from_and_gets_this_nodes_version() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            nativeModel.answers = deliveredEvent;
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, deliveredEvent, CheckpointWriteCondition.notOlderThan(1)));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted());

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from")
                            .isNotEmpty().containsOnly(deliveredEvent.asString())
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_node_with_an_older_version_starts_from_the_first_position_a_node_with_a_newer_one_stored_and_leaves_it_and_its_version_as_they_were() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        ExecutorService executor = Executors.newCachedThreadPool();
        EarlierPositionModel nativeModel = new EarlierPositionModel(eventCollection.getNamespace().getCollectionName(), executor);
        try {
            MongoOperationTimeCheckpoint earlier = (MongoOperationTimeCheckpoint) requireNonNull(nativeModel.globalCheckpoint());
            // Ahead of the cluster time, so no event of this test lies at or after it and nothing delivered can replace it
            MongoOperationTimeCheckpoint laterFirstPosition = new MongoOperationTimeCheckpoint(new BsonTimestamp(earlier.operationTime.getTime() + 3600, 0));
            nativeModel.answers = earlier;
            RaceHookStorage storage = new RaceHookStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, laterFirstPosition, CheckpointWriteCondition.notOlderThan(2)), true);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(1));

            assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted()).doesNotThrowAnyException();

            assertAll(
                    () -> assertThat(storage.refused.get()).as("the version of this node, older than the stored one, was refused")
                            .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored position").isEqualTo(laterFirstPosition.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from")
                            .isNotEmpty().containsOnly(laterFirstPosition.asString())
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    private static MongoCollection<Document> checkpointCollection() {
        return database.getCollection("checkpoints-" + UUID.randomUUID());
    }

    // A token taken from the change stream, since a subscription starting from it asks the change stream to resume after it
    private static MongoResumeTokenCheckpoint resumeTokenOfAWrittenEvent(MongoEventStore eventStore, MongoCollection<Document> eventCollection) {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = eventCollection.watch().cursor()) {
            eventStore.write("delivered-stream", List.of(event("delivered", "SomethingHappened")));
            cursor.next();
            return new MongoResumeTokenCheckpoint(requireNonNull(cursor.getResumeToken()));
        }
    }

    /**
     * Lets another node store a checkpoint right before this node settles its first position, and records what the
     * settling answered or refused. With {@code beforeAnyFirstWrite} the other node also writes before an
     * {@code ifAbsent()} write, whichever of the two this node makes first.
     */
    private static final class RaceHookStorage extends NativeMongoCheckpointStorage {
        private final Consumer<NativeMongoCheckpointStorage> otherNodeWrites;
        private final boolean beforeAnyFirstWrite;
        private final AtomicBoolean otherNodeHasWritten = new AtomicBoolean();
        final AtomicReference<@Nullable Checkpoint> answered = new AtomicReference<>();
        final AtomicReference<@Nullable RuntimeException> refused = new AtomicReference<>();

        RaceHookStorage(MongoCollection<Document> checkpointCollection, Consumer<NativeMongoCheckpointStorage> otherNodeWrites) {
            this(checkpointCollection, otherNodeWrites, false);
        }

        RaceHookStorage(MongoCollection<Document> checkpointCollection, Consumer<NativeMongoCheckpointStorage> otherNodeWrites, boolean beforeAnyFirstWrite) {
            super(checkpointCollection);
            this.otherNodeWrites = otherNodeWrites;
            this.beforeAnyFirstWrite = beforeAnyFirstWrite;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            if (beforeAnyFirstWrite && condition instanceof CheckpointWriteCondition.IfAbsent) {
                otherNodeWritesFirst();
            }
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            otherNodeWritesFirst();
            try {
                Optional<Checkpoint> governing = super.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
                governing.ifPresent(answered::set);
                return governing;
            } catch (CheckpointWriteConditionNotFulfilledException e) {
                refused.set(e);
                throw e;
            }
        }

        private void otherNodeWritesFirst() {
            if (otherNodeHasWritten.compareAndSet(false, true)) {
                otherNodeWrites.accept(this);
            }
        }
    }

    /**
     * Answers {@link #answers} as the global checkpoint once it is set, the position this node computed before the
     * other node's later one was stored.
     */
    private static final class EarlierPositionModel extends NativeMongoSubscriptionModel {
        @Nullable
        volatile Checkpoint answers;
        final List<String> positionsStartedFrom = new CopyOnWriteArrayList<>();

        EarlierPositionModel(String eventCollectionName, ExecutorService executor) {
            super(database, eventCollectionName, RFC_3339_STRING, executor);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            Checkpoint fixed = answers;
            return fixed != null ? fixed : super.globalCheckpoint();
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return super.subscribe(subscriptionId, filter, recordingWhatItResolvesTo(startAt), action);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return super.subscribePaused(subscriptionId, filter, recordingWhatItResolvesTo(startAt), action);
        }

        // The position a dynamic start resolves to when the feed evaluates it, which is where the feed starts from
        private StartAt recordingWhatItResolvesTo(StartAt startAt) {
            if (!startAt.isDynamic()) {
                return startAt;
            }
            return StartAt.dynamic(context -> {
                StartAt resolved = startAt.get(context);
                if (resolved instanceof StartAt.StartAtCheckpoint checkpoint) {
                    positionsStartedFrom.add(checkpoint.checkpoint.asString());
                }
                return resolved;
            });
        }

        @Nullable Checkpoint superGlobalCheckpoint() {
            return super.globalCheckpoint();
        }
    }
}
