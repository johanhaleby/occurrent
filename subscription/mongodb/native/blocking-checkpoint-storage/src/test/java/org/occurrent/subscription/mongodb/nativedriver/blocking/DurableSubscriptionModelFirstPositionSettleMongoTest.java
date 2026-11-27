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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoResumeTokenCheckpoint;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * Whichever of the subscribing thread and an evaluation of the start position settles the first position of a
 * subscribe first, only that one writes to storage, and a checkpoint another node stored is left as it was when that
 * write is refused. Each order is forced with latches.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionSettleMongoTest {

    private static final String DATABASE = "durablefirstpositionsettle";
    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration BOUND = Duration.ofSeconds(10);

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
    void an_evaluation_that_reads_the_checkpoint_another_node_stored_while_the_subscribing_thread_is_refused_writing_does_not_store_this_nodes_version_with_it() throws Exception {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        ExecutorService threads = Executors.newCachedThreadPool();
        DrivenModel nativeModel = new DrivenModel(eventCollection.getNamespace().getCollectionName(), executor, false);
        CountDownLatch writeMayContinue = new CountDownLatch(1);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            HoldingStorage storage = new HoldingStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, deliveredEvent), writeMayContinue);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Future<Throwable> subscribing = threads.submit(() -> catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            })));
            assertThat(storage.otherNodeStored.await(BOUND.toSeconds(), TimeUnit.SECONDS)).as("test setup, the other node stored its checkpoint while the subscribing thread wrote").isTrue();
            AtomicReference<@Nullable Thread> evaluator = new AtomicReference<>();
            Future<Throwable> evaluation = threads.submit(() -> {
                evaluator.set(Thread.currentThread());
                return catchThrowable(nativeModel::evaluateStartAt);
            });
            // Parked in the settle, which is after it read the checkpoint, and where an evaluation that stores the version has done so or waits to
            await().alias("the evaluation read the checkpoint and waits in the settle").atMost(BOUND)
                    .until(() -> evaluator.get() != null && isParkedInSettle(requireNonNull(evaluator.get())));
            writeMayContinue.countDown();

            Throwable thrown = subscribing.get(BOUND.toSeconds(), TimeUnit.SECONDS);
            Throwable evaluationThrew = evaluation.get(BOUND.toSeconds(), TimeUnit.SECONDS);

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class,
                            e -> assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue(deliveredEvent.asString())),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                    () -> assertThat(storage.writesOfAFirstPosition.get()).as("writes of a first position or a version with a checkpoint read").isEqualTo(1),
                    () -> assertThat(evaluationThrew).as("thrown by the evaluation").isInstanceOf(IllegalStateException.class)
                            .hasCauseInstanceOf(StartPositionAlreadyPinnedException.class),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from").isEmpty()
            );
        } finally {
            writeMayContinue.countDown();
            threads.shutdownNow();
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void the_subscribing_thread_that_reads_the_checkpoint_another_node_stored_after_an_evaluation_was_refused_writing_is_refused_with_it_and_stores_no_version() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        DrivenModel nativeModel = new DrivenModel(eventCollection.getNamespace().getCollectionName(), executor, true);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            HoldingStorage storage = new HoldingStorage(checkpointCollection(), other -> other.save(SUBSCRIPTION_ID, deliveredEvent), new CountDownLatch(0));
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }));

            assertAll(
                    () -> assertThat(nativeModel.evaluationFailures).as("test setup, the evaluation in the wrapped subscribe was refused first")
                            .singleElement().isInstanceOf(StartPositionAlreadyPinnedException.class),
                    () -> assertThat(thrown).as("thrown by subscribe").isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class,
                            e -> assertThat(e.positionStored.map(Checkpoint::asString)).as("position stored").hasValue(deliveredEvent.asString())),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").isEmpty(),
                    () -> assertThat(storage.writesOfAFirstPosition.get()).as("writes of a first position or a version with a checkpoint read").isEqualTo(1),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from").isEmpty()
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void the_subscribing_thread_alone_stores_this_nodes_version_with_a_checkpoint_another_node_stored_before_it_read() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        DrivenModel nativeModel = new DrivenModel(eventCollection.getNamespace().getCollectionName(), executor, false);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            HoldingStorage storage = new HoldingStorage(checkpointCollection(), other -> {
            }, new CountDownLatch(0));
            storage.save(SUBSCRIPTION_ID, deliveredEvent);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted());

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThat(storage.writesOfAFirstPosition.get()).as("writes of a first position or a version with a checkpoint read").isEqualTo(1)
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void an_evaluation_alone_stores_this_nodes_version_with_a_checkpoint_another_node_stored_before_it_read_and_starts_from_it() {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        DrivenModel nativeModel = new DrivenModel(eventCollection.getNamespace().getCollectionName(), executor, true);
        try {
            MongoResumeTokenCheckpoint deliveredEvent = resumeTokenOfAWrittenEvent(eventStore, eventCollection);
            HoldingStorage storage = new HoldingStorage(checkpointCollection(), other -> {
            }, new CountDownLatch(0));
            storage.save(SUBSCRIPTION_ID, deliveredEvent);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, id -> OptionalLong.of(2));

            Throwable thrown = catchThrowable(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
            }).waitUntilStarted());

            assertAll(
                    () -> assertThat(thrown).as("thrown by subscribe").isNull(),
                    () -> assertThat(nativeModel.evaluationFailures).as("failures of the evaluation in the wrapped subscribe").isEmpty(),
                    () -> assertThat(storage.read(SUBSCRIPTION_ID).asString()).as("stored checkpoint").isEqualTo(deliveredEvent.asString()),
                    () -> assertThat(storage.writeVersion(SUBSCRIPTION_ID)).as("stored version").hasValue(2L),
                    () -> assertThat(nativeModel.positionsStartedFrom).as("positions the feed started from").isNotEmpty().containsOnly(deliveredEvent.asString())
            );
        } finally {
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    private static boolean isParkedInSettle(Thread thread) {
        StackTraceElement[] stack = thread.getStackTrace();
        boolean parked = thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.BLOCKED;
        return parked && Arrays.stream(stack).anyMatch(frame -> frame.getClassName().endsWith("DurableSubscriptionModel$FirstPosition") && frame.getMethodName().equals("settle"));
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
     * Lets another node store a checkpoint with no version while this node writes its first position, and holds that
     * write until {@code writeMayContinue} is released. Counts every write of a first position or of a version with
     * a checkpoint read, which is what {@code resolveFirstCheckpointRace} is called for.
     */
    private static final class HoldingStorage extends NativeMongoCheckpointStorage {
        private final Consumer<NativeMongoCheckpointStorage> otherNodeWrites;
        private final CountDownLatch writeMayContinue;
        final CountDownLatch otherNodeStored = new CountDownLatch(1);
        final AtomicInteger writesOfAFirstPosition = new AtomicInteger();

        HoldingStorage(MongoCollection<Document> checkpointCollection, Consumer<NativeMongoCheckpointStorage> otherNodeWrites, CountDownLatch writeMayContinue) {
            super(checkpointCollection);
            this.otherNodeWrites = otherNodeWrites;
            this.writeMayContinue = writeMayContinue;
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            if (writesOfAFirstPosition.incrementAndGet() == 1) {
                otherNodeWrites.accept(this);
                otherNodeStored.countDown();
                try {
                    if (!writeMayContinue.await(BOUND.toSeconds(), TimeUnit.SECONDS)) {
                        throw new IllegalStateException("The write of the first position was never let continue.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while the write of the first position was held.", e);
                }
            }
            return super.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }
    }

    /**
     * Takes the start position the durable model hands it and evaluates it only when the test says so, or inside its
     * own subscribe when {@code evaluatesInSubscribe} is set, where a failure is kept the way the blocking MongoDB
     * models keep one before they retry. Starts no feed.
     */
    private static final class DrivenModel extends NativeMongoSubscriptionModel {
        private final boolean evaluatesInSubscribe;
        private volatile @Nullable StartAt captured;
        final List<String> positionsStartedFrom = new CopyOnWriteArrayList<>();
        final List<Throwable> evaluationFailures = new CopyOnWriteArrayList<>();

        DrivenModel(String eventCollectionName, ExecutorService executor, boolean evaluatesInSubscribe) {
            super(database, eventCollectionName, RFC_3339_STRING, executor);
            this.evaluatesInSubscribe = evaluatesInSubscribe;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return accept(subscriptionId, startAt);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return accept(subscriptionId, startAt);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            // Nothing was started
        }

        StartAt evaluateStartAt() {
            StartAt resolved = requireNonNull(captured, "the wrapped subscribe was not called").get(new StartAt.SubscriptionModelContext(NativeMongoSubscriptionModel.class));
            if (resolved instanceof StartAt.StartAtCheckpoint checkpoint) {
                positionsStartedFrom.add(checkpoint.checkpoint.asString());
            }
            return resolved;
        }

        private Subscription accept(String subscriptionId, StartAt startAt) {
            captured = startAt;
            if (evaluatesInSubscribe) {
                try {
                    evaluateStartAt();
                } catch (Throwable failure) {
                    evaluationFailures.add(failure);
                }
            }
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
    }
}
