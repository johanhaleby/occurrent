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

package org.occurrent.subscription.blocking.competingconsumers;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.BsonTimestamp;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModelConfig;
import org.occurrent.subscription.mongodb.MongoOperationTimeCheckpoint;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A first position that no model could record when the subscribe was made is recorded by the start position
 * evaluation, which the native MongoDB model runs on a thread of its own after {@code subscribe} returned. When that
 * evaluation finds a newer write version stored, another node holds the lease and the storage's exception reaches the
 * native model, which doesn't retry it. Nothing is written, so what the new lease holder stored is kept. A lease
 * strategy that then says another node holds the lease has the competing consumer model pause the subscription and
 * compete for the lease again.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerRefusedWriteInAStartEvaluationOverNativeMongoTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final Duration EVENTUALLY = Duration.ofSeconds(15);
    private static final String NODE = "this-node";
    private static final String ID = "subscription";
    private static final long OTHER_NODES_VERSION = 3;

    private final Strategy strategy = new Strategy();
    private MongoClient mongoClient;
    private NoPositionTheFirstTime nativeModel;
    private NewerVersionStoringStorage checkpointStorage;
    private MongoOperationTimeCheckpoint otherNodesPosition;
    private CompetingConsumerSubscriptionModel competing;

    @BeforeEach
    void a_competing_consumer_model_over_a_durable_model_that_records_its_first_position_in_the_start_evaluation() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(requireNonNull(connectionString.getDatabase()));
        nativeModel = new NoPositionTheFirstTime(database, requireNonNull(connectionString.getCollection()));
        otherNodesPosition = new MongoOperationTimeCheckpoint(new BsonTimestamp((int) (System.currentTimeMillis() / 1000) + 3600, 0));
        checkpointStorage = new NewerVersionStoringStorage(database, otherNodesPosition);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, checkpointStorage,
                new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true), id -> OptionalLong.of(2));
        competing = new CompetingConsumerSubscriptionModel(durable, strategy);
        strategy.grantOnRegister = true;
    }

    @AfterEach
    void shutdown() {
        nativeModel.subscribeReturned.countDown();
        try {
            competing.shutdown();
        } finally {
            mongoClient.close();
        }
    }

    @Test
    void a_newer_write_version_found_by_the_start_evaluation_writes_nothing_and_is_not_tried_again() {
        // Given
        Throwable thrown = catchThrowable(() -> competing.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
        }));
        nativeModel.subscribeReturned.countDown();

        // When the evaluation has found the newer version stored. The native model's thread of its own ends with the refusal, which the await would report as an uncaught exception
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> !nativeModel.evaluationFailures.isEmpty());

        // Then
        assertThat(thrown).as("thrown by subscribe").isNull();
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).dontCatchUncaughtExceptions()
                .untilAsserted(() -> assertThat(checkpointStorage.resolverCalls).as("calls of the first position resolver").hasValue(1));
        assertAll(
                () -> assertThat(nativeModel.evaluationFailures).as("failures of the start position evaluation")
                        .singleElement().isInstanceOf(CheckpointWriteConditionNotFulfilledException.class),
                () -> assertThat(nativeModel.positionsStartedFrom).as("positions the native model started from").isEmpty(),
                () -> assertThat(checkpointStorage.read(ID)).as("stored position").isEqualTo(otherNodesPosition),
                () -> assertThat(checkpointStorage.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION),
                () -> assertThat(checkpointStorage.saved).as("checkpoints saved by this node").isEmpty()
        );
    }

    @Test
    void a_node_told_another_node_holds_the_lease_after_a_start_evaluation_found_a_newer_write_version_pauses_the_subscription_and_competes_again() {
        // Given a subscription whose start evaluation found a newer write version stored
        competing.subscribe(NODE, ID, null, StartAt.subscriptionModelDefault(), __ -> {
        });
        nativeModel.subscribeReturned.countDown();
        // Recorded once the evaluation throws the refusal, which is after the other node's position was stored and the resolver refused
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> !nativeModel.evaluationFailures.isEmpty());

        // When the lease strategy says another node holds the lease
        strategy.holders.remove(ID);
        Throwable thrown = catchThrowable(() -> competing.onConsumeProhibited(ID, NODE));

        // Then
        assertAll(
                () -> assertThat(thrown).as("thrown when the lease is prohibited").isNull(),
                () -> assertThat(nativeModel.positionsStartedFrom).as("positions the native model started from").isEmpty(),
                () -> assertThat(competing.isRunning(ID)).as("running").isFalse(),
                () -> assertThat(strategy.registrations).as("registered as a candidate for the lease").containsExactly(ID),
                () -> assertThat(checkpointStorage.resolverCalls).as("calls of the first position resolver").hasValue(1),
                () -> assertThat(checkpointStorage.read(ID)).as("stored position").isEqualTo(otherNodesPosition),
                () -> assertThat(checkpointStorage.writeVersion(ID)).as("stored version").hasValue(OTHER_NODES_VERSION)
        );
    }

    /**
     * Answers no global checkpoint the first time the subscribing thread asks, so the first position is recorded by the
     * start position evaluation the native model runs on a thread of its own, and the position it computes then is a
     * later one. That evaluation is held in its ask until the subscribe has returned, so the subscribing thread always
     * settles first.
     */
    private static final class NoPositionTheFirstTime extends NativeMongoSubscriptionModel {
        final List<String> positionsStartedFrom = new CopyOnWriteArrayList<>();
        final List<Throwable> evaluationFailures = new CopyOnWriteArrayList<>();
        final CountDownLatch subscribeReturned = new CountDownLatch(1);
        private final AtomicBoolean noPositionGiven = new AtomicBoolean();
        // The thread that builds the model is the one that subscribes
        private final Thread subscribingThread = Thread.currentThread();

        NoPositionTheFirstTime(MongoDatabase database, String eventCollectionName) {
            super(database, eventCollectionName, TimeRepresentation.RFC_3339_STRING, Executors.newCachedThreadPool());
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return super.subscribe(subscriptionId, filter, recordingWhatItEvaluatesTo(startAt), action);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return super.subscribePaused(subscriptionId, filter, recordingWhatItEvaluatesTo(startAt), action);
        }

        // What the native model's own evaluation of a dynamic start answers, or fails with
        private StartAt recordingWhatItEvaluatesTo(StartAt startAt) {
            if (!startAt.isDynamic()) {
                return startAt;
            }
            return StartAt.dynamic(context -> {
                try {
                    StartAt resolved = startAt.get(context);
                    if (resolved instanceof StartAt.StartAtCheckpoint checkpoint) {
                        positionsStartedFrom.add(checkpoint.checkpoint.asString());
                    }
                    return resolved;
                } catch (RuntimeException e) {
                    evaluationFailures.add(e);
                    throw e;
                }
            });
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            if (Thread.currentThread() == subscribingThread) {
                return noPositionGiven.compareAndSet(false, true) ? null : super.globalCheckpoint();
            }
            try {
                if (!subscribeReturned.await(EVENTUALLY.toSeconds(), TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The subscribe never returned.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while held until the subscribe returned.", e);
            }
            return super.globalCheckpoint();
        }
    }

    /**
     * Stores the position of another node, with a newer write version than this node's, right before the first
     * position resolver of this node runs, and counts what this node saves and how often it resolves.
     */
    private static final class NewerVersionStoringStorage extends NativeMongoCheckpointStorage {
        final AtomicInteger resolverCalls = new AtomicInteger();
        final List<Checkpoint> saved = new CopyOnWriteArrayList<>();
        private final Checkpoint otherNodesPosition;

        NewerVersionStoringStorage(MongoDatabase database, Checkpoint otherNodesPosition) {
            super(database, "checkpoints-" + UUID.randomUUID());
            this.otherNodesPosition = otherNodesPosition;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            saved.add(checkpoint);
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public Optional<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate, long writeVersion) {
            if (resolverCalls.incrementAndGet() == 1) {
                // The other node's own write, so it is not counted as a save of this node
                super.save(subscriptionId, otherNodesPosition, CheckpointWriteCondition.notOlderThan(OTHER_NODES_VERSION));
            }
            return super.resolveFirstCheckpointRace(subscriptionId, candidate, writeVersion);
        }
    }

    // Grants the lease on register when told to, and otherwise leaves the consumer waiting for a grant the test gives
    private static final class Strategy implements CompetingConsumerStrategy {
        final Set<String> holders = ConcurrentHashMap.newKeySet();
        final Set<String> registrations = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        volatile boolean grantOnRegister;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.add(subscriptionId);
            if (grantOnRegister) {
                holders.add(subscriptionId);
                return true;
            }
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            registrations.remove(subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        @Override
        public void shutdown() {
        }
    }
}
