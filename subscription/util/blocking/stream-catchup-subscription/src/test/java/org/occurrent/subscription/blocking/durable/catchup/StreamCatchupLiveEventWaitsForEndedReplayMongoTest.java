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

package org.occurrent.subscription.blocking.durable.catchup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.useCheckpointStorage;

/**
 * A live-only subscribe that follows a cancel whose replay is still inside the subscriber's action gives its live
 * event to the action once that replay has returned, also when the cancel of the new subscription cancelled the
 * live delegate and then failed to delete its stored position. The live delegate is the real
 * {@link NativeMongoSubscriptionModel} under a {@link DurableSubscriptionModel}.
 */
@Testcontainers
@Timeout(60)
@DisplayNameGeneration(ReplaceUnderscores.class)
class StreamCatchupLiveEventWaitsForEndedReplayMongoTest {

    private static final URI SOURCE = URI.create("urn:test");

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final AtomicBoolean deleteFails = new AtomicBoolean();

    private MongoClient mongoClient;
    private MongoEventStore eventStore;
    private CloudEventConverter<DomainEvent> cloudEventConverter;
    private ExecutorService dispatcher;
    private CheckpointStorage storage;
    private StreamCatchupSubscriptionModel catchup;

    @BeforeEach
    void create_instances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".events");
        mongoClient = MongoClients.create(connectionString);
        String databaseName = requireNonNull(connectionString.getDatabase());
        String collectionName = requireNonNull(connectionString.getCollection());
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new MongoEventStore(mongoClient, databaseName, collectionName, eventStoreConfig);
        cloudEventConverter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), SOURCE).idMapper(DomainEvent::eventId).build();
        dispatcher = Executors.newCachedThreadPool();
        storage = storageWhoseDeleteFailsWhileFlagged(new NativeMongoCheckpointStorage(database, "checkpoints-" + UUID.randomUUID()));
        NativeMongoSubscriptionModel liveDelegate = new NativeMongoSubscriptionModel(database, collectionName, TimeRepresentation.RFC_3339_STRING, dispatcher);
        catchup = new StreamCatchupSubscriptionModel(new DurableSubscriptionModel(liveDelegate, storage), eventStore,
                new CatchupSubscriptionModelConfig(100, useCheckpointStorage(storage).andPersistCheckpointDuringCatchupPhaseForEveryNEvents(1)));
    }

    @AfterEach
    void shutdown() {
        if (catchup != null) {
            catchup.shutdown();
        }
        if (dispatcher != null) {
            dispatcher.shutdownNow();
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    void a_live_event_waiting_for_an_ended_replay_is_not_stored_past_and_reaches_the_action_when_the_cancel_failed_after_cancelling_the_live_delegate_and_a_subscribe_after_the_cancel_is_retried_gets_the_next_one() throws InterruptedException {
        // Given an ended replay still inside the action, and a live-only subscribe that followed its cancel
        String subscriptionId = UUID.randomUUID().toString();
        append(nameDefined("history", 0));
        CopyOnWriteArrayList<String> received = new CopyOnWriteArrayList<>();
        CountDownLatch replayCallEntered = new CountDownLatch(1);
        CountDownLatch releaseReplayCall = new CountDownLatch(1);
        AtomicBoolean firstCall = new AtomicBoolean(true);
        java.util.function.Consumer<CloudEvent> action = event -> {
            received.add(nameOf(event));
            if (firstCall.getAndSet(false)) {
                replayCallEntered.countDown();
                awaitLatch(releaseReplayCall);
            }
        };

        try {
            catchup.subscribe(subscriptionId, StartAtTime.beginningOfTime(), action);
            assertThat(replayCallEntered.await(10, TimeUnit.SECONDS)).as("the replay's call is in the action").isTrue();
            catchup.cancelSubscription(subscriptionId);
            catchup.subscribe(subscriptionId, StartAt.now(), action).waitUntilStarted();
            Checkpoint storedBeforeLiveEvent = storage.read(subscriptionId);
            append(nameDefined("live1", 1));
            await("the live event waits for the ended replay").atMost(10, TimeUnit.SECONDS).until(StreamCatchupLiveEventWaitsForEndedReplayMongoTest::liveCallWaitsForTheEndedReplays);

            // When the cancel cancels the live delegate and then fails to delete the stored position
            deleteFails.set(true);

            // Then the cancel fails
            assertThatThrownBy(() -> catchup.cancelSubscription(subscriptionId)).as("the cancel").hasMessage("the delete failed");
            deleteFails.set(false);

            // And the position stored is still before the live event, which the action hasn't returned for
            assertThat(storage.read(subscriptionId)).as("the position stored while the action hasn't returned for the live event").isEqualTo(storedBeforeLiveEvent);

            // When the replay's call is released, the cancel is retried and the subscription is subscribed again
            releaseReplayCall.countDown();
            await("the action is given the live event").atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(received).contains("live1"));
            assertThat(received).as("the events the action was given").containsExactly("history", "live1");
            // On this stack resumeSubscription after the failed cancel throws UnknownSubscriptionException, since the native model's cancel already forgot the id
            catchup.cancelSubscription(subscriptionId);
            catchup.subscribe(subscriptionId, StartAt.now(), action).waitUntilStarted();
            append(nameDefined("live2", 2));

            // Then the live events reach the action in order
            await("the action is given the event written after the subscribe").atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(received).containsExactly("history", "live1", "live2"));
        } finally {
            releaseReplayCall.countDown();
        }
    }

    private CheckpointStorage storageWhoseDeleteFailsWhileFlagged(CheckpointStorage delegate) {
        return new CheckpointStorage() {
            @Override
            public Checkpoint read(String subscriptionId) {
                return delegate.read(subscriptionId);
            }

            @Override
            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, org.occurrent.subscription.CheckpointWriteCondition condition) {
                return delegate.save(subscriptionId, checkpoint, condition);
            }

            @Override
            public void delete(String subscriptionId) {
                if (deleteFails.get()) {
                    throw new IllegalStateException("the delete failed");
                }
                delegate.delete(subscriptionId);
            }

            @Override
            public java.util.OptionalLong writeVersion(String subscriptionId) {
                return delegate.writeVersion(subscriptionId);
            }

            @Override
            public boolean exists(String subscriptionId) {
                return delegate.exists(subscriptionId);
            }

            @Override
            public boolean evaluatesWriteConditions() {
                return delegate.evaluatesWriteConditions();
            }

            @Override
            public boolean evaluatesWriteConditionsFor(String subscriptionId) {
                return delegate.evaluatesWriteConditionsFor(subscriptionId);
            }
        };
    }

    // Whether a live call is parked in the wait for the replays a cancel or a stop ended
    private static boolean liveCallWaitsForTheEndedReplays() {
        return Thread.getAllStackTraces().entrySet().stream()
                .anyMatch(thread -> thread.getKey().getState() != Thread.State.RUNNABLE && Arrays.stream(thread.getValue()).anyMatch(frame -> frame.getMethodName().equals("acceptAfterEndedReplays")));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String nameOf(CloudEvent cloudEvent) {
        return ((NameDefined) cloudEventConverter.toDomainEvent(cloudEvent)).name();
    }

    private static NameDefined nameDefined(String name, int secondsAfterStart) {
        return new NameDefined(UUID.randomUUID().toString(), LocalDateTime.of(2026, 1, 1, 0, 0, secondsAfterStart), "name", name);
    }

    private void append(DomainEvent event) {
        eventStore.write(UUID.randomUUID().toString(), cloudEventConverter.toCloudEvents(List.of(event)));
    }
}
