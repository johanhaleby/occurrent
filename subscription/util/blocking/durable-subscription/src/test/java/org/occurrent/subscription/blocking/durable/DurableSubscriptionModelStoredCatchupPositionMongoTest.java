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

package org.occurrent.subscription.blocking.durable;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * A {@link DurableSubscriptionModel} over a {@code NativeMongoSubscriptionModel} whose storage holds a position a
 * catch-up subscription model stored, which the MongoDB model cannot open a change stream at. The durable model hands
 * the MongoDB model a dynamic start that reads storage, and the MongoDB model refuses an answer such as that, since it
 * would otherwise open at the present and skip every event written before it.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelStoredCatchupPositionMongoTest {

    private static final String DATABASE = "durablestoredcatchupposition";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

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
    void a_subscription_whose_stored_checkpoint_is_a_global_checkpoint_is_refused_instead_of_opened_at_the_present() {
        // Given a checkpoint a catch-up subscription model stored, and an event written before the subscribe
        String subscriptionId = UUID.randomUUID().toString();
        String eventCollectionName = "events-" + UUID.randomUUID();
        MongoCollection<Document> eventCollection = database.getCollection(eventCollectionName);
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        GlobalCheckpoint stored = GlobalCheckpoint.of(0);
        storage.save(subscriptionId, stored);
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollectionName, RFC_3339_STRING, executor);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        try {
            eventStore.write("stream", List.of(event("written-before-the-subscribe", "SomethingHappened")));

            // When it subscribes with the default start
            Subscription subscription = durable.subscribe(subscriptionId, handled::add);

            // Then the subscription stops, never starts and keeps the stored checkpoint
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(durable.isRunning(subscriptionId)).isFalse());
            Throwable refusal = catchThrowable(() -> subscription.waitUntilStarted(Duration.ofMillis(200)));
            assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                    .hasMessageContaining("\"" + stored.asString() + "\"");
            assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());

            // And the event written before it is never delivered, also not once a subscription at the present has been delivered what is written after the subscribe
            List<CloudEvent> control = subscribeAtThePresent(nativeModel);
            eventStore.write("stream", List.of(event("written-after-the-subscribe", "SomethingHappened")));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(control).extracting(CloudEvent::getId).contains("written-after-the-subscribe"));
            assertThat(durable.isRunning(subscriptionId)).isFalse();
            assertThat(handled).isEmpty();
        } finally {
            durable.shutdown();
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    @Test
    void a_resume_of_a_run_a_pause_closed_while_storage_was_read_is_refused_once_a_catchup_position_is_stored() throws Exception {
        // Given a live checkpoint stored for the subscription, and an event written after it
        String subscriptionId = UUID.randomUUID().toString();
        String eventCollectionName = "events-" + UUID.randomUUID();
        MongoCollection<Document> eventCollection = database.getCollection(eventCollectionName);
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollectionName, RFC_3339_STRING, executor);
        String helperId = UUID.randomUUID().toString();
        List<CloudEvent> seen = new CopyOnWriteArrayList<>();
        nativeModel.subscribe(helperId, StartAt.now(), seen::add).waitUntilStarted(TIMEOUT);
        eventStore.write("stream", List.of(event("written-before-the-checkpoint", "SomethingHappened")));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(seen).isNotEmpty());
        Checkpoint live = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(seen.getFirst());
        nativeModel.cancelSubscription(helperId);
        eventStore.write("stream", List.of(event("written-after-the-checkpoint", "SomethingHappened")));

        // And a storage whose first read on an executor thread is held, so a run is closed while it asks where to start
        Thread testThread = Thread.currentThread();
        CountDownLatch readHeld = new CountDownLatch(1);
        CountDownLatch mayRead = new CountDownLatch(1);
        AtomicBoolean holdTheFirstRead = new AtomicBoolean(true);
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Checkpoint read(String id) {
                if (Thread.currentThread() != testThread && holdTheFirstRead.getAndSet(false)) {
                    readHeld.countDown();
                    try {
                        mayRead.await(20, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.read(id);
            }
        };
        storage.save(subscriptionId, live);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage);
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        try {
            durable.subscribe(subscriptionId, handled::add);
            assertThat(readHeld.await(10, SECONDS)).isTrue();

            // And the run is paused while it reads, so it answers the live checkpoint and ends without opening a change stream
            durable.pauseSubscription(subscriptionId);
            mayRead.countDown();
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(executor.getActiveCount()).isZero());

            // When a catch-up subscription model stores its position, and the subscription is resumed
            GlobalCheckpoint stored = GlobalCheckpoint.of(0);
            storage.save(subscriptionId, stored);
            Subscription resumed = durable.resumeSubscription(subscriptionId);

            // Then it is refused instead of opened at the present, and delivers nothing
            Throwable refusal = catchThrowable(() -> resumed.waitUntilStarted(TIMEOUT));
            assertThat(refusal).isExactlyInstanceOf(UnsupportedStartAtException.class)
                    .hasMessageContaining("subscription \"" + subscriptionId + "\"")
                    .hasMessageContaining("\"" + stored.asString() + "\"");
            assertThat(durable.isRunning(subscriptionId)).isFalse();
            List<CloudEvent> control = subscribeAtThePresent(nativeModel);
            eventStore.write("stream", List.of(event("written-after-the-resume", "SomethingHappened")));
            await().atMost(TIMEOUT).untilAsserted(() -> assertThat(control).extracting(CloudEvent::getId).contains("written-after-the-resume"));
            assertThat(handled).isEmpty();
        } finally {
            mayRead.countDown();
            durable.shutdown();
            nativeModel.shutdown();
            executor.shutdown();
        }
    }

    private static List<CloudEvent> subscribeAtThePresent(NativeMongoSubscriptionModel nativeModel) {
        List<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        nativeModel.subscribe(UUID.randomUUID().toString(), StartAt.now(), delivered::add).waitUntilStarted(TIMEOUT);
        return delivered;
    }
}
