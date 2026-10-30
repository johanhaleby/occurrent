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
import org.occurrent.subscription.GlobalCheckpoint;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
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
            assertThat(subscription.waitUntilStarted(Duration.ofMillis(200))).isFalse();
            assertThat(storage.read(subscriptionId).asString()).isEqualTo(stored.asString());

            // And the event written before it is never delivered, also not once the model has had time to open the change stream
            eventStore.write("stream", List.of(event("written-after-the-subscribe", "SomethingHappened")));
            assertThat(durable.isRunning(subscriptionId)).isFalse();
            assertThat(handled).isEmpty();
        } finally {
            durable.shutdown();
            nativeModel.shutdown();
            executor.shutdown();
        }
    }
}
