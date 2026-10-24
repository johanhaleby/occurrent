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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
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
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * A {@link DurableSubscriptionModel} over a {@code NativeMongoSubscriptionModel} that is resumed at a
 * {@link GlobalCheckpoint} keeps the checkpoint it had stored. A restart subscribes the MongoDB model at the stored
 * checkpoint, and the MongoDB model opens a {@code GlobalCheckpoint} at the present, so storing one made a restart skip
 * the events written before it.
 */
@Testcontainers
@DisplayName("DurableSubscriptionModelResumeAtGlobalCheckpointRestartMongo")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelResumeAtGlobalCheckpointRestartMongoTest {

    private static final String DATABASE = "durableresumeatglobalcheckpoint";
    private static final String SUBSCRIPTION_ID = "someSubscription";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(DATABASE));
        database = mongoClient.getDatabase(DATABASE);
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Test
    void a_restart_after_a_resume_at_a_GlobalCheckpoint_delivers_the_events_written_before_the_restart() throws InterruptedException {
        // Given a subscription that has delivered e1 and stored its checkpoint, and has been paused
        String eventCollectionName = "events-" + UUID.randomUUID();
        MongoCollection<Document> eventCollection = database.getCollection(eventCollectionName);
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ExecutorService firstExecutor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel firstNativeModel = new NativeMongoSubscriptionModel(database, eventCollectionName, RFC_3339_STRING, firstExecutor);
        DurableSubscriptionModel firstDurable = new DurableSubscriptionModel(firstNativeModel, storage);
        List<CloudEvent> deliveredBeforeTheRestart = new CopyOnWriteArrayList<>();
        ExecutorService secondExecutor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel secondNativeModel = new NativeMongoSubscriptionModel(database, eventCollectionName, RFC_3339_STRING, secondExecutor);
        try {
            firstDurable.subscribe(SUBSCRIPTION_ID, deliveredBeforeTheRestart::add).waitUntilStarted();
            CloudEvent e1 = event("e1", "SomethingHappened");
            eventStore.write("stream", List.of(e1));
            assertThat(eventually(() -> deliveredBeforeTheRestart.size() == 1 && storage.exists(SUBSCRIPTION_ID))).as("e1 delivered and its checkpoint stored").isTrue();
            firstDurable.pauseSubscription(SUBSCRIPTION_ID);

            // When it is resumed at a GlobalCheckpoint, and the node is then replaced by one that subscribes with the default start
            RepositionableSubscriptions.findIn(firstDurable).orElseThrow().resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(GlobalCheckpoint.of(1)));
            firstDurable.shutdown();
            CloudEvent e2 = event("e2", "SomethingHappened");
            eventStore.write("stream", List.of(e2));
            DurableSubscriptionModel secondDurable = new DurableSubscriptionModel(secondNativeModel, storage);
            List<CloudEvent> deliveredAfterTheRestart = new CopyOnWriteArrayList<>();
            secondDurable.subscribe(SUBSCRIPTION_ID, deliveredAfterTheRestart::add).waitUntilStarted();
            boolean e2Delivered = eventually(() -> idsOf(deliveredAfterTheRestart).contains(e2.getId()));

            // Then e2, written after the resume and before the restart, is delivered by the restarted node, and e1 is not delivered again
            assertThat(e2Delivered).as("e2 written before the restart is delivered after it").isTrue();
            assertThat(idsOf(deliveredAfterTheRestart)).as("delivered after the restart").containsExactly(e2.getId());
        } finally {
            firstNativeModel.shutdown();
            secondNativeModel.shutdown();
        }
    }

    private static boolean eventually(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }
}
