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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.blocking.durable.WinnerHiddenFromTheConfirmReadStorage.ConfirmRead;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * What {@link NativeMongoSubscriptionModel} does with a start position that {@link DurableSubscriptionModel} refuses
 * to evaluate, because another node stored the first position and reading it back did not name it. The model logs
 * the refusal and opens the change stream again on its retry strategy's backoff, and the first attempt after
 * storage can be read again starts from the position the other node stored.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionRaceMongoTest {

    private static final String DATABASE = "durablefirstpositionrace";
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
    void an_event_after_the_stored_position_is_delivered_once_a_failing_read_back_succeeds() throws InterruptedException {
        anEventAfterTheStoredPositionIsDeliveredOnceTheReadBackNamesIt(ConfirmRead.FAILS);
    }

    @Test
    void an_event_after_the_stored_position_is_delivered_once_a_read_back_that_found_nothing_finds_it() throws InterruptedException {
        anEventAfterTheStoredPositionIsDeliveredOnceTheReadBackNamesIt(ConfirmRead.FINDS_NOTHING);
    }

    private static void anEventAfterTheStoredPositionIsDeliveredOnceTheReadBackNamesIt(ConfirmRead confirmRead) throws InterruptedException {
        MongoCollection<Document> eventCollection = database.getCollection("events-" + UUID.randomUUID());
        MongoEventStore eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicBoolean answersNothingOnce = new AtomicBoolean(false);
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollection, RFC_3339_STRING, executor,
                RetryStrategy.fixed(Duration.ofMillis(100))) {
            // Answering nothing to the call subscribe makes before handing the start position to this model, with
            // startWhenNoStartPositionCanBeRecorded below, means the first position is recorded by the start position
            // this model evaluates on its own thread
            @Override
            public @Nullable Checkpoint globalCheckpoint() {
                return answersNothingOnce.getAndSet(false) ? null : super.globalCheckpoint();
            }
        };
        try {
            Checkpoint positionTheOtherNodeStores = nativeModel.globalCheckpoint();
            CloudEvent writtenAfterIt = event("written-after-the-stored-position", "SomethingHappened");
            eventStore.write("stream", List.of(writtenAfterIt));
            WinnerHiddenFromTheConfirmReadStorage storage = new WinnerHiddenFromTheConfirmReadStorage(confirmRead, positionTheOtherNodeStores);
            DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage,
                    new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));
            List<CloudEvent> handled = new CopyOnWriteArrayList<>();

            answersNothingOnce.set(true);
            durable.subscribe(SUBSCRIPTION_ID, handled::add);

            boolean evaluatedAgain = eventually(() -> storage.readsThatHidTheStoredPosition() >= 3);
            assertThat(handled).as("nothing starts while the stored position cannot be read back").isEmpty();

            storage.answersReadsAgain();

            assertThat(eventually(() -> !handled.isEmpty()))
                    .as("started from the position the other node stored, which is before this event")
                    .isTrue();
            assertThat(idsOf(handled)).containsExactly(writtenAfterIt.getId());
            assertThat(evaluatedAgain)
                    .as("the start position is evaluated again, not given up on, while storage cannot name what it holds")
                    .isTrue();
        } finally {
            nativeModel.shutdown();
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
