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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * A {@link DurableSubscriptionModel} over a {@code NativeMongoSubscriptionModel} whose subscription of an id starts at
 * a position of its own, such as a replay, while an earlier run of the id stored a later checkpoint. The replay has
 * stored nothing of its own when it is paused, so a plain resume must go on from the position the replay has read to
 * and not from the checkpoint the earlier run stored, which lies after events the replay has not delivered.
 */
@Testcontainers
@DisplayName("DurableSubscriptionModelOwnStartResumeMongo")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelOwnStartResumeMongoTest {

    private static final String DATABASE = "durableownstartresume";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    private static final MongoDBContainer mongoDBContainer =
            ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static MongoClient mongoClient;
    private static MongoDatabase database;

    private final String subscriptionId = UUID.randomUUID().toString();
    private final String eventCollectionName = "events-" + UUID.randomUUID();
    private final AtomicBoolean failNextRead = new AtomicBoolean();
    private final AtomicBoolean readFailed = new AtomicBoolean();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
        @Override
        public Checkpoint read(String id) {
            if (failNextRead.getAndSet(false)) {
                readFailed.set(true);
                throw new IllegalStateException("Cannot read the checkpoint of " + id);
            }
            return super.read(id);
        }
    };
    private final List<AutoCloseable> toClose = new ArrayList<>();
    private MongoEventStore eventStore;

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

    @AfterEach
    void shutdownModels() throws Exception {
        for (AutoCloseable closeable : toClose) {
            closeable.close();
        }
    }

    @Test
    void a_plain_resume_of_a_replay_paused_before_it_stored_anything_goes_on_from_the_replay_and_not_from_the_checkpoint_an_earlier_run_stored() {
        // Given an earlier run that stored a checkpoint after e3, and a replay of the id from after e1 that throws for e2
        Checkpoint afterE1 = theEarlierRun().afterE1();
        DurableSubscriptionModel durable = newDurableModel(new DurableSubscriptionModelConfig(1));
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean throwing = new AtomicBoolean(true);
        CountDownLatch threwForE2 = new CountDownLatch(1);
        durable.subscribe(subscriptionId, null, StartAt.checkpoint(afterE1), cloudEvent -> {
            if (throwing.get()) {
                threwForE2.countDown();
                throw new IllegalStateException("Cannot handle " + cloudEvent.getId());
            }
            delivered.add(cloudEvent.getId());
        }).waitUntilStarted(TIMEOUT);
        awaitLatch(threwForE2);

        // When it is paused, stops throwing and is resumed with a plain resume, and e4 is written
        durable.pauseSubscription(subscriptionId);
        throwing.set(false);
        durable.resumeSubscription(subscriptionId).waitUntilStarted(TIMEOUT);
        write("e4");
        await().atMost(TIMEOUT).until(() -> delivered.contains("e4"));

        // Then e2 and e3, which the replay had not delivered, are delivered before e4
        assertThat(delivered).containsExactly("e2", "e3", "e4");
    }

    @Test
    void a_resume_at_the_subscription_model_default_of_a_replay_paused_before_it_stored_anything_goes_on_from_the_replay_and_not_from_the_checkpoint_an_earlier_run_stored() {
        // Given an earlier run that stored a checkpoint after e3, and a replay of the id from after e1 that throws for e2
        Checkpoint afterE1 = theEarlierRun().afterE1();
        DurableSubscriptionModel durable = newDurableModel(new DurableSubscriptionModelConfig(1));
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean throwing = new AtomicBoolean(true);
        CountDownLatch threwForE2 = new CountDownLatch(1);
        durable.subscribe(subscriptionId, null, StartAt.checkpoint(afterE1), cloudEvent -> {
            if (throwing.get()) {
                threwForE2.countDown();
                throw new IllegalStateException("Cannot handle " + cloudEvent.getId());
            }
            delivered.add(cloudEvent.getId());
        }).waitUntilStarted(TIMEOUT);
        awaitLatch(threwForE2);

        // When it is paused, stops throwing and is resumed at the subscription model default, and e4 is written
        durable.pauseSubscription(subscriptionId);
        throwing.set(false);
        durable.resumeSubscription(subscriptionId, StartAt.subscriptionModelDefault()).waitUntilStarted(TIMEOUT);
        write("e4");
        await().atMost(TIMEOUT).until(() -> delivered.contains("e4"));

        // Then e2 and e3, which the replay had not delivered, are delivered before e4
        assertThat(delivered).containsExactly("e2", "e3", "e4");
    }

    @Test
    void a_plain_resume_of_a_replay_that_persists_every_n_events_and_is_paused_before_the_nth_goes_on_from_the_replay_and_not_from_the_checkpoint_an_earlier_run_stored() throws Exception {
        // Given an earlier run that stored a checkpoint after e3, and a replay of the id from after e1 that stores a checkpoint only every 100th event
        Checkpoint afterE1 = theEarlierRun().afterE1();
        DurableSubscriptionModel durable = newDurableModel(new DurableSubscriptionModelConfig(100));
        List<String> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch inE2 = new CountDownLatch(1);
        CountDownLatch releaseE2 = new CountDownLatch(1);
        durable.subscribe(subscriptionId, null, StartAt.checkpoint(afterE1), cloudEvent -> {
            delivered.add(cloudEvent.getId());
            if (cloudEvent.getId().equals("e2")) {
                inE2.countDown();
                awaitLatch(releaseE2);
            }
        }).waitUntilStarted(TIMEOUT);
        awaitLatch(inE2);

        // When it is paused while it handles e2, which lets e2 return and delivers nothing more, and is then resumed with a plain resume, and e4 is written
        CompletableFuture<Void> pausing = CompletableFuture.runAsync(() -> durable.pauseSubscription(subscriptionId));
        await().atMost(TIMEOUT).until(() -> durable.isPaused(subscriptionId));
        releaseE2.countDown();
        pausing.get(10, SECONDS);
        assertThat(delivered).as("delivered before the resume").containsExactly("e2");
        durable.resumeSubscription(subscriptionId).waitUntilStarted(TIMEOUT);
        write("e4");
        await().atMost(TIMEOUT).until(() -> delivered.contains("e4"));

        // Then e3, which the replay had not delivered, is delivered before e4
        assertThat(delivered).containsExactly("e2", "e3", "e4");
    }

    @Test
    void a_plain_resume_of_a_replay_goes_on_from_a_different_checkpoint_stored_after_it_started_as_another_node_would_store_it() {
        // Given an earlier run that stored a checkpoint after e3, and a replay of the id from after e1 that throws for e2
        EarlierRun earlierRun = theEarlierRun();
        DurableSubscriptionModel durable = newDurableModel(new DurableSubscriptionModelConfig(1));
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean throwing = new AtomicBoolean(true);
        CountDownLatch threwForE2 = new CountDownLatch(1);
        durable.subscribe(subscriptionId, null, StartAt.checkpoint(earlierRun.afterE1()), cloudEvent -> {
            if (throwing.get()) {
                threwForE2.countDown();
                throw new IllegalStateException("Cannot handle " + cloudEvent.getId());
            }
            delivered.add(cloudEvent.getId());
        }).waitUntilStarted(TIMEOUT);
        awaitLatch(threwForE2);
        durable.pauseSubscription(subscriptionId);
        throwing.set(false);

        // When another node stores the checkpoint after e2, and the subscription is resumed with a plain resume, and e4 is written
        storage.save(subscriptionId, earlierRun.afterE2());
        durable.resumeSubscription(subscriptionId).waitUntilStarted(TIMEOUT);
        write("e4");
        await().atMost(TIMEOUT).until(() -> delivered.contains("e4"));

        // Then it resumes at the stored checkpoint, so e2, which that node handled, is not delivered again
        assertThat(delivered).containsExactly("e3", "e4");
    }

    @Test
    void a_plain_resume_of_a_replay_goes_on_from_the_replay_when_the_checkpoint_stored_could_not_be_read_at_subscribe_and_the_subscribe_does_not_fail() {
        // Given an earlier run that stored a checkpoint after e3, and a storage whose next read fails
        Checkpoint afterE1 = theEarlierRun().afterE1();
        failNextRead.set(true);
        DurableSubscriptionModel durable = newDurableModel(new DurableSubscriptionModelConfig(1));
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicBoolean throwing = new AtomicBoolean(true);
        CountDownLatch threwForE2 = new CountDownLatch(1);

        // When it subscribes at the replay, which throws for e2
        AtomicReference<Subscription> subscription = new AtomicReference<>();
        assertThatCode(() -> subscription.set(durable.subscribe(subscriptionId, null, StartAt.checkpoint(afterE1), cloudEvent -> {
            if (throwing.get()) {
                threwForE2.countDown();
                throw new IllegalStateException("Cannot handle " + cloudEvent.getId());
            }
            delivered.add(cloudEvent.getId());
        }))).doesNotThrowAnyException();
        subscription.get().waitUntilStarted(TIMEOUT);
        awaitLatch(threwForE2);

        // And it is paused, stops throwing and is resumed with a plain resume, and e4 is written
        durable.pauseSubscription(subscriptionId);
        throwing.set(false);
        durable.resumeSubscription(subscriptionId).waitUntilStarted(TIMEOUT);
        write("e4");
        await().atMost(TIMEOUT).until(() -> delivered.contains("e4"));

        // Then the read did fail, and e2 and e3, which the replay had not delivered, are delivered before e4
        assertThat(readFailed).isTrue();
        assertThat(delivered).containsExactly("e2", "e3", "e4");
    }

    private record EarlierRun(Checkpoint afterE1, Checkpoint afterE2) {
    }

    private EarlierRun theEarlierRun() {
        DurableSubscriptionModel earlierRun = newDurableModel(new DurableSubscriptionModelConfig(1));
        List<CloudEvent> handled = new CopyOnWriteArrayList<>();
        earlierRun.subscribe(subscriptionId, handled::add).waitUntilStarted(TIMEOUT);
        write("e1");
        write("e2");
        write("e3");
        await().atMost(TIMEOUT).until(() -> handled.size() == 3);
        Checkpoint afterE1 = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(handled.get(0));
        Checkpoint afterE2 = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(handled.get(1));
        String afterE3 = CheckpointAwareCloudEvent.getCheckpointOrThrowIAE(handled.get(2)).asString();
        await().atMost(TIMEOUT).until(() -> storage.exists(subscriptionId) && storage.read(subscriptionId).asString().equals(afterE3));
        earlierRun.shutdown();
        return new EarlierRun(afterE1, afterE2);
    }

    private DurableSubscriptionModel newDurableModel(DurableSubscriptionModelConfig config) {
        ExecutorService executor = Executors.newCachedThreadPool();
        NativeMongoSubscriptionModel nativeModel = new NativeMongoSubscriptionModel(database, eventCollectionName, RFC_3339_STRING, executor);
        DurableSubscriptionModel durable = new DurableSubscriptionModel(nativeModel, storage, config);
        toClose.add(durable::shutdown);
        toClose.add(nativeModel::shutdown);
        toClose.add(executor::shutdown);
        return durable;
    }

    private void write(String id) {
        if (eventStore == null) {
            MongoCollection<Document> eventCollection = database.getCollection(eventCollectionName);
            eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(RFC_3339_STRING));
        }
        eventStore.write("stream", List.of(event(id, "SomethingHappened")));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
