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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.tck.subscription.blocking.RestartConformance;
import org.occurrent.tck.subscription.blocking.RestartableSubscriptionModelFixture;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What every conformance suite wired against {@link NativeMongoSubscriptionModel} shares: the model watching a
 * collection of its own, and a {@link MongoEventStore} in front of it that {@link #publish(List)} writes through,
 * since a change stream only ever observes a write made this way.
 * <p>
 * Every fixture gets its own event collection, named with a UUID, and never drops it: dropping a collection or a
 * database kills a live change stream, so cleanup only ever deletes documents.
 * <p>
 * This fixture also answers {@link RestartConformance}, because rebuilding the model is nothing more than running the
 * same construction a second time over the event collection the first run left behind: the collection and the event
 * store in front of it are fields that {@link #restart()} never touches, only the model and its executor are torn
 * down and replaced.
 */
class NativeMongoSubscriptionModelFixture implements RestartableSubscriptionModelFixture {

    private final MongoDatabase database;
    private final MongoCollection<Document> eventCollection;
    private final MongoEventStore eventStore;
    private final TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
    private final String streamId = UUID.randomUUID().toString();
    private final AtomicLong streamVersion = new AtomicLong(0);

    private ExecutorService subscriptionExecutor;
    private NativeMongoSubscriptionModel subscriptionModel;

    NativeMongoSubscriptionModelFixture(MongoClient mongoClient, MongoDatabase database) {
        this.database = database;
        this.eventCollection = database.getCollection("events-" + UUID.randomUUID());
        this.eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(timeRepresentation));
        buildModel();
    }

    /**
     * Builds a fresh {@link NativeMongoSubscriptionModel} over the event collection this fixture already has. Used by
     * both the constructor and {@link #restart()}, which is exactly the point: a restart is this same construction
     * run a second time over whatever the first run left behind, nothing more.
     */
    private void buildModel() {
        this.subscriptionExecutor = Executors.newCachedThreadPool();
        this.subscriptionModel = new NativeMongoSubscriptionModel(database, eventCollection, timeRepresentation, subscriptionExecutor,
                RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500), 2.0f));
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        // Advanced only once the write has been accepted, so a rejected write does not leave the counter ahead of the
        // stream and turn the next publish into a version conflict that names the wrong problem.
        long expectedVersion = streamVersion.get();
        eventStore.write(streamId, expectedVersion, events);
        streamVersion.addAndGet(events.size());
    }

    @Override
    public SubscriptionModel restart() {
        // shutdown() only tears down this model's own cursors and executor; the event collection and the event
        // store in front of it are fields buildModel() never recreates, so they outlive this call the way a
        // database outlives an application restart.
        subscriptionModel.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        buildModel();
        return subscriptionModel;
    }

    /**
     * False: this model keeps no checkpoint of its own. A fresh instance subscribing with
     * {@code StartAt.subscriptionModelDefault()} (what a caller gets from {@code subscribe(id, action)}) runs
     * straight into {@code MongoCommons.applyStartPosition}, which for a default start position returns the change
     * stream builder unmodified, so {@code eventCollection.watch(..)} opens at wherever the server's oplog is right
     * now rather than at any position this model remembers. Events published while nothing was running are gone,
     * which is exactly why {@code DurableSubscriptionModel} exists to wrap this model in a checkpoint: the durable
     * wiring of this same suite asserts {@code true} for exactly that wrapper.
     */
    @Override
    public boolean resumesAfterARestart() {
        return false;
    }

    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    /**
     * This model reads a change stream and is checkpoint aware, so the honest answer is whatever it reports from
     * its own {@code globalCheckpoint()}.
     */
    @Override
    public Checkpoint aCheckpointToStartFrom() {
        return SubscriptionModelFixture.orGlobalPositionZero(subscriptionModel.globalCheckpoint());
    }

    @Override
    public void close() {
        subscriptionModel.shutdown();
        subscriptionExecutor.shutdown();
        ExecutorShutdown.shutdownSafely(subscriptionExecutor, 10, TimeUnit.SECONDS);
        // Delete documents rather than dropping the collection: dropping kills a live change stream, and shutdown()
        // above has only just asked this model's own stream to close.
        eventCollection.deleteMany(new Document());
    }
}
