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
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What both conformance suites share: a {@link NativeMongoSubscriptionModel} watching a collection of its own, and a
 * {@link MongoEventStore} in front of it that {@link #publish(List)} writes through, since a change stream only ever
 * observes a write made this way.
 * <p>
 * Every fixture gets its own event collection, named with a UUID, and never drops it: dropping a collection or a
 * database kills a live change stream, so cleanup only ever deletes documents.
 */
class NativeMongoSubscriptionModelFixture implements SubscriptionModelFixture {

    private final MongoCollection<Document> eventCollection;
    private final MongoEventStore eventStore;
    private final ExecutorService subscriptionExecutor;
    private final NativeMongoSubscriptionModel subscriptionModel;
    private final String streamId = UUID.randomUUID().toString();
    private final AtomicLong streamVersion = new AtomicLong(0);

    NativeMongoSubscriptionModelFixture(MongoClient mongoClient, MongoDatabase database) {
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        this.eventCollection = database.getCollection("events-" + UUID.randomUUID());
        this.eventStore = new MongoEventStore(mongoClient, database, eventCollection, new EventStoreConfig(timeRepresentation));
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
        long expectedVersion = streamVersion.getAndAdd(events.size());
        eventStore.write(streamId, expectedVersion, events);
    }

    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    @Override
    public boolean retriesAFailingHandler() {
        return true;
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
