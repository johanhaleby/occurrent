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

package org.occurrent.subscription.mongodb.spring.blocking;

import com.mongodb.client.MongoClient;
import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.mongodb.spring.blocking.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.blocking.SpringMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Feeds {@link SpringMongoSubscriptionModel} through a {@link SpringMongoEventStore}, since the model watches a
 * MongoDB collection rather than accepting events directly. Every fixture instance gets its own collection, named
 * with a UUID, so tests never share one and cleanup never has to drop it: dropping a collection kills a live change
 * stream, so {@link #close()} only shuts the model down and deletes documents.
 * <p>
 * There is exactly one event stream behind every fixture. The suite hands over plain CloudEvents with no stream
 * bookkeeping on them at all, so this class is the thing that owns the stream id and the version counter, and the
 * event store is what stamps the Occurrent stream extensions on write.
 */
class SpringMongoSubscriptionModelFixture implements SubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();
    private final AtomicLong version = new AtomicLong(0);

    private final EventStore eventStore;
    private final SpringMongoSubscriptionModel subscriptionModel;
    private final MongoTemplate mongoTemplate;
    private final String eventCollectionName;

    SpringMongoSubscriptionModelFixture(MongoClient mongoClient, MongoTemplate mongoTemplate, String databaseName) {
        // A collection of its own per test, rather than a shared one, so the change stream this test watches never
        // sees another test's writes and cleanup never has to drop it.
        this.mongoTemplate = mongoTemplate;
        this.eventCollectionName = "events-" + UUID.randomUUID();
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(mongoTransactionManager)
                .timeRepresentation(timeRepresentation)
                .build();
        this.eventStore = new SpringMongoEventStore(mongoTemplate, eventStoreConfig);
        this.subscriptionModel = new SpringMongoSubscriptionModel(mongoTemplate, eventCollectionName, timeRepresentation);
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        long expectedVersion = version.get();
        eventStore.write(streamId, expectedVersion, events);
        version.addAndGet(events.size());
    }

    /**
     * This model tracks the change-stream position it has read to and rebuilds the change-stream request from there, so
     * an event written while a subscription was paused arrives once it resumes (#522). The native driver's model and
     * the reactor one answer the same way, for the same reason.
     * <p>
     * The cost is redelivery, which is the direction Occurrent errs in: a competing consumer is paused precisely
     * because another consumer holds the lease, and that consumer has already delivered the events in the gap, so a
     * gap-free resume hands them over a second time. Wasted work beats a lost event (ADR 57).
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    /**
     * The listener is wrapped in {@code executeWithRetry(action, __ -> !shutdown, retryStrategy)}, so a throwing
     * handler is retried rather than propagating out of {@link #publish(List)}.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    @Override
    public void close() {
        subscriptionModel.shutdown();
        // Delete documents rather than dropping the collection: dropping kills a live change stream, and shutdown()
        // above has only just asked this model's own stream to close.
        mongoTemplate.remove(new Query(), eventCollectionName);
    }
}
