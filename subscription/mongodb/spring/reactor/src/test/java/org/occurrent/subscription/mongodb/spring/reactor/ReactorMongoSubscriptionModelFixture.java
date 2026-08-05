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

package org.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.reactivestreams.client.MongoClient;
import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;
import org.occurrent.tck.subscription.reactor.BlockingSubscriptionOverReactive;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.ReactiveTransactionManager;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.UUID;

/**
 * Feeds {@link ReactorMongoSubscriptionModel} through a {@link ReactorMongoEventStore}, mirroring
 * {@code SpringMongoSubscriptionModelFixture} on the blocking side. Every fixture instance gets its own collection,
 * named with a UUID, so tests never share one and cleanup never has to drop it: dropping a collection kills a live
 * change stream, so {@link #close()} only shuts the model down and deletes documents.
 */
class ReactorMongoSubscriptionModelFixture implements SubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();

    private final ReactorMongoEventStore eventStore;
    private final ReactorMongoSubscriptionModel reactorSubscriptionModel;
    private final SubscriptionModel subscriptionModel;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final String eventCollectionName;

    ReactorMongoSubscriptionModelFixture(MongoClient mongoClient, String databaseName) {
        this.eventCollectionName = "events-" + UUID.randomUUID();
        this.reactiveMongoTemplate = new ReactiveMongoTemplate(mongoClient, databaseName);
        TimeRepresentation timeRepresentation = TimeRepresentation.RFC_3339_STRING;
        ReactiveTransactionManager reactiveMongoTransactionManager =
                new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, databaseName));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(eventCollectionName)
                .transactionConfig(reactiveMongoTransactionManager)
                .timeRepresentation(timeRepresentation)
                .build();
        this.eventStore = new ReactorMongoEventStore(reactiveMongoTemplate, eventStoreConfig);
        this.reactorSubscriptionModel = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, timeRepresentation);
        // The checkpoint-aware factory, because this model genuinely is: the bridge then also answers the blocking
        // CheckpointAwareSubscriptionModel, which is what lets CheckpointAwareSubscriptionModelConformance run.
        this.subscriptionModel = BlockingSubscriptionOverReactive.ofCheckpointAware(reactorSubscriptionModel);
    }

    ReactorMongoSubscriptionModel reactorSubscriptionModel() {
        return reactorSubscriptionModel;
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        eventStore.write(streamId, Flux.fromIterable(events)).block();
    }

    /**
     * A named subscription advances its tracked resume position only once the action's {@code Mono} completes
     * ({@code ReactorMongoSubscriptionModel.startInternalSubscription}'s {@code doOnSuccess}). Pausing here disposes
     * the change stream but keeps that tracked position, so resuming restarts the change stream with
     * {@code startAfter}/{@code resumeAt} from it, replaying whatever committed while paused. Both blocking MongoDB
     * models answer the same way, for the same reason (#522).
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    /**
     * A failing action's {@code Mono} is retried with the model's configured backoff, the reactor counterpart of the
     * blocking models' {@code RetryStrategy} around the handler. Delivery is asynchronous, so the failure never
     * reaches {@link #publish(List)}, which is a plain, independent event-store write.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    @Override
    public void close() {
        reactorSubscriptionModel.shutdown();
        // Delete documents rather than dropping the collection: dropping kills a live change stream, and shutdown()
        // above has only just asked this model's own stream to close.
        reactiveMongoTemplate.remove(new Query(), eventCollectionName).block();
    }
}
