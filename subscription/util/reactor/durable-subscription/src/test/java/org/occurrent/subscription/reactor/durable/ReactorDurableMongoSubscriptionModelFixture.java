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

package org.occurrent.subscription.reactor.durable;

import com.mongodb.reactivestreams.client.MongoClient;
import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
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
 * Feeds {@code Durable(Mongo)}, the composition in which the wrapped model manages named subscriptions itself, so
 * {@link ReactorDurableSubscriptionModel} hands the subscription over and inherits everything
 * {@link ReactorMongoSubscriptionModel} already does for one.
 * <p>
 * It is the counterpart of {@link ReactorDurableSubscriptionModelFixture}, which wraps a catch-up model that offers
 * only the cold primitive and therefore takes the other path through the durable model. Both are shipped compositions,
 * which is why both are covered.
 */
class ReactorDurableMongoSubscriptionModelFixture implements SubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();

    private final ReactorMongoEventStore eventStore;
    private final ReactorDurableSubscriptionModel durableModel;
    private final SubscriptionModel subscriptionModel;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final String eventCollectionName;
    private final String checkpointCollectionName;

    ReactorDurableMongoSubscriptionModelFixture(MongoClient mongoClient, String databaseName) {
        this.eventCollectionName = "events-" + UUID.randomUUID();
        this.checkpointCollectionName = "checkpoints-" + UUID.randomUUID();
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
        ReactorMongoSubscriptionModel mongoModel = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, timeRepresentation);
        ReactorCheckpointStorage checkpointStorage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        this.durableModel = new ReactorDurableSubscriptionModel(mongoModel, checkpointStorage);
        this.subscriptionModel = BlockingSubscriptionOverReactive.of(durableModel);
    }

    ReactorDurableSubscriptionModel durableModel() {
        return durableModel;
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
     * Pausing and resuming are the wrapped model's, which keeps the position of the last event it delivered and
     * restarts the change stream from it, so whatever was written while the subscription was paused is delivered on
     * resume. Same answer as {@code ReactorMongoSubscriptionModelFixture} gives, for the same reason.
     */
    @Override
    public boolean deliversEventsPublishedWhilePaused() {
        return true;
    }

    /**
     * The action is the wrapped model's action now, so a failing one is retried with that model's configured backoff
     * instead of ending the subscription. Delivery is asynchronous, so the failure never reaches
     * {@link #publish(List)}, which is a plain, independent event-store write.
     */
    @Override
    public boolean retriesAFailingHandler() {
        return true;
    }

    @Override
    public void close() {
        durableModel.shutdown();
        // Delete documents rather than dropping either collection: dropping kills a live change stream, and
        // shutdown() above has only just asked this model's own stream to close.
        reactiveMongoTemplate.remove(new Query(), eventCollectionName).block();
        reactiveMongoTemplate.remove(new Query(), checkpointCollectionName).block();
    }
}
