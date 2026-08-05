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

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.tck.subscription.reactor.ReactiveSubscriptionModelFixture;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.ReactiveTransactionManager;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.UUID;

import static com.mongodb.reactivestreams.client.MongoClients.create;

/**
 * The reactive-only counterpart of {@link ReactorMongoSubscriptionModelFixture}, feeding
 * {@link ReactiveSubscriptionModelConformance} the reactor {@link ReactorMongoSubscriptionModel} directly rather than
 * through the blocking bridge.
 */
class ReactorMongoReactiveSubscriptionModelFixture implements ReactiveSubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();

    private final com.mongodb.reactivestreams.client.MongoClient mongoClient;
    private final ReactorMongoEventStore eventStore;
    private final ReactorMongoSubscriptionModel subscriptionModel;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final String eventCollectionName;

    ReactorMongoReactiveSubscriptionModelFixture(String replicaSetUrl, String databaseName) {
        this.mongoClient = create(replicaSetUrl);
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
        this.subscriptionModel = new ReactorMongoSubscriptionModel(reactiveMongoTemplate, eventCollectionName, timeRepresentation);
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return subscriptionModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        eventStore.write(streamId, Flux.fromIterable(events)).block();
    }

    @Override
    public void close() {
        subscriptionModel.shutdown();
        // Delete documents rather than dropping the collection, mirroring ReactorMongoSubscriptionModelFixture:
        // dropping kills a live change stream, and shutdown() above has only just asked this model's own stream to
        // close. Without this, a run with Testcontainers reuse enabled accumulates documents across runs.
        reactiveMongoTemplate.remove(new Query(), eventCollectionName).block();
        mongoClient.close();
    }
}
