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

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.mongodb.spring.reactor.EventStoreConfig;
import org.occurrent.eventstore.mongodb.spring.reactor.ReactorMongoEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.reactor.ReactorMongoSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.tck.subscription.reactor.ReactiveSubscriptionModelConformance;
import org.occurrent.tck.subscription.reactor.ReactiveSubscriptionModelFixture;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.UUID;

import static com.mongodb.reactivestreams.client.MongoClients.create;

/**
 * The reactive-only counterpart of {@link ReactorDurableSubscriptionModelFixture}, feeding
 * {@link ReactiveSubscriptionModelConformance} the {@code Durable(Catchup(Mongo))} composition directly rather than
 * through the blocking bridge.
 */
class ReactorDurableReactiveSubscriptionModelFixture implements ReactiveSubscriptionModelFixture {

    private final String streamId = UUID.randomUUID().toString();

    private final com.mongodb.reactivestreams.client.MongoClient mongoClient;
    private final ReactorMongoEventStore eventStore;
    private final ReactorDurableSubscriptionModel durableModel;
    private final ReactiveMongoTemplate reactiveMongoTemplate;
    private final String eventCollectionName;
    private final String checkpointCollectionName;

    ReactorDurableReactiveSubscriptionModelFixture(String replicaSetUrl, String databaseName) {
        this.mongoClient = create(replicaSetUrl);
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
        // A default filter, or the catch-up model refuses a subscription made with no SubscriptionFilter at all.
        CheckpointAwareSubscriptionModel catchupModel = new ReactorCatchupSubscriptionModel(mongoModel, eventStore, Filter.all());
        ReactorCheckpointStorage checkpointStorage = new ReactorCheckpointStorage(reactiveMongoTemplate, checkpointCollectionName);
        this.durableModel = new ReactorDurableSubscriptionModel(catchupModel, checkpointStorage);
    }

    @Override
    public SubscriptionModel subscriptionModel() {
        return durableModel;
    }

    @Override
    public void publish(List<CloudEvent> events) {
        eventStore.write(streamId, Flux.fromIterable(events)).block();
    }

    @Override
    public void close() {
        durableModel.shutdown();
        mongoClient.close();
    }
}
