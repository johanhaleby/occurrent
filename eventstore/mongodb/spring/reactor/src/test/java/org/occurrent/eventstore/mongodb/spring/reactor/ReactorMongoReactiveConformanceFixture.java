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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.tck.eventstore.reactor.ReactiveEventStoreFixture;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;

import static java.util.Objects.requireNonNull;

/**
 * Hands the reactive store over unwrapped, unlike {@link ReactorMongoEventStoreConformanceFixture}, which wraps it in
 * the blocking bridge. The reactive contract is about the publishers themselves, so a bridge over them is the one thing
 * that cannot be in the way.
 */
class ReactorMongoReactiveConformanceFixture implements ReactiveEventStoreFixture {

    private final MongoClient mongoClient;
    private final ReactorMongoEventStore eventStore;

    ReactorMongoReactiveConformanceFixture(ConnectionString connectionString) {
        this.mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, database);
        ReactiveMongoTransactionManager transactionManager = new ReactiveMongoTransactionManager(
                new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(EventStoreCapability.STREAM)
                .build();
        this.eventStore = new ReactorMongoEventStore(mongoTemplate, eventStoreConfig);
    }

    @Override
    public EventStore eventStore() {
        return eventStore;
    }

    @Override
    public EventStoreQueries queries() {
        return eventStore;
    }

    @Override
    public EventStoreOperations operations() {
        return eventStore;
    }

    @Override
    public PositionOrderedReader positionOrderedReader() {
        return eventStore;
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
