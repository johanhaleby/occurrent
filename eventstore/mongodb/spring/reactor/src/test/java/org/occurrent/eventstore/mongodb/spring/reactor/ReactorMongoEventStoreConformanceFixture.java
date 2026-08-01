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
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.eventstore.blocking.StoreWithoutPosition;
import org.occurrent.tck.eventstore.reactor.BlockingEventStoreOverReactive;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;

import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.DATE;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.RFC_3339_STRING;

/**
 * Runs the blocking conformance suites against the reactive store through {@link BlockingEventStoreOverReactive}, so
 * the scenarios are described once rather than a second time in terms of {@code Mono} and {@code Flux}.
 */
class ReactorMongoEventStoreConformanceFixture implements EventStoreFixture {

    private final MongoClient mongoClient;
    private final ReactiveMongoTemplate mongoTemplate;
    private final ReactiveMongoTransactionManager transactionManager;
    private final BlockingEventStoreOverReactive bridge;
    private final TimeRepresentation timeRepresentation;

    ReactorMongoEventStoreConformanceFixture(ConnectionString connectionString) {
        this(connectionString, RFC_3339_STRING);
    }

    ReactorMongoEventStoreConformanceFixture(ConnectionString connectionString, TimeRepresentation timeRepresentation) {
        this.timeRepresentation = timeRepresentation;
        this.mongoClient = MongoClients.create(connectionString);
        String database = requireNonNull(connectionString.getDatabase());
        this.mongoTemplate = new ReactiveMongoTemplate(mongoClient, database);
        this.transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(connectionString.getCollection())
                .transactionConfig(transactionManager)
                .timeRepresentation(timeRepresentation)
                .build();
        this.bridge = BlockingEventStoreOverReactive.of(new ReactorMongoEventStore(mongoTemplate, eventStoreConfig));
    }

    @Override
    public Set<EventStoreCapability> capabilities() {
        return Set.of(EventStoreCapability.STREAM);
    }

    @Override
    public ChronoUnit timePrecision() {
        // DATE stores a millisecond epoch value, so anything finer is lost rather than kept.
        return timeRepresentation == DATE ? ChronoUnit.MILLIS : ChronoUnit.NANOS;
    }

    @Override
    public boolean preservesTimeOffset() {
        // DATE has no offset field alongside the epoch value, so it cannot hold anything but UTC.
        return timeRepresentation != DATE;
    }

    @Override
    public EventStore eventStore() {
        return bridge;
    }

    @Override
    public EventStoreQueries queries() {
        return bridge;
    }

    @Override
    public EventStoreOperations operations() {
        return bridge;
    }

    @Override
    public ReadEventStreamWithFilter filteredReader() {
        return bridge;
    }

    @Override
    public PositionOrderedReader positionOrderedReader() {
        return bridge;
    }

    @Override
    public Optional<StoreWithoutPosition> storeWithoutPosition() {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events-without-position")
                .transactionConfig(transactionManager)
                .timeRepresentation(timeRepresentation)
                .withoutStreamPosition()
                .build();
        BlockingEventStoreOverReactive withoutPosition = BlockingEventStoreOverReactive.of(new ReactorMongoEventStore(mongoTemplate, eventStoreConfig));
        return Optional.of(new StoreWithoutPosition(withoutPosition, withoutPosition));
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
