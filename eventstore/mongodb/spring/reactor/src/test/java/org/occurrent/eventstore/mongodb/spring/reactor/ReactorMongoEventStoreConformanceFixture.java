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
import org.occurrent.eventstore.api.blocking.*;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.tck.eventstore.blocking.*;
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
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
                .build();
        this.bridge = BlockingEventStoreOverReactive.of(new ReactorMongoEventStore(mongoTemplate, eventStoreConfig));
    }

    @Override
    public Set<EventStoreCapability> capabilities() {
        return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
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
    public DcbEventStore dcbEventStore() {
        return bridge;
    }

    /**
     * The store answers a token-qualified condition from per-boundary tag markers rather than by rescanning events
     * (ADR 21).
     */
    @Override
    public DcbAppendConditionModel appendConditionModel() {
        return DcbAppendConditionModel.TAG_MARKER;
    }

    @Override
    public Optional<EventStoreWithoutPosition> eventStoreWithoutPosition() {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName("events-without-position")
                .transactionConfig(transactionManager)
                .timeRepresentation(timeRepresentation)
                .withoutStreamPosition()
                .build();
        BlockingEventStoreOverReactive withoutPosition = BlockingEventStoreOverReactive.of(new ReactorMongoEventStore(mongoTemplate, eventStoreConfig));
        return Optional.of(new EventStoreWithoutPosition(withoutPosition, withoutPosition));
    }

    @Override
    public Optional<EventStoreWithoutDcb> eventStoreWithoutDcb() {
        BlockingEventStoreOverReactive withoutDcb = restrictedTo("events-stream-only", EventStoreCapability.STREAM);
        return Optional.of(new EventStoreWithoutDcb(withoutDcb, withoutDcb));
    }

    @Override
    public Optional<EventStoreWithoutStream> eventStoreWithoutStream() {
        BlockingEventStoreOverReactive withoutStream = restrictedTo("events-dcb-only", EventStoreCapability.DCB);
        return Optional.of(new EventStoreWithoutStream(withoutStream, withoutStream, withoutStream, withoutStream, withoutStream));
    }

    /**
     * A store on its own collection, so the capability it was denied cannot be answered from events the main store
     * wrote. The reactive store answers a denied call with {@code Mono.error}, which the bridge turns into the throw
     * the guard suite asserts.
     */
    private BlockingEventStoreOverReactive restrictedTo(String collection, EventStoreCapability capability) {
        EventStoreConfig eventStoreConfig = new EventStoreConfig.Builder()
                .eventStoreCollectionName(collection)
                .transactionConfig(transactionManager)
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(capability)
                .build();
        return BlockingEventStoreOverReactive.of(new ReactorMongoEventStore(mongoTemplate, eventStoreConfig));
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
