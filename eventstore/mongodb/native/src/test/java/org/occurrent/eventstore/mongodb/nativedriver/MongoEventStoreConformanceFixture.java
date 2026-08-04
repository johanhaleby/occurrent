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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.tck.eventstore.blocking.DcbAppendConditionModel;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.eventstore.blocking.StoreWithoutDcb;
import org.occurrent.tck.eventstore.blocking.StoreWithoutPosition;
import org.occurrent.tck.eventstore.blocking.StoreWithoutStream;

import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.DATE;

class MongoEventStoreConformanceFixture implements EventStoreFixture {

    private final MongoClient mongoClient;
    private final String database;
    private final MongoEventStore eventStore;
    private final TimeRepresentation timeRepresentation;

    MongoEventStoreConformanceFixture(ConnectionString connectionString) {
        this(connectionString, TimeRepresentation.RFC_3339_STRING);
    }

    MongoEventStoreConformanceFixture(ConnectionString connectionString, TimeRepresentation timeRepresentation) {
        this.timeRepresentation = timeRepresentation;
        this.mongoClient = MongoClients.create(connectionString);
        this.database = requireNonNull(connectionString.getDatabase());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
                .build();
        this.eventStore = new MongoEventStore(mongoClient, database, "events", config);
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
    public ReadEventStreamWithFilter filteredReader() {
        return eventStore;
    }

    @Override
    public PositionOrderedReader positionOrderedReader() {
        return eventStore;
    }

    @Override
    public DcbEventStore dcbEventStore() {
        return eventStore;
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
    public Optional<StoreWithoutPosition> storeWithoutPosition() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(EventStoreCapability.STREAM)
                .withoutStreamPosition()
                .build();
        MongoEventStore withoutPosition = new MongoEventStore(mongoClient, database, "events-without-position", config);
        return Optional.of(new StoreWithoutPosition(withoutPosition, withoutPosition));
    }

    @Override
    public Optional<StoreWithoutDcb> storeWithoutDcb() {
        MongoEventStore withoutDcb = restrictedTo("events-stream-only", EventStoreCapability.STREAM);
        return Optional.of(new StoreWithoutDcb(withoutDcb, withoutDcb));
    }

    @Override
    public Optional<StoreWithoutStream> storeWithoutStream() {
        MongoEventStore withoutStream = restrictedTo("events-dcb-only", EventStoreCapability.DCB);
        return Optional.of(new StoreWithoutStream(withoutStream, withoutStream, withoutStream, withoutStream, withoutStream));
    }

    /**
     * A store on its own collection, so the capability it was denied cannot be answered from events the main store
     * wrote.
     */
    private MongoEventStore restrictedTo(String collection, EventStoreCapability capability) {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(capability)
                .build();
        return new MongoEventStore(mongoClient, database, collection, config);
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
