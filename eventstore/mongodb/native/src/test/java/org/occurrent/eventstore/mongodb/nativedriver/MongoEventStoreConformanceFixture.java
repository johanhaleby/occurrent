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
import org.occurrent.eventstore.api.blocking.*;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.tck.eventstore.blocking.*;
import org.occurrent.tck.latecommit.AppendHold;
import org.occurrent.tck.latecommit.HeldAppend;
import org.occurrent.tck.latecommit.LateCommits;
import org.occurrent.testsupport.mongodb.HoldingMongoClient;

import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.occurrent.mongodb.timerepresentation.TimeRepresentation.DATE;

class MongoEventStoreConformanceFixture implements EventStoreFixture {

    private final MongoClient mongoClient;
    private final String database;
    private final MongoEventStore eventStore;
    private final EventStoreConfig eventStoreConfig;
    private final TimeRepresentation timeRepresentation;

    MongoEventStoreConformanceFixture(ConnectionString connectionString) {
        this(connectionString, TimeRepresentation.RFC_3339_STRING);
    }

    MongoEventStoreConformanceFixture(ConnectionString connectionString, TimeRepresentation timeRepresentation) {
        this.timeRepresentation = timeRepresentation;
        this.mongoClient = MongoClients.create(connectionString);
        this.database = requireNonNull(connectionString.getDatabase());
        this.eventStoreConfig = new EventStoreConfig.Builder()
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(EventStoreCapability.STREAM, EventStoreCapability.DCB)
                .build();
        this.eventStore = new MongoEventStore(mongoClient, database, "events", eventStoreConfig);
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

    /**
     * The store reserves the position block before the append transaction starts, so an append can be held between the
     * two. The hold is a client that blocks the first {@code startSession}, which the store calls after it has reserved
     * its position and before it begins the transaction.
     */
    @Override
    public LateCommits lateCommits() {
        return LateCommits.holding(() -> {
            AppendHold hold = new AppendHold();
            MongoClient holdingClient = HoldingMongoClient.beforeEachSession(mongoClient, () -> {
                if (hold.engage()) {
                    hold.awaitRelease();
                }
            });
            MongoEventStore holding = new MongoEventStore(holdingClient, database, "events", eventStoreConfig);
            return HeldAppend.of(hold, holding, () -> {
            });
        });
    }

    @Override
    public Optional<EventStoreWithoutPosition> eventStoreWithoutPosition() {
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(timeRepresentation)
                .eventStoreCapabilities(EventStoreCapability.STREAM)
                .withoutStreamPosition()
                .build();
        MongoEventStore withoutPosition = new MongoEventStore(mongoClient, database, "events-without-position", config);
        return Optional.of(new EventStoreWithoutPosition(withoutPosition, withoutPosition));
    }

    @Override
    public Optional<EventStoreWithoutDcb> eventStoreWithoutDcb() {
        MongoEventStore withoutDcb = restrictedTo("events-stream-only", EventStoreCapability.STREAM);
        return Optional.of(new EventStoreWithoutDcb(withoutDcb, withoutDcb));
    }

    @Override
    public Optional<EventStoreWithoutStream> eventStoreWithoutStream() {
        MongoEventStore withoutStream = restrictedTo("events-dcb-only", EventStoreCapability.DCB);
        return Optional.of(new EventStoreWithoutStream(withoutStream, withoutStream, withoutStream, withoutStream, withoutStream));
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
