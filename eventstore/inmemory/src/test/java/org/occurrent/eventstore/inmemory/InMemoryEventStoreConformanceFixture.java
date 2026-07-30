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

package org.occurrent.eventstore.inmemory;

import org.occurrent.eventstore.api.EventStoreCapability;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;

import java.util.Set;

/**
 * A fresh instance per test is all the cleanup an in-memory store needs, so there is nothing to close.
 */
class InMemoryEventStoreConformanceFixture implements EventStoreFixture {

    private final InMemoryEventStore eventStore = new InMemoryEventStore();

    @Override
    public Set<EventStoreCapability> capabilities() {
        return Set.of(EventStoreCapability.STREAM, EventStoreCapability.DCB);
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
    public DcbEventStore dcbEventStore() {
        return eventStore;
    }

    @Override
    public PositionOrderedReader positionOrderedReader() {
        return eventStore;
    }

    /**
     * The in-memory store treats a natural sort step as an insertion-order tiebreaker for the preceding fields,
     * rather than rejecting the compound sort. See {@link org.occurrent.eventstore.api.SortBy#natural} for the
     * documented variation this override exercises.
     */
    @Override
    public boolean composesNaturalSortWithFieldSorts() {
        return true;
    }
}
