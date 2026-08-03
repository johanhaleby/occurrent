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
import org.occurrent.inmemory.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.tck.eventstore.blocking.DcbAppendConditionModel;
import org.occurrent.tck.eventstore.blocking.EventStoreFixture;
import org.occurrent.tck.eventstore.blocking.StoreWithoutPosition;

import java.util.Optional;
import java.util.Set;

/**
 * A fresh instance per test is all the cleanup an in-memory store needs, so there is nothing to close.
 */
class InMemoryEventStoreConformanceFixture implements EventStoreFixture {

    private final InMemoryEventStore eventStore;
    private final boolean supportsDataFilter;

    InMemoryEventStoreConformanceFixture() {
        this(true);
    }

    /**
     * @param withDataFieldReader whether the store under test can reach into a payload. Both answers are a supported
     *                            configuration, so the suite asserts a documented outcome either way.
     */
    InMemoryEventStoreConformanceFixture(boolean withDataFieldReader) {
        this.supportsDataFilter = withDataFieldReader;
        this.eventStore = withDataFieldReader
                ? new InMemoryEventStore().withDataFieldReader(new JacksonDataFieldReader())
                : new InMemoryEventStore();
    }

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
    public DcbAppendConditionModel appendConditionModel() {
        return DcbAppendConditionModel.EXACT_CRITERIA;
    }

    @Override
    public PositionOrderedReader positionOrderedReader() {
        return eventStore;
    }

    @Override
    public Optional<StoreWithoutPosition> storeWithoutPosition() {
        InMemoryEventStore withoutPosition = new InMemoryEventStore().withoutStreamPosition();
        return Optional.of(new StoreWithoutPosition(withoutPosition, withoutPosition));
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

    /**
     * The in-memory store's natural order is a list it appends to on every write, across every stream, so it can
     * promise more than the documented "could be undefined" variation on
     * {@link org.occurrent.eventstore.api.SortBy#natural}: its natural order is insertion order, always.
     */
    @Override
    public boolean naturalOrderIsInsertionOrder() {
        return true;
    }

    /**
     * The store under test is built with a Jackson-backed reader, so it can reach into a payload. A store built
     * without one refuses instead, which {@link InMemoryEventStoreQueriesWithoutDataReaderConformanceTest} covers.
     */
    @Override
    public boolean supportsDataFilter() {
        return supportsDataFilter;
    }
}
