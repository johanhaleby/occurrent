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

package org.occurrent.tck.eventstore.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.DcbEventStore;

import static java.util.Objects.requireNonNull;

/**
 * A store built with the DCB capability alone, presented as every stream interface it must refuse and the DCB
 * interface it must still serve.
 * <p>
 * A store implements these interfaces whether or not
 * {@link org.occurrent.eventstore.api.EventStoreCapability#STREAM} was enabled on it, so there is always an object to
 * call. {@link CapabilityGuardConformance} asserts that every method on the four stream views refuses, and appends
 * through {@code dcbEventStore} to show the store is otherwise alive rather than closed or broken.
 * <p>
 * Every view is handed over separately because none of these interfaces extends another, exactly as
 * {@link StoreWithoutPosition} does. An implementation where one object is all of them passes the same instance five
 * times.
 *
 * @param eventStore     reads a stream, writes to one, and answers whether a stream exists
 * @param filteredReader reads a stream through a {@link org.occurrent.eventstore.api.StreamReadFilter}
 * @param queries        queries, counts and tests existence across streams
 * @param operations     deletes streams and single events, deletes by filter, and updates an event
 * @param dcbEventStore  the DCB view, which this store must still serve
 */
@NullMarked
public record StoreWithoutStream(EventStore eventStore,
                                 ReadEventStreamWithFilter filteredReader,
                                 EventStoreQueries queries,
                                 EventStoreOperations operations,
                                 DcbEventStore dcbEventStore) {

    public StoreWithoutStream {
        requireNonNull(eventStore, "eventStore cannot be null");
        requireNonNull(filteredReader, "filteredReader cannot be null");
        requireNonNull(queries, "queries cannot be null");
        requireNonNull(operations, "operations cannot be null");
        requireNonNull(dcbEventStore, "dcbEventStore cannot be null");
    }
}
