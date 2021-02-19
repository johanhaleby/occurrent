/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.filter.Filter;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.SortBy.SortDirection.ASCENDING;

/**
 * Additional querying capabilities that may be supported by an {@link EventStore} implementation that is not typically part of a
 * "transactional" use case.
 */
public interface EventStoreQueries {

    /**
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified filter, skip, limit and sort by <code>sortBy</code>.
     */
    Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy);

    /**
     * Count specific events in the event store that matches the supplied {@code filter}.
     *
     * @return The number of events in the event store matching the {@code filter}.
     */
    long count(Filter filter);

    /**
     * Count all events in the event store
     *
     * @return The number of events in the event store
     */
    default long count() {
        return count(Filter.all());
    }

    /**
     * Check if any events exists that matches the given {@code filter}.
     *
     * @return <code>true</code> if any events exists that are matching the {@code filter}, <code>fase</code> otherwise.
     */
    boolean exists(Filter filter);

    /**
     * @return All cloud events matching the specified filter sorted by <code>sortBy</code>.
     */
    default Stream<CloudEvent> query(Filter filter, SortBy sortBy) {
        return query(filter, 0, Integer.MAX_VALUE, sortBy);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    default Stream<CloudEvent> query(Filter filter, int skip, int limit) {
        return query(filter, skip, limit, SortBy.natural(ASCENDING));
    }

    /**
     * @return All cloud events in insertion order
     */
    default Stream<CloudEvent> all(int skip, int limit, SortBy sortBy) {
        return query(Filter.all(), skip, limit, sortBy);
    }


    /**
     * @return All cloud events sorted by <code>sortBy</code>
     */
    default Stream<CloudEvent> all(SortBy sortBy) {
        return query(Filter.all(), sortBy);
    }


    /**
     * @return All cloud events in insertion order
     */
    default Stream<CloudEvent> all(int skip, int limit) {
        return query(Filter.all(), skip, limit);
    }

    /**
     * @return All cloud events in an unspecified order (most likely insertion order but this is not guaranteed and it is database/implementation specific)
     */
    default Stream<CloudEvent> all() {
        return all(0, Integer.MAX_VALUE);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    default Stream<CloudEvent> query(Filter filter) {
        requireNonNull(filter, "Filter cannot be null");
        return query(filter, 0, Integer.MAX_VALUE);
    }
}