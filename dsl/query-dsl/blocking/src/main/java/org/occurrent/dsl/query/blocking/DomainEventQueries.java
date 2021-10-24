/*
 *
 *  Copyright 2021 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.query.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A wrapper around {@link EventStoreQueries} that maps the result of a query into your domain event type
 * using a {@link CloudEventConverter}.
 *
 * @param <T> The type of your event
 */
public class DomainEventQueries<T> {

    private final EventStoreQueries eventStoreQueries;
    private final CloudEventConverter<T> cloudEventConverter;

    public DomainEventQueries(EventStoreQueries eventStoreQueries, CloudEventConverter<T> cloudEventConverter) {
        Objects.requireNonNull(eventStoreQueries, EventStoreQueries.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null");
        this.eventStoreQueries = eventStoreQueries;
        this.cloudEventConverter = cloudEventConverter;
    }

    /**
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified filter, skip, limit and sort by <code>sortBy</code>.
     */
    public <E extends T> Optional<E> queryOne(Filter filter) {
        return this.<E>toDomainEvents(eventStoreQueries.query(filter)).findFirst();
    }

    /**
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified filter, skip, limit and sort by <code>sortBy</code>.
     */
    public <E extends T> Stream<E> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return toDomainEvents(eventStoreQueries.query(filter, skip, limit, sortBy));
    }

    /**
     * Count specific events in the event store that matches the supplied {@code filter}.
     *
     * @return The number of events in the event store matching the {@code filter}.
     */
    public long count(Filter filter) {
        return eventStoreQueries.count(filter);
    }

    /**
     * Count all events in the event store
     *
     * @return The number of events in the event store
     */
    public long count() {
        return count(Filter.all());
    }

    /**
     * Check if any events exists that matches the given {@code filter}.
     *
     * @return <code>true</code> if any events exists that are matching the {@code filter}, <code>false</code> otherwise.
     */
    public boolean exists(Filter filter) {
        return eventStoreQueries.exists(filter);
    }

    /**
     * @return All cloud events matching the specified filter sorted by <code>sortBy</code>.
     */
    public <E extends T> Stream<E> query(Filter filter, SortBy sortBy) {
        return toDomainEvents(eventStoreQueries.query(filter, sortBy));
    }

    /**
     * @return All cloud events matching the specified filter
     */
    public <E extends T> Stream<E> query(Filter filter, int skip, int limit) {
        return toDomainEvents(eventStoreQueries.query(filter, skip, limit));
    }

    /**
     * @return All cloud events in insertion order
     */
    public Stream<T> all(int skip, int limit, SortBy sortBy) {
        return eventStoreQueries.all(skip, limit, sortBy).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events sorted by <code>sortBy</code>
     */
    public Stream<T> all(SortBy sortBy) {
        return eventStoreQueries.all(sortBy).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events in insertion order
     */
    public Stream<T> all(int skip, int limit) {
        return eventStoreQueries.all(skip, limit).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events in an unspecified order (most likely insertion order but this is not guaranteed and it is database/implementation specific)
     */
    public Stream<T> all() {
        return eventStoreQueries.all().map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    public <E extends T> Stream<E> query(Filter filter) {
        return toDomainEvents(eventStoreQueries.query(filter));
    }

    @SuppressWarnings("unchecked")
    private <E extends T> Stream<E> toDomainEvents(Stream<CloudEvent> stream) {
        return stream.map(cloudEventConverter::toDomainEvent).map(t -> (E) t);
    }
}
