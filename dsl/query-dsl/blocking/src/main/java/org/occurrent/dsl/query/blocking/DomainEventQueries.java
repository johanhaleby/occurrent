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
import org.jetbrains.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.typemapper.ReflectionTypeMapper;
import org.occurrent.application.typemapper.TypeMapper;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;

import java.util.*;
import java.util.stream.Collectors;
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
    private final TypeMapper<T> typeMapper;

    public DomainEventQueries(EventStoreQueries eventStoreQueries, CloudEventConverter<T> cloudEventConverter) {
        this(eventStoreQueries, cloudEventConverter, ReflectionTypeMapper.qualified());
    }

    public DomainEventQueries(EventStoreQueries eventStoreQueries, CloudEventConverter<T> cloudEventConverter, TypeMapper<T> typeMapper) {
        this.typeMapper = typeMapper;
        Objects.requireNonNull(eventStoreQueries, EventStoreQueries.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(typeMapper, TypeMapper.class.getSimpleName() + " cannot be null");
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
     * Query for the first event of the given type.
     *
     * @return All cloud events matching the specified type.
     */
    public <E extends T> Optional<E> queryOne(Class<E> type) {
        return query(type).findFirst();
    }

    /**
     * Query by event type (will use the supplied {@link TypeMapper} to get the cloud event type from the class.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type.
     */
    public <E extends T> Stream<E> query(Class<E> type) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(typeMapper.getCloudEventType(type))));
    }

    /**
     * Query by event type (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also include {@code skip} and {@code limit}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip and limit.
     */
    public <E extends T> Stream<E> query(Class<E> type, int skip, int limit) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(typeMapper.getCloudEventType(type)), skip, limit));
    }

    /**
     * Query by event type (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also include {@code skip}, {@code limit} and {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit and sort by <code>sortBy</code>.
     */
    public <E extends T> Stream<E> query(Class<E> type, int skip, int limit, SortBy sortBy) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(typeMapper.getCloudEventType(type)), skip, limit, sortBy));
    }

    /**
     * Query by event type (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also including sorting .
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, sorted by <code>sortBy</code>.
     */
    public <E extends T> Stream<E> query(Class<E> type, SortBy sortBy) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(typeMapper.getCloudEventType(type)), sortBy));
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
     * Query by event types (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also include {@code skip}, {@code limit} and {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit and sort by <code>sortBy</code>.
     */
    public Stream<T> query(Collection<Class<? extends T>> types, int skip, int limit, SortBy sortBy) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Stream.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse, skip, limit, sortBy));
    }

    /**
     * Query by event types (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also include {@code skip}, {@code limit}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    public Stream<T> query(Collection<Class<? extends T>> types, int skip, int limit) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Stream.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse, skip, limit));
    }

    /**
     * Query by event types (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also include {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    public Stream<T> query(Collection<Class<? extends T>> types, SortBy sortBy) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Stream.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse, sortBy));
    }

    /**
     * Query by event types (will use the supplied {@link TypeMapper} to get the cloud event type from the class..
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    public Stream<T> query(Collection<Class<? extends T>> types) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Stream.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse));
    }

    /**
     * Query by event types (will use the supplied {@link TypeMapper} to get the cloud event type from the class, also include {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    @SuppressWarnings("unchecked")
    @SafeVarargs
    public final Stream<T> query(Class<? extends T> type, Class<? extends T>... types) {
        final List<T> list = new ArrayList<>();
        list.add((T) type);
        if (types != null && types.length > 0) {
            list.addAll(Arrays.stream(types).map(t -> (T) t).collect(Collectors.toList()));
        }
        return query((Collection<Class<? extends T>>) list);
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

    @Nullable
    private Filter createFilterFrom(Collection<Class<? extends T>> types) {
        return (types == null ? Stream.<Class<? extends T>>empty() : types.stream())
                .map(type -> Filter.type(typeMapper.getCloudEventType(type)))
                .reduce(Filter::or)
                .orElse(null);
    }
}
