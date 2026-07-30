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

package org.occurrent.dsl.query.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A wrapper around a reactive {@link EventStoreQueries} that maps the result of a query into your domain event type
 * using a {@link CloudEventConverter}. This is the reactive counterpart of the blocking {@code DomainEventQueries}.
 * <p>
 * A {@code Projection} from the projection DSL can be folded over these queries for a strongly consistent read on
 * demand, with {@code Projections.project(projection, queries)} from Java or the {@code queries.project(projection)}
 * extension from Kotlin.
 *
 * @param <T> The type of your event
 */
@NullMarked
public class DomainEventQueries<T> {

    private final EventStoreQueries eventStoreQueries;
    private final CloudEventConverter<T> cloudEventConverter;

    /**
     * Creates an instance of {@code DomainEventQueries}
     */
    public DomainEventQueries(EventStoreQueries eventStoreQueries, CloudEventConverter<T> cloudEventConverter) {
        Objects.requireNonNull(eventStoreQueries, EventStoreQueries.class.getSimpleName() + " cannot be null");
        Objects.requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null");
        this.eventStoreQueries = eventStoreQueries;
        this.cloudEventConverter = cloudEventConverter;
    }

    /**
     * The underlying {@link EventStoreQueries} this instance reads from. Useful for capabilities layered on top of
     * the event store, such as DCB queries when the store also implements the reactive {@code DcbEventStore}.
     */
    public EventStoreQueries eventStoreQueries() {
        return eventStoreQueries;
    }

    /**
     * The {@link CloudEventConverter} this instance uses to map between CloudEvents and domain events. Useful for
     * capabilities layered on top, such as resolving a domain event class to its CloudEvent type for a DCB criteria.
     */
    public CloudEventConverter<T> cloudEventConverter() {
        return cloudEventConverter;
    }

    /**
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return The first domain event matching the specified filter, or an empty {@link Mono} if none match.
     */
    public <E extends T> Mono<E> queryOne(Filter filter) {
        return this.<E>toDomainEvents(eventStoreQueries.query(filter, 0, 1)).next();
    }

    /**
     * Query for the first event of the given type.
     *
     * @return The domain event matching the specified type, or an empty {@link Mono} if none match.
     */
    public <E extends T> Mono<E> queryOne(Class<E> type) {
        return query(type, 0, 1).next();
    }

    /**
     * Query for the first event of the given type sorting by {@code sortBy}.
     *
     * @return The domain event matching the specified type, or an empty {@link Mono} if none match.
     */
    public <E extends T> Mono<E> queryOne(Class<E> type, SortBy sortBy) {
        return queryOne(type, 0, 1, sortBy);
    }

    /**
     * Query for the first event of the given type using {@code skip} and {@code limit}.
     *
     * @return The domain event matching the specified type, or an empty {@link Mono} if none match.
     */
    public <E extends T> Mono<E> queryOne(Class<E> type, int skip, int limit) {
        return queryOne(type, skip, limit, SortBy.unsorted());
    }

    /**
     * Query for the first event of the given using {@code skip}, {@code limit} and {@code sortBy}.
     *
     * @return The domain event matching the specified type, or an empty {@link Mono} if none match.
     */
    public <E extends T> Mono<E> queryOne(Class<E> type, int skip, int limit, SortBy sortBy) {
        Objects.requireNonNull(type, "type cannot be null");
        return this.<E>toDomainEvents(eventStoreQueries.query(Filter.type(cloudEventConverter.getCloudEventType(type)), skip, limit, sortBy)).next();
    }

    /**
     * Query by event type (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type.
     */
    public <E extends T> Flux<E> query(Class<E> type) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(cloudEventConverter.getCloudEventType(type))));
    }

    /**
     * Query by event type (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also include {@code skip} and {@code limit}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip and limit.
     */
    public <E extends T> Flux<E> query(Class<E> type, int skip, int limit) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(cloudEventConverter.getCloudEventType(type)), skip, limit));
    }

    /**
     * Query by event type (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also include {@code skip}, {@code limit} and {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit and sort by <code>sortBy</code>.
     */
    public <E extends T> Flux<E> query(Class<E> type, int skip, int limit, SortBy sortBy) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(cloudEventConverter.getCloudEventType(type)), skip, limit, sortBy));
    }

    /**
     * Query by event type (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also including sorting .
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, sorted by <code>sortBy</code>.
     */
    public <E extends T> Flux<E> query(Class<E> type, SortBy sortBy) {
        return this.toDomainEvents(eventStoreQueries.query(Filter.type(cloudEventConverter.getCloudEventType(type)), sortBy));
    }

    /**
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified filter, skip, limit and sort by <code>sortBy</code>.
     */
    public <E extends T> Flux<E> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return toDomainEvents(eventStoreQueries.query(filter, skip, limit, sortBy));
    }

    /**
     * Query by event types (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also include {@code skip}, {@code limit} and {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit and sort by <code>sortBy</code>.
     */
    public Flux<T> query(Collection<Class<? extends T>> types, int skip, int limit, SortBy sortBy) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Flux.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse, skip, limit, sortBy));
    }

    /**
     * Query by event types (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also include {@code skip}, {@code limit}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    public Flux<T> query(Collection<Class<? extends T>> types, int skip, int limit) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Flux.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse, skip, limit));
    }

    /**
     * Query by event types (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also include {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    public Flux<T> query(Collection<Class<? extends T>> types, SortBy sortBy) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Flux.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse, sortBy));
    }

    /**
     * Query by event types (will use the supplied {@link CloudEventConverter} to get the cloud event type from the class..
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    public Flux<T> query(Collection<Class<? extends T>> types) {
        final Filter filterToUse = createFilterFrom(types);
        if (filterToUse == null) {
            return Flux.empty();
        }

        return this.toDomainEvents(eventStoreQueries.query(filterToUse));
    }

    /**
     * Query by event types (will use the supplied {@link CloudEventConverter}) to get the cloud event type from the class, also include {@code sortBy}.
     * <p>
     * Note that it's recommended to create an index the fields you're sorting on in order to make them efficient.
     *
     * @return All cloud events matching the specified type, skip, limit.
     */
    @SafeVarargs
    public final Flux<T> query(Class<? extends T> type, @Nullable Class<? extends T>... types) {
        final List<Class<? extends T>> list = new ArrayList<>();
        list.add(type);
        if (types != null && types.length > 0) {
            list.addAll(Arrays.asList(types));
        }
        return query(list);
    }

    /**
     * Count specific events in the event store that matches the supplied {@code filter}.
     *
     * @return The number of events in the event store matching the {@code filter}.
     */
    public Mono<Long> count(Filter filter) {
        return eventStoreQueries.count(filter);
    }

    /**
     * Count all events in the event store
     *
     * @return The number of events in the event store
     */
    public Mono<Long> count() {
        return count(Filter.all());
    }

    /**
     * Check if any events exists that matches the given {@code filter}.
     *
     * @return A {@link Mono} of <code>true</code> if any events exists that are matching the {@code filter}, <code>false</code> otherwise.
     */
    public Mono<Boolean> exists(Filter filter) {
        return eventStoreQueries.exists(filter);
    }

    /**
     * @return All cloud events matching the specified filter sorted by <code>sortBy</code>.
     */
    public <E extends T> Flux<E> query(Filter filter, SortBy sortBy) {
        return toDomainEvents(eventStoreQueries.query(filter, sortBy));
    }

    /**
     * @return All cloud events matching the specified filter
     */
    public <E extends T> Flux<E> query(Filter filter, int skip, int limit) {
        return toDomainEvents(eventStoreQueries.query(filter, skip, limit));
    }

    /**
     * @return All cloud events in insertion order
     */
    public Flux<T> all(int skip, int limit, SortBy sortBy) {
        return eventStoreQueries.all(skip, limit, sortBy).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events sorted by <code>sortBy</code>
     */
    public Flux<T> all(SortBy sortBy) {
        return eventStoreQueries.all(sortBy).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events in insertion order
     */
    public Flux<T> all(int skip, int limit) {
        return eventStoreQueries.all(skip, limit).map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events in an unspecified order (most likely insertion order but this is not guaranteed and it is database/implementation specific)
     */
    public Flux<T> all() {
        return eventStoreQueries.all().map(cloudEventConverter::toDomainEvent);
    }

    /**
     * @return All cloud events matching the specified filter
     */
    public <E extends T> Flux<E> query(Filter filter) {
        return toDomainEvents(eventStoreQueries.query(filter));
    }

    // ------------------------------------------------------------------------------------------------------
    // Position-ordered reads
    // ------------------------------------------------------------------------------------------------------

    /**
     * Reads domain events strictly after the global sequence {@code position}, in ascending position order.
     * Equivalent to {@code readInPositionOrder(Filter.all(), PositionRange.afterPosition(position))}.
     *
     * @return a {@link Flux} that emits an {@link UnsupportedOperationException} if the underlying event store does
     * not write a position.
     * @see PositionOrderedReader#readInPositionOrder(Filter, PositionRange)
     */
    public Flux<T> afterPosition(long position) {
        return readInPositionOrder(Filter.all(), PositionRange.afterPosition(position));
    }

    /**
     * Reads domain events matching {@code filter} within {@code range}, in ascending position order.
     *
     * @return a {@link Flux} that emits an {@link UnsupportedOperationException} if the underlying event store does
     * not write a position.
     * @see PositionOrderedReader#readInPositionOrder(Filter, PositionRange)
     */
    public Flux<T> readInPositionOrder(Filter filter, PositionRange range) {
        Objects.requireNonNull(filter, "Filter cannot be null");
        Objects.requireNonNull(range, "Range cannot be null");
        return requirePositionOrderedReader().flatMapMany(reader -> toDomainEvents(reader.readInPositionOrder(filter, range)));
    }

    /**
     * The store's current position high-watermark, i.e. the position of the most recently positioned event.
     * Emits {@code 0} when no positioned event has been written yet.
     *
     * @return a {@link Mono} that emits an {@link UnsupportedOperationException} if the underlying event store does
     * not write a position.
     * @see PositionOrderedReader#currentPosition()
     */
    public Mono<Long> currentPosition() {
        return requirePositionOrderedReader().flatMap(PositionOrderedReader::currentPosition);
    }

    private Mono<PositionOrderedReader> requirePositionOrderedReader() {
        if (!(eventStoreQueries instanceof PositionOrderedReader positionOrderedReader) || !positionOrderedReader.writesPosition()) {
            return Mono.error(new UnsupportedOperationException("This event store does not write a position. Position-ordered reads require the underlying event store to implement "
                    + PositionOrderedReader.class.getSimpleName() + " with writesPosition() true, but was " + eventStoreQueries.getClass().getName()));
        }
        return Mono.just(positionOrderedReader);
    }

    /**
     * Convert a {@link Flux} of {@link CloudEvent}s into domain events using the configured {@link CloudEventConverter}.
     * Useful when you already have CloudEvents and want them as domain events.
     */
    @SuppressWarnings("unchecked")
    public <E extends T> Flux<E> toDomainEvents(Flux<CloudEvent> flux) {
        Objects.requireNonNull(flux, "Flux cannot be null");
        return flux.map(cloudEventConverter::toDomainEvent).map(t -> (E) t);
    }

    private @Nullable Filter createFilterFrom(Collection<Class<? extends T>> types) {
        return (types == null ? Stream.<Class<? extends T>>empty() : types.stream())
                .map(type -> Filter.type(cloudEventConverter.getCloudEventType(type)))
                .reduce(Filter::or)
                .orElse(null);
    }
}
