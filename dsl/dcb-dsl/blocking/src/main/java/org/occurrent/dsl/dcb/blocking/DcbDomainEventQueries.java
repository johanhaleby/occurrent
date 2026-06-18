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

package org.occurrent.dsl.dcb.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.filter.Filter;

import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Helpers for querying DCB event stores to domain events.
 */
@NullMarked
public class DcbDomainEventQueries<E> {

    private final DomainEventQueries<E> domainEventQueries;
    private final DcbEventStore dcbEventStore;

    public DcbDomainEventQueries(DomainEventQueries<E> domainEventQueries, DcbEventStore dcbEventStore) {
        this.domainEventQueries = domainEventQueries;
        this.dcbEventStore = dcbEventStore;
    }

    /**
     * Queries matching DCB events from the beginning of the DCB sequence.
     */
    public Stream<E> query(DcbQuery query) {
        return queryWithPosition(query).stream();
    }

    /**
     * Queries matching DCB events using the supplied read options.
     */
    public Stream<E> query(DomainEventQueries<E> domainEventQueries, DcbQuery query, DcbReadOptions options) {
        return queryWithPosition(query, options).stream();
    }

    /**
     * Queries matching DCB events and returns both domain events and the observed DCB sequence position.
     */
    public DcbDomainEventStream<E> queryWithPosition(DcbQuery query) {
        return queryWithPosition(query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options and returns both domain events and the observed DCB sequence position.
     */
    public DcbDomainEventStream<E> queryWithPosition(DcbQuery query, DcbReadOptions options) {
        requireNonNull(domainEventQueries, DomainEventQueries.class.getSimpleName() + " cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        DcbEventStream eventStream = dcbEventStore.read(query, options);
        return new DcbDomainEventStream<>(domainEventQueries, eventStream.lastSequencePosition());
    }


    // Delegate all other query methods to the wrapped DomainEventQueries

    /**
     * @see DomainEventQueries#queryOne(Filter)
     */
    @Nullable
    public <E1 extends E> E1 queryOne(Filter filter) {
        return domainEventQueries.queryOne(filter);
    }

    /**
     * @see DomainEventQueries#queryOne(Class)
     */
    @Nullable
    public <E1 extends E> E1 queryOne(Class<E1> type) {
        return domainEventQueries.queryOne(type);
    }

    /**
     * @see DomainEventQueries#queryOne(Class, SortBy)
     */
    @Nullable
    public <E1 extends E> E1 queryOne(Class<E1> type, SortBy sortBy) {
        return domainEventQueries.queryOne(type, sortBy);
    }

    /**
     * @see DomainEventQueries#queryOne(Class, int, int)
     */
    @Nullable
    public <E1 extends E> E1 queryOne(Class<E1> type, int skip, int limit) {
        return domainEventQueries.queryOne(type, skip, limit);
    }

    /**
     * @see DomainEventQueries#queryOne(Class, int, int, SortBy)
     */
    @Nullable
    public <E1 extends E> E1 queryOne(Class<E1> type, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.queryOne(type, skip, limit, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Class)
     */
    public <E1 extends E> Stream<E1> query(Class<E1> type) {
        return domainEventQueries.query(type);
    }

    /**
     * @see DomainEventQueries#query(Class, int, int)
     */
    public <E1 extends E> Stream<E1> query(Class<E1> type, int skip, int limit) {
        return domainEventQueries.query(type, skip, limit);
    }

    /**
     * @see DomainEventQueries#query(Class, int, int, SortBy)
     */
    public <E1 extends E> Stream<E1> query(Class<E1> type, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.query(type, skip, limit, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Class, SortBy)
     */
    public <E1 extends E> Stream<E1> query(Class<E1> type, SortBy sortBy) {
        return domainEventQueries.query(type, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Filter, int, int, SortBy)
     */
    public <E1 extends E> Stream<E1> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.query(filter, skip, limit, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Collection, int, int, SortBy)
     */
    public Stream<E> query(Collection<Class<? extends E>> types, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.query(types, skip, limit, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Collection, int, int)
     */
    public Stream<E> query(Collection<Class<? extends E>> types, int skip, int limit) {
        return domainEventQueries.query(types, skip, limit);
    }

    /**
     * @see DomainEventQueries#query(Collection, SortBy)
     */
    public Stream<E> query(Collection<Class<? extends E>> types, SortBy sortBy) {
        return domainEventQueries.query(types, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Collection)
     */
    public Stream<E> query(Collection<Class<? extends E>> types) {
        return domainEventQueries.query(types);
    }

    /**
     * @see DomainEventQueries#query(Class, Class[])
     */
    public Stream<E> query(Class<? extends E> type, @Nullable Class<? extends E>... types) {
        return domainEventQueries.query(type, types);
    }

    /**
     * @see DomainEventQueries#count(Filter)
     */
    public long count(Filter filter) {
        return domainEventQueries.count(filter);
    }

    /**
     * @see DomainEventQueries#count()
     */
    public long count() {
        return domainEventQueries.count();
    }

    /**
     * @see DomainEventQueries#exists(Filter)
     */
    public boolean exists(Filter filter) {
        return domainEventQueries.exists(filter);
    }

    /**
     * @see DomainEventQueries#query(Filter, SortBy)
     */
    public <E1 extends E> Stream<E1> query(Filter filter, SortBy sortBy) {
        return domainEventQueries.query(filter, sortBy);
    }

    /**
     * @see DomainEventQueries#query(Filter, int, int)
     */
    public <E1 extends E> Stream<E1> query(Filter filter, int skip, int limit) {
        return domainEventQueries.query(filter, skip, limit);
    }

    /**
     * @see DomainEventQueries#all(int, int, SortBy)
     */
    public Stream<E> all(int skip, int limit, SortBy sortBy) {
        return domainEventQueries.all(skip, limit, sortBy);
    }

    /**
     * @see DomainEventQueries#all(SortBy)
     */
    public Stream<E> all(SortBy sortBy) {
        return domainEventQueries.all(sortBy);
    }

    /**
     * @see DomainEventQueries#all(int, int)
     */
    public Stream<E> all(int skip, int limit) {
        return domainEventQueries.all(skip, limit);
    }

    /**
     * @see DomainEventQueries#all()
     */
    public Stream<E> all() {
        return domainEventQueries.all();
    }

    /**
     * @see DomainEventQueries#query(Filter)
     */
    public <E1 extends E> Stream<E1> query(Filter filter) {
        return domainEventQueries.query(filter);
    }
}
