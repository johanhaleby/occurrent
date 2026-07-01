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

package org.occurrent.dsl.dcb.reactor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.dcb.DcbDomainEventStream;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * Queries a reactive DCB-capable event store and converts the matched CloudEvents into your domain event type.
 *
 * <p>This wraps a {@link DomainEventQueries} so a DCB application can use a single object for both DCB queries
 * (the {@link #query(DcbQuery)} family) and the regular stream-oriented queries (which are delegated to the
 * wrapped {@link DomainEventQueries} unchanged). This is the reactive counterpart to the blocking
 * {@code DcbDomainEventQueries}. The wrapped instance must be backed by an event store that also implements the
 * reactive {@link DcbEventStore} (for example the Spring MongoDB event store with the DCB capability enabled);
 * otherwise the constructor throws.</p>
 *
 * @param <E> the domain event type
 */
@NullMarked
public class DcbDomainEventQueries<E> {

    private final DomainEventQueries<E> domainEventQueries;
    private final DcbEventStore dcbEventStore;

    /**
     * Wraps a {@link DomainEventQueries} backed by a reactive DCB-capable event store.
     *
     * @throws IllegalArgumentException if the wrapped {@link DomainEventQueries} is not backed by a reactive {@link DcbEventStore}
     */
    public DcbDomainEventQueries(DomainEventQueries<E> domainEventQueries) {
        this.domainEventQueries = requireNonNull(domainEventQueries, DomainEventQueries.class.getSimpleName() + " cannot be null");
        this.dcbEventStore = requireDcbEventStore(domainEventQueries);
    }

    // ------------------------------------------------------------------------------------------------------
    // DCB queries
    // ------------------------------------------------------------------------------------------------------

    /**
     * Queries matching DCB events from the beginning of the DCB sequence.
     */
    public Flux<E> query(DcbQuery query) {
        return query(query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options, converting the matched CloudEvents to domain events.
     */
    public Flux<E> query(DcbQuery query, DcbReadOptions options) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return dcbEventStore.read(query, options).flatMapMany(eventStream -> domainEventQueries.toDomainEvents(Flux.fromStream(eventStream.stream())));
    }

    /**
     * Queries matching DCB events and returns both the domain events and the observed DCB sequence position.
     */
    public Mono<DcbDomainEventStream<E>> queryWithPosition(DcbQuery query) {
        return queryWithPosition(query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options and returns the domain events, the observed DCB
     * sequence position, and the consistency token for a later conditional append.
     */
    public Mono<DcbDomainEventStream<E>> queryWithPosition(DcbQuery query, DcbReadOptions options) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return dcbEventStore.read(query, options).flatMap(eventStream ->
                domainEventQueries.<E>toDomainEvents(Flux.fromStream(eventStream.stream())).collectList()
                        .map(events -> new DcbDomainEventStream<>(events, eventStream.lastSequencePosition(), eventStream.consistencyToken())));
    }

    // ------------------------------------------------------------------------------------------------------
    // Stream queries delegated to the wrapped DomainEventQueries
    // ------------------------------------------------------------------------------------------------------

    public <E1 extends E> Mono<E1> queryOne(Filter filter) {
        return domainEventQueries.queryOne(filter);
    }

    public <E1 extends E> Mono<E1> queryOne(Class<E1> type) {
        return domainEventQueries.queryOne(type);
    }

    public <E1 extends E> Mono<E1> queryOne(Class<E1> type, SortBy sortBy) {
        return domainEventQueries.queryOne(type, sortBy);
    }

    public <E1 extends E> Mono<E1> queryOne(Class<E1> type, int skip, int limit) {
        return domainEventQueries.queryOne(type, skip, limit);
    }

    public <E1 extends E> Mono<E1> queryOne(Class<E1> type, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.queryOne(type, skip, limit, sortBy);
    }

    public <E1 extends E> Flux<E1> query(Class<E1> type) {
        return domainEventQueries.query(type);
    }

    public <E1 extends E> Flux<E1> query(Class<E1> type, int skip, int limit) {
        return domainEventQueries.query(type, skip, limit);
    }

    public <E1 extends E> Flux<E1> query(Class<E1> type, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.query(type, skip, limit, sortBy);
    }

    public <E1 extends E> Flux<E1> query(Class<E1> type, SortBy sortBy) {
        return domainEventQueries.query(type, sortBy);
    }

    public <E1 extends E> Flux<E1> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.query(filter, skip, limit, sortBy);
    }

    public Flux<E> query(Collection<Class<? extends E>> types, int skip, int limit, SortBy sortBy) {
        return domainEventQueries.query(types, skip, limit, sortBy);
    }

    public Flux<E> query(Collection<Class<? extends E>> types, int skip, int limit) {
        return domainEventQueries.query(types, skip, limit);
    }

    public Flux<E> query(Collection<Class<? extends E>> types, SortBy sortBy) {
        return domainEventQueries.query(types, sortBy);
    }

    public Flux<E> query(Collection<Class<? extends E>> types) {
        return domainEventQueries.query(types);
    }

    @SafeVarargs
    public final Flux<E> query(Class<? extends E> type, @Nullable Class<? extends E>... types) {
        return domainEventQueries.query(type, types);
    }

    public Mono<Long> count(Filter filter) {
        return domainEventQueries.count(filter);
    }

    public Mono<Long> count() {
        return domainEventQueries.count();
    }

    public Mono<Boolean> exists(Filter filter) {
        return domainEventQueries.exists(filter);
    }

    public <E1 extends E> Flux<E1> query(Filter filter, SortBy sortBy) {
        return domainEventQueries.query(filter, sortBy);
    }

    public <E1 extends E> Flux<E1> query(Filter filter, int skip, int limit) {
        return domainEventQueries.query(filter, skip, limit);
    }

    public Flux<E> all(int skip, int limit, SortBy sortBy) {
        return domainEventQueries.all(skip, limit, sortBy);
    }

    public Flux<E> all(SortBy sortBy) {
        return domainEventQueries.all(sortBy);
    }

    public Flux<E> all(int skip, int limit) {
        return domainEventQueries.all(skip, limit);
    }

    public Flux<E> all() {
        return domainEventQueries.all();
    }

    public <E1 extends E> Flux<E1> query(Filter filter) {
        return domainEventQueries.query(filter);
    }

    private static DcbEventStore requireDcbEventStore(DomainEventQueries<?> domainEventQueries) {
        EventStoreQueries eventStoreQueries = domainEventQueries.eventStoreQueries();
        if (!(eventStoreQueries instanceof DcbEventStore dcbEventStore)) {
            throw new IllegalArgumentException("DCB queries require the " + DomainEventQueries.class.getSimpleName() + " to be backed by a reactive "
                    + DcbEventStore.class.getSimpleName() + ", but was " + eventStoreQueries.getClass().getName());
        }
        return dcbEventStore;
    }
}
