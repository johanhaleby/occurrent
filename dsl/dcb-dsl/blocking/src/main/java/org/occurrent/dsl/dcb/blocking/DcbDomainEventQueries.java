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
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Helpers for querying a DCB-capable event store through a {@link DomainEventQueries}, converting the
 * matched CloudEvents into your domain event type.
 *
 * <p>The supplied {@link DomainEventQueries} must be backed by an event store that also implements
 * {@link DcbEventStore} (for example the in-memory event store, or the Spring MongoDB event store with the
 * DCB capability enabled). Stream-based usage of {@link DomainEventQueries} is unaffected.</p>
 */
@NullMarked
public final class DcbDomainEventQueries {

    private DcbDomainEventQueries() {
    }

    /**
     * Queries matching DCB events from the beginning of the DCB sequence.
     */
    public static <E> Stream<E> query(DomainEventQueries<E> domainEventQueries, DcbQuery query) {
        return queryWithPosition(domainEventQueries, query).stream();
    }

    /**
     * Queries matching DCB events using the supplied read options.
     */
    public static <E> Stream<E> query(DomainEventQueries<E> domainEventQueries, DcbQuery query, DcbReadOptions options) {
        return queryWithPosition(domainEventQueries, query, options).stream();
    }

    /**
     * Queries matching DCB events and returns both the domain events and the observed DCB sequence position.
     */
    public static <E> DcbDomainEventStream<E> queryWithPosition(DomainEventQueries<E> domainEventQueries, DcbQuery query) {
        return queryWithPosition(domainEventQueries, query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options and returns both the domain events and the
     * observed DCB sequence position.
     */
    public static <E> DcbDomainEventStream<E> queryWithPosition(DomainEventQueries<E> domainEventQueries, DcbQuery query, DcbReadOptions options) {
        requireNonNull(domainEventQueries, DomainEventQueries.class.getSimpleName() + " cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        DcbEventStore dcbEventStore = requireDcbEventStore(domainEventQueries);
        DcbEventStream eventStream = dcbEventStore.read(query, options);
        List<E> events = domainEventQueries.<E>toDomainEvents(eventStream.stream()).toList();
        return new DcbDomainEventStream<>(events, eventStream.lastSequencePosition());
    }

    private static DcbEventStore requireDcbEventStore(DomainEventQueries<?> domainEventQueries) {
        EventStoreQueries eventStoreQueries = domainEventQueries.eventStoreQueries();
        if (!(eventStoreQueries instanceof DcbEventStore dcbEventStore)) {
            throw new IllegalArgumentException("DCB queries require the " + DomainEventQueries.class.getSimpleName() + " to be backed by a "
                    + DcbEventStore.class.getSimpleName() + ", but was " + (eventStoreQueries == null ? "null" : eventStoreQueries.getClass().getName()));
        }
        return dcbEventStore;
    }
}
