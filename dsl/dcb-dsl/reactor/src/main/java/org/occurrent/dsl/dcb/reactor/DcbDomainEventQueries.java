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

import org.occurrent.dsl.dcb.DcbDomainEventStream;

import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

/**
 * Queries a reactive DCB event store and converts the matched CloudEvents into your domain event type.
 *
 * <p>This is the reactive counterpart to the blocking {@code DcbDomainEventQueries}. It is built directly on a
 * reactive {@link DcbEventStore} and a {@link CloudEventConverter}, and exposes only the DCB query methods. The
 * reactive side has no general {@code DomainEventQueries} to wrap, so the stream-oriented query delegation of the
 * blocking version is intentionally absent.</p>
 *
 * @param <E> the domain event type
 */
@NullMarked
public class DcbDomainEventQueries<E> {

    private final DcbEventStore dcbEventStore;
    private final CloudEventConverter<E> cloudEventConverter;

    public DcbDomainEventQueries(DcbEventStore dcbEventStore, CloudEventConverter<E> cloudEventConverter) {
        this.dcbEventStore = requireNonNull(dcbEventStore, DcbEventStore.class.getSimpleName() + " cannot be null");
        this.cloudEventConverter = requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null");
    }

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
        return dcbEventStore.read(query, options).flatMapMany(eventStream -> Flux.fromStream(cloudEventConverter.toDomainEvents(eventStream.stream())));
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
        return dcbEventStore.read(query, options).map(eventStream ->
                new DcbDomainEventStream<>(cloudEventConverter.toDomainEvents(eventStream.stream()).toList(), eventStream.lastSequencePosition(), eventStream.consistencyToken()));
    }
}
