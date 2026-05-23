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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Static helpers for converting DCB reads to domain events.
 * <p>
 * Use these when you want DCB query semantics and a {@link CloudEventConverter}
 * should map the matching CloudEvents to your domain event type.
 */
@NullMarked
public final class DcbDomainEventQueries {

    private DcbDomainEventQueries() {
    }

    /**
     * Queries matching DCB events from the beginning of the DCB sequence.
     */
    public static <E> Stream<E> query(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, DcbQuery query) {
        return queryWithPosition(eventStore, cloudEventConverter, query).stream();
    }

    /**
     * Queries matching DCB events using the supplied read options.
     */
    public static <E> Stream<E> query(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, DcbQuery query, DcbReadOptions options) {
        return queryWithPosition(eventStore, cloudEventConverter, query, options).stream();
    }

    /**
     * Queries matching DCB events and returns both domain events and the observed DCB sequence position.
     */
    public static <E> DcbDomainEventStream<E> queryWithPosition(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, DcbQuery query) {
        return queryWithPosition(eventStore, cloudEventConverter, query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options and returns both domain events and the observed DCB sequence position.
     */
    public static <E> DcbDomainEventStream<E> queryWithPosition(DcbEventStore eventStore, CloudEventConverter<E> cloudEventConverter, DcbQuery query, DcbReadOptions options) {
        requireNonNull(eventStore, DcbEventStore.class.getSimpleName() + " cannot be null");
        requireNonNull(cloudEventConverter, CloudEventConverter.class.getSimpleName() + " cannot be null");
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        DcbEventStream eventStream = eventStore.read(query, options);
        return new DcbDomainEventStream<>(cloudEventConverter.toDomainEvents(eventStream.stream()).toList(), eventStream.lastSequencePosition());
    }
}
