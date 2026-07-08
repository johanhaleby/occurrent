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
import org.occurrent.dsl.dcb.DcbCriteriaBuilder;
import org.occurrent.dsl.dcb.DcbDomainEventStream;
import org.occurrent.dsl.query.reactor.DomainEventQueries;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Queries a reactive DCB-capable event store and converts the matched CloudEvents into your domain event type.
 *
 * <p>This wraps a {@link DomainEventQueries} so a DCB application can use a single object for both DCB queries
 * (the {@link #query(DcbCriteria)} family) and the regular stream-oriented queries, reached through
 * {@link #domainEventQueries()}. This is the reactive counterpart to the blocking {@code DcbDomainEventQueries}. The
 * wrapped instance must be backed by an event store that also implements the reactive {@link DcbEventStore} (for
 * example the Spring MongoDB event store with the DCB capability enabled); otherwise the constructor throws.</p>
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

    /**
     * The wrapped {@link DomainEventQueries}, for the regular stream-oriented queries ({@code query}, {@code queryOne},
     * {@code count}, {@code exists}, {@code all}, ...) that this type does not itself add DCB semantics to.
     */
    public DomainEventQueries<E> domainEventQueries() {
        return domainEventQueries;
    }

    /**
     * A {@link DcbCriteriaBuilder} bound to this instance's {@link org.occurrent.application.converter.CloudEventConverter},
     * so criteria can be built from domain event classes (mapped to their CloudEvent type strings) rather than raw type strings.
     */
    public DcbCriteriaBuilder<E> criteria() {
        return new DcbCriteriaBuilder<>(domainEventQueries.cloudEventConverter());
    }

    // ------------------------------------------------------------------------------------------------------
    // DCB queries
    // ------------------------------------------------------------------------------------------------------

    /**
     * Queries matching DCB events from the beginning of the DCB sequence.
     */
    public Flux<E> query(DcbCriteria query) {
        return query(query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options, converting the matched CloudEvents to domain events.
     */
    public Flux<E> query(DcbCriteria query, DcbReadOptions options) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return dcbEventStore.read(query, options).flatMapMany(eventStream -> domainEventQueries.toDomainEvents(Flux.fromStream(eventStream.stream())));
    }

    /**
     * Queries matching DCB events and returns both the domain events and the observed DCB sequence position.
     */
    public Mono<DcbDomainEventStream<E>> queryWithPosition(DcbCriteria query) {
        return queryWithPosition(query, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options and returns the domain events, the observed DCB
     * sequence position, and the consistency token for a later conditional append.
     */
    public Mono<DcbDomainEventStream<E>> queryWithPosition(DcbCriteria query, DcbReadOptions options) {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return dcbEventStore.read(query, options).flatMap(eventStream ->
                domainEventQueries.<E>toDomainEvents(Flux.fromStream(eventStream.stream())).collectList()
                        .map(events -> new DcbDomainEventStream<>(events, eventStream.lastSequencePosition(), eventStream.consistencyToken())));
    }

    // ------------------------------------------------------------------------------------------------------
    // Convenience queries (one-liners over query(criteria), read from the beginning of the DCB sequence).
    // For read options or mixed criteria, use criteria() together with query(criteria, options).
    // ------------------------------------------------------------------------------------------------------

    /**
     * Queries DCB events of the given type, mapped to its CloudEvent type string through the converter.
     */
    public <SUB extends E> Flux<SUB> query(Class<SUB> type) {
        requireNonNull(type, "Type cannot be null");
        return query(criteria().type(type)).cast(type);
    }

    /**
     * Queries DCB events of any of the given types, each mapped to its CloudEvent type string through the converter.
     */
    @SafeVarargs
    public final Flux<E> query(Class<? extends E> first, Class<? extends E>... rest) {
        return query(criteria().types(first, rest));
    }

    /**
     * Queries DCB events tagged with all the given tags.
     */
    public Flux<E> tags(Tag first, Tag... rest) {
        return query(DcbCriteria.tags(first, rest));
    }

    /**
     * Queries DCB events tagged with all the given tags, each parsed from {@code "key:value"} form.
     */
    public Flux<E> tags(String first, String... rest) {
        return tags(Tag.parse(first), parseTags(rest));
    }

    /**
     * Queries DCB events tagged with any of the given tags.
     */
    public Flux<E> tagsAnyOf(Tag first, Tag... rest) {
        return query(DcbCriteria.tagsAnyOf(first, rest));
    }

    /**
     * Queries DCB events tagged with any of the given tags, each parsed from {@code "key:value"} form.
     */
    public Flux<E> tagsAnyOf(String first, String... rest) {
        return tagsAnyOf(Tag.parse(first), parseTags(rest));
    }

    private static Tag[] parseTags(String[] tags) {
        return Arrays.stream(tags).map(Tag::parse).toArray(Tag[]::new);
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
