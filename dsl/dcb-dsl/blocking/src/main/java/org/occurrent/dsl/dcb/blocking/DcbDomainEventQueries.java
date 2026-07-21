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
import org.occurrent.dsl.dcb.DcbCriteriaBuilder;
import org.occurrent.dsl.dcb.DcbDomainEventStream;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Queries a DCB-capable event store and converts the matched CloudEvents into your domain event type.
 *
 * <p>This wraps a {@link DomainEventQueries} so a DCB application can use a single object for both DCB queries
 * (the {@link #query(DcbCriteria)} family) and the regular stream-oriented queries, reached through
 * {@link #domainEventQueries()}. The wrapped instance must be backed by an event store that also implements
 * {@link DcbEventStore} (for example the in-memory event store, or the Spring MongoDB event store with the DCB
 * capability enabled); otherwise the constructor throws.</p>
 *
 * @param <E> the domain event type
 */
@NullMarked
public class DcbDomainEventQueries<E> {

    private final DomainEventQueries<E> domainEventQueries;
    private final DcbEventStore dcbEventStore;

    /**
     * Wraps a {@link DomainEventQueries} backed by a DCB-capable event store.
     *
     * @throws IllegalArgumentException if the wrapped {@link DomainEventQueries} is not backed by a {@link DcbEventStore}
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

    /**
     * A {@link DcbCriteriaBuilder} seeded with a boundary criterion: {@code type}/{@code types}/{@code tags} refine the
     * boundary (setting their dimension, keeping the others), so a shared tag boundary can be reused and given
     * query-specific event types.
     */
    public DcbCriteriaBuilder<E> criteria(DcbCriterion boundary) {
        requireNonNull(boundary, "Boundary cannot be null");
        return new DcbCriteriaBuilder<>(domainEventQueries.cloudEventConverter(), boundary);
    }

    // ------------------------------------------------------------------------------------------------------
    // DCB queries
    // ------------------------------------------------------------------------------------------------------

    /**
     * Queries matching DCB events from the beginning of the DCB sequence.
     */
    public Stream<E> query(DcbCriteria criteria) {
        return query(criteria, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options. The CloudEvents are converted to domain
     * events lazily, so terminal short-circuiting operations such as {@code findFirst} or {@code limit} avoid
     * converting events that are never consumed.
     */
    public Stream<E> query(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        return domainEventQueries.toDomainEvents(dcbEventStore.read(criteria, options).stream());
    }

    /**
     * Queries matching DCB events and returns both the domain events and the observed DCB sequence position.
     */
    public DcbDomainEventStream<E> queryWithPosition(DcbCriteria criteria) {
        return queryWithPosition(criteria, DcbReadOptions.fromBeginning());
    }

    /**
     * Queries matching DCB events using the supplied read options and returns the domain events, the observed DCB
     * sequence position, and the consistency token for a later conditional append.
     */
    public DcbDomainEventStream<E> queryWithPosition(DcbCriteria criteria, DcbReadOptions options) {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(options, "Read options cannot be null");
        DcbEventStream eventStream = dcbEventStore.read(criteria, options);
        List<E> events = domainEventQueries.toDomainEvents(eventStream.stream()).toList();
        return new DcbDomainEventStream<>(events, eventStream.lastSequencePosition(), eventStream.consistencyToken());
    }

    // ------------------------------------------------------------------------------------------------------
    // Convenience queries (one-liners over query(criteria), read from the beginning of the DCB sequence).
    // For read options or mixed criteria, use criteria() together with query(criteria, options).
    // ------------------------------------------------------------------------------------------------------

    /**
     * Queries DCB events of the given type, mapped to its CloudEvent type string through the converter.
     */
    public <SUB extends E> Stream<SUB> types(Class<SUB> type) {
        requireNonNull(type, "Type cannot be null");
        return query(criteria().type(type)).map(type::cast);
    }

    /**
     * Queries DCB events of any of the given types, each mapped to its CloudEvent type string through the converter.
     */
    @SafeVarargs
    public final Stream<E> types(Class<? extends E> first, Class<? extends E>... rest) {
        return query(criteria().types(first, rest));
    }

    /**
     * Queries DCB events tagged with all the given tags.
     */
    public Stream<E> tags(Tag first, Tag... rest) {
        return query(DcbCriteria.tags(first, rest));
    }

    /**
     * Queries DCB events tagged with all the given tags, each parsed from {@code "key:value"} form.
     */
    public Stream<E> tags(String first, String... rest) {
        return tags(Tag.parse(first), parseTags(rest));
    }

    /**
     * Queries DCB events tagged with any of the given tags.
     */
    public Stream<E> tagsAnyOf(Tag first, Tag... rest) {
        return query(DcbCriteria.tagsAnyOf(first, rest));
    }

    /**
     * Queries DCB events tagged with any of the given tags, each parsed from {@code "key:value"} form.
     */
    public Stream<E> tagsAnyOf(String first, String... rest) {
        return tagsAnyOf(Tag.parse(first), parseTags(rest));
    }

    private static Tag[] parseTags(String[] tags) {
        return Arrays.stream(tags).map(Tag::parse).toArray(Tag[]::new);
    }

    private static DcbEventStore requireDcbEventStore(DomainEventQueries<?> domainEventQueries) {
        EventStoreQueries eventStoreQueries = domainEventQueries.eventStoreQueries();
        if (!(eventStoreQueries instanceof DcbEventStore dcbEventStore)) {
            throw new IllegalArgumentException("DCB queries require the " + DomainEventQueries.class.getSimpleName() + " to be backed by a "
                    + DcbEventStore.class.getSimpleName() + ", but was " + eventStoreQueries.getClass().getName());
        }
        return dcbEventStore;
    }
}
