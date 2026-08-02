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

package org.occurrent.tck.eventstore.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.StreamReadFilter;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.ReadEventStreamWithFilter;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Presents a reactive event store as a blocking one, so the blocking conformance suites can be run against it
 * unchanged instead of being written a second time in terms of {@code Mono} and {@code Flux}.
 * <p>
 * This is a test bridge and nothing more. It is not a general-purpose adapter and must not be used in production:
 * every method blocks the calling thread, which is exactly what a reactive store exists to avoid.
 * <p>
 * A bridge cannot see everything. Whether a failure arrives as {@code Mono.error} rather than being thrown from the
 * assembly call, whether a store does any work before something subscribes, and what cancellation does are all
 * invisible once the result has been blocked on. Those are the reactive contract, not the behavioural one, and they
 * belong to {@link ReactiveEventStoreConformance} rather than here.
 */
@NullMarked
public final class BlockingEventStoreOverReactive
        implements EventStore, EventStoreQueries, EventStoreOperations, ReadEventStreamWithFilter, PositionOrderedReader, DcbEventStore {

    private final org.occurrent.eventstore.api.reactor.EventStore eventStore;
    private final org.occurrent.eventstore.api.reactor.EventStoreQueries queries;
    private final org.occurrent.eventstore.api.reactor.EventStoreOperations operations;
    private final org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter filteredReader;
    private final org.occurrent.eventstore.api.reactor.PositionOrderedReader positionOrderedReader;
    private final org.occurrent.eventstore.api.dcb.reactor.DcbEventStore dcbEventStore;

    private BlockingEventStoreOverReactive(org.occurrent.eventstore.api.reactor.EventStore eventStore,
                                          org.occurrent.eventstore.api.reactor.EventStoreQueries queries,
                                          org.occurrent.eventstore.api.reactor.EventStoreOperations operations,
                                          org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter filteredReader,
                                          org.occurrent.eventstore.api.reactor.PositionOrderedReader positionOrderedReader,
                                          org.occurrent.eventstore.api.dcb.reactor.DcbEventStore dcbEventStore) {
        this.eventStore = requireNonNull(eventStore, "Reactive event store cannot be null");
        this.queries = requireNonNull(queries, "Reactive event store queries cannot be null");
        this.operations = requireNonNull(operations, "Reactive event store operations cannot be null");
        this.filteredReader = requireNonNull(filteredReader, "Reactive filtered reader cannot be null");
        this.positionOrderedReader = requireNonNull(positionOrderedReader, "Reactive position ordered reader cannot be null");
        this.dcbEventStore = requireNonNull(dcbEventStore, "Reactive DCB event store cannot be null");
    }

    /**
     * Bridges a store that implements every reactive capability at once, which is what all of Occurrent's reactive
     * stores do and what an out-of-tree store is likely to do too.
     */
    public static <T extends org.occurrent.eventstore.api.reactor.EventStore
            & org.occurrent.eventstore.api.reactor.EventStoreQueries
            & org.occurrent.eventstore.api.reactor.EventStoreOperations
            & org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter
            & org.occurrent.eventstore.api.reactor.PositionOrderedReader
            & org.occurrent.eventstore.api.dcb.reactor.DcbEventStore> BlockingEventStoreOverReactive of(T store) {
        requireNonNull(store, "Reactive event store cannot be null");
        return new BlockingEventStoreOverReactive(store, store, store, store, store, store);
    }

    /**
     * Bridges capabilities that live on different objects.
     */
    public static BlockingEventStoreOverReactive of(org.occurrent.eventstore.api.reactor.EventStore eventStore,
                                                    org.occurrent.eventstore.api.reactor.EventStoreQueries queries,
                                                    org.occurrent.eventstore.api.reactor.EventStoreOperations operations,
                                                    org.occurrent.eventstore.api.reactor.ReadEventStreamWithFilter filteredReader,
                                                    org.occurrent.eventstore.api.reactor.PositionOrderedReader positionOrderedReader,
                                                    org.occurrent.eventstore.api.dcb.reactor.DcbEventStore dcbEventStore) {
        return new BlockingEventStoreOverReactive(eventStore, queries, operations, filteredReader, positionOrderedReader, dcbEventStore);
    }

    // EventStore

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        return blockingStreamOf(eventStore.read(streamId, skip, limit));
    }

    @Override
    public WriteResult write(String streamId, List<CloudEvent> events) {
        return blockRequiringAValue(eventStore.write(streamId, Flux.fromIterable(events)), "write(String, Flux)");
    }

    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, List<CloudEvent> events) {
        return blockRequiringAValue(eventStore.write(streamId, writeCondition, Flux.fromIterable(events)),
                "write(String, WriteCondition, Flux)");
    }

    @Override
    public boolean exists(String streamId) {
        return blockRequiringAValue(eventStore.exists(streamId), "exists(String)");
    }

    // ReadEventStreamWithFilter

    @Override
    public EventStream<CloudEvent> read(String streamId, StreamReadFilter filter, int skip, int limit) {
        return blockingStreamOf(filteredReader.read(streamId, filter, skip, limit));
    }

    // EventStoreQueries

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return queries.query(filter, skip, limit, sortBy).toStream();
    }

    @Override
    public long count(Filter filter) {
        return blockRequiringAValue(queries.count(filter), "count(Filter)");
    }

    @Override
    public boolean exists(Filter filter) {
        return blockRequiringAValue(queries.exists(filter), "exists(Filter)");
    }

    // EventStoreOperations

    @Override
    public void deleteEventStream(String streamId) {
        block(operations.deleteEventStream(streamId));
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        block(operations.deleteEvent(cloudEventId, cloudEventSource));
    }

    @Override
    public void delete(Filter filter) {
        block(operations.delete(filter));
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        return Optional.ofNullable(block(operations.updateEvent(cloudEventId, cloudEventSource, updateFunction)));
    }

    // PositionOrderedReader

    @Override
    public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange positionRange) {
        return positionOrderedReader.readInPositionOrder(filter, positionRange).toStream();
    }

    @Override
    public long currentPosition() {
        return blockRequiringAValue(positionOrderedReader.currentPosition(), "currentPosition()");
    }

    @Override
    public boolean writesPosition() {
        return positionOrderedReader.writesPosition();
    }

    // DcbEventStore

    // exists(DcbCriteria, DcbReadOptions) and count(DcbCriteria, DcbReadOptions) are overridden here rather than left
    // to the blocking interface's defaults, because those defaults would perform their own read on top of a value
    // this bridge has already blocked for. The reactive store may implement them more efficiently, and the bridge
    // must exercise that implementation rather than the blocking interface default.

    @Override
    public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
        return blockRequiringAValue(dcbEventStore.read(criteria, options), "read(DcbCriteria, DcbReadOptions)");
    }

    @Override
    public boolean exists(DcbCriteria criteria, DcbReadOptions options) {
        return blockRequiringAValue(dcbEventStore.exists(criteria, options), "exists(DcbCriteria, DcbReadOptions)");
    }

    @Override
    public long count(DcbCriteria criteria, DcbReadOptions options) {
        return blockRequiringAValue(dcbEventStore.count(criteria, options), "count(DcbCriteria, DcbReadOptions)");
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events) {
        return blockRequiringAValue(dcbEventStore.append(events), "append(List)");
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
        return blockRequiringAValue(dcbEventStore.append(events, condition), "append(List, DcbAppendCondition)");
    }

    /**
     * Materialises a reactive event stream into a blocking one.
     * <p>
     * The events are collected eagerly rather than kept as a lazy {@code Flux}, so a suite that reads a stream, writes
     * more events and then walks the first stream sees the snapshot it read. That is a property of this bridge, not
     * of the store underneath it, which is one reason read skew is not something the shared suites assert.
     */
    private EventStream<CloudEvent> blockingStreamOf(Mono<org.occurrent.eventstore.api.reactor.EventStream<CloudEvent>> mono) {
        org.occurrent.eventstore.api.reactor.EventStream<CloudEvent> reactive = block(mono);
        if (reactive == null) {
            throw new IllegalStateException("The reactive event store completed empty instead of emitting an event "
                    + "stream. read(..) must always emit exactly one EventStream, an empty one for a stream that does "
                    + "not exist.");
        }
        String id = reactive.id();
        long version = reactive.version();
        List<CloudEvent> events = reactive.events().collectList().block();
        List<CloudEvent> materialised = events == null ? List.of() : List.copyOf(events);
        return new EventStream<>() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public long version() {
                return version;
            }

            @Override
            public Stream<CloudEvent> events() {
                return materialised.stream();
            }
        };
    }

    private static <T> @Nullable T block(Mono<T> mono) {
        return mono.block();
    }

    /**
     * Blocks on a {@code Mono} that the contract says always emits, and fails loudly when it completes empty instead.
     * <p>
     * Coercing an empty completion into {@code 0} or {@code false} would let a reactive store that never emits pass
     * the conformance suites, which is the one outcome a TCK must not allow. The suites are the only caller, so
     * throwing here turns that bug into a failing test naming the method that misbehaved.
     */
    private static <T> T blockRequiringAValue(Mono<T> mono, String method) {
        T value = mono.block();
        if (value == null) {
            throw new IllegalStateException("The reactive event store completed empty from " + method
                    + " instead of emitting a value. That method is documented to always emit, so an empty completion "
                    + "is a bug in the store rather than something this bridge should turn into a default value.");
        }
        return value;
    }
}
