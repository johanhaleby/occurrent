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

package org.occurrent.tck.eventstore.blocking;

import io.cloudevents.CloudEvent;
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

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A store that implements every capability interface and honours none of them. Two jobs, both about the TCK rather
 * than about any real store:
 * <ul>
 *     <li>It lets the guard tests build a fixture without also building a working store, since those tests never get
 *     as far as calling one.</li>
 *     <li>Run a suite against it and every single test must fail. Nothing may pass, and nothing may be skipped. That
 *     is what {@code SuiteNeverSkipsTest} asserts, and it is the only mechanical check that the suites really do
 *     refuse to skip rather than relying on nobody reaching for {@code Assumptions}.</li>
 * </ul>
 */
final class NoopStore
        implements EventStore, EventStoreQueries, EventStoreOperations, ReadEventStreamWithFilter, PositionOrderedReader, DcbEventStore {

    static final NoopStore INSTANCE = new NoopStore();

    private NoopStore() {
    }

    private static UnsupportedOperationException notImplemented() {
        return new UnsupportedOperationException("NoopStore implements nothing on purpose");
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
        throw notImplemented();
    }

    @Override
    public WriteResult write(String streamId, List<CloudEvent> events) {
        throw notImplemented();
    }

    @Override
    public WriteResult write(String streamId, WriteCondition writeCondition, List<CloudEvent> events) {
        throw notImplemented();
    }

    @Override
    public boolean exists(String streamId) {
        throw notImplemented();
    }

    @Override
    public EventStream<CloudEvent> read(String streamId, StreamReadFilter filter, int skip, int limit) {
        throw notImplemented();
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        throw notImplemented();
    }

    @Override
    public long count(Filter filter) {
        throw notImplemented();
    }

    @Override
    public boolean exists(Filter filter) {
        throw notImplemented();
    }

    @Override
    public void deleteEventStream(String streamId) {
        throw notImplemented();
    }

    @Override
    public void deleteEvent(String cloudEventId, URI cloudEventSource) {
        throw notImplemented();
    }

    @Override
    public void delete(Filter filter) {
        throw notImplemented();
    }

    @Override
    public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        throw notImplemented();
    }

    @Override
    public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange positionRange) {
        throw notImplemented();
    }

    @Override
    public long currentPosition() {
        throw notImplemented();
    }

    @Override
    public boolean writesPosition() {
        throw notImplemented();
    }

    @Override
    public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
        throw notImplemented();
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events) {
        throw notImplemented();
    }

    @Override
    public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition appendCondition) {
        throw notImplemented();
    }
}
