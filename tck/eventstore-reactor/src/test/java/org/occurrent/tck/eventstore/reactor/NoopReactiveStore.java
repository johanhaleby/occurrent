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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.reactor.*;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.function.Function;

/**
 * A reactive store that implements every interface {@link ReactiveEventStoreConformance} touches and honours none of
 * them. The reactive counterpart of the blocking leaf's {@code NoopStore}, and it exists for the same one reason: run
 * the suite against it and every test must fail, with nothing passing and nothing skipped.
 * <p>
 * It answers with {@code Mono.error} rather than throwing, because throwing is the failure this suite is looking for and
 * a no-op store that threw would make the assertions pass for the wrong reason.
 */
final class NoopReactiveStore implements EventStore, EventStoreQueries, EventStoreOperations, PositionOrderedReader {

    static final NoopReactiveStore INSTANCE = new NoopReactiveStore();

    private NoopReactiveStore() {
    }

    private static <T> Mono<T> notImplemented() {
        return Mono.error(new UnsupportedOperationException("NoopReactiveStore implements nothing on purpose"));
    }

    @Override
    public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
        return notImplemented();
    }

    @Override
    public Mono<WriteResult> write(String streamId, Flux<CloudEvent> events) {
        return notImplemented();
    }

    @Override
    public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
        return notImplemented();
    }

    @Override
    public Mono<Boolean> exists(String streamId) {
        return notImplemented();
    }

    @Override
    public Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return Flux.error(new UnsupportedOperationException("NoopReactiveStore implements nothing on purpose"));
    }

    @Override
    public Mono<Long> count(Filter filter) {
        return notImplemented();
    }

    @Override
    public Mono<Boolean> exists(Filter filter) {
        return notImplemented();
    }

    @Override
    public Mono<Void> deleteEventStream(String streamId) {
        return notImplemented();
    }

    @Override
    public Mono<Void> deleteEvent(String cloudEventId, URI cloudEventSource) {
        return notImplemented();
    }

    @Override
    public Mono<Void> delete(Filter filter) {
        return notImplemented();
    }

    @Override
    public Mono<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
        return notImplemented();
    }

    @Override
    public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
        return Flux.error(new UnsupportedOperationException("NoopReactiveStore implements nothing on purpose"));
    }

    @Override
    public Mono<Long> currentPosition() {
        return notImplemented();
    }

    /**
     * The one member here that is not a publisher, so it throws where the rest hand back an error. Nothing in
     * {@link ReactiveEventStoreConformance} asks yet, and answering {@code false} would be a way for the first test
     * that does ask to pass against a store honouring nothing. The blocking {@code NoopStore} throws here too.
     */
    @Override
    public boolean writesPosition() {
        throw new UnsupportedOperationException("NoopReactiveStore implements nothing on purpose");
    }
}
