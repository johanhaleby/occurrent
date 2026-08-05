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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.inmemory.filtermatching.FilterMatcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A minimal in-memory {@link PositionOrderedReader} test double, standing in for the reactive in-memory event store
 * this repository does not have (see {@code ORCHESTRATOR.md}). Exists only to drive
 * {@link CatchupThenPushSubscriptionModel}'s replay in the TCK wirings in this package, exactly the role
 * {@code InMemoryEventStore} plays in the blocking {@code CatchupThenPushSubscriptionModelTest}.
 * <p>
 * {@link #append(List)} does not forward to a live feed itself: the two are separate objects on the constructor of
 * {@link CatchupThenPushSubscriptionModel}, so a fixture using this reader calls both, matching what
 * {@code InMemoryEventStore(feed::accept)} does in one call on the blocking side.
 */
final class InMemoryReactivePositionOrderedReader implements PositionOrderedReader {

    private final List<CloudEvent> events = new CopyOnWriteArrayList<>();

    void append(List<CloudEvent> newEvents) {
        events.addAll(newEvents);
    }

    @Override
    public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
        return Flux.fromIterable(List.copyOf(events)).filter(cloudEvent -> FilterMatcher.matchesFilter(cloudEvent, filter));
    }

    @Override
    public Mono<Long> currentPosition() {
        return Mono.just((long) events.size());
    }

    @Override
    public boolean writesPosition() {
        return true;
    }
}
