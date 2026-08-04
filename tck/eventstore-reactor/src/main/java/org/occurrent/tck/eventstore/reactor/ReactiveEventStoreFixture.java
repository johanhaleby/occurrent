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

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;

/**
 * What a reactive event store hands {@link ReactiveEventStoreConformance}.
 * <p>
 * Deliberately smaller than {@link org.occurrent.tck.eventstore.blocking.EventStoreFixture}, and not a subtype of it.
 * The behavioural contract is asserted once, through {@link BlockingEventStoreOverReactive} and the blocking suites, so
 * an implementation supplies the blocking fixture for all of that. This one exists only for what blocking on a result
 * destroys, which is a stream-capability question on every store shipping with Occurrent and needs neither a capability
 * declaration nor the DCB views.
 * <p>
 * As with the blocking fixture, the store handed back <strong>must contain no events</strong>, and a fresh fixture is
 * created for every test method.
 */
@NullMarked
public interface ReactiveEventStoreFixture {

    /**
     * Reads a stream, writes to one, and answers whether a stream exists.
     */
    EventStore eventStore();

    /**
     * Queries, counts and tests existence across streams.
     */
    EventStoreQueries queries();

    /**
     * Deletes streams and single events, deletes by filter, and updates an event.
     */
    EventStoreOperations operations();

    /**
     * Position-ordered reads, for the one {@code Mono} on it that must always emit.
     */
    PositionOrderedReader positionOrderedReader();

    /**
     * Releases whatever the fixture holds. Called after every test method, including a failing one.
     */
    default void close() {
    }
}
