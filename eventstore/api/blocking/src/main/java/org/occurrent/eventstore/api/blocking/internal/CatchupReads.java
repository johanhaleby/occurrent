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

package org.occurrent.eventstore.api.blocking.internal;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.filter.Filter;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. Implemented by an event store that a catch-up subscription reads through. The
 * catch-up opens one {@link CatchupReadScope} for each replay attempt and reads the live start and every event of that
 * attempt through it, so the store decides how those reads see each other.
 * <p>
 * A catch-up refuses at construction an {@link EventStoreQueries} that doesn't implement this interface. A decorator
 * around a store, such as one that adds tracing, metrics or tenancy, therefore can't be handed to a catch-up. Hand the
 * catch-up the store itself and decorate around the subscription instead.
 */
@NullMarked
public interface CatchupReads {

    /**
     * Opens the scope of one replay attempt. The caller closes it when the attempt ends, whether it completed, failed or
     * was cancelled.
     *
     * @param notBefore The live start an earlier attempt stored, when this attempt resumes from it, otherwise {@code null}.
     *                  A store that reports a live start makes every read of the scope see at least everything that
     *                  committed before {@code notBefore}. A value the store doesn't recognize as one of its own live
     *                  starts is ignored.
     * @return The scope of the attempt. Must be closed.
     */
    CatchupReadScope openCatchupReadScope(@Nullable String notBefore);

    /**
     * Reads {@code queries} and {@code reader} directly, in no session of their own, and reports no live start. The
     * catch-up then takes its live start from the subscription model it wraps, as it did before this interface.
     * <p>
     * Only correct for a store that assigns a position and makes the event visible in one atomic step, so no event can
     * commit below a position a read has already seen. The in-memory event store does, since it assigns the position and
     * adds the event in one critical section. A MongoDB store doesn't, because it reserves a position before its write
     * commits, so using this over one brings back the event loss in
     * <a href="https://github.com/johanhaleby/occurrent/issues/1230">#1230</a>.
     *
     * @param queries The store's queries, used for {@link CatchupReadScope#query(Filter, int, int, SortBy)}
     * @param reader  The store's position reader, used for the other reads
     */
    static CatchupReads unscoped(EventStoreQueries queries, PositionOrderedReader reader) {
        requireNonNull(queries, EventStoreQueries.class.getSimpleName() + " cannot be null");
        requireNonNull(reader, PositionOrderedReader.class.getSimpleName() + " cannot be null");
        return notBefore -> new CatchupReadScope() {
            @Override
            public @Nullable String liveStart() {
                return null;
            }

            @Override
            public long currentPosition() {
                return reader.currentPosition();
            }

            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return reader.readInPositionOrder(filter, range);
            }

            @Override
            public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
                return queries.query(filter, skip, limit, sortBy);
            }

            @Override
            public void close() {
            }
        };
    }
}
