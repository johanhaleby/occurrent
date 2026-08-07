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

package org.occurrent.eventstore.api.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.filter.Filter;

import java.util.stream.Stream;

/**
 * Implemented by event stores that can read events ordered by the global sequence position within a bounded
 * {@link PositionRange}, filtered by a {@link Filter}. Stream and DCB reads use the same {@link PositionRange}
 * window, so callers can replay either through one abstraction.
 * <p>
 * Whether a store actually carries a position is a runtime property, not just a matter of type. A STREAM-only store
 * can be configured without one, so an instance may implement this interface and still report
 * {@code writesPosition() == false}. Check {@code writesPosition()} before calling the read methods, since a store
 * without a position throws {@link UnsupportedOperationException} rather than returning an empty or incorrect result.
 * An {@code instanceof PositionOrderedReader} check alone does not tell you whether this instance writes a position.
 */
@NullMarked
public interface PositionOrderedReader {

    /**
     * Reads events matching {@code filter} ordered by position ascending, within {@code range}.
     * <p>
     * <b>The returned stream must be closed.</b> It is lazy and may hold a database resource, such as a server
     * cursor, so use it in a try-with-resources block, or close it explicitly, in particular when you stop reading
     * before the end of the range. Consuming it to exhaustion releases the resource too, so a full read that runs to
     * completion leaks nothing, but a caller cannot tell from here whether it will.
     *
     * @param filter The filter events must match to be included.
     * @param range  The position window to read.
     * @return The matching events, in position order. Must be closed.
     */
    Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range);

    /**
     * The store's current position high-watermark, i.e. the position of the most recently positioned event.
     * Returns {@code 0} when no positioned event has been written yet.
     */
    long currentPosition();

    /**
     * Whether this store carries a global position, that is, whether {@link #readInPositionOrder(Filter, PositionRange)}
     * and {@link #currentPosition()} are safe to call.
     */
    boolean writesPosition();
}
