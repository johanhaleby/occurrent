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
 * {@link PositionRange}, filtered by a {@link Filter}. This is the stream analogue of the DCB position-ordered read
 * (which already exists via {@code DcbEventStore.read(DcbQuery, DcbReadOptions)}), sharing the same
 * {@link PositionRange} window so a catch-up subscription model or the query DSL can replay stream and DCB history
 * through one abstraction.
 * <p>
 * Only implemented by stores where {@code writesPosition()} is {@code true}; a store without position throws
 * {@link UnsupportedOperationException} rather than returning an empty or incorrect result.
 */
@NullMarked
public interface PositionOrderedReader {

    /**
     * Reads events matching {@code filter} ordered by position ascending, within {@code range}.
     *
     * @param filter The filter events must match to be included.
     * @param range  The position window to read.
     * @return The matching events, in position order.
     */
    Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range);

    /**
     * The store's current position high-watermark, i.e. the position of the most recently positioned event.
     * Returns {@code 0} when no positioned event has been written yet.
     */
    long currentPosition();
}
