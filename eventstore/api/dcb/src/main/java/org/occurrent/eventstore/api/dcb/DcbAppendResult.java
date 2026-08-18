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

package org.occurrent.eventstore.api.dcb;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.AppendId;

import java.util.Objects;
import java.util.Optional;

/**
 * Sequence-position result from a successful DCB append.
 *
 * <p>DCB sequence positions are global to the event store, start at 1, and are strictly increasing (monotonic). A
 * single successful append is assigned a contiguous block of positions ({@code firstSequencePosition}..{@code
 * lastSequencePosition}). Across separate appends only the relative ordering of positions is guaranteed, so callers
 * must compare positions for ordering and must not assume the positions of different appends are contiguous.</p>
 *
 * @param firstSequencePosition the first global DCB sequence position assigned to the appended events
 * @param lastSequencePosition the last global DCB sequence position assigned to the appended events
 * @param eventCount the number of events appended
 * @param appendId the identifier stamped on every appended event. A successful DCB append always persists at least
 *                 one event, so a store-produced result has one here. A result built through the three-argument
 *                 constructor is always empty, and nothing stops a caller from passing {@link Optional#empty()} to
 *                 the four-argument one directly, so this component's presence is a property of where the result
 *                 came from, not one this record enforces on its own.
 */
@NullMarked
public record DcbAppendResult(long firstSequencePosition, long lastSequencePosition, int eventCount, Optional<AppendId> appendId) {

    public DcbAppendResult {
        if (firstSequencePosition <= 0) {
            throw new IllegalArgumentException("First sequence position must be greater than zero");
        }
        if (lastSequencePosition < firstSequencePosition) {
            throw new IllegalArgumentException("Last sequence position must be greater than or equal to first sequence position");
        }
        if (eventCount <= 0) {
            throw new IllegalArgumentException("Event count must be greater than zero");
        }
        Objects.requireNonNull(appendId, "Append id cannot be null");
    }

    /**
     * Builds a result with no append id. Use {@link #DcbAppendResult(long, long, int, Optional)} to report one.
     */
    public DcbAppendResult(long firstSequencePosition, long lastSequencePosition, int eventCount) {
        this(firstSequencePosition, lastSequencePosition, eventCount, Optional.empty());
    }
}
