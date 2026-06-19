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

/**
 * Sequence-position result from a successful DCB append.
 *
 * <p>DCB sequence positions are global to the event store, start at 1, and are strictly increasing (monotonic). A
 * successful append is assigned a contiguous block of positions ({@code firstSequencePosition}..{@code lastSequencePosition}).
 * Positions across separate appends may contain gaps (for example a failed or concurrently-retried append may leave a
 * reserved position unused), so callers must rely only on the relative ordering of positions, never on contiguity.</p>
 *
 * @param firstSequencePosition the first global DCB sequence position assigned to the appended events
 * @param lastSequencePosition the last global DCB sequence position assigned to the appended events
 * @param eventCount the number of events appended
 */
@NullMarked
public record DcbAppendResult(long firstSequencePosition, long lastSequencePosition, int eventCount) {

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
    }
}
