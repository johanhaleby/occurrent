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
import org.occurrent.eventstore.api.PositionRange;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Options that scope a DCB read. Composes the shared {@link PositionRange} window, so DCB reads share one
 * position-window abstraction with stream position reads and the query DSL.
 *
 * @param positionRange the position window to read
 */
@NullMarked
public record DcbReadOptions(PositionRange positionRange) {

    public DcbReadOptions {
        requireNonNull(positionRange, "Position range cannot be null");
    }

    /**
     * Optional exclusive lower bound; when present, only events with a DCB sequence position strictly greater than
     * this value are returned.
     */
    public OptionalLong afterSequencePosition() {
        return positionRange.afterPosition();
    }

    /**
     * Optional inclusive upper bound; when present, only events with a DCB sequence position less than or equal to
     * this value are returned. When absent, the read includes everything up to the store's DCB head at read time.
     */
    public OptionalLong upToSequencePosition() {
        return positionRange.upToPosition();
    }

    /**
     * Reads from the beginning of the DCB sequence up to the store head.
     */
    public static DcbReadOptions fromBeginning() {
        return new DcbReadOptions(PositionRange.fromBeginning());
    }

    /**
     * Reads only events after the supplied DCB sequence position (exclusive).
     */
    public static DcbReadOptions afterSequencePosition(long position) {
        return new DcbReadOptions(PositionRange.afterPosition(position));
    }

    /**
     * Reads from the beginning up to and including the supplied DCB sequence position.
     */
    public static DcbReadOptions upToSequencePosition(long position) {
        return new DcbReadOptions(PositionRange.upToPosition(position));
    }

    /**
     * Reads events after {@code afterSequencePosition} (exclusive) and up to and including {@code upToSequencePosition}.
     */
    public static DcbReadOptions between(long afterSequencePosition, long upToSequencePosition) {
        return new DcbReadOptions(PositionRange.between(afterSequencePosition, upToSequencePosition));
    }
}
