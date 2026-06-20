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

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Options that scope a DCB read.
 *
 * @param afterSequencePosition optional exclusive lower bound; when present, only events with a DCB sequence position
 *                              strictly greater than this value are returned
 * @param upToSequencePosition  optional inclusive upper bound; when present, only events with a DCB sequence position
 *                              less than or equal to this value are returned. When absent, the read includes everything
 *                              up to the store's DCB head at read time.
 */
@NullMarked
public record DcbReadOptions(OptionalLong afterSequencePosition, OptionalLong upToSequencePosition) {

    public DcbReadOptions {
        requireNonNull(afterSequencePosition, "After sequence position cannot be null");
        requireNonNull(upToSequencePosition, "Up to sequence position cannot be null");
        afterSequencePosition.ifPresent(position -> {
            if (position < 0) {
                throw new IllegalArgumentException("After sequence position cannot be negative");
            }
        });
        upToSequencePosition.ifPresent(position -> {
            if (position < 0) {
                throw new IllegalArgumentException("Up to sequence position cannot be negative");
            }
        });
        if (afterSequencePosition.isPresent() && upToSequencePosition.isPresent()
                && afterSequencePosition.getAsLong() >= upToSequencePosition.getAsLong()) {
            throw new IllegalArgumentException("After sequence position must be less than up to sequence position");
        }
    }

    /**
     * Reads from the beginning of the DCB sequence up to the store head.
     */
    public static DcbReadOptions fromBeginning() {
        return new DcbReadOptions(OptionalLong.empty(), OptionalLong.empty());
    }

    /**
     * Reads only events after the supplied DCB sequence position (exclusive).
     */
    public static DcbReadOptions afterSequencePosition(long position) {
        return new DcbReadOptions(OptionalLong.of(position), OptionalLong.empty());
    }

    /**
     * Reads from the beginning up to and including the supplied DCB sequence position.
     */
    public static DcbReadOptions upToSequencePosition(long position) {
        return new DcbReadOptions(OptionalLong.empty(), OptionalLong.of(position));
    }

    /**
     * Reads events after {@code afterExclusive} (exclusive) and up to and including {@code upToInclusive}.
     */
    public static DcbReadOptions between(long afterExclusive, long upToInclusive) {
        return new DcbReadOptions(OptionalLong.of(afterExclusive), OptionalLong.of(upToInclusive));
    }
}
