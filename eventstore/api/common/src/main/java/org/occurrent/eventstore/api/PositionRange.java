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

package org.occurrent.eventstore.api;

import org.jspecify.annotations.NullMarked;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A bounded window over the global, monotonic sequence position. Shared by every position-ordered read: DCB reads,
 * stream position reads, and the query DSL.
 *
 * @param afterPosition optional exclusive lower bound. When present, only events with a position strictly greater
 *                       than this value are included.
 * @param upToPosition  optional inclusive upper bound. When present, only events with a position less than or equal
 *                       to this value are included. When absent, the range includes everything up to the store's
 *                       position head at read time.
 */
@NullMarked
public record PositionRange(OptionalLong afterPosition, OptionalLong upToPosition) {

    public PositionRange {
        requireNonNull(afterPosition, "After position cannot be null");
        requireNonNull(upToPosition, "Up to position cannot be null");
        afterPosition.ifPresent(position -> {
            if (position < 0) {
                throw new IllegalArgumentException("After position cannot be negative");
            }
        });
        upToPosition.ifPresent(position -> {
            if (position < 0) {
                throw new IllegalArgumentException("Up to position cannot be negative");
            }
        });
        if (afterPosition.isPresent() && upToPosition.isPresent()
                && afterPosition.getAsLong() > upToPosition.getAsLong()) {
            // An equal pair is a valid empty range (lower bound exclusive, upper inclusive). Only reject an inverted
            // range where after is greater than upTo.
            throw new IllegalArgumentException("After position cannot be greater than up to position");
        }
    }

    /**
     * A range spanning from the beginning of the sequence up to the store head.
     */
    public static PositionRange fromBeginning() {
        return new PositionRange(OptionalLong.empty(), OptionalLong.empty());
    }

    /**
     * A range containing only positions after the supplied position (exclusive).
     */
    public static PositionRange afterPosition(long position) {
        return new PositionRange(OptionalLong.of(position), OptionalLong.empty());
    }

    /**
     * A range from the beginning up to and including the supplied position.
     */
    public static PositionRange upToPosition(long position) {
        return new PositionRange(OptionalLong.empty(), OptionalLong.of(position));
    }

    /**
     * A range after {@code afterPosition} (exclusive) and up to and including {@code upToPosition}.
     */
    public static PositionRange between(long afterPosition, long upToPosition) {
        return new PositionRange(OptionalLong.of(afterPosition), OptionalLong.of(upToPosition));
    }
}
