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

import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Options that scope a DCB read. Uses the shared {@link PositionRange} window, the same window used by stream
 * position reads and the query DSL, and optionally caps the number of matching events returned and selects which end
 * of the matching range the cap keeps.
 * <p>
 * {@code direction} and {@code limit} do not change the order in which events are returned: a {@link DcbEventStream}
 * always lists its events in ascending DCB sequence-position order. They only select <em>which</em> matching events are
 * returned. {@code direction} chooses the end the {@code limit} keeps &mdash; {@link Direction#FORWARD} keeps the
 * lowest-position matches (the oldest), {@link Direction#BACKWARD} keeps the highest-position matches (the newest)
 * &mdash; and {@code limit} caps the count. So {@code fromBeginning().backwards().limit(1)} returns the single
 * highest-position event that matches the criteria, which is how a gapless sequence reads its last entry in one round
 * trip instead of folding the whole stream (see ADR 0056).
 * <p>
 * {@code direction} and {@code limit} never affect the {@link DcbEventStream#consistencyToken() consistency token}: the
 * token reflects the whole matching set observed at read time, not the returned page, so a limited read still guards an
 * append against <em>any</em> later matching event.
 *
 * @param positionRange the position window to read
 * @param direction     which end of the matching range the {@code limit} keeps; never changes the returned order
 * @param limit         optional cap on the number of matching events returned; when present must be positive
 */
@NullMarked
public record DcbReadOptions(PositionRange positionRange, Direction direction, OptionalInt limit) {

    /**
     * Selects which end of the matching range a {@link DcbReadOptions#limit() limit} keeps. Does not change the order
     * of the returned events, which is always ascending by DCB sequence position.
     */
    public enum Direction {
        /**
         * Keep the lowest-position (oldest) matching events.
         */
        FORWARD,
        /**
         * Keep the highest-position (newest) matching events.
         */
        BACKWARD
    }

    public DcbReadOptions {
        requireNonNull(positionRange, "Position range cannot be null");
        requireNonNull(direction, "Direction cannot be null");
        requireNonNull(limit, "Limit cannot be null");
        if (limit.isPresent() && limit.getAsInt() <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }
    }

    /**
     * Reads the whole position window forwards with no limit.
     */
    public DcbReadOptions(PositionRange positionRange) {
        this(positionRange, Direction.FORWARD, OptionalInt.empty());
    }

    /**
     * Optional exclusive lower bound. When present, only events with a DCB sequence position strictly greater than
     * this value are returned.
     */
    public OptionalLong afterPosition() {
        return positionRange.afterPosition();
    }

    /**
     * Optional inclusive upper bound. When present, only events with a DCB sequence position less than or equal to
     * this value are returned. When absent, the read includes everything up to the store's DCB head at read time.
     */
    public OptionalLong upToPosition() {
        return positionRange.upToPosition();
    }

    /**
     * Returns a copy of these options that keep the highest-position (newest) matches when a {@code limit} is set
     * (see {@link Direction#BACKWARD}). The returned events are always in ascending position order regardless.
     */
    public DcbReadOptions backwards() {
        return new DcbReadOptions(positionRange, Direction.BACKWARD, limit);
    }

    /**
     * Returns a copy of these options that keep the lowest-position (oldest) matches when a {@code limit} is set
     * (see {@link Direction#FORWARD}). The returned events are always in ascending position order regardless.
     */
    public DcbReadOptions forwards() {
        return new DcbReadOptions(positionRange, Direction.FORWARD, limit);
    }

    /**
     * Returns a copy of these options capped to at most {@code max} matching events.
     */
    public DcbReadOptions limit(int max) {
        return new DcbReadOptions(positionRange, direction, OptionalInt.of(max));
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
    public static DcbReadOptions afterPosition(long position) {
        return new DcbReadOptions(PositionRange.afterPosition(position));
    }

    /**
     * Reads from the beginning up to and including the supplied DCB sequence position.
     */
    public static DcbReadOptions upToPosition(long position) {
        return new DcbReadOptions(PositionRange.upToPosition(position));
    }

    /**
     * Reads events after {@code afterPosition} (exclusive) and up to and including {@code upToPosition}.
     */
    public static DcbReadOptions between(long afterPosition, long upToPosition) {
        return new DcbReadOptions(PositionRange.between(afterPosition, upToPosition));
    }

    /**
     * Reads at most {@code max} matching events, keeping the highest-position (newest) ones. A gapless sequence uses
     * {@code backwardsLimited(1)} to read its last entry in a single round trip. The events are still returned in
     * ascending position order (a single event when {@code max} is 1).
     */
    public static DcbReadOptions backwardsLimited(int max) {
        return new DcbReadOptions(PositionRange.fromBeginning(), Direction.BACKWARD, OptionalInt.of(max));
    }
}
