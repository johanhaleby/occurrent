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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Options that select events from a DCB read. The position range and criteria find the matching events. The direction
 * chooses which end to start from, then {@code skip} and {@code limit} select the events to return.
 * <p>
 * Direction, skip, and limit do not change the returned order. A {@link DcbEventStream} always lists events ascending
 * by DCB position. For example, {@code fromBeginning().backwards().skip(1).limit(2)} skips the newest match and returns
 * the 2 matches before it in ascending order.
 * <p>
 * The position range, direction, skip, and limit do not affect the {@link DcbEventStream#consistencyToken() consistency
 * token}. It reflects the whole matching set observed at read time, not only the events returned by these options.
 *
 * @param positionRange the position window to read
 * @param direction     which end of the matching events to start selecting from
 * @param skip          number of matching events to skip from the selected end
 * @param limit         optional cap on the number of matching events returned, when present it must be positive
 * @param versionsRequested whether the read asks the store to report the marker versions of its criteria in
 *                      {@link DcbEventStream#markerVersions()}, see {@link #withVersions()}
 * @param afterVersions optional marker versions to resume after, see {@link #afterVersions(DcbMarkerVersions)}. When
 *                      present, {@code versionsRequested} must be {@code true} and the position range must have neither
 *                      bound.
 */
@NullMarked
public record DcbReadOptions(PositionRange positionRange, Direction direction, int skip, OptionalInt limit,
                             boolean versionsRequested, Optional<DcbMarkerVersions> afterVersions) {

    /**
     * Selects which end of the matching events to start from before applying skip and limit. The returned events stay
     * in ascending DCB position order.
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
        if (skip < 0) {
            throw new IllegalArgumentException("Skip cannot be negative");
        }
        if (limit.isPresent() && limit.getAsInt() <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }
        requireNonNull(afterVersions, "After versions cannot be null");
        if (afterVersions.isPresent()) {
            if (!versionsRequested) {
                throw new IllegalArgumentException("Reading after marker versions requires the read to ask for versions");
            }
            if (positionRange.afterPosition().isPresent() || positionRange.upToPosition().isPresent()) {
                throw new IllegalArgumentException("Reading after marker versions cannot be combined with a position bound");
            }
        }
    }

    /**
     * Reads the position window in the supplied direction, skipping {@code skip} matching events and keeping at most
     * {@code limit}, without asking for marker versions.
     */
    public DcbReadOptions(PositionRange positionRange, Direction direction, int skip, OptionalInt limit) {
        this(positionRange, direction, skip, limit, false, Optional.empty());
    }

    /**
     * Reads the whole position window forwards with no limit.
     */
    public DcbReadOptions(PositionRange positionRange) {
        this(positionRange, Direction.FORWARD, 0, OptionalInt.empty());
    }

    /**
     * Reads the position window in the supplied direction without skipping any matching events.
     */
    public DcbReadOptions(PositionRange positionRange, Direction direction, OptionalInt limit) {
        this(positionRange, direction, 0, limit);
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
     * Returns a copy of these options that select matching events from the highest-position (newest) end
     * (see {@link Direction#BACKWARD}). The returned events are always in ascending position order regardless.
     */
    public DcbReadOptions backwards() {
        return new DcbReadOptions(positionRange, Direction.BACKWARD, skip, limit, versionsRequested, afterVersions);
    }

    /**
     * Returns a copy of these options that select matching events from the lowest-position (oldest) end
     * (see {@link Direction#FORWARD}). The returned events are always in ascending position order regardless.
     */
    public DcbReadOptions forwards() {
        return new DcbReadOptions(positionRange, Direction.FORWARD, skip, limit, versionsRequested, afterVersions);
    }

    /**
     * Returns a copy of these options that skips {@code count} matching events from the selected end.
     */
    public DcbReadOptions skip(int count) {
        return new DcbReadOptions(positionRange, direction, count, limit, versionsRequested, afterVersions);
    }

    /**
     * Returns a copy of these options capped to at most {@code max} matching events.
     */
    public DcbReadOptions limit(int max) {
        return new DcbReadOptions(positionRange, direction, skip, OptionalInt.of(max), versionsRequested, afterVersions);
    }

    /**
     * Returns a copy of these options that asks the store to report the marker versions of the read's criteria in
     * {@link DcbEventStream#markerVersions()}. A snapshot saves those versions and resumes from them with
     * {@link #afterVersions(DcbMarkerVersions)}.
     * <p>
     * The store reports them only when the read has no limit, no skip, no position bound and doesn't read backwards,
     * because such a read may stop before the end of the boundary, or start after an event that commits late below
     * its lower bound. A MongoDB store reads the markers, the head and the
     * events in one short read-only transaction, so asking for versions costs a commit. A read that doesn't ask takes
     * the same path as before.
     */
    public DcbReadOptions withVersions() {
        return new DcbReadOptions(positionRange, direction, skip, limit, true, afterVersions);
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
     * Reads only the matching events appended after {@code markerVersions}, the versions a previous read or append
     * reported, and asks for the versions of this read as {@link #withVersions()} does. No position of the previous read
     * is compared, so an event that committed late, below a position the previous read saw, is still returned.
     * An event the previous read missed although its versions hold the event's markers isn't returned. A failover can
     * cause that, as {@link DcbAppendCondition} says.
     * <p>
     * The read throws {@link DcbSnapshotInvalidException} when the store can't compare the versions with its own, for
     * example after its markers were pruned or when resuming from versions is turned off. Read the boundary in full
     * with {@link #withVersions()} then. Combining this with a position bound throws {@link IllegalArgumentException}.
     * <p>
     * Pass versions only to a read on the same criteria as the read or append that reported them. A store doesn't always
     * refuse versions from other criteria. Versions from a read on {@code type("X").tags(a)} pass on a read on
     * {@code tags(a)}, which returns only what comes after them, with no exception.
     */
    public static DcbReadOptions afterVersions(DcbMarkerVersions markerVersions) {
        requireNonNull(markerVersions, "Marker versions cannot be null");
        return new DcbReadOptions(PositionRange.fromBeginning(), Direction.FORWARD, 0, OptionalInt.empty(), true, Optional.of(markerVersions));
    }

}
