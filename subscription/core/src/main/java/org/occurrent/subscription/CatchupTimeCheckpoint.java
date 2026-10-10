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

package org.occurrent.subscription;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * The {@link Checkpoint} a time-based catch-up stores while it replays history by event time. It holds the time of
 * the last event the replay handled, the live start, which is the position in the live feed the catch-up read before
 * its replay started, and the time that replay started from.
 * <p>
 * A resume replays the events at or after {@link #time()} and then goes live from that same live start. A live start
 * read after a restart would be too late for an event whose time is earlier than {@link #time()} but which was
 * written only after the earlier replay had read past that time, and that event would never be delivered. Every event
 * written after the live start arrives live, so a resume can deliver such an event twice, once from the replay and
 * once live. That includes every event written while the subscription was down.
 * <p>
 * Both times are kept in the RFC 3339 form the catch-up writes them in. The string form is
 * {@code "<time>;origin:<replay origin>;liveFrom:<live start>"}, where {@code <live start>} is the live start's own
 * {@link Checkpoint#asString()}. This lets it round-trip through a {@code CheckpointStorage} that keeps strings, which
 * reads it back as a {@link StringBasedCheckpoint} that {@link #parse(Checkpoint)} turns into a
 * {@code CatchupTimeCheckpoint} again.
 */
@NullMarked
public final class CatchupTimeCheckpoint implements Checkpoint {

    private static final String ORIGIN = ";origin:";
    private static final String LIVE_FROM = ";liveFrom:";

    private final String time;
    private final Checkpoint liveFrom;
    private final String replayOrigin;

    private CatchupTimeCheckpoint(String time, Checkpoint liveFrom, String replayOrigin) {
        this.time = requireTime(time, "time");
        this.liveFrom = Objects.requireNonNull(liveFrom, "liveFrom cannot be null");
        this.replayOrigin = requireTime(replayOrigin, "replayOrigin");
    }

    /**
     * Create a {@code CatchupTimeCheckpoint} for a catch-up whose replay has handled the events up to {@code time}.
     *
     * @param time         The time of the last event the replay handled, in RFC 3339 form
     * @param liveFrom     The position in the live feed the catch-up read before its replay started
     * @param replayOrigin The time the first attempt at the replay started from, in RFC 3339 form. A resume replays
     *                     from here again, with a new live start, when the live feed no longer has the history from
     *                     {@code liveFrom}.
     * @throws IllegalArgumentException if {@code time} or {@code replayOrigin} is not an RFC 3339 time
     */
    public static CatchupTimeCheckpoint of(String time, Checkpoint liveFrom, String replayOrigin) {
        return new CatchupTimeCheckpoint(time, liveFrom, replayOrigin);
    }

    /**
     * The time of the last event the replay handled, in RFC 3339 form.
     */
    public String time() {
        return time;
    }

    /**
     * The position in the live feed the catch-up read before its replay started.
     */
    public Checkpoint liveFrom() {
        return liveFrom;
    }

    /**
     * The time the first attempt at the replay started from, in RFC 3339 form.
     */
    public String replayOrigin() {
        return replayOrigin;
    }

    @Override
    public String asString() {
        return time + ORIGIN + replayOrigin + LIVE_FROM + liveFrom.asString();
    }

    /**
     * Whether {@code checkpoint} is a {@code CatchupTimeCheckpoint}, or a {@link StringBasedCheckpoint} holding the
     * string form of one.
     */
    public static boolean isCatchupTimeCheckpoint(Checkpoint checkpoint) {
        if (checkpoint instanceof CatchupTimeCheckpoint) {
            return true;
        }
        try {
            parse(checkpoint);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Reads a {@code CatchupTimeCheckpoint} back from {@code checkpoint}. A {@code CatchupTimeCheckpoint} is returned
     * as it is, and any other checkpoint holding the string form of one is parsed. A live start parsed this way is a
     * {@link StringBasedCheckpoint} holding the live start's string form.
     *
     * @throws IllegalArgumentException if {@code checkpoint} is not in the string form of a {@code CatchupTimeCheckpoint}
     */
    public static CatchupTimeCheckpoint parse(Checkpoint checkpoint) {
        Objects.requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        if (checkpoint instanceof CatchupTimeCheckpoint catchupTimeCheckpoint) {
            return catchupTimeCheckpoint;
        }
        String value = checkpoint.asString();
        int originAt = value.indexOf(ORIGIN);
        // The live start comes last and runs to the end, so its own string form needs no escaping
        int liveFromAt = originAt < 0 ? -1 : value.indexOf(LIVE_FROM, originAt);
        if (value.startsWith(GlobalCheckpoint.PREFIX) || liveFromAt < 0 || liveFromAt + LIVE_FROM.length() == value.length()) {
            throw notACatchupTimeCheckpoint(value);
        }
        String time = value.substring(0, originAt);
        String replayOrigin = value.substring(originAt + ORIGIN.length(), liveFromAt);
        if (!isTime(time) || !isTime(replayOrigin)) {
            throw notACatchupTimeCheckpoint(value);
        }
        return new CatchupTimeCheckpoint(time, new StringBasedCheckpoint(value.substring(liveFromAt + LIVE_FROM.length())), replayOrigin);
    }

    private static String requireTime(String time, String name) {
        Objects.requireNonNull(time, name + " cannot be null");
        if (!isTime(time)) {
            throw new IllegalArgumentException(name + " must be an RFC 3339 time, was \"" + time + "\"");
        }
        return time;
    }

    private static boolean isTime(String time) {
        try {
            OffsetDateTime.parse(time, RFC_3339_DATE_TIME_FORMATTER);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static IllegalArgumentException notACatchupTimeCheckpoint(String value) {
        return new IllegalArgumentException("Not a catch-up time checkpoint: " + value);
    }

    // The live start is compared by its string form, since the same live start is a typed checkpoint when the
    // catch-up read it and a StringBasedCheckpoint once a storage that keeps strings read it back
    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof CatchupTimeCheckpoint that)) return false;
        return time.equals(that.time) && replayOrigin.equals(that.replayOrigin) && liveFrom.asString().equals(that.liveFrom.asString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, replayOrigin, liveFrom.asString());
    }

    @Override
    public String toString() {
        return asString();
    }
}
