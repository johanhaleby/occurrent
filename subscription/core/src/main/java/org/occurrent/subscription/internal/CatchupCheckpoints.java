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

package org.occurrent.subscription.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

/**
 * Builds and reads the checkpoints a catch-up stores during its replay. The marked forms
 * {@code position:<n>;v:2;origin:<o>;replayTo:<h>;liveFrom:<l>} and
 * {@code position:time;v:2;at:<t>;origin:<o>;liveFrom:<l>} are written where a reader from before them would resume
 * the checkpoint without the live start it holds, and that reader throws on both. A caller wraps the string in a
 * {@link StringBasedCheckpoint}, so a marked form is never a {@link GlobalCheckpoint} instance.
 */
public final class CatchupCheckpoints {

    private static final String POSITION = "position:";
    private static final String MARKER = ";v:2";
    private static final String TIME = POSITION + "time" + MARKER + ";at:";
    private static final String ORIGIN = ";origin:";
    private static final String REPLAY_TO = ";replayTo:";
    private static final String LIVE_FROM = ";liveFrom:";

    private CatchupCheckpoints() {
    }

    /**
     * The marked form of a position replay that has delivered through {@code position}.
     *
     * @param position     The global sequence position the replay has delivered through
     * @param replayOrigin The global sequence position the first attempt at the replay started from
     * @param replayTo     The head of the global sequence the replay read after it read {@code liveFrom}
     * @param liveFrom     The live start the replay goes live from
     */
    public static String format(long position, long replayOrigin, long replayTo, Checkpoint liveFrom) {
        return POSITION + position + MARKER + ORIGIN + replayOrigin + REPLAY_TO + replayTo + LIVE_FROM + liveFrom.asString();
    }

    /**
     * The marked form of a time replay that has delivered every event before {@code time}.
     *
     * @param time         The RFC 3339 time below which the replay has delivered every event
     * @param replayOrigin The RFC 3339 time the first attempt at the replay started from
     * @param liveFrom     The live start the replay goes live from
     */
    public static String format(String time, String replayOrigin, Checkpoint liveFrom) {
        return TIME + time + ORIGIN + replayOrigin + LIVE_FROM + liveFrom.asString();
    }

    /**
     * Whether {@code checkpoint} is in one of the marked forms, judged by its string form.
     */
    public static boolean isMarked(Checkpoint checkpoint) {
        String value = checkpoint.asString();
        if (!value.startsWith(POSITION)) {
            return false;
        }
        int firstSegmentEnd = value.indexOf(';');
        return firstSegmentEnd >= 0 && value.startsWith(MARKER + ";", firstSegmentEnd);
    }

    /**
     * Whether {@code checkpoint} is the marked form of a time replay, judged by its string form. Asked before any test
     * of the {@code position:} prefix, since that form starts with it too.
     */
    public static boolean isMarkedTime(Checkpoint checkpoint) {
        return checkpoint.asString().startsWith(TIME);
    }

    /**
     * Reads where a catch-up replay starts from {@code checkpoint}: a marked form, a {@link GlobalCheckpoint} with or
     * without a live start, a {@link CatchupTimeCheckpoint}, or a bare RFC 3339 time. Each is read by its string form,
     * so the same checkpoint read back from storage as a {@link StringBasedCheckpoint} reads the same.
     *
     * @throws IllegalArgumentException if {@code checkpoint} is none of those
     */
    public static Start parse(Checkpoint checkpoint) {
        Objects.requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        String value = checkpoint.asString();
        if (value.startsWith(TIME)) {
            return parseMarkedTime(value);
        }
        if (isMarked(checkpoint)) {
            return parseMarkedPosition(value);
        }
        if (value.startsWith(POSITION)) {
            GlobalCheckpoint global = GlobalCheckpoint.parse(checkpoint);
            return new Position(global.position(), global.liveFrom().orElse(null), global.replayOrigin().orElse(global.position()),
                    global.replayTo().isPresent() ? global.replayTo().getAsLong() : null);
        }
        if (CatchupTimeCheckpoint.isCatchupTimeCheckpoint(checkpoint)) {
            CatchupTimeCheckpoint time = CatchupTimeCheckpoint.parse(checkpoint);
            return new Time(time.time(), time.liveFrom(), time.replayOrigin());
        }
        if (isTime(value)) {
            return new Time(value, null, value);
        }
        throw notACatchupCheckpoint(value, null);
    }

    private static Position parseMarkedPosition(String value) {
        int originAt = value.indexOf(MARKER + ORIGIN);
        int replayToAt = value.indexOf(REPLAY_TO, originAt);
        int liveFromAt = replayToAt < 0 ? -1 : value.indexOf(LIVE_FROM, replayToAt);
        if (originAt < 0 || liveFromAt < 0 || liveFromAt + LIVE_FROM.length() == value.length()) {
            throw notACatchupCheckpoint(value, null);
        }
        long position = parseLong(value.substring(POSITION.length(), originAt), value);
        long replayOrigin = parseLong(value.substring(originAt + MARKER.length() + ORIGIN.length(), replayToAt), value);
        long replayTo = parseLong(value.substring(replayToAt + REPLAY_TO.length(), liveFromAt), value);
        if (position < 0 || replayOrigin < 0 || replayOrigin > position || replayTo < 0) {
            throw notACatchupCheckpoint(value, null);
        }
        return new Position(position, new StringBasedCheckpoint(value.substring(liveFromAt + LIVE_FROM.length())), replayOrigin, replayTo);
    }

    private static Time parseMarkedTime(String value) {
        int originAt = value.indexOf(ORIGIN, TIME.length());
        // The live start comes last and runs to the end, so its own string form needs no escaping
        int liveFromAt = originAt < 0 ? -1 : value.indexOf(LIVE_FROM, originAt);
        if (liveFromAt < 0 || liveFromAt + LIVE_FROM.length() == value.length()) {
            throw notACatchupCheckpoint(value, null);
        }
        String time = value.substring(TIME.length(), originAt);
        String replayOrigin = value.substring(originAt + ORIGIN.length(), liveFromAt);
        if (!isTime(time) || !isTime(replayOrigin)) {
            throw notACatchupCheckpoint(value, null);
        }
        return new Time(time, new StringBasedCheckpoint(value.substring(liveFromAt + LIVE_FROM.length())), replayOrigin);
    }

    private static long parseLong(String number, String value) {
        try {
            return Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw notACatchupCheckpoint(value, e);
        }
    }

    private static boolean isTime(String time) {
        try {
            OffsetDateTime.parse(time, RFC_3339_DATE_TIME_FORMATTER);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static IllegalArgumentException notACatchupCheckpoint(String value, @Nullable Throwable cause) {
        return new IllegalArgumentException("Not a catch-up checkpoint: " + value, cause);
    }

    /**
     * Where a catch-up replay starts, read by {@link #parse(Checkpoint)}.
     */
    public sealed interface Start permits Position, Time {
        /**
         * The live start an earlier attempt stored, or null when it stored none
         */
        @Nullable Checkpoint liveFrom();
    }

    /**
     * The start of a position replay.
     *
     * @param position     The global sequence position the replay reads after
     * @param liveFrom     The live start an earlier attempt stored, or null when it stored none
     * @param replayOrigin The global sequence position the first attempt at the replay started from, which is
     *                     {@code position} when no earlier attempt stored one
     * @param replayTo     The head of the global sequence the earlier attempt read after it read {@code liveFrom}, or
     *                     null when there is no {@code liveFrom}
     */
    public record Position(long position, @Nullable Checkpoint liveFrom, long replayOrigin, @Nullable Long replayTo) implements Start {
    }

    /**
     * The start of a time replay.
     *
     * @param time         The RFC 3339 time the replay reads from, inclusive
     * @param liveFrom     The live start an earlier attempt stored, or null when it stored none
     * @param replayOrigin The RFC 3339 time the first attempt at the replay started from, which is {@code time} when no
     *                     earlier attempt stored one
     */
    public record Time(String time, @Nullable Checkpoint liveFrom, String replayOrigin) implements Start {
    }
}
