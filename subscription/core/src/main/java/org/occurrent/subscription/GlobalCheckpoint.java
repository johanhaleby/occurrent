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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A {@link Checkpoint} that points at a global sequence position. Used by the catch-up subscription model
 * to resume a replay from where it left off.
 * <p>
 * A checkpoint stored while a catch-up replays history also holds the live start, which is the position in the
 * live feed the catch-up read before its replay started, the position the replay started from, and the replay end,
 * which is the head of the global sequence the catch-up read after it read the live start. A resume replays from
 * {@link #position()} up to the replay end and then goes live from that same live start. A live start read after a
 * restart would be too late for an event whose position was reserved below {@link #position()} but written only after
 * the earlier replay had read past it, and that event would never be delivered. An event above the replay end had its
 * position reserved after the live start was read, so it was written after the live start and arrives live.
 * <p>
 * The string form is {@code "position:<n>"}, or {@code "position:<n>;origin:<s>;replayTo:<h>;liveFrom:<live start>"}
 * when the checkpoint has a live start, where {@code <live start>} is the live start's own
 * {@link Checkpoint#asString()}.
 * This lets it round-trip through a {@code CheckpointStorage}, which reads it back as a {@link StringBasedCheckpoint}
 * that {@link #parse(Checkpoint)} turns into a {@code GlobalCheckpoint} again, and stay distinguishable from a time
 * position or a change-stream resume token.
 */
@NullMarked
public class GlobalCheckpoint implements Checkpoint {

    static final String PREFIX = "position:";
    private static final String ORIGIN = ";origin:";
    private static final String REPLAY_TO = ";replayTo:";
    private static final String LIVE_FROM = ";liveFrom:";

    private final long position;
    private final @Nullable Checkpoint liveFrom;
    private final long replayOrigin;
    private final long replayTo;

    public GlobalCheckpoint(long position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative");
        }
        this.position = position;
        this.liveFrom = null;
        this.replayOrigin = position;
        this.replayTo = position;
    }

    private GlobalCheckpoint(long position, Checkpoint liveFrom, long replayOrigin, long replayTo) {
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative");
        }
        if (replayOrigin < 0 || replayOrigin > position) {
            throw new IllegalArgumentException("Replay origin must be between 0 and the position " + position + ", was " + replayOrigin);
        }
        if (replayTo < 0) {
            throw new IllegalArgumentException("Replay end cannot be negative, was " + replayTo);
        }
        this.position = position;
        this.liveFrom = Objects.requireNonNull(liveFrom, "liveFrom cannot be null");
        this.replayOrigin = replayOrigin;
        this.replayTo = replayTo;
    }

    /**
     * Create a {@code GlobalCheckpoint} at the given global sequence position. Use {@code 0} to replay from
     * the beginning of the sequence (positions are assigned from {@code 1}).
     */
    public static GlobalCheckpoint of(long position) {
        return new GlobalCheckpoint(position);
    }

    /**
     * Create a {@code GlobalCheckpoint} for a catch-up that has replayed up to {@code position}. A resume from it
     * replays from {@code position} up to {@code replayTo} and then goes live from {@code liveFrom}.
     *
     * @param position     The global sequence position the replay has delivered up to
     * @param liveFrom     The position in the live feed the catch-up read before its replay started
     * @param replayOrigin The global sequence position the replay started from. A resume replays from here again,
     *                     with a new live start, when the live feed no longer has the history from {@code liveFrom}.
     * @param replayTo     The head of the global sequence the catch-up read after it read {@code liveFrom}. It can be
     *                     below {@code position}, since the replay goes on to read events written while it ran.
     * @throws IllegalArgumentException if {@code replayOrigin} is negative or greater than {@code position}, or if
     *                                  {@code replayTo} is negative
     */
    public static GlobalCheckpoint of(long position, Checkpoint liveFrom, long replayOrigin, long replayTo) {
        return new GlobalCheckpoint(position, liveFrom, replayOrigin, replayTo);
    }

    /**
     * The global sequence position this checkpoint points at.
     */
    public long position() {
        return position;
    }

    /**
     * The position in the live feed the catch-up read before its replay started, or empty for a checkpoint created
     * with {@link #of(long)}.
     */
    public Optional<Checkpoint> liveFrom() {
        return Optional.ofNullable(liveFrom);
    }

    /**
     * The global sequence position the replay started from, or empty for a checkpoint created with {@link #of(long)}.
     */
    public OptionalLong replayOrigin() {
        return liveFrom == null ? OptionalLong.empty() : OptionalLong.of(replayOrigin);
    }

    /**
     * The head of the global sequence the catch-up read after it read {@link #liveFrom()}, or empty for a checkpoint
     * created with {@link #of(long)}. A resume replays no further than this, since every event above it arrives live.
     */
    public OptionalLong replayTo() {
        return liveFrom == null ? OptionalLong.empty() : OptionalLong.of(replayTo);
    }

    @Override
    public String asString() {
        if (liveFrom == null) {
            return PREFIX + position;
        }
        return PREFIX + position + ORIGIN + replayOrigin + REPLAY_TO + replayTo + LIVE_FROM + liveFrom.asString();
    }

    /**
     * Whether the supplied position is a global sequence position, either a {@link GlobalCheckpoint} or a
     * {@link StringBasedCheckpoint} written by one (the form read back from storage).
     */
    public static boolean isGlobalCheckpoint(Checkpoint checkpoint) {
        return checkpoint instanceof GlobalCheckpoint ||
                (checkpoint instanceof StringBasedCheckpoint && checkpoint.asString().startsWith(PREFIX));
    }

    /**
     * Reads the global sequence position out of a position produced by a {@link GlobalCheckpoint}, whether
     * it is still one or has been read back from storage as a {@link StringBasedCheckpoint}.
     */
    public static long positionOf(Checkpoint checkpoint) {
        return parse(checkpoint).position();
    }

    /**
     * Reads a {@code GlobalCheckpoint} back from {@code checkpoint}. A {@code GlobalCheckpoint} is returned as it is,
     * and a {@link StringBasedCheckpoint} holding the string form of one is parsed. A live start parsed this way is a
     * {@link StringBasedCheckpoint} holding the live start's string form. A string form with a live start but no
     * replay end gives a {@code GlobalCheckpoint} without a live start, so a resume from it reads a live start of its
     * own, as from a plain position.
     *
     * @throws IllegalArgumentException if {@code checkpoint} is not in the string form of a {@code GlobalCheckpoint}
     */
    public static GlobalCheckpoint parse(Checkpoint checkpoint) {
        Objects.requireNonNull(checkpoint, Checkpoint.class.getSimpleName() + " cannot be null");
        if (checkpoint instanceof GlobalCheckpoint global) {
            return global;
        }
        String value = checkpoint.asString();
        if (!value.startsWith(PREFIX)) {
            throw notAGlobalCheckpoint(value, null);
        }
        int originAt = value.indexOf(ORIGIN);
        if (originAt < 0) {
            return new GlobalCheckpoint(parseLong(value.substring(PREFIX.length()), value));
        }
        // The live start comes last and runs to the end, so its own string form needs no escaping
        int liveFromAt = value.indexOf(LIVE_FROM, originAt);
        if (liveFromAt < 0 || liveFromAt + LIVE_FROM.length() == value.length()) {
            throw notAGlobalCheckpoint(value, null);
        }
        long position = parseLong(value.substring(PREFIX.length(), originAt), value);
        int replayToAt = value.substring(0, liveFromAt).indexOf(REPLAY_TO, originAt);
        long replayOrigin = parseLong(value.substring(originAt + ORIGIN.length(), replayToAt < 0 ? liveFromAt : replayToAt), value);
        if (position < 0 || replayOrigin < 0 || replayOrigin > position) {
            throw notAGlobalCheckpoint(value, null);
        }
        if (replayToAt < 0) {
            return new GlobalCheckpoint(position);
        }
        long replayTo = parseLong(value.substring(replayToAt + REPLAY_TO.length(), liveFromAt), value);
        if (replayTo < 0) {
            throw notAGlobalCheckpoint(value, null);
        }
        return new GlobalCheckpoint(position, new StringBasedCheckpoint(value.substring(liveFromAt + LIVE_FROM.length())), replayOrigin, replayTo);
    }

    private static long parseLong(String number, String value) {
        try {
            return Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw notAGlobalCheckpoint(value, e);
        }
    }

    private static IllegalArgumentException notAGlobalCheckpoint(String value, @Nullable Throwable cause) {
        return new IllegalArgumentException("Not a global checkpoint: " + value, cause);
    }

    // The live start is compared by its string form, since the same live start is a typed checkpoint when the
    // catch-up read it and a StringBasedCheckpoint once a storage that keeps strings read it back
    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof GlobalCheckpoint that)) return false;
        return position == that.position && replayOrigin == that.replayOrigin && replayTo == that.replayTo
                && Objects.equals(liveFromString(), that.liveFromString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, replayOrigin, replayTo, liveFromString());
    }

    private @Nullable String liveFromString() {
        return liveFrom == null ? null : liveFrom.asString();
    }

    @Override
    public String toString() {
        return asString();
    }
}
