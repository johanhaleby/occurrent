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

/**
 * A {@link Checkpoint} that points at a global sequence position. Used by the catch-up subscription model
 * to resume a replay from where it left off.
 * <p>
 * The string form is {@code "position:<n>"}. This lets it round-trip through a {@code CheckpointStorage}
 * (which reads it back as a {@link StringBasedCheckpoint}) and stay distinguishable from a time position
 * or a change-stream resume token.
 */
@NullMarked
public class GlobalCheckpoint implements Checkpoint {

    static final String PREFIX = "position:";

    private final long position;

    public GlobalCheckpoint(long position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative");
        }
        this.position = position;
    }

    /**
     * Create a {@code GlobalCheckpoint} at the given global sequence position. Use {@code 0} to replay from
     * the beginning of the sequence (positions are assigned from {@code 1}).
     */
    public static GlobalCheckpoint of(long position) {
        return new GlobalCheckpoint(position);
    }

    /**
     * The global sequence position this checkpoint points at.
     */
    public long position() {
        return position;
    }

    @Override
    public String asString() {
        return PREFIX + position;
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
        if (checkpoint instanceof GlobalCheckpoint global) {
            return global.position();
        }
        String value = checkpoint.asString();
        if (!value.startsWith(PREFIX)) {
            throw new IllegalArgumentException("Not a global checkpoint: " + value);
        }
        return Long.parseLong(value.substring(PREFIX.length()));
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof GlobalCheckpoint that)) return false;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(position);
    }

    @Override
    public String toString() {
        return asString();
    }
}
