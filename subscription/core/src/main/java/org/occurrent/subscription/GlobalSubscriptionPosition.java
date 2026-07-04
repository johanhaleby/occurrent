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
 * A {@link SubscriptionPosition} that points at a global sequence position. Used by the catch-up subscription model
 * to resume a replay from where it left off.
 * <p>
 * The string form is {@code "position:<n>"}. This lets it round-trip through a {@code SubscriptionPositionStorage}
 * (which reads it back as a {@link StringBasedSubscriptionPosition}) and stay distinguishable from a time position
 * or a change-stream resume token.
 */
@NullMarked
public class GlobalSubscriptionPosition implements SubscriptionPosition {

    static final String PREFIX = "position:";

    private final long position;

    public GlobalSubscriptionPosition(long position) {
        if (position < 0) {
            throw new IllegalArgumentException("Position cannot be negative");
        }
        this.position = position;
    }

    /**
     * Create a {@code GlobalSubscriptionPosition} at the given global sequence position. Use {@code 0} to replay from
     * the beginning of the sequence (positions are assigned from {@code 1}).
     */
    public static GlobalSubscriptionPosition of(long position) {
        return new GlobalSubscriptionPosition(position);
    }

    /**
     * The global sequence position this subscription position points at.
     */
    public long position() {
        return position;
    }

    @Override
    public String asString() {
        return PREFIX + position;
    }

    /**
     * Whether the supplied position is a global sequence position, either a {@link GlobalSubscriptionPosition} or a
     * {@link StringBasedSubscriptionPosition} written by one (the form read back from storage).
     */
    public static boolean isGlobalSubscriptionPosition(SubscriptionPosition subscriptionPosition) {
        return subscriptionPosition instanceof GlobalSubscriptionPosition ||
                (subscriptionPosition instanceof StringBasedSubscriptionPosition && subscriptionPosition.asString().startsWith(PREFIX));
    }

    /**
     * Reads the global sequence position out of a position produced by a {@link GlobalSubscriptionPosition}, whether
     * it is still one or has been read back from storage as a {@link StringBasedSubscriptionPosition}.
     */
    public static long positionOf(SubscriptionPosition subscriptionPosition) {
        if (subscriptionPosition instanceof GlobalSubscriptionPosition global) {
            return global.position();
        }
        String value = subscriptionPosition.asString();
        if (!value.startsWith(PREFIX)) {
            throw new IllegalArgumentException("Not a global subscription position: " + value);
        }
        return Long.parseLong(value.substring(PREFIX.length()));
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof GlobalSubscriptionPosition that)) return false;
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
