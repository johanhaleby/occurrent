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

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.StringBasedSubscriptionPosition;
import org.occurrent.subscription.SubscriptionPosition;

import java.util.Objects;

/**
 * A {@link SubscriptionPosition} that points at a DCB sequence position ({@code dcbposition}). It is used by
 * {@link CatchupSubscriptionModel} in DCB mode to resume a catch-up replay from where it left off.
 * <p>
 * The string form is self-describing ({@code "dcbposition:<n>"}) so that it round-trips through a
 * {@code SubscriptionPositionStorage} (which reads positions back as a {@link StringBasedSubscriptionPosition}) and
 * stays unambiguously distinguishable from an RFC3339 time position and from a database change-stream resume token.
 */
@NullMarked
public class DcbSubscriptionPosition implements SubscriptionPosition {

    static final String PREFIX = "dcbposition:";

    private final long dcbPosition;

    public DcbSubscriptionPosition(long dcbPosition) {
        if (dcbPosition < 0) {
            throw new IllegalArgumentException("DCB position cannot be negative");
        }
        this.dcbPosition = dcbPosition;
    }

    /**
     * Create a {@code DcbSubscriptionPosition} at the given DCB sequence position. Use {@code 0} to replay from the
     * beginning of the DCB sequence (positions are assigned from {@code 1}).
     */
    public static DcbSubscriptionPosition of(long dcbPosition) {
        return new DcbSubscriptionPosition(dcbPosition);
    }

    /**
     * The DCB sequence position this subscription position points at.
     */
    public long dcbPosition() {
        return dcbPosition;
    }

    @Override
    public String asString() {
        return PREFIX + dcbPosition;
    }

    /**
     * Returns whether the supplied subscription position is a DCB sequence position, either a
     * {@link DcbSubscriptionPosition} or a {@link StringBasedSubscriptionPosition} whose string form was written by one
     * (the form a {@code SubscriptionPositionStorage} reads back).
     */
    public static boolean isDcbSubscriptionPosition(SubscriptionPosition subscriptionPosition) {
        return subscriptionPosition instanceof DcbSubscriptionPosition ||
                (subscriptionPosition instanceof StringBasedSubscriptionPosition && subscriptionPosition.asString().startsWith(PREFIX));
    }

    /**
     * Reads the DCB sequence position out of a subscription position previously produced by a
     * {@link DcbSubscriptionPosition}, whether it is still a {@code DcbSubscriptionPosition} or has been read back from
     * storage as a {@link StringBasedSubscriptionPosition}.
     */
    public static long dcbPositionOf(SubscriptionPosition subscriptionPosition) {
        if (subscriptionPosition instanceof DcbSubscriptionPosition dcb) {
            return dcb.dcbPosition();
        }
        String value = subscriptionPosition.asString();
        if (!value.startsWith(PREFIX)) {
            throw new IllegalArgumentException("Not a DCB subscription position: " + value);
        }
        return Long.parseLong(value.substring(PREFIX.length()));
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof DcbSubscriptionPosition that)) return false;
        return dcbPosition == that.dcbPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dcbPosition);
    }

    @Override
    public String toString() {
        return asString();
    }
}
