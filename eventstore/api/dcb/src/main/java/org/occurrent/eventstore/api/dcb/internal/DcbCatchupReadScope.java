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

package org.occurrent.eventstore.api.dcb.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. The reads of one DCB catch-up replay attempt are the live start, the head and
 * every replay read. A store that reports a live start makes these reads see everything that committed before it. Close
 * the scope when the attempt ends.
 */
@NullMarked
public interface DcbCatchupReadScope extends AutoCloseable {

    /**
     * The live start the catch-up hands to the subscription model it wraps, so that every event committed after the
     * scope opened is delivered live. {@code null} when the store reports none, and the catch-up then asks the wrapped
     * subscription model for it.
     */
    @Nullable String liveStart();

    /**
     * Reads the events matching {@code criteria} in a position range, and reports the head of the DCB sequence as seen by
     * this scope in {@link DcbEventStream#lastSequencePosition()}. Only {@link DcbReadOptions#between(long, long)} style
     * options are allowed, so a read that asks for marker versions, skips, limits or reads backwards throws
     * {@link IllegalArgumentException}.
     */
    DcbEventStream read(DcbCriteria criteria, DcbReadOptions options);

    /**
     * Ends the scope and releases what it holds. Reading after this is not allowed.
     */
    @Override
    void close();

    /**
     * Returns {@code options} if it is a plain position range, otherwise throws {@link IllegalArgumentException}.
     */
    static DcbReadOptions requirePositionRange(DcbReadOptions options) {
        requireNonNull(options, "Read options cannot be null");
        if (options.versionsRequested() || options.direction() != DcbReadOptions.Direction.FORWARD || options.skip() != 0 || options.limit().isPresent()) {
            throw new IllegalArgumentException("A catch-up read scope reads position ranges only, built with " + DcbReadOptions.class.getSimpleName() + ".between(..), but got " + options);
        }
        return options;
    }
}
