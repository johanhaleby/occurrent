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

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Optimistic conflict condition for a DCB append.
 * <p>
 * {@code failIfEventsMatch} describes the events that would conflict with the append.
 * {@code afterSequencePosition} optionally limits the conflict check to events written
 * after a position observed by a prior read.
 */
@NullMarked
public record DcbAppendCondition(DcbQuery failIfEventsMatch, OptionalLong afterSequencePosition) {

    public DcbAppendCondition {
        requireNonNull(failIfEventsMatch, "Fail-if-events-match query cannot be null");
        requireNonNull(afterSequencePosition, "After sequence position cannot be null");
        afterSequencePosition.ifPresent(position -> {
            if (position < 0) {
                throw new IllegalArgumentException("After sequence position cannot be negative");
            }
        });
    }

    /**
     * Creates a condition that fails if any existing event matches {@code query}.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbQuery query) {
        return new DcbAppendCondition(query, OptionalLong.empty());
    }

    /**
     * Creates a condition that fails if an event after {@code afterSequencePosition} matches {@code query}.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbQuery query, long afterSequencePosition) {
        return new DcbAppendCondition(query, OptionalLong.of(afterSequencePosition));
    }
}
