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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Optimistic conflict condition for a DCB append.
 * <p>
 * {@code query} describes the events that would conflict with the append. {@code consistencyToken} optionally limits the
 * conflict check to events committed after a boundary observed by a prior read (see
 * {@link DcbEventStream#consistencyToken()}); when empty, the append fails if any existing event matches {@code query}.
 */
@NullMarked
public record DcbAppendCondition(DcbQuery query, Optional<DcbConsistencyToken> consistencyToken) {

    public DcbAppendCondition {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(consistencyToken, "Consistency token cannot be null");
    }

    /**
     * Creates a condition that fails if any existing event matches {@code query}.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbQuery query) {
        return new DcbAppendCondition(query, Optional.empty());
    }

    /**
     * Creates a condition that fails if an event matching {@code query} was committed after the read that produced
     * {@code consistencyToken}.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbQuery query, DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(query, Optional.of(consistencyToken));
    }
}
