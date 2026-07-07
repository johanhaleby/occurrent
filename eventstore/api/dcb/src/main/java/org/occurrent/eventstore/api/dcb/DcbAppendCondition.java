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
public record DcbAppendCondition(DcbCriteria query, Optional<DcbConsistencyToken> consistencyToken) {

    public DcbAppendCondition {
        requireNonNull(query, "Query cannot be null");
        requireNonNull(consistencyToken, "Consistency token cannot be null");
    }

    /**
     * Creates a condition that fails if any existing event matches {@code query}.
     * <p>
     * A {@code MatchAll} query (from {@link DcbCriteria#all()}) makes this a whole-store optimistic lock that is not
     * skew-safe against concurrent tag-scoped or type-scoped appends, so use it only for single-writer or empty-store
     * guards (see ADR 30). Prefer a scoped {@code query} for a real consistency boundary on a multi-writer store. Prefer
     * {@link #wholeStoreLock()} over spelling this out as {@code failIfEventsMatch(DcbCriteria.all())}: the dedicated
     * factory names the whole-store boundary explicitly at the call site instead of relying on a reader recognizing
     * {@code all()} as a lock rather than a read-everything query.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbCriteria query) {
        return new DcbAppendCondition(query, Optional.empty());
    }

    /**
     * Creates a condition that fails if an event matching {@code query} was committed after the read that produced
     * {@code consistencyToken}. Prefer {@link #wholeStoreLock(DcbConsistencyToken)} when {@code query} would otherwise
     * be {@link DcbCriteria#all()}.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbCriteria query, DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(query, Optional.of(consistencyToken));
    }

    /**
     * Creates the whole-store optimistic lock: fails if any DCB event exists in the store. Equivalent to
     * {@code failIfEventsMatch(DcbCriteria.all())}, spelled out explicitly so it cannot be mistaken for a
     * read-everything query at the append-condition call site.
     * <p>
     * This is not skew-safe against a concurrent tag-scoped or type-scoped append (see ADR 30): it is correct only for
     * single-writer operations or an empty-store or bootstrap guard.
     */
    public static DcbAppendCondition wholeStoreLock() {
        return failIfEventsMatch(DcbCriteria.all());
    }

    /**
     * Creates the whole-store optimistic lock qualified by a prior read's {@code consistencyToken}: fails if any DCB
     * event was committed after that read. Equivalent to {@code failIfEventsMatch(DcbCriteria.all(), consistencyToken)},
     * spelled out explicitly so it cannot be mistaken for a read-everything query at the append-condition call site.
     * <p>
     * This is not skew-safe against a concurrent tag-scoped or type-scoped append (see ADR 30): it only detects another
     * whole-store append, not a scoped one.
     */
    public static DcbAppendCondition wholeStoreLock(DcbConsistencyToken consistencyToken) {
        return failIfEventsMatch(DcbCriteria.all(), consistencyToken);
    }
}
