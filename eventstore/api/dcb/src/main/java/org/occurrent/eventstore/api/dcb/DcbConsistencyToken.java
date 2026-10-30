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
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * What a DCB read saw of the events matching its criteria, which a later conditional append is checked against.
 * <p>
 * {@link DcbEventStore#read(DcbCriteria, DcbReadOptions)} returns a token on {@link DcbEventStream#consistencyToken()}.
 * Pass it to {@link DcbAppendCondition#failIfEventsMatch(DcbConsistencyToken)}, and the store refuses the append when an
 * event matching the read's criteria was committed after the read. For a token read with
 * {@link DcbCriteria#all()}, the MongoDB stores don't refuse the append for an event committed after the read by an
 * append with no condition on {@code all()}, which {@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)}
 * describes. A store can also refuse the append when no matching event was committed, which
 * {@link DcbAppendCondition} describes.
 * <p>
 * The token holds the {@link #criteria() criteria} it was read with. An append condition with other criteria throws
 * {@link IllegalArgumentException} when it's created, on every store, so code that reads with one criteria and appends
 * with another, passing the token the read returned, fails before it reaches the store.
 * <p>
 * The {@link #version() version} is defined by the store that created the token and means nothing outside it. Only
 * store implementations read it. Don't save it to turn it back into a token later, and don't assume the criteria and
 * the version are all a token holds. A token value saved under Occurrent 0.34 isn't valid on 0.35, so read again
 * instead.
 * <p>
 * It's a separate type from the {@link DcbEventStream#lastSequencePosition() sequence position} so the two can't be
 * mixed up. The position is the store's head, which works for replay but doesn't protect an append on its own.
 */
@NullMarked
public final class DcbConsistencyToken {

    private final DcbCriteria criteria;
    private final long version;

    private DcbConsistencyToken(DcbCriteria criteria, long version) {
        this.criteria = criteria;
        this.version = version;
    }

    /**
     * Creates a token for a read with {@code criteria}. A {@link DcbEventStore} implementation calls it with the
     * criteria the read was given and a version it can check a later append against.
     *
     * @param criteria the criteria the read was given
     * @param version  the store's version for the read, never negative
     * @throws IllegalArgumentException if {@code version} is negative
     */
    public static DcbConsistencyToken of(DcbCriteria criteria, long version) {
        requireNonNull(criteria, "Criteria cannot be null");
        if (version < 0) {
            throw new IllegalArgumentException("Consistency token version cannot be negative");
        }
        return new DcbConsistencyToken(criteria, version);
    }

    /**
     * The criteria the read that returned this token was given.
     * {@link DcbAppendCondition#failIfEventsMatch(DcbConsistencyToken)} uses them as the condition's criteria.
     */
    public DcbCriteria criteria() {
        return criteria;
    }

    /**
     * The store's version for the read. Only the store that created the token knows what it means, so code outside a
     * store shouldn't read it or keep it.
     */
    public long version() {
        return version;
    }

    /**
     * Tokens whose criteria aren't the same, or whose versions differ, aren't equal. Two criteria are the same when they
     * hold the same set of alternatives, as {@link DcbAppendCondition} describes.
     */
    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DcbConsistencyToken other)) {
            return false;
        }
        return version == other.version && DcbCriteriaIdentity.same(criteria, other.criteria);
    }

    @Override
    public int hashCode() {
        return 31 * DcbCriteriaIdentity.hash(criteria) + Long.hashCode(version);
    }

    @Override
    public String toString() {
        return "DcbConsistencyToken[criteria=" + criteria + ", version=" + version + "]";
    }
}
