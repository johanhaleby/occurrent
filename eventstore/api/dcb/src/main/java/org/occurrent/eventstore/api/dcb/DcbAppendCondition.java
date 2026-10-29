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
 * {@code criteria} describes the events that would conflict with the append. When {@code consistencyToken} is empty,
 * the append fails if any existing event matches {@code criteria}. When it's present, the append fails only if a
 * matching event was committed after the read that returned the token (see {@link DcbEventStream#consistencyToken()}).
 * <p>
 * A token belongs to the criteria it was read with, so the constructor throws {@link IllegalArgumentException} when the
 * token's criteria aren't the same as {@code criteria}. Every factory goes through the constructor, so this happens
 * before any store is called. Use {@link #failIfEventsMatch(DcbConsistencyToken)}, which takes the criteria from the
 * token.
 * <p>
 * Two criteria are the same when they hold the same set of alternatives, in any order. {@code anyOf(a, b)} is the same
 * as {@code anyOf(b, a)}, a {@link DcbCriterion} is the same as an {@code anyOf(..)} holding only that criterion, and
 * {@link DcbCriteria#all()} is the same only as {@code all()}. Criteria that select the same events but are built
 * differently aren't the same, so {@code types("A", "B")} isn't the same as {@code anyOf(type("A"), type("B"))}.
 */
@NullMarked
public record DcbAppendCondition(DcbCriteria criteria, Optional<DcbConsistencyToken> consistencyToken) {

    public DcbAppendCondition {
        requireNonNull(criteria, "Criteria cannot be null");
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        if (consistencyToken.isPresent() && !DcbCriteriaIdentity.same(consistencyToken.get().criteria(), criteria)) {
            throw new IllegalArgumentException(tokenReadWithOtherCriteria(criteria, consistencyToken.get().criteria()));
        }
    }

    /**
     * Creates a condition that fails if any existing event matches {@code criteria}.
     * <p>
     * With {@link DcbCriteria#all()} this is a whole-store optimistic lock that doesn't conflict with a concurrent
     * tag-scoped or type-scoped append, so use it only for single-writer operations or for an append that only an empty
     * store should accept (see ADR 30). Use scoped {@code criteria} for a consistency boundary on a store with several
     * writers. Use {@link #wholeStoreLock()} instead of {@code failIfEventsMatch(DcbCriteria.all())}, so the call site says it's a
     * whole-store lock and doesn't read like a read-everything query.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbCriteria criteria) {
        return new DcbAppendCondition(criteria, Optional.empty());
    }

    /**
     * Creates a condition that fails if an event matching the read's criteria was committed after the read that
     * returned {@code consistencyToken}. The condition's criteria are the ones the token was read with, so pass the
     * token from {@link DcbEventStream#consistencyToken()} unchanged. For a token read with {@link DcbCriteria#all()},
     * this is the same condition as {@link #wholeStoreLock(DcbConsistencyToken)}.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(consistencyToken.criteria(), Optional.of(consistencyToken));
    }

    /**
     * Creates a condition that fails if an event matching {@code criteria} was committed after the read that returned
     * {@code consistencyToken}.
     *
     * @throws IllegalArgumentException if {@code criteria} isn't the same as the criteria the token was read with
     * @deprecated Use {@link #failIfEventsMatch(DcbConsistencyToken)}, which takes the criteria from the token. This
     * form still works, and it throws the same way when the criteria aren't the same.
     */
    @Deprecated
    public static DcbAppendCondition failIfEventsMatch(DcbCriteria criteria, DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(criteria, Optional.of(consistencyToken));
    }

    /**
     * Creates the whole-store optimistic lock, which fails if any DCB event exists in the store. It's the same
     * condition as {@code failIfEventsMatch(DcbCriteria.all())}, spelled out so it can't be mistaken for a
     * read-everything query at the call site.
     * <p>
     * It doesn't conflict with a concurrent tag-scoped or type-scoped append (see ADR 30), so it's correct only for
     * single-writer operations, or for a bootstrap append that only an empty store should accept.
     */
    public static DcbAppendCondition wholeStoreLock() {
        return failIfEventsMatch(DcbCriteria.all());
    }

    /**
     * Creates the whole-store optimistic lock for a prior read, which fails if any DCB event was committed after the
     * read that returned {@code consistencyToken}. The token has to come from a read with {@link DcbCriteria#all()}.
     * It's the same condition as {@link #failIfEventsMatch(DcbConsistencyToken)} with such a token, spelled out so it
     * can't be mistaken for a read-everything query at the call site.
     * <p>
     * It only detects another whole-store append, not a concurrent tag-scoped or type-scoped one (see ADR 30).
     *
     * @throws IllegalArgumentException if the token was read with other criteria than {@link DcbCriteria#all()}
     */
    public static DcbAppendCondition wholeStoreLock(DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(DcbCriteria.all(), Optional.of(consistencyToken));
    }

    private static String tokenReadWithOtherCriteria(DcbCriteria conditionCriteria, DcbCriteria tokenCriteria) {
        if (conditionCriteria instanceof DcbCriteria.MatchAll) {
            return "A whole-store lock needs a consistency token read with DcbCriteria.all(), but the token was read with "
                    + tokenCriteria + ". Read with DcbCriteria.all() to lock the whole store, or use"
                    + " DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken) to append on the criteria the token was read with.";
        }
        return "The consistency token was read with " + tokenCriteria + ", which isn't the same criteria as the append"
                + " condition's " + conditionCriteria + ". Use DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken),"
                + " which takes the criteria from the token, or read again with the condition's criteria.";
    }
}
