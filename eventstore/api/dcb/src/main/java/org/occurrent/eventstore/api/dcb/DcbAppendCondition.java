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
 * the append fails if any existing event matches {@code criteria}. When it's present, the append fails if a matching
 * event was committed after the read that returned the token (see {@link DcbEventStream#consistencyToken()}). On the
 * MongoDB stores, a tag-scoped or type-scoped append committed after a read with {@link DcbCriteria#all()} doesn't make
 * it fail (see {@link #wholeStoreLock(DcbConsistencyToken)}). A store may also make it fail when no matching event was
 * committed. The MongoDB stores can do that on a replica set, for example when a marker document was deleted after the
 * read or a 0.34 node appended after it. On anything else, such as a sharded cluster, they do that whenever a marker of
 * {@code criteria} moved after the read, for example for an event that shares a tag with {@code criteria} but doesn't
 * match it, or for another conditional append on the same criteria, whatever events it appended. The MongoDB stores
 * rely on a marker document being deleted only in a transaction that also finds no event with its tag or type, at read
 * concern snapshot on a sharded cluster, and not while a 0.34 node can still append, since 0.34 recreates a deleted
 * marker with no epoch and restarts its version at 1.
 * <p>
 * A store can answer from the events as they are when the append runs. A matching event committed after the read that
 * was deleted by then, or changed so that it no longer matches, may not make the append fail. The in-memory store
 * answers that way, and so do the MongoDB stores on a replica set when the markers don't refuse first.
 * <p>
 * A token belongs to the criteria it was read with, so the constructor throws {@link IllegalArgumentException} when the
 * token's criteria aren't the same as {@code criteria}. Every factory goes through the constructor, so this happens
 * before any store is called. Use {@link #failIfEventsMatch(DcbConsistencyToken)}, which takes the criteria from the
 * token.
 * <p>
 * Two criteria are the same when they hold the same set of alternatives, in any order. {@code anyOf(a, b)} is the same
 * as {@code anyOf(b, a)}, a {@link DcbCriterion} is the same as an {@code anyOf(..)} holding only that criterion, and
 * {@link DcbCriteria#all()} is the same only as {@code all()}. Criteria with different alternatives aren't the same,
 * even when they select the same events, so {@code types("A", "B")} isn't the same as
 * {@code anyOf(type("A"), type("B"))}.
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
     * With {@link DcbCriteria#all()} this is a whole-store optimistic lock. On the MongoDB stores it doesn't conflict
     * with a concurrent tag-scoped or type-scoped append, so use it only for single-writer operations, or for a bootstrap
     * append that races only with other bootstrap appends (see ADR 30). Use scoped {@code criteria} for a consistency
     * boundary on a store with several writers. Use {@link #wholeStoreLock()} instead of {@code failIfEventsMatch(DcbCriteria.all())},
     * so the call site says it's a whole-store lock and doesn't read like a read-everything query.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbCriteria criteria) {
        return new DcbAppendCondition(criteria, Optional.empty());
    }

    /**
     * Creates a condition that fails if an event matching the read's criteria was committed after the read that
     * returned {@code consistencyToken}. The condition's criteria are the ones the token was read with, so pass the
     * token from {@link DcbEventStream#consistencyToken()} unchanged. For a token read with {@link DcbCriteria#all()},
     * this is the same condition as {@link #wholeStoreLock(DcbConsistencyToken)}, so on the MongoDB stores a tag-scoped
     * or type-scoped append committed after the read doesn't make it fail.
     */
    public static DcbAppendCondition failIfEventsMatch(DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(consistencyToken.criteria(), Optional.of(consistencyToken));
    }

    /**
     * Creates a condition that fails if an event matching {@code criteria} was committed after the read that returned
     * {@code consistencyToken}. With {@link DcbCriteria#all()} it has the limit that
     * {@link #wholeStoreLock(DcbConsistencyToken)} describes.
     *
     * @throws IllegalArgumentException if {@code criteria} isn't the same as the criteria the token was read with
     * @deprecated Use {@link #failIfEventsMatch(DcbConsistencyToken)}, which takes the criteria from the token. This
     * form still works, and it throws the same way when the criteria aren't the same.
     */
    @Deprecated(since = "0.35.0")
    public static DcbAppendCondition failIfEventsMatch(DcbCriteria criteria, DcbConsistencyToken consistencyToken) {
        requireNonNull(consistencyToken, "Consistency token cannot be null");
        return new DcbAppendCondition(criteria, Optional.of(consistencyToken));
    }

    /**
     * Creates the whole-store optimistic lock, which fails if any DCB event exists in the store. It's the same
     * condition as {@code failIfEventsMatch(DcbCriteria.all())}, spelled out so it can't be mistaken for a
     * read-everything query at the call site.
     * <p>
     * On the MongoDB stores it doesn't conflict with a concurrent tag-scoped or type-scoped append (see ADR 30), so it's
     * correct only for single-writer operations, or for a bootstrap append that races only with other bootstrap appends.
     */
    public static DcbAppendCondition wholeStoreLock() {
        return failIfEventsMatch(DcbCriteria.all());
    }

    /**
     * Creates the whole-store optimistic lock for a prior read. The token has to come from a read with
     * {@link DcbCriteria#all()}. It's the same condition as {@link #failIfEventsMatch(DcbConsistencyToken)} with such a
     * token, spelled out so it can't be mistaken for a read-everything query at the call site.
     * <p>
     * It fails when another whole-store append was committed after the read. A tag-scoped or type-scoped append
     * committed after the read makes it fail on the in-memory store but not on the MongoDB stores (see ADR 30).
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
                    + tokenCriteria + ". To lock the whole store, read with DcbCriteria.all() before you decide, and decide from"
                    + " that read. To append on the criteria the token was read with, use"
                    + " DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken). On the MongoDB stores a whole-store lock"
                    + " doesn't conflict with a tag-scoped or type-scoped append.";
        }
        String message = "The consistency token was read with " + tokenCriteria + ", which isn't the same criteria as the append"
                + " condition's " + conditionCriteria + ". If the append needs the condition's criteria, read with them"
                + " before you decide, and decide from that read. A token read after the decision can hide an event"
                + " committed in between. If the append needs the read's criteria, use"
                + " DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken), which takes the criteria from the token.";
        if (tokenCriteria instanceof DcbCriteria.MatchAll) {
            return message + " With a token read with DcbCriteria.all() that's a whole-store lock, and on the MongoDB stores"
                    + " a whole-store lock doesn't conflict with a tag-scoped or type-scoped append.";
        }
        return message;
    }
}
