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
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Decides whether two criteria are the same, for a {@link DcbConsistencyToken} and the {@link DcbAppendCondition} it is
 * used in.
 * <p>
 * Two criteria are the same when they hold the same set of alternatives. {@link DcbCriteria#all()} is the same only as
 * {@code all()}, a {@link DcbCriterion} is the same as an {@link DcbCriteria.Items} holding only that criterion, and two
 * {@code Items} are the same when they hold the same criteria in any order. Criteria that select the same events but are
 * built differently aren't the same, so {@code types("A", "B")} isn't the same as {@code anyOf(type("A"), type("B"))}.
 * {@link DcbCriteria}'s own {@code equals} still compares an {@code Items} in order.
 */
@NullMarked
final class DcbCriteriaIdentity {

    private DcbCriteriaIdentity() {
    }

    static boolean same(DcbCriteria first, DcbCriteria second) {
        return alternatives(first).equals(alternatives(second));
    }

    static int hash(DcbCriteria criteria) {
        return alternatives(criteria).hashCode();
    }

    // Empty for MatchAll, so it is the same only as another MatchAll
    private static Optional<Set<DcbCriterion>> alternatives(DcbCriteria criteria) {
        requireNonNull(criteria, "Criteria cannot be null");
        return switch (criteria) {
            case DcbCriteria.MatchAll ignored -> Optional.empty();
            case DcbCriterion criterion -> Optional.of(Set.of(criterion));
            case DcbCriteria.Items items -> Optional.of(Set.copyOf(items.items()));
        };
    }
}
