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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Decides whether two criteria are the same, for a {@link DcbConsistencyToken} and the {@link DcbAppendCondition} it is
 * used in.
 * <p>
 * Two criteria are the same when they hold the same set of alternatives. {@link DcbCriteria#all()} is the same only as
 * {@code all()}, a {@link DcbCriterion} is the same as an {@link DcbCriteria.Items} holding only that criterion, and two
 * {@code Items} are the same when they hold the same criteria in any order. Criteria with different alternatives aren't
 * the same, even when they select the same events, so {@code types("A", "B")} isn't the same as
 * {@code anyOf(type("A"), type("B"))}.
 * {@link DcbCriteria}'s own {@code equals} still compares an {@code Items} in order.
 * <p>
 * {@link #canonical(DcbCriteria)} gives the same rule as a string, which {@link DcbMarkerVersions} keeps, so two criteria
 * are the same exactly when their canonical forms are equal.
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

    // all for MatchAll, otherwise a JSON array of the distinct alternatives in sorted order, each with its types, tags and
    // excluded types sorted. Saved DcbMarkerVersions of criteria whose string changes no longer resume
    static String canonical(DcbCriteria criteria) {
        return alternatives(criteria)
                .map(items -> items.stream().map(DcbCriteriaIdentity::canonical).collect(Collectors.toCollection(TreeSet::new)))
                .map(items -> String.join(",", items))
                .map(items -> "[" + items + "]")
                .orElse("all");
    }

    private static String canonical(DcbCriterion criterion) {
        return "{\"types\":" + array(criterion.types())
                + ",\"tags\":" + array(criterion.tags().stream().map(Tag::canonical).toList())
                + ",\"excludedTypes\":" + array(criterion.excludedTypes()) + "}";
    }

    private static String array(Collection<String> values) {
        return values.stream().sorted().map(DcbCriteriaIdentity::quoted).collect(Collectors.joining(",", "[", "]"));
    }

    private static String quoted(String value) {
        StringBuilder quoted = new StringBuilder(value.length() + 2).append('"');
        value.chars().forEach(c -> {
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                default -> {
                    if (c < 0x20) {
                        quoted.append(String.format("\\u%04x", c));
                    } else {
                        quoted.append((char) c);
                    }
                }
            }
        });
        return quoted.append('"').toString();
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
