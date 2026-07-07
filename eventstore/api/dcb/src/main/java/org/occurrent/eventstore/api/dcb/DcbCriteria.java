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

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Criteria that select CloudEvents by DCB metadata.
 * <p>
 * A criteria is {@link MatchAll} (every DCB event), a single {@link DcbCriterion} alternative, or {@link Items}, several
 * alternatives OR-ed together. Inside a criterion, types are matched as any-of, tags as all-of, and excluded types as
 * none-of.
 * <p>
 * Build criteria fluently: {@code DcbCriteria.type("OrderPlaced").tags(Tag.of("order", "1"))} for one alternative, and
 * {@code DcbCriteria.anyOf(DcbCriteria.tags(Tag.of("order", "1")), DcbCriteria.tags(Tag.of("customer", "1")))} (or the
 * {@link #tagsAnyOf(Tag, Tag...)} shortcut) to OR several. The model is deliberately an OR of items rather than a general
 * boolean tree (see ADR 32).
 * <p>
 * Naming note: this type is {@code DcbCriteria}, but {@link DcbEventStore#read(DcbCriteria)} and
 * {@link DcbAppendCondition#query()} both name their parameter {@code query}. Same value, two names depending on
 * whether the surrounding API is talking about the type/builder ({@code criteria}) or a call-site argument
 * ({@code query}); there is no semantic difference.
 */
@NullMarked
public sealed interface DcbCriteria permits DcbCriteria.MatchAll, DcbCriteria.Items, DcbCriterion {

    /**
     * A criteria that matches every DCB event.
     */
    record MatchAll() implements DcbCriteria {
    }

    /**
     * A criteria that matches an event when the event matches any of the {@code items}.
     */
    record Items(List<DcbCriterion> items) implements DcbCriteria {
        public Items {
            requireNonNull(items, "Items cannot be null");
            items.forEach(item -> requireNonNull(item, "Criterion cannot be null"));
            items = List.copyOf(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("A criteria must contain at least one criterion");
            }
        }
    }

    /**
     * Creates a criteria that matches every DCB event.
     * <p>
     * As a read criteria this simply matches everything. Take care using it as a {@link DcbAppendCondition} boundary: it
     * is a whole-store optimistic lock that is skew-safe only against other whole-store conditions, not against a
     * concurrent tag-scoped or type-scoped append, so on a store taking concurrent scoped writes it can under-protect
     * (see ADR 30). It is correct for single-writer operations and for an empty-store or bootstrap guard. When building
     * an append condition, prefer {@link DcbAppendCondition#wholeStoreLock()} /
     * {@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} over passing {@code all()} to
     * {@code failIfEventsMatch}, so the whole-store lock is spelled out explicitly and not confused with a
     * read-everything query.
     */
    static DcbCriteria all() {
        return new MatchAll();
    }

    /**
     * Creates a criteria matching events whose CloudEvent type is {@code type}. Refine it fluently with
     * {@link DcbCriterion#tags(Tag, Tag...)} or {@link DcbCriterion#excludingTypes(String, String...)}.
     */
    static DcbCriterion type(String type) {
        return new DcbCriterion(Set.of(requireNonNull(type, "Type cannot be null")), Set.of());
    }

    /**
     * Creates a criteria matching events whose CloudEvent type is any of the supplied types (any-of).
     */
    static DcbCriterion types(String first, String... rest) {
        return new DcbCriterion(combine(first, rest), Set.of());
    }

    /**
     * Creates a criteria matching events whose CloudEvent type is any of the supplied types (any-of).
     */
    static DcbCriterion types(Collection<String> types) {
        return new DcbCriterion(DcbCriterion.copyWithoutNulls(types, "Type cannot be null"), Set.of());
    }

    /**
     * Creates a criteria matching events containing all the supplied DCB tags (all-of).
     */
    static DcbCriterion tags(Tag first, Tag... rest) {
        return new DcbCriterion(Set.of(), combineTags(first, rest));
    }

    /**
     * Creates a criteria matching events containing all the supplied DCB tags (all-of).
     */
    static DcbCriterion tags(Collection<Tag> tags) {
        return new DcbCriterion(Set.of(), DcbCriterion.copyTagsWithoutNulls(tags));
    }

    /**
     * Creates a criteria matching an event when it matches any of the supplied alternatives. A {@link DcbCriterion}
     * contributes itself, an {@link Items} contributes all its alternatives, and a {@link MatchAll} collapses the whole
     * criteria to match everything.
     */
    static DcbCriteria anyOf(DcbCriteria first, DcbCriteria... rest) {
        requireNonNull(first, "First criteria cannot be null");
        requireNonNull(rest, "Additional criteria cannot be null");
        List<DcbCriteria> criteria = new ArrayList<>();
        criteria.add(first);
        for (DcbCriteria criterion : rest) {
            criteria.add(requireNonNull(criterion, "Criteria cannot be null"));
        }
        return anyOf(criteria);
    }

    /**
     * Creates a criteria matching an event when it matches any of the supplied alternatives.
     */
    static DcbCriteria anyOf(Collection<? extends DcbCriteria> criteria) {
        requireNonNull(criteria, "Criteria cannot be null");
        if (criteria.isEmpty()) {
            throw new IllegalArgumentException("A criteria must contain at least one criterion");
        }
        List<DcbCriterion> items = new ArrayList<>();
        for (DcbCriteria criterion : criteria) {
            requireNonNull(criterion, "Criteria cannot be null");
            switch (criterion) {
                case MatchAll ignored -> {
                    return new MatchAll();
                }
                case DcbCriterion item -> items.add(item);
                case Items existing -> items.addAll(existing.items());
            }
        }
        return items.size() == 1 ? items.getFirst() : new Items(items);
    }

    /**
     * Creates a criteria matching events that carry any one of the supplied DCB tags, one tag per alternative. Shorthand
     * for {@code anyOf(tags(a), tags(b), ...)}. Each alternative is a single tag, not a compound tag set: to OR
     * alternatives that each require several tags together, build them with {@link #tags(Tag, Tag...)} and combine them
     * with {@link #anyOf(DcbCriteria, DcbCriteria...)} instead.
     */
    static DcbCriteria tagsAnyOf(Tag first, Tag... rest) {
        requireNonNull(first, "First tag cannot be null");
        requireNonNull(rest, "Additional tags cannot be null");
        List<DcbCriteria> items = new ArrayList<>();
        items.add(tags(first));
        for (Tag tag : rest) {
            items.add(tags(requireNonNull(tag, "Tag cannot be null")));
        }
        return anyOf(items);
    }

    private static Set<String> combine(String first, String[] additional) {
        requireNonNull(first, "Value cannot be null");
        requireNonNull(additional, "Additional values cannot be null");
        LinkedHashSet<String> values = new LinkedHashSet<>();
        values.add(first);
        for (String value : additional) {
            values.add(requireNonNull(value, "Value cannot be null"));
        }
        return Set.copyOf(values);
    }

    private static Set<Tag> combineTags(Tag first, Tag[] additional) {
        requireNonNull(first, "Tag cannot be null");
        requireNonNull(additional, "Additional tags cannot be null");
        LinkedHashSet<Tag> tags = new LinkedHashSet<>();
        tags.add(first);
        for (Tag tag : additional) {
            tags.add(requireNonNull(tag, "Tag cannot be null"));
        }
        return Set.copyOf(tags);
    }
}
