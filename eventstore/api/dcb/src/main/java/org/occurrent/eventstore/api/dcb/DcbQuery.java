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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Query that selects CloudEvents by DCB metadata.
 * <p>
 * A query is {@link MatchAll} (every DCB event), a single {@link DcbQueryItem} alternative, or {@link Items}, several
 * alternatives OR-ed together. Inside an item, types are matched as any-of, tags as all-of, and excluded types as
 * none-of.
 * <p>
 * Build queries fluently: {@code DcbQuery.type("OrderPlaced").tags("order:1")} for one alternative, and
 * {@code DcbQuery.anyOf(DcbQuery.tags("order:1"), DcbQuery.tags("customer:1"))} (or the {@link #tagsAnyOf(String, String...)}
 * shortcut) to OR several. The model is deliberately an OR of items rather than a general boolean tree (see ADR 32).
 */
@NullMarked
public sealed interface DcbQuery permits DcbQuery.MatchAll, DcbQuery.Items, DcbQueryItem {

    /**
     * A query that matches every DCB event.
     */
    record MatchAll() implements DcbQuery {
    }

    /**
     * A query that matches an event when the event matches any of the {@code items}.
     */
    record Items(List<DcbQueryItem> items) implements DcbQuery {
        public Items {
            requireNonNull(items, "Items cannot be null");
            items.forEach(item -> requireNonNull(item, "Query item cannot be null"));
            items = List.copyOf(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("A query must contain at least one query item");
            }
        }
    }

    /**
     * Creates a query that matches every DCB event.
     * <p>
     * As a read query this simply matches everything. Take care using it as a {@link DcbAppendCondition} boundary: it is
     * a whole-store optimistic lock that is skew-safe only against other whole-store conditions, not against a concurrent
     * tag-scoped or type-scoped append, so on a store taking concurrent scoped writes it can under-protect (see ADR 30).
     * It is correct for single-writer operations and for an empty-store or bootstrap guard.
     */
    static DcbQuery all() {
        return new MatchAll();
    }

    /**
     * Creates a query matching events whose CloudEvent type is {@code type}. Refine it fluently with
     * {@link DcbQueryItem#tags(String, String...)} or {@link DcbQueryItem#excludingTypes(String, String...)}.
     */
    static DcbQueryItem type(String type) {
        return new DcbQueryItem(Set.of(requireNonNull(type, "Type cannot be null")), Set.of());
    }

    /**
     * Creates a query matching events whose CloudEvent type is any of the supplied types (any-of).
     */
    static DcbQueryItem types(String first, String... rest) {
        return new DcbQueryItem(combine(first, rest), Set.of());
    }

    /**
     * Creates a query matching events whose CloudEvent type is any of the supplied types (any-of).
     */
    static DcbQueryItem types(Collection<String> types) {
        return new DcbQueryItem(DcbQueryItem.copyWithoutNulls(types, "Type cannot be null"), Set.of());
    }

    /**
     * Creates a query matching events containing all the supplied DCB tags (all-of).
     */
    static DcbQueryItem tags(String first, String... rest) {
        return new DcbQueryItem(Set.of(), combine(first, rest));
    }

    /**
     * Creates a query matching events containing all the supplied DCB tags (all-of).
     */
    static DcbQueryItem tags(Collection<String> tags) {
        return new DcbQueryItem(Set.of(), DcbQueryItem.copyWithoutNulls(tags, "Tag cannot be null"));
    }

    /**
     * Creates a query matching an event when it matches any of the supplied alternatives. A {@link DcbQueryItem}
     * contributes itself, an {@link Items} contributes all its alternatives, and a {@link MatchAll} collapses the whole
     * query to match everything.
     */
    static DcbQuery anyOf(DcbQuery first, DcbQuery... rest) {
        requireNonNull(first, "First query cannot be null");
        requireNonNull(rest, "Additional queries cannot be null");
        List<DcbQuery> queries = new ArrayList<>();
        queries.add(first);
        for (DcbQuery query : rest) {
            queries.add(requireNonNull(query, "Query cannot be null"));
        }
        return anyOf(queries);
    }

    /**
     * Creates a query matching an event when it matches any of the supplied alternatives.
     */
    static DcbQuery anyOf(Collection<? extends DcbQuery> queries) {
        requireNonNull(queries, "Queries cannot be null");
        if (queries.isEmpty()) {
            throw new IllegalArgumentException("A query must contain at least one query item");
        }
        List<DcbQueryItem> items = new ArrayList<>();
        for (DcbQuery query : queries) {
            requireNonNull(query, "Query cannot be null");
            if (query instanceof MatchAll) {
                return new MatchAll();
            } else if (query instanceof DcbQueryItem item) {
                items.add(item);
            } else if (query instanceof Items existing) {
                items.addAll(existing.items());
            }
        }
        return items.size() == 1 ? items.get(0) : new Items(items);
    }

    /**
     * Creates a query matching events that carry any one of the supplied DCB tags. Shorthand for
     * {@code anyOf(tags(a), tags(b), ...)}.
     */
    static DcbQuery tagsAnyOf(String first, String... rest) {
        requireNonNull(first, "First tag cannot be null");
        requireNonNull(rest, "Additional tags cannot be null");
        List<DcbQuery> items = new ArrayList<>();
        items.add(tags(first));
        for (String tag : rest) {
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
}
