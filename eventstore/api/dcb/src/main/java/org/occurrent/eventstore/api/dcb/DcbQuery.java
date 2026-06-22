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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Query that selects CloudEvents by DCB metadata.
 * <p>
 * A query is either {@link MatchAll} (every DCB event) or {@link Items}, a non-empty list of
 * {@link DcbQueryItem} alternatives that are OR-ed together. Inside an item, types are matched as
 * any-of, tags as all-of, and excluded types as none-of.
 */
@NullMarked
public sealed interface DcbQuery permits DcbQuery.MatchAll, DcbQuery.Items {

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
     */
    static DcbQuery all() {
        return new MatchAll();
    }

    /**
     * Creates a query that matches an event when it matches any of the supplied query items.
     */
    static DcbQuery anyOf(Collection<DcbQueryItem> items) {
        requireNonNull(items, "Items cannot be null");
        return new Items(List.copyOf(items));
    }

    /**
     * Creates a query that matches an event when it matches any of the supplied query items.
     */
    static DcbQuery anyOf(DcbQueryItem first, DcbQueryItem... rest) {
        requireNonNull(first, "First item cannot be null");
        requireNonNull(rest, "Additional items cannot be null");
        List<DcbQueryItem> items = new ArrayList<>();
        items.add(first);
        for (DcbQueryItem item : rest) {
            items.add(requireNonNull(item, "Item cannot be null"));
        }
        return new Items(items);
    }

    /**
     * Creates a query that matches any event whose CloudEvent type is {@code type}. Shorthand for the single-type case
     * of {@link #types(String, String...)}.
     */
    static DcbQuery type(String type) {
        return types(type);
    }

    /**
     * Creates a query that matches any event whose CloudEvent type is one of the supplied types.
     */
    static DcbQuery types(String type, String... additionalTypes) {
        return anyOf(List.of(DcbQueryItem.types(combine(type, additionalTypes))));
    }

    /**
     * Creates a query that matches events containing all supplied DCB tags.
     */
    static DcbQuery tagsAllOf(String tag, String... additionalTags) {
        return anyOf(List.of(DcbQueryItem.tagsAllOf(combine(tag, additionalTags))));
    }

    /**
     * Creates a query that matches any of the supplied CloudEvent types and all supplied DCB tags.
     */
    static DcbQuery typeAndTagsAllOf(Collection<String> types, Collection<String> tags) {
        return anyOf(List.of(DcbQueryItem.typeAndTagsAllOf(types, tags)));
    }

    /**
     * Creates a query that matches events containing all supplied DCB tags except
     * events whose CloudEvent type is excluded.
     */
    static DcbQuery tagsAllOfExcludingTypes(Collection<String> tags, Collection<String> excludedTypes) {
        return anyOf(List.of(DcbQueryItem.tagsAllOfExcludingTypes(tags, excludedTypes)));
    }

    /**
     * Creates a query that matches any of the supplied CloudEvent types and all supplied
     * DCB tags, except events whose CloudEvent type is excluded.
     */
    static DcbQuery typeAndTagsAllOfExcludingTypes(Collection<String> types, Collection<String> tags, Collection<String> excludedTypes) {
        return anyOf(List.of(DcbQueryItem.typeAndTagsAllOfExcludingTypes(types, tags, excludedTypes)));
    }

    private static Set<String> combine(String first, String[] additional) {
        requireNonNull(first, "Value cannot be null");
        requireNonNull(additional, "Additional values cannot be null");
        LinkedHashSet<String> values = new LinkedHashSet<>();
        values.add(first);
        values.addAll(Arrays.stream(additional)
                .map(value -> requireNonNull(value, "Value cannot be null"))
                .collect(toUnmodifiableSet()));
        return Set.copyOf(values);
    }
}
