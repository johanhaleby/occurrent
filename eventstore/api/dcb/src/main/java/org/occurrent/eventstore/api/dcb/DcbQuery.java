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
 * A query either matches all DCB events or consists of one or more query items. Each
 * item contributes an alternative match, while tags inside an item are matched as an
 * all-of boundary.
 */
@NullMarked
public record DcbQuery(boolean matchAll, List<DcbQueryItem> items) {

    public DcbQuery {
        requireNonNull(items, "Items cannot be null");
        items = List.copyOf(items);
        if (matchAll && !items.isEmpty()) {
            throw new IllegalArgumentException("A query that matches all events cannot contain query items");
        }
        if (!matchAll && items.isEmpty()) {
            throw new IllegalArgumentException("A query must contain at least one query item unless it matches all events");
        }
    }

    /**
     * Creates a query that matches every DCB event.
     */
    public static DcbQuery all() {
        return new DcbQuery(true, List.of());
    }

    /**
     * Creates a query from explicit query items.
     */
    public static DcbQuery fromItems(Collection<DcbQueryItem> items) {
        return new DcbQuery(false, List.copyOf(items));
    }

    /**
     * Creates a query that matches any event whose CloudEvent type is one of the supplied types.
     */
    public static DcbQuery type(String type, String... additionalTypes) {
        return fromItems(List.of(DcbQueryItem.types(combine(type, additionalTypes))));
    }

    /**
     * Creates a query that matches events containing all supplied DCB tags.
     */
    public static DcbQuery tagsAllOf(String tag, String... additionalTags) {
        return fromItems(List.of(DcbQueryItem.tagsAllOf(combine(tag, additionalTags))));
    }

    /**
     * Creates a query that matches any of the supplied CloudEvent types and all supplied DCB tags.
     */
    public static DcbQuery typeAndTagsAllOf(Collection<String> types, Collection<String> tags) {
        return fromItems(List.of(DcbQueryItem.typeAndTagsAllOf(types, tags)));
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
