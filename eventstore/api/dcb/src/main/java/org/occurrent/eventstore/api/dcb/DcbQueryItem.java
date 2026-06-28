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
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * One alternative inside a {@link DcbQuery}, and itself a single-alternative query.
 * <p>
 * {@code types} match CloudEvent types as any-of. {@code tags} match DCB tags as
 * all-of. {@code excludedTypes} removes matching events whose CloudEvent type is
 * present in that set.
 * <p>
 * Build it through {@link DcbQuery#type(String)}, {@link DcbQuery#types(String, String...)}, or
 * {@link DcbQuery#tags(String, String...)} and refine it fluently, for example
 * {@code DcbQuery.type("OrderPlaced").tags("order:1")}.
 */
@NullMarked
public record DcbQueryItem(Set<String> types, Set<String> tags, Set<String> excludedTypes) implements DcbQuery {

    public DcbQueryItem(Set<String> types, Set<String> tags) {
        this(types, tags, Set.of());
    }

    public DcbQueryItem {
        requireNonNull(types, "Types cannot be null");
        requireNonNull(tags, "Tags cannot be null");
        requireNonNull(excludedTypes, "Excluded types cannot be null");
        types = copyWithoutNulls(types, "Type cannot be null");
        excludedTypes = copyWithoutNulls(excludedTypes, "Excluded type cannot be null");
        types = stripAndValidate(types, "Types");
        excludedTypes = stripAndValidate(excludedTypes, "Excluded types");
        // Validate query tags the same way stored tags are canonicalized (strip, no blanks, no newlines), so a query
        // can never carry a tag that no stored event could match.
        tags = DcbCloudEvents.canonicalizeTags(tags);
        if (types.isEmpty() && tags.isEmpty()) {
            throw new IllegalArgumentException("A query item must contain at least one type or tag");
        }
        if (types.stream().anyMatch(excludedTypes::contains)) {
            throw new IllegalArgumentException("Types and excluded types cannot overlap");
        }
    }

    /**
     * Returns a copy of this item matching any of the supplied CloudEvent types (any-of).
     */
    public DcbQueryItem types(String first, String... rest) {
        return new DcbQueryItem(combine(first, rest), tags, excludedTypes);
    }

    /**
     * Returns a copy of this item matching any of the supplied CloudEvent types (any-of).
     */
    public DcbQueryItem types(Collection<String> types) {
        return new DcbQueryItem(copyWithoutNulls(types, "Type cannot be null"), tags, excludedTypes);
    }

    /**
     * Returns a copy of this item matching events containing all the supplied DCB tags (all-of).
     */
    public DcbQueryItem tags(String first, String... rest) {
        return new DcbQueryItem(types, combine(first, rest), excludedTypes);
    }

    /**
     * Returns a copy of this item matching events containing all the supplied DCB tags (all-of).
     */
    public DcbQueryItem tags(Collection<String> tags) {
        return new DcbQueryItem(types, copyWithoutNulls(tags, "Tag cannot be null"), excludedTypes);
    }

    /**
     * Returns a copy of this item that excludes events whose CloudEvent type is any of the supplied types (none-of).
     */
    public DcbQueryItem excludingTypes(String first, String... rest) {
        return new DcbQueryItem(types, tags, combine(first, rest));
    }

    /**
     * Returns a copy of this item that excludes events whose CloudEvent type is any of the supplied types (none-of).
     */
    public DcbQueryItem excludingTypes(Collection<String> excludedTypes) {
        return new DcbQueryItem(types, tags, copyWithoutNulls(excludedTypes, "Excluded type cannot be null"));
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

    private static Set<String> copyWithoutNulls(Collection<String> values, String nullMessage) {
        requireNonNull(values, "Values cannot be null");
        return values.stream()
                .map(value -> requireNonNull(value, nullMessage))
                .collect(toUnmodifiableSet());
    }

    private static Set<String> stripAndValidate(Set<String> values, String name) {
        Set<String> stripped = values.stream().map(String::strip).collect(toUnmodifiableSet());
        if (stripped.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException(name + " cannot contain blank values");
        }
        return stripped;
    }
}
