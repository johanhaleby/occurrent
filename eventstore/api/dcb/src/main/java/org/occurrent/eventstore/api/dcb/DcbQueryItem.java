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
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * One alternative inside a {@link DcbQuery}.
 * <p>
 * {@code types} match CloudEvent types as any-of. {@code tags} match DCB tags as
 * all-of. {@code excludedTypes} removes matching events whose CloudEvent type is
 * present in that set.
 */
@NullMarked
public record DcbQueryItem(Set<String> types, Set<String> tags, Set<String> excludedTypes) {

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
     * Creates an item that matches events whose CloudEvent type is {@code type}. Shorthand for the single-type case of
     * {@link #types(Collection)}.
     */
    public static DcbQueryItem type(String type) {
        return types(Set.of(type));
    }

    /**
     * Creates an item that matches any of the supplied CloudEvent types.
     */
    public static DcbQueryItem types(Collection<String> types) {
        return new DcbQueryItem(copyWithoutNulls(types, "Type cannot be null"), Set.of());
    }

    /**
     * Creates an item that matches events containing all supplied DCB tags.
     */
    public static DcbQueryItem tagsAllOf(Collection<String> tags) {
        return new DcbQueryItem(Set.of(), copyWithoutNulls(tags, "Tag cannot be null"));
    }

    /**
     * Creates an item that matches any supplied CloudEvent type and all supplied DCB tags.
     */
    public static DcbQueryItem typeAndTagsAllOf(Collection<String> types, Collection<String> tags) {
        return new DcbQueryItem(copyWithoutNulls(types, "Type cannot be null"), copyWithoutNulls(tags, "Tag cannot be null"));
    }

    /**
     * Creates an item that matches events containing all supplied DCB tags except
     * events whose CloudEvent type is excluded.
     */
    public static DcbQueryItem tagsAllOfExcludingTypes(Collection<String> tags, Collection<String> excludedTypes) {
        return new DcbQueryItem(Set.of(), copyWithoutNulls(tags, "Tag cannot be null"), copyWithoutNulls(excludedTypes, "Excluded type cannot be null"));
    }

    /**
     * Creates an item that matches any supplied CloudEvent type and all supplied DCB tags,
     * except events whose CloudEvent type is excluded.
     */
    public static DcbQueryItem typeAndTagsAllOfExcludingTypes(Collection<String> types, Collection<String> tags, Collection<String> excludedTypes) {
        return new DcbQueryItem(copyWithoutNulls(types, "Type cannot be null"), copyWithoutNulls(tags, "Tag cannot be null"), copyWithoutNulls(excludedTypes, "Excluded type cannot be null"));
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
