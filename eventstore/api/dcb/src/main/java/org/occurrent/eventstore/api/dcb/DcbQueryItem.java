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
 * all-of, which is how an item expresses one Dynamic Consistency Boundary.
 */
@NullMarked
public record DcbQueryItem(Set<String> types, Set<String> tags) {

    public DcbQueryItem {
        requireNonNull(types, "Types cannot be null");
        requireNonNull(tags, "Tags cannot be null");
        types = copyWithoutNulls(types, "Type cannot be null");
        tags = copyWithoutNulls(tags, "Tag cannot be null");
        types = stripAndValidate(types, "Types");
        tags = stripAndValidate(tags, "Tags");
        if (types.isEmpty() && tags.isEmpty()) {
            throw new IllegalArgumentException("A query item must contain at least one type or tag");
        }
    }

    /**
     * Creates an item that matches any of the supplied CloudEvent types.
     */
    public static DcbQueryItem types(Collection<String> types) {
        return new DcbQueryItem(Set.copyOf(types), Set.of());
    }

    /**
     * Creates an item that matches events containing all supplied DCB tags.
     */
    public static DcbQueryItem tagsAllOf(Collection<String> tags) {
        return new DcbQueryItem(Set.of(), Set.copyOf(tags));
    }

    /**
     * Creates an item that matches any supplied CloudEvent type and all supplied DCB tags.
     */
    public static DcbQueryItem typeAndTagsAllOf(Collection<String> types, Collection<String> tags) {
        return new DcbQueryItem(Set.copyOf(types), Set.copyOf(tags));
    }

    private static Set<String> copyWithoutNulls(Collection<String> values, String nullMessage) {
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
