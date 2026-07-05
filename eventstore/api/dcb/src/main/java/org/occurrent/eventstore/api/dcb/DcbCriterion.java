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
 * One alternative inside a {@link DcbCriteria}, and itself a single-alternative criteria.
 * <p>
 * {@code types} match CloudEvent types as any-of. {@code tags} match DCB tags as
 * all-of. {@code excludedTypes} removes matching events whose CloudEvent type is
 * present in that set.
 * <p>
 * Build it through {@link DcbCriteria#type(String)}, {@link DcbCriteria#types(String, String...)}, or
 * {@link DcbCriteria#tags(Tag, Tag...)} and refine it fluently, for example
 * {@code DcbCriteria.type("OrderPlaced").tags(Tag.of("order", "1"))}.
 */
@NullMarked
public record DcbCriterion(Set<String> types, Set<Tag> tags, Set<String> excludedTypes) implements DcbCriteria {

    public DcbCriterion(Set<String> types, Set<Tag> tags) {
        this(types, tags, Set.of());
    }

    public DcbCriterion {
        requireNonNull(types, "Types cannot be null");
        requireNonNull(tags, "Tags cannot be null");
        requireNonNull(excludedTypes, "Excluded types cannot be null");
        types = copyWithoutNulls(types, "Type cannot be null");
        excludedTypes = copyWithoutNulls(excludedTypes, "Excluded type cannot be null");
        types = stripAndValidate(types, "Types");
        excludedTypes = stripAndValidate(excludedTypes, "Excluded types");
        // Canonicalize criterion tags the same way stored tags are canonicalized (dedup, sorted), so a criterion can
        // never carry a tag that no stored event could match.
        tags = DcbCloudEvents.canonicalizeTags(tags);
        if (types.isEmpty() && tags.isEmpty()) {
            throw new IllegalArgumentException("A criterion must contain at least one type or tag");
        }
        if (types.stream().anyMatch(excludedTypes::contains)) {
            throw new IllegalArgumentException("Types and excluded types cannot overlap");
        }
    }

    /**
     * Returns a copy of this criterion matching any of the supplied CloudEvent types (any-of).
     */
    public DcbCriterion types(String first, String... rest) {
        return new DcbCriterion(combine(first, rest), tags, excludedTypes);
    }

    /**
     * Returns a copy of this criterion matching any of the supplied CloudEvent types (any-of).
     */
    public DcbCriterion types(Collection<String> types) {
        return new DcbCriterion(copyWithoutNulls(types, "Type cannot be null"), tags, excludedTypes);
    }

    /**
     * Returns a copy of this criterion matching events containing all the supplied DCB tags (all-of).
     */
    public DcbCriterion tags(Tag first, Tag... rest) {
        return new DcbCriterion(types, combineTags(first, rest), excludedTypes);
    }

    /**
     * Returns a copy of this criterion matching events containing all the supplied DCB tags (all-of).
     */
    public DcbCriterion tags(Collection<Tag> tags) {
        return new DcbCriterion(types, copyTagsWithoutNulls(tags), excludedTypes);
    }

    /**
     * Returns a copy of this criterion that excludes events whose CloudEvent type is any of the supplied types (none-of).
     */
    public DcbCriterion excludingTypes(String first, String... rest) {
        return new DcbCriterion(types, tags, combine(first, rest));
    }

    /**
     * Returns a copy of this criterion that excludes events whose CloudEvent type is any of the supplied types (none-of).
     */
    public DcbCriterion excludingTypes(Collection<String> excludedTypes) {
        return new DcbCriterion(types, tags, copyWithoutNulls(excludedTypes, "Excluded type cannot be null"));
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

    static Set<String> copyWithoutNulls(Collection<String> values, String nullMessage) {
        requireNonNull(values, "Values cannot be null");
        return values.stream()
                .map(value -> requireNonNull(value, nullMessage))
                .collect(toUnmodifiableSet());
    }

    static Set<Tag> copyTagsWithoutNulls(Collection<Tag> tags) {
        requireNonNull(tags, "Tags cannot be null");
        return tags.stream()
                .map(tag -> requireNonNull(tag, "Tag cannot be null"))
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
