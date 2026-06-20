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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.NullMarked;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/**
 * Utilities for reading and writing DCB metadata on CloudEvents.
 * <p>
 * DCB tags are stored in the {@value #TAGS} extension and the global sequence
 * position is stored in the {@value #POSITION} extension.
 */
@NullMarked
public final class DcbCloudEvents {
    /**
     * CloudEvent extension name that contains newline-separated DCB tags.
     */
    public static final String TAGS = "dcbtags";
    /**
     * CloudEvent extension name that contains the DCB sequence position.
     */
    public static final String POSITION = "dcbposition";
    private static final String TAG_SEPARATOR = "\n";

    private DcbCloudEvents() {
    }

    /**
     * Returns a copy of {@code cloudEvent} with canonical DCB tags in the {@value #TAGS} extension.
     */
    public static CloudEvent withTags(CloudEvent cloudEvent, Collection<String> tags) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        return CloudEventBuilder.v1(cloudEvent).withExtension(TAGS, encodeTags(tags)).build();
    }

    /**
     * Returns a copy of {@code cloudEvent} with the DCB sequence position in the {@value #POSITION} extension.
     */
    public static CloudEvent withPosition(CloudEvent cloudEvent, long position) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        if (position <= 0) {
            throw new IllegalArgumentException("Position must be greater than zero");
        }
        return CloudEventBuilder.v1(cloudEvent).withExtension(POSITION, position).build();
    }

    /**
     * Reads canonical DCB tags from a CloudEvent, or an empty set when the event has no DCB tags.
     */
    public static Set<String> getTags(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        Object tags = cloudEvent.getExtension(TAGS);
        if (tags == null) {
            return Set.of();
        }
        if (!(tags instanceof String encodedTags)) {
            throw new IllegalArgumentException("DCB tags extension must be a String");
        }
        return decodeTags(encodedTags);
    }

    /**
     * Decodes a canonical DCB tag set from the encoded string stored in the DCB tags extension, or an empty set
     * when the string is empty. Splitting keeps trailing empty segments so a malformed encoding fails fast in
     * {@link #canonicalizeTags(Collection)} rather than being silently accepted.
     */
    public static Set<String> decodeTags(String encodedTags) {
        requireNonNull(encodedTags, "Encoded tags cannot be null");
        if (encodedTags.isEmpty()) {
            return Set.of();
        }
        return canonicalizeTags(Arrays.asList(encodedTags.split(TAG_SEPARATOR, -1)));
    }

    /**
     * Reads the DCB sequence position from a CloudEvent, or {@code 0} when it has no DCB position.
     */
    public static long getPosition(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        Object position = cloudEvent.getExtension(POSITION);
        if (position == null) {
            return 0;
        }
        if (position instanceof Number number) {
            return number.longValue();
        }
        if (position instanceof String string) {
            return Long.parseLong(string);
        }
        throw new IllegalArgumentException("DCB position extension must be a Number or String");
    }

    /**
     * Returns whether {@code cloudEvent} matches the supplied DCB query.
     */
    public static boolean matches(CloudEvent cloudEvent, DcbQuery query) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        requireNonNull(query, "Query cannot be null");
        if (query instanceof DcbQuery.Items items) {
            return items.items().stream().anyMatch(item -> matches(cloudEvent, item));
        }
        return true;
    }

    private static boolean matches(CloudEvent cloudEvent, DcbQueryItem item) {
        boolean typeMatches = item.types().isEmpty() || item.types().contains(cloudEvent.getType());
        boolean tagsMatch = getTags(cloudEvent).containsAll(item.tags());
        boolean excludedTypeMatches = item.excludedTypes().contains(cloudEvent.getType());
        return typeMatches && tagsMatch && !excludedTypeMatches;
    }

    /**
     * Strips, validates, de-duplicates, and sorts DCB tags into their canonical set form.
     */
    public static Set<String> canonicalizeTags(Collection<String> tags) {
        requireNonNull(tags, "Tags cannot be null");
        Set<String> canonicalTags = tags.stream()
                .map(tag -> requireNonNull(tag, "Tag cannot be null"))
                .map(String::strip)
                .collect(toCollection(TreeSet::new));
        if (canonicalTags.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("Tags cannot contain blank values");
        }
        if (canonicalTags.stream().anyMatch(tag -> tag.contains(TAG_SEPARATOR))) {
            throw new IllegalArgumentException("Tags cannot contain newline characters");
        }
        return Set.copyOf(canonicalTags);
    }

    /**
     * Encodes DCB tags for the {@value #TAGS} CloudEvent extension.
     */
    public static String encodeTags(Collection<String> tags) {
        return String.join(TAG_SEPARATOR, new TreeSet<>(canonicalizeTags(tags)));
    }
}
