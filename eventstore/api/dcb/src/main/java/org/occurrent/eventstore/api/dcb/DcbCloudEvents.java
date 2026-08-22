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
import org.occurrent.eventstore.api.EventStoreCloudEventExtensions;

import java.util.*;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;

/**
 * Utilities for reading and writing DCB metadata on CloudEvents.
 * <p>
 * DCB tags are stored in the {@value #TAGS} extension. The global sequence position that every DCB event carries is
 * the general {@link org.occurrent.cloudevents.OccurrentCloudEventExtension#POSITION} extension; see
 * {@link org.occurrent.cloudevents.OccurrentCloudEventExtension#withPosition(CloudEvent, long)} and
 * {@link org.occurrent.cloudevents.OccurrentCloudEventExtension#getPosition(CloudEvent)}.
 */
@NullMarked
public final class DcbCloudEvents {
    /**
     * CloudEvent extension name that contains newline-separated DCB tags. Aliases the shared single source of truth,
     * {@link EventStoreCloudEventExtensions#DCB_TAGS}, so the literal is defined in exactly one place.
     */
    public static final String TAGS = EventStoreCloudEventExtensions.DCB_TAGS;
    private static final String TAG_SEPARATOR = "\n";

    private DcbCloudEvents() {
    }

    /**
     * Returns a copy of {@code cloudEvent} with canonical DCB tags in the {@value #TAGS} extension.
     */
    public static CloudEvent withTags(CloudEvent cloudEvent, Collection<Tag> tags) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        return CloudEventBuilder.v1(cloudEvent).withExtension(TAGS, encodeTags(tags)).build();
    }

    /**
     * Reads canonical DCB tags from a CloudEvent, or an empty set when the event has no DCB tags.
     */
    public static Set<Tag> getTags(CloudEvent cloudEvent) {
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
    public static Set<Tag> decodeTags(String encodedTags) {
        requireNonNull(encodedTags, "Encoded tags cannot be null");
        if (encodedTags.isEmpty()) {
            return Set.of();
        }
        return canonicalizeTags(Arrays.stream(encodedTags.split(TAG_SEPARATOR, -1))
                .map(Tag::parse)
                .toList());
    }

    /**
     * Returns whether {@code cloudEvent} is a DCB-written event, i.e. it carries the {@value #TAGS} extension. A DCB
     * append always stamps this extension (even for an empty tag set), while a stream-written event never does, so this
     * is the reliable discriminator between the two. It must be used instead of a "has a position" check when telling
     * DCB events apart from stream events, since stream events also carry a global position once stream position is
     * enabled (on by default).
     */
    public static boolean isDcbEvent(CloudEvent cloudEvent) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        return cloudEvent.getExtension(TAGS) != null;
    }

    /**
     * Returns a copy of {@code updated} with {@code original}'s exact DCB tag state, present or absent, the way
     * {@link org.occurrent.cloudevents.OccurrentCloudEventExtension#preserveAppendId} treats the append id. An
     * event store's {@code updateEvent} calls this so a replacement event an updater builds from scratch cannot
     * silently drop the tags an original DCB event carried, and cannot pick up tags it never had. Either mistake
     * would move the event across the consistency boundary its tags define. The store owns this value the same
     * way it owns {@code streamId}, {@code streamVersion} and the append id, so it is reapplied rather than left
     * to the updater.
     */
    public static CloudEvent preserveTags(CloudEvent original, CloudEvent updated) {
        requireNonNull(original, "Original CloudEvent cannot be null");
        requireNonNull(updated, "Updated CloudEvent cannot be null");
        if (isDcbEvent(original)) {
            return withTags(updated, getTags(original));
        }
        return CloudEventBuilder.v1(updated).withoutExtension(TAGS).build();
    }

    /**
     * Returns whether {@code cloudEvent} matches the supplied DCB criteria.
     */
    public static boolean matches(CloudEvent cloudEvent, DcbCriteria criteria) {
        requireNonNull(cloudEvent, "CloudEvent cannot be null");
        requireNonNull(criteria, "Criteria cannot be null");
        return switch (criteria) {
            case DcbCriteria.MatchAll ignored -> true;
            case DcbCriterion item -> matches(cloudEvent, item);
            case DcbCriteria.Items items -> items.items().stream().anyMatch(item -> matches(cloudEvent, item));
        };
    }

    private static boolean matches(CloudEvent cloudEvent, DcbCriterion item) {
        boolean typeMatches = item.types().isEmpty() || item.types().contains(cloudEvent.getType());
        boolean tagsMatch = getTags(cloudEvent).containsAll(item.tags());
        boolean excludedTypeMatches = item.excludedTypes().contains(cloudEvent.getType());
        return typeMatches && tagsMatch && !excludedTypeMatches;
    }

    /**
     * Returns the union of the tags the criteria constrains on, that is the consistency boundary it defines. A
     * {@link DcbCriteria.MatchAll} criteria and a criteria that only constrains on types both yield an empty set. This is
     * the stable per-boundary tag set a store can use to place DCB-written events, rather than the per-event tags which
     * a {@code TagGenerator} may extend differently per event.
     */
    public static Set<Tag> tagsOf(DcbCriteria criteria) {
        requireNonNull(criteria, "Criteria cannot be null");
        return Set.copyOf(itemsOf(criteria).stream()
                .flatMap(item -> item.tags().stream())
                .collect(toCollection(TreeSet::new)));
    }

    private static List<DcbCriterion> itemsOf(DcbCriteria criteria) {
        return switch (criteria) {
            case DcbCriteria.MatchAll ignored -> List.of();
            case DcbCriterion item -> List.of(item);
            case DcbCriteria.Items items -> items.items();
        };
    }

    /**
     * De-duplicates and sorts DCB tags into their canonical set form. Per-tag validation lives in {@link Tag}; this
     * keeps the set order-independent and sorted so the partition hash is stable across equivalent tag sets.
     */
    public static Set<Tag> canonicalizeTags(Collection<Tag> tags) {
        requireNonNull(tags, "Tags cannot be null");
        TreeSet<Tag> canonicalTags = tags.stream()
                .map(tag -> requireNonNull(tag, "Tag cannot be null"))
                .collect(toCollection(TreeSet::new));
        return Collections.unmodifiableSet(canonicalTags);
    }

    /**
     * Encodes DCB tags for the {@value #TAGS} CloudEvent extension.
     */
    public static String encodeTags(Collection<Tag> tags) {
        return canonicalizeTags(tags).stream()
                .map(Tag::canonical)
                .collect(joining(TAG_SEPARATOR));
    }
}
