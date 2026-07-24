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

package org.occurrent.dsl.snapshot;

import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbCriterion;
import org.occurrent.eventstore.api.dcb.Tag;

import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Derives a stable snapshot key from a {@link DcbCriteria}, the default keying for the DCB snapshot executors.
 * <p>
 * {@link DcbCriteria#toString()} is not usable as a key because its types, tags, and alternatives live in unordered
 * sets, so the same logical boundary can render in different orders and miss its snapshot. This canonicalizes by sorting
 * every part, so a boundary always yields the same key regardless of the order tags or types were supplied in.
 * <p>
 * Every type and tag value is length-prefixed ({@code value.length() + ":" + value}) before it is joined, so a
 * delimiter character occurring inside a value cannot be mistaken for the boundary between two values. That makes the
 * encoding injective, so two structurally distinct criteria always produce different keys instead of colliding on one
 * snapshot (for example a single type named {@code "A,B"} and the two types {@code "A"} and {@code "B"}, which would
 * both render as {@code types[A,B]} without the length prefix).
 * <p>
 * The DCB snapshot is keyed by the criteria (rather than by the decider) on purpose: the criteria carries the
 * per-instance identity, and a change to the boundary is what should invalidate and rebuild a stale snapshot. See ADR
 * 0061 for the full rationale.
 */
public final class DcbSnapshotKeys {

    private DcbSnapshotKeys() {
    }

    /**
     * A deterministic, order-insensitive key for {@code criteria}.
     */
    public static String canonicalKey(DcbCriteria criteria) {
        requireNonNull(criteria, "criteria cannot be null");
        String canonical = switch (criteria) {
            case DcbCriteria.MatchAll ignored -> "all";
            case DcbCriterion criterion -> criterion(criterion);
            case DcbCriteria.Items items -> items.items().stream().map(DcbSnapshotKeys::criterion).sorted().collect(joining(",", "anyOf[", "]"));
        };
        return canonical;
    }

    private static String criterion(DcbCriterion criterion) {
        String types = lengthPrefixedJoin(criterion.types().stream());
        String tags = lengthPrefixedJoin(criterion.tags().stream().map(Tag::value));
        String excludedTypes = lengthPrefixedJoin(criterion.excludedTypes().stream());
        return "types[" + types + "]tags[" + tags + "]excludingTypes[" + excludedTypes + "]";
    }

    /**
     * Joins {@code values}, sorted, with each element prefixed by its length so that a delimiter occurring inside an
     * element cannot be mistaken for the boundary between two elements (the collision {@link DcbSnapshotKeys} guards
     * against).
     */
    private static String lengthPrefixedJoin(Stream<String> values) {
        return values.sorted().map(value -> value.length() + ":" + value).collect(joining(","));
    }
}
