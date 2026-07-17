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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Derives a stable snapshot key from a {@link DcbCriteria}, the default keying for the DCB snapshot executors.
 * <p>
 * {@link DcbCriteria#toString()} is not usable as a key because its types, tags, and alternatives live in unordered
 * sets, so the same logical boundary can render in different orders and miss its snapshot. This canonicalizes by sorting
 * every part, so a boundary always yields the same key regardless of the order tags or types were supplied in.
 */
public final class DcbSnapshotKeys {

    private DcbSnapshotKeys() {
    }

    /**
     * A deterministic, order-insensitive key for {@code criteria}.
     */
    public static String canonicalKey(DcbCriteria criteria) {
        requireNonNull(criteria, "criteria cannot be null");
        return switch (criteria) {
            case DcbCriteria.MatchAll ignored -> "all";
            case DcbCriterion criterion -> criterion(criterion);
            case DcbCriteria.Items items -> items.items().stream().map(DcbSnapshotKeys::criterion).sorted().collect(joining(",", "anyOf[", "]"));
        };
    }

    private static String criterion(DcbCriterion criterion) {
        String types = criterion.types().stream().sorted().collect(joining(",", "types[", "]"));
        String tags = criterion.tags().stream().map(Tag::value).sorted().collect(joining(",", "tags[", "]"));
        String excludedTypes = criterion.excludedTypes().stream().sorted().collect(joining(",", "excludingTypes[", "]"));
        return types + tags + excludedTypes;
    }
}
