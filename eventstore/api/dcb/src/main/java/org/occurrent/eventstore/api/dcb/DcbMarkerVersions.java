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
import org.jspecify.annotations.Nullable;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * The versions of the conflict markers of a DCB boundary, one entry per marker key, as a store reported them on a read
 * or an append. A snapshot of a DCB boundary saves this as its version and passes it back to
 * {@link DcbReadOptions#afterVersions(DcbMarkerVersions)} to read only the events appended after it.
 * <p>
 * A marker key is {@code tag:<tag>} for a tag. A key for an event type is the store's own, as {@link #criteria()} and
 * the versions are. Each entry holds the marker's epoch, its version, and the number of its increments that wrote no
 * marker versions on their events. All three are plain values, so a snapshot store can persist them as they are.
 * Compare these versions only within the store that reported them.
 * <p>
 * The versions also hold the criteria they were read with, as a string, so that a read after them can refuse versions
 * from other criteria. Two criteria are the same when they hold the same alternatives in any order, as for a
 * {@link DcbConsistencyToken}, so {@code types("A", "B")} isn't the same as {@code anyOf(type("A"), type("B"))}.
 *
 * @param criteria the criteria these versions were read with, in the canonical form {@link #of(DcbCriteria, Map)} creates.
 *                 Persist it as it is and pass it back to {@link #of(String, Map)}.
 * @param markers  the marker versions by marker key, never empty
 */
@NullMarked
public record DcbMarkerVersions(String criteria, Map<String, MarkerVersion> markers) {

    /**
     * The version of one marker, or {@link #absent()} for a marker the store found none of.
     *
     * @param epoch     an opaque identifier of this incarnation of the marker. A marker that is removed and created again
     *                  gets a new epoch, so versions with different epochs can't be compared. It's {@code null} only
     *                  for {@link #absent()}.
     * @param version   how many times appends have incremented the marker, at least 1, or 0 for {@link #absent()}
     * @param unstamped how many of those increments wrote no marker versions on their events, from 0 up to {@code version}
     */
    public record MarkerVersion(@Nullable String epoch, long version, long unstamped) {
        private static final MarkerVersion ABSENT = new MarkerVersion(null, 0, 0);

        public MarkerVersion {
            if (version < 0) {
                throw new IllegalArgumentException("Version cannot be negative");
            }
            if (version == 0) {
                if (epoch != null) {
                    throw new IllegalArgumentException("A marker without a version cannot have an epoch");
                }
                if (unstamped != 0) {
                    throw new IllegalArgumentException("A marker without a version cannot have an unstamped count");
                }
            } else {
                requireNonNull(epoch, "Epoch cannot be null");
                if (epoch.isBlank()) {
                    throw new IllegalArgumentException("Epoch cannot be blank");
                }
                if (unstamped < 0) {
                    throw new IllegalArgumentException("Unstamped count cannot be negative");
                }
                if (unstamped > version) {
                    throw new IllegalArgumentException("Unstamped count cannot be greater than the version");
                }
            }
        }

        /**
         * A marker the store found none of when it read the versions. Only the marker key of an event type may hold
         * it, and a missing tag marker gives no versions. A read after versions that holds it selects the events of that
         * marker once it exists.
         */
        public static MarkerVersion absent() {
            return ABSENT;
        }

        /**
         * Whether this is {@link #absent()}.
         */
        public boolean isAbsent() {
            return version == 0;
        }
    }

    public DcbMarkerVersions {
        requireNonNull(criteria, "Criteria cannot be null");
        if (criteria.isBlank()) {
            throw new IllegalArgumentException("Criteria cannot be blank");
        }
        requireNonNull(markers, "Markers cannot be null");
        if (markers.isEmpty()) {
            throw new IllegalArgumentException("Markers cannot be empty");
        }
        TreeMap<String, MarkerVersion> copy = new TreeMap<>();
        markers.forEach((key, markerVersion) -> {
            requireNonNull(key, "Marker key cannot be null");
            requireNonNull(markerVersion, "Marker version cannot be null");
            if (key.isBlank()) {
                throw new IllegalArgumentException("Marker key cannot be blank");
            }
            copy.put(key, markerVersion);
        });
        markers = Collections.unmodifiableSortedMap(copy);
    }

    /**
     * Creates marker versions of the supplied entries, read with {@code criteria}.
     */
    public static DcbMarkerVersions of(DcbCriteria criteria, Map<String, MarkerVersion> markers) {
        return new DcbMarkerVersions(DcbCriteriaIdentity.canonical(criteria), markers);
    }

    /**
     * Creates marker versions of the supplied entries, read with the criteria whose canonical form is {@code criteria},
     * as {@link #criteria()} gave it. A snapshot store uses this to load the versions it persisted.
     */
    public static DcbMarkerVersions of(String criteria, Map<String, MarkerVersion> markers) {
        return new DcbMarkerVersions(criteria, markers);
    }

    /**
     * Whether these versions were read with {@code criteria}, or with criteria that are the same.
     */
    public boolean readWith(DcbCriteria criteria) {
        return this.criteria.equals(DcbCriteriaIdentity.canonical(criteria));
    }

    /**
     * The marker keys these versions cover, in ascending order.
     */
    public Set<String> keys() {
        return markers.keySet();
    }

    /**
     * The version of the marker with the supplied key, empty when these versions don't cover it.
     */
    public Optional<MarkerVersion> get(String key) {
        requireNonNull(key, "Marker key cannot be null");
        return Optional.ofNullable(markers.get(key));
    }
}
