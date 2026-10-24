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

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * The versions of the conflict markers of a DCB boundary, one entry per marker key, as a store reported them on a read
 * or an append. A snapshot of a DCB boundary saves this as its version and passes it back to
 * {@link DcbReadOptions#afterVersions(DcbMarkerVersions)} to read only the events appended after it.
 * <p>
 * A marker key is {@code tag:<tag>} for a tag and {@code type:<type>} for an event type. Each entry holds the marker's
 * epoch, its version, and the number of its increments that wrote no marker versions on their events. All three are
 * plain values, so a snapshot store can persist them as they are. Compare these versions only within the store that
 * reported them.
 *
 * @param markers the marker versions by marker key, never empty
 */
@NullMarked
public record DcbMarkerVersions(Map<String, MarkerVersion> markers) {

    /**
     * The version of one marker.
     *
     * @param epoch     an opaque identifier of this incarnation of the marker. A marker that is removed and created again
     *                  gets a new epoch, so versions with different epochs can't be compared.
     * @param version   how many times appends have incremented the marker, at least 1
     * @param unstamped how many of those increments wrote no marker versions on their events, from 0 up to {@code version}
     */
    public record MarkerVersion(String epoch, long version, long unstamped) {
        public MarkerVersion {
            requireNonNull(epoch, "Epoch cannot be null");
            if (epoch.isBlank()) {
                throw new IllegalArgumentException("Epoch cannot be blank");
            }
            if (version < 1) {
                throw new IllegalArgumentException("Version must be greater than zero");
            }
            if (unstamped < 0) {
                throw new IllegalArgumentException("Unstamped count cannot be negative");
            }
            if (unstamped > version) {
                throw new IllegalArgumentException("Unstamped count cannot be greater than the version");
            }
        }
    }

    public DcbMarkerVersions {
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
     * Creates marker versions from the supplied entries.
     */
    public static DcbMarkerVersions of(Map<String, MarkerVersion> markers) {
        return new DcbMarkerVersions(markers);
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
