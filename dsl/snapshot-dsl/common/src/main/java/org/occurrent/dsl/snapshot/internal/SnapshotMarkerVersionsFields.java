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

package org.occurrent.dsl.snapshot.internal;

import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions.MarkerVersion;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Converts {@link DcbMarkerVersions} to and from the plain values a snapshot store persists. That's a map with the field
 * {@code c}, the criteria the versions were read with, and {@code m}, a list with one map per marker key holding the
 * fields {@code k} (key), {@code e} (epoch), {@code v} (version) and {@code u} (unstamped). An absent marker has no
 * {@code e}. A list of maps is used
 * instead of one map keyed by marker key, since a marker key can contain a dot, which a MongoDB field name can't.
 */
public final class SnapshotMarkerVersionsFields {
    private static final String CRITERIA = "c";
    private static final String MARKERS = "m";
    private static final String KEY = "k";
    private static final String EPOCH = "e";
    private static final String VERSION = "v";
    private static final String UNSTAMPED = "u";

    private SnapshotMarkerVersionsFields() {
    }

    /**
     * The fields to persist for {@code markerVersions}, with one map per marker key in ascending key order.
     */
    public static Map<String, Object> toFields(DcbMarkerVersions markerVersions) {
        requireNonNull(markerVersions, "markerVersions cannot be null");
        List<Map<String, Object>> markers = new ArrayList<>(markerVersions.markers().size());
        markerVersions.markers().forEach((key, markerVersion) -> {
            Map<String, Object> marker = new LinkedHashMap<>();
            marker.put(KEY, key);
            if (markerVersion.epoch() != null) {
                marker.put(EPOCH, markerVersion.epoch());
            }
            marker.put(VERSION, markerVersion.version());
            marker.put(UNSTAMPED, markerVersion.unstamped());
            markers.add(marker);
        });
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put(CRITERIA, markerVersions.criteria());
        fields.put(MARKERS, markers);
        return fields;
    }

    /**
     * The marker versions that {@link #toFields(DcbMarkerVersions)} produced. Throws when a field is missing or has the
     * wrong type, so the caller treats the snapshot as unreadable. A list of marker maps without the criteria around it
     * is refused that way too.
     */
    public static DcbMarkerVersions fromFields(Object fields) {
        requireNonNull(fields, "fields cannot be null");
        if (!(fields instanceof Map<?, ?> stored)) {
            throw new IllegalArgumentException("Marker versions must be a map with the criteria they were read with, was " + fields);
        }
        String criteria = (String) requireNonNull(stored.get(CRITERIA), "Criteria are missing");
        if (!(requireNonNull(stored.get(MARKERS), "Marker versions are missing") instanceof List<?> markerFields)) {
            throw new IllegalArgumentException("Marker versions must be a list, was " + stored.get(MARKERS));
        }
        Map<String, MarkerVersion> markers = new LinkedHashMap<>();
        for (Object element : markerFields) {
            if (!(element instanceof Map<?, ?> field)) {
                throw new IllegalArgumentException("A marker version must be a map, was " + element);
            }
            String key = (String) requireNonNull(field.get(KEY), "Marker key is missing");
            String epoch = (String) field.get(EPOCH);
            long version = ((Number) requireNonNull(field.get(VERSION), "Version is missing for marker key " + key)).longValue();
            long unstamped = ((Number) requireNonNull(field.get(UNSTAMPED), "Unstamped count is missing for marker key " + key)).longValue();
            if (version != 0) {
                requireNonNull(epoch, "Epoch is missing for marker key " + key);
            }
            if (markers.put(key, new MarkerVersion(epoch, version, unstamped)) != null) {
                throw new IllegalArgumentException("Marker key " + key + " is stored more than once");
            }
        }
        return DcbMarkerVersions.of(criteria, markers);
    }
}
