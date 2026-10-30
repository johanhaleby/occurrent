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
 * Converts {@link DcbMarkerVersions} to and from the plain values a snapshot store persists, one map per marker key with
 * the fields {@code k} (key), {@code e} (epoch), {@code v} (version) and {@code u} (unstamped). A list of maps is used
 * instead of one map keyed by marker key, since a marker key can contain a dot, which a MongoDB field name can't.
 */
public final class SnapshotMarkerVersionsFields {
    private static final String KEY = "k";
    private static final String EPOCH = "e";
    private static final String VERSION = "v";
    private static final String UNSTAMPED = "u";

    private SnapshotMarkerVersionsFields() {
    }

    /**
     * The fields to persist for {@code markerVersions}, one map per marker key in ascending key order.
     */
    public static List<Map<String, Object>> toFields(DcbMarkerVersions markerVersions) {
        requireNonNull(markerVersions, "markerVersions cannot be null");
        List<Map<String, Object>> fields = new ArrayList<>(markerVersions.markers().size());
        markerVersions.markers().forEach((key, markerVersion) -> {
            Map<String, Object> field = new LinkedHashMap<>();
            field.put(KEY, key);
            field.put(EPOCH, markerVersion.epoch());
            field.put(VERSION, markerVersion.version());
            field.put(UNSTAMPED, markerVersion.unstamped());
            fields.add(field);
        });
        return fields;
    }

    /**
     * The marker versions that {@link #toFields(DcbMarkerVersions)} produced. Throws when a field is missing or has the
     * wrong type, so the caller treats the snapshot as unreadable.
     */
    public static DcbMarkerVersions fromFields(List<?> fields) {
        requireNonNull(fields, "fields cannot be null");
        Map<String, MarkerVersion> markers = new LinkedHashMap<>();
        for (Object element : fields) {
            if (!(element instanceof Map<?, ?> field)) {
                throw new IllegalArgumentException("A marker version must be a map, was " + element);
            }
            String key = (String) requireNonNull(field.get(KEY), "Marker key is missing");
            String epoch = (String) requireNonNull(field.get(EPOCH), "Epoch is missing for marker key " + key);
            long version = ((Number) requireNonNull(field.get(VERSION), "Version is missing for marker key " + key)).longValue();
            long unstamped = ((Number) requireNonNull(field.get(UNSTAMPED), "Unstamped count is missing for marker key " + key)).longValue();
            if (markers.put(key, new MarkerVersion(epoch, version, unstamped)) != null) {
                throw new IllegalArgumentException("Marker key " + key + " is stored more than once");
            }
        }
        return DcbMarkerVersions.of(markers);
    }
}
