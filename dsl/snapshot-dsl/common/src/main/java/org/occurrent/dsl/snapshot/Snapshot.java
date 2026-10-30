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

import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A cached fold result: the {@code state} produced by folding every event up to and including {@code version}, tagged
 * with the {@code schemaVersion} of the fold that produced it.
 * <p>
 * A snapshot is a discardable optimization. It is always reproducible by folding the events, so losing one only means a
 * fuller replay next time, never lost data. The {@code schemaVersion} lets a reader detect that the {@code state} shape
 * has changed since the snapshot was written and fall back to a full replay rather than deserializing a stale shape.
 * <p>
 * A snapshot of a DCB boundary is folded up to {@code markerVersions} instead of a version. Create it with
 * {@link #atMarkerVersions(Object, DcbMarkerVersions, long, int)}. Its {@code version} is {@code 0}.
 *
 * @param state          the folded state (may be {@code null} for a state type that models "no state yet" as null)
 * @param version        the stream version the state was folded up to. Events strictly after this version are folded
 *                       onto {@code state} to resume. {@code 0} for a snapshot with {@code markerVersions}.
 * @param schemaVersion  the schema version of the fold that produced this state, bumped by the author whenever the state
 *                       shape changes so older snapshots invalidate instead of being read into the new shape
 * @param markerVersions the marker versions of the DCB boundary the state was folded up to. The events appended after
 *                       them are folded onto {@code state} to resume. Empty for a stream snapshot.
 * @param highestFoldedPosition the highest position among the events folded into {@code state}. An event read after
 *                       {@code markerVersions} that sits below it committed late, so folding it onto {@code state}
 *                       could give another state than a read in position order, and the boundary is read in full
 *                       instead. {@code 0} for a stream snapshot.
 * @param <S>            the state type
 */
public record Snapshot<S extends @Nullable Object>(S state, long version, int schemaVersion, Optional<DcbMarkerVersions> markerVersions, long highestFoldedPosition) {
    public Snapshot {
        if (version < 0) {
            throw new IllegalArgumentException("version cannot be negative");
        }
        if (schemaVersion < 0) {
            throw new IllegalArgumentException("schemaVersion cannot be negative");
        }
        requireNonNull(markerVersions, "markerVersions cannot be null");
        if (markerVersions.isPresent() && version != 0) {
            throw new IllegalArgumentException("version must be 0 for a snapshot with marker versions");
        }
        if (highestFoldedPosition < 0) {
            throw new IllegalArgumentException("highestFoldedPosition cannot be negative");
        }
        if (markerVersions.isEmpty() && highestFoldedPosition != 0) {
            throw new IllegalArgumentException("highestFoldedPosition must be 0 for a snapshot without marker versions");
        }
    }

    /**
     * Creates a snapshot folded up to {@code version}, with no marker versions.
     */
    public Snapshot(S state, long version, int schemaVersion) {
        this(state, version, schemaVersion, Optional.empty(), 0);
    }

    /**
     * Creates a snapshot of a DCB boundary folded up to {@code markerVersions}, where {@code highestFoldedPosition} is the
     * highest position among the events folded into {@code state}.
     */
    public static <S extends @Nullable Object> Snapshot<S> atMarkerVersions(S state, DcbMarkerVersions markerVersions, long highestFoldedPosition, int schemaVersion) {
        requireNonNull(markerVersions, "markerVersions cannot be null");
        return new Snapshot<>(state, 0, schemaVersion, Optional.of(markerVersions), highestFoldedPosition);
    }
}
