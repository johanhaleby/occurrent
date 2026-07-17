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

/**
 * A cached fold result: the {@code state} produced by folding every event up to and including {@code version}, tagged
 * with the {@code schemaVersion} of the fold that produced it.
 * <p>
 * A snapshot is a discardable optimization. It is always reproducible by folding the events, so losing one only means a
 * fuller replay next time, never lost data. The {@code schemaVersion} lets a reader detect that the {@code state} shape
 * has changed since the snapshot was written and fall back to a full replay rather than deserializing a stale shape.
 *
 * @param state         the folded state (may be {@code null} for a state type that models "no state yet" as null)
 * @param version       the version the state was folded up to: the stream version for the stream path, or the global
 *                      DCB position for the DCB path. Events strictly after this version are folded onto {@code state} to resume.
 * @param schemaVersion the schema version of the fold that produced this state, bumped by the author whenever the state
 *                      shape changes so older snapshots invalidate instead of being read into the new shape
 * @param <S>           the state type
 */
public record Snapshot<S extends @Nullable Object>(S state, long version, int schemaVersion) {
    public Snapshot {
        if (version < 0) {
            throw new IllegalArgumentException("version cannot be negative");
        }
        if (schemaVersion < 0) {
            throw new IllegalArgumentException("schemaVersion cannot be negative");
        }
    }
}
