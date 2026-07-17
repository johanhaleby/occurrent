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
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SnapshotView} paired with the {@link DcbCriteria} that selects which events feed it, the DCB read-side mirror
 * of {@code org.occurrent.dsl.dcb.DcbDecider}. The handlers drive the fold, the criteria drives the read.
 * <p>
 * This is the descriptor a DCB {@code @Snapshot} factory returns. Because a DCB boundary has no stream id, a snapshot of
 * it is single-instance, keyed by the criteria rather than per stream.
 *
 * @param snapshotView the deciders-free fold and its schema version
 * @param criteria     the DCB read boundary selecting the events that feed the fold
 * @param <S>          the state type
 * @param <E>          the event type
 */
public record DcbSnapshotView<S extends @Nullable Object, E>(SnapshotView<S, E> snapshotView, DcbCriteria criteria) {

    public DcbSnapshotView {
        requireNonNull(snapshotView, "snapshotView cannot be null");
        requireNonNull(criteria, "criteria cannot be null");
    }

    /** The schema version tagging the state this fold produces (from the underlying {@link SnapshotView}). */
    public int schemaVersion() {
        return snapshotView.schemaVersion();
    }
}
