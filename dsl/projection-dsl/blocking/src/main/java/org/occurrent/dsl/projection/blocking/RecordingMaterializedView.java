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

package org.occurrent.dsl.projection.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedPositionStore;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAwareMaterializedView;

/**
 * The {@link MaterializedView} {@link Projections#recordingAppliedPosition(MaterializedView, AppliedPositionStore, String)}
 * builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0111-a-projection-records-the-position-it-has-applied.md">ADR 111</a>).
 * Delegates every update to the wrapped view and then advances {@code store}, so the recorded position is written
 * only after the state it describes.
 * <p>
 * Implements {@link ReplayAwareMaterializedView} and forwards every lifecycle call to the delegate when it implements
 * the capability too. During a replay the delegate may be buffering (a coalescing view), so the position seen so far
 * is kept in memory and only written in {@link #replayCompleted()}, after the delegate has flushed.
 * {@link #replayAbandoned()} discards it instead, since the next replay recomputes everything anyway.
 */
@NullMarked
final class RecordingMaterializedView<E> implements MaterializedView<E>, ReplayAwareMaterializedView {

    private final MaterializedView<E> delegate;
    private final AppliedPositionStore store;
    private final String projectionId;

    // Volatile because the replay runs on the catch-up thread and live updates run on whichever thread delivers them,
    // so the two hand over across threads. Only the replay thread ever writes highestPositionSeenDuringReplay, since
    // live events are buffered elsewhere until the replay is over, so its read-modify-write cannot lose an update.
    private volatile boolean replaying = false;
    private volatile long highestPositionSeenDuringReplay = 0;

    RecordingMaterializedView(MaterializedView<E> delegate, AppliedPositionStore store, String projectionId) {
        this.delegate = delegate;
        this.store = store;
        this.projectionId = projectionId;
    }

    @Override
    public void update(E event) {
        update(EventMetadata.empty(), event);
    }

    @Override
    public void update(EventMetadata metadata, E event) {
        @Nullable Long position = metadata.getPosition();
        if (position == null) {
            throw new IllegalStateException(("Projection '%s' is configured to record its applied position, but received an event with no position. " +
                    "Either the event store has position writing turned off, or the event arrived on a path that carries no metadata " +
                    "(a live domain-event feed the application did not pass metadata into, or the metadata-less query/replay path).").formatted(projectionId));
        }
        delegate.update(metadata, event);
        if (replaying) {
            if (position > highestPositionSeenDuringReplay) {
                highestPositionSeenDuringReplay = position;
            }
        } else {
            store.advance(projectionId, position);
        }
    }

    @Override
    public void replayStarted() {
        if (delegate instanceof ReplayAwareMaterializedView replayAware) {
            replayAware.replayStarted();
        }
        replaying = true;
        highestPositionSeenDuringReplay = 0;
    }

    @Override
    public void replayCompleted() {
        if (delegate instanceof ReplayAwareMaterializedView replayAware) {
            replayAware.replayCompleted();
        }
        if (highestPositionSeenDuringReplay > 0) {
            store.advance(projectionId, highestPositionSeenDuringReplay);
        }
        replaying = false;
    }

    @Override
    public void replayAbandoned() {
        if (delegate instanceof ReplayAwareMaterializedView replayAware) {
            replayAware.replayAbandoned();
        }
        highestPositionSeenDuringReplay = 0;
        replaying = false;
    }
}
