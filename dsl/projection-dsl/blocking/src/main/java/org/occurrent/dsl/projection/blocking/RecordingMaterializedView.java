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
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.dsl.projection.internal.AppliedAppendRecording;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;

import static java.util.Objects.requireNonNull;

/**
 * The recording view {@link Projections#recordingAppliedAppends(MaterializedView, String, AppliedAppendStore, ReplayPhase)}
 * builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
 * Always delegates {@link #update(EventMetadata, Object)} first, then records the delivered event's append id into
 * {@code store}, so the recorded membership never claims state the delegate has not actually written yet. Nothing
 * is recorded while the wrapped projection is replaying, and {@link #update(Object)} (no metadata, so no append id
 * to read) delegates with no attempt to record.
 * <p>
 * Implements {@link ReplayAware} and forwards every call to the delegate when it is one too, so a batching view
 * underneath keeps batching, and drives its own replay bookkeeping either way: a domain-feed or catch-up-feed
 * composition is the only source of these calls (see {@code CatchupProjectionFeed}), so this is also this class's
 * only replay signal on that path, distinct from the {@link ReplayPhase} a subscription-fed composition supplies
 * instead. The first {@link MaterializedView} wrapping another {@link MaterializedView} in this library.
 */
@NullMarked
public final class RecordingMaterializedView<E> implements MaterializedView<E>, ReplayAware, AppliedAppendRecorder {

    private final MaterializedView<E> delegate;
    private final AppliedAppendRecording recording;

    RecordingMaterializedView(MaterializedView<E> delegate, String projectionId, AppliedAppendStore store, ReplayPhase phase) {
        this.delegate = requireNonNull(delegate, "delegate cannot be null");
        this.recording = new AppliedAppendRecording(projectionId, store, phase);
    }

    @Override
    public void update(E event) {
        // No metadata means no appendid to read, so there is nothing to attempt recording for. Skip straight to the
        // delegate rather than routing through update(EventMetadata, E) with EventMetadata.empty() only to have
        // AppliedAppendRecording.record(..) log a debug line for every such event.
        delegate.update(event);
    }

    @Override
    public void update(EventMetadata metadata, E event) {
        delegate.update(metadata, event);
        recording.recordIfReady(metadata);
    }

    @Override
    public void replayObserved() {
        recording.replayObserved();
    }

    @Override
    public void retryPendingClear() {
        recording.retryPendingClear();
    }

    @Override
    public void replayStarted() {
        if (delegate instanceof ReplayAware replayAware) {
            replayAware.replayStarted();
        }
        recording.replayStarted();
    }

    @Override
    public void replayCompleted() {
        if (delegate instanceof ReplayAware replayAware) {
            replayAware.replayCompleted();
        }
        recording.replayCompleted();
    }

    @Override
    public void replayAbandoned() {
        if (delegate instanceof ReplayAware replayAware) {
            replayAware.replayAbandoned();
        }
        recording.replayAbandoned();
    }
}
