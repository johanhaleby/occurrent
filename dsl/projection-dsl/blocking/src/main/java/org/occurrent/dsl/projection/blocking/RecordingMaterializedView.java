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
import org.occurrent.dsl.projection.AppliedAppendRecorder;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.internal.AppliedAppendRecording;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ReplayAware;

import static java.util.Objects.requireNonNull;

/**
 * The recording view {@link Projections#recordingAppliedAppends(MaterializedView, String, AppliedAppendStore)}
 * builds
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
 * Always delegates {@link #update(EventMetadata, Object)} first, then records the delivered event's append id into
 * {@code store}, so the recorded membership never claims state the delegate has not actually written yet. Nothing
 * is recorded while the wrapped projection is replaying, and {@link #update(Object)} (no metadata, so no append id
 * to read) delegates with no attempt to record. Nothing is recorded either for a delegate that reports skipping the
 * event, {@link CoalescingMaterializedView} when its id mapper resolves to no key, since such an event never
 * changed the read model this recording claims to describe.
 * <p>
 * Implements {@link ReplayAware} and forwards every call to the delegate when it is one too, so a batching view
 * underneath keeps batching, and drives its own replay bookkeeping either way: a domain-feed or catch-up-feed
 * composition is the only source of these calls (see {@code CatchupProjectionFeed}), so this is also this class's
 * only replay signal on that path, mapped onto the same two catch-up signals a subscription model sends
 * instead. The first {@link MaterializedView} wrapping another {@link MaterializedView} in this library.
 */
@NullMarked
public final class RecordingMaterializedView<E> implements MaterializedView<E>, ReplayAware, AppliedAppendRecorder {

    private final MaterializedView<E> delegate;
    private final AppliedAppendRecording recording;
    // The episode minted for the replay a pull feed is currently driving, so its completion names the same one.
    private volatile @Nullable Object feedEpisode = null;

    RecordingMaterializedView(MaterializedView<E> delegate, String projectionId, AppliedAppendStore store) {
        this.delegate = requireNonNull(delegate, "delegate cannot be null");
        this.recording = new AppliedAppendRecording(projectionId, store);
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
        if (applyDelegate(metadata, event)) {
            recording.recordIfReady(metadata);
        }
    }

    // The unchecked cast is safe: only a CoalescingMaterializedView<?, E, ?> for this same E ever implements
    // SkippableUpdate<E>, since it is package-private and delegate's own generic parameter already fixes E.
    @SuppressWarnings("unchecked")
    private boolean applyDelegate(EventMetadata metadata, E event) {
        if (delegate instanceof SkippableUpdate<?> skippable) {
            return ((SkippableUpdate<E>) skippable).applyReportingWhetherApplied(metadata, event);
        }
        delegate.update(metadata, event);
        return true;
    }

    @Override
    public void catchupStarted(Object episode) {
        recording.catchupStarted(episode);
    }

    @Override
    public void historyRead(Object episode) {
        recording.historyRead(episode);
    }

    @Override
    public void retryPendingClear() {
        recording.retryPendingClear();
    }

    @Override
    public boolean pollForClear() {
        return recording.pollForClear();
    }

    // The replay lifecycle a pull feed drives, mapped onto the two catch-up signals. The feed does not mint an
    // episode, so one is minted here, which is the same thing once per replay it starts.
    @Override
    public void replayStarted() {
        if (delegate instanceof ReplayAware replayAware) {
            replayAware.replayStarted();
        }
        Object started = new Object();
        feedEpisode = started;
        recording.catchupStarted(started);
    }

    @Override
    public void replayCompleted() {
        if (delegate instanceof ReplayAware replayAware) {
            replayAware.replayCompleted();
        }
        Object started = feedEpisode;
        if (started != null) {
            recording.historyRead(started);
        }
        // A feed is not polled, so nothing else would retry a clear its replay left owed, and this call runs on a
        // thread the engine already blocks on.
        recording.retryPendingClear();
    }

    @Override
    public void replayAbandoned() {
        if (delegate instanceof ReplayAware replayAware) {
            replayAware.replayAbandoned();
        }
        // No boundary for a replay that stopped part way through. The next one announces itself.
    }
}
