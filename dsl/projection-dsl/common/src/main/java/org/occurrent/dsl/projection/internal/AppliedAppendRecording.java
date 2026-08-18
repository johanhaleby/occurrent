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

package org.occurrent.dsl.projection.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.ReplayPhase;
import org.occurrent.eventstore.api.AppendId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The stack-free state machine both recording wrappers ({@code RecordingMaterializedView} in the blocking DSL,
 * {@code RecordingReactiveUpdate} in the reactor DSL) delegate to
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>).
 * Package-private-shaped state, exposed as a public helper so both stack-specific modules can reuse it without a
 * third copy of the same rules.
 * <p>
 * All I/O this class performs ({@link AppliedAppendStore#clear(String)} and {@link AppliedAppendStore#recordApplied(String, org.occurrent.eventstore.api.AppendId)})
 * runs on whichever thread the caller invokes it from. It performs no scheduling or thread-hopping of its own, so the
 * reactor wrapper is responsible for calling {@link #readyToRecord()} and {@link #record(EventMetadata)} only after
 * hopping to a blocking-safe scheduler, and {@link #replayStarted()} is deliberately I/O-free so a reactive
 * lifecycle signal that is never awaited can call it inline without blocking.
 */
@NullMarked
public final class AppliedAppendRecording {

    private static final Logger log = LoggerFactory.getLogger(AppliedAppendRecording.class);

    private final String projectionId;
    private final AppliedAppendStore store;
    private final ReplayPhase phase;

    // Guards a clear attempt so the poll and a live delivery can never run store.clear() concurrently for the same
    // projection. Held only around the clear itself, not around readyToRecord()'s fast path, so an ordinary live
    // delivery pays nothing while no clear is pending.
    private final Object clearLock = new Object();
    private volatile boolean pendingClear = false;
    // Set by the view-DSL replay lifecycle (CatchupProjectionFeed/DomainEventFeed), independent of what phase says.
    // A composition fed that way has no ReplayPhase to ask (it is neverReplays()), so this is its only replay signal.
    private volatile boolean lifecycleReplaying = false;
    // Tracks whether the current run of clear failures has already logged once at ERROR, so a clear stuck failing
    // for a while logs loudly exactly once and DEBUG on every retry after that, per ADR 132 decision 7.
    private volatile boolean clearFailureLogged = false;
    // One-slot dedup: a single append usually delivers several events and only the first needs a write. Reset
    // whenever a clear succeeds, since the store no longer has anything recorded to deduplicate against.
    private volatile @Nullable AppendId lastRecorded = null;

    public AppliedAppendRecording(String projectionId, AppliedAppendStore store, ReplayPhase phase) {
        this.projectionId = requireNonNull(projectionId, "projectionId cannot be null");
        this.store = requireNonNull(store, "store cannot be null");
        this.phase = requireNonNull(phase, "phase cannot be null");
    }

    /**
     * Whether the caller may record now. {@code false} while replaying (lifecycle or phase, whichever says so),
     * which also marks a clear as needed. {@code false} while a needed clear has not yet succeeded, attempting that
     * clear as a side effect. Safe to call from any thread; may block on {@link AppliedAppendStore#clear(String)}
     * when a clear is due, so a reactive caller must already be off the event loop before calling this.
     */
    public boolean readyToRecord() {
        if (lifecycleReplaying || phase.isReplaying()) {
            pendingClear = true;
            return false;
        }
        if (pendingClear) {
            attemptClear();
        }
        return !pendingClear;
    }

    /**
     * Records {@code metadata}'s append id, unless it has none (predates this feature, or arrived through a push
     * feed whose producer supplied none), is malformed, or is the same one just recorded for this instance. Call
     * only after {@link #readyToRecord()} answered {@code true}. May block on {@link AppliedAppendStore#recordApplied}.
     */
    public void record(EventMetadata metadata) {
        requireNonNull(metadata, "metadata cannot be null");
        Optional<AppendId> appendId;
        try {
            appendId = AppendId.from(metadata);
        } catch (IllegalArgumentException e) {
            log.debug("Projection '{}' handled an event whose appendid extension is not a valid UUID, skipping recording for it.", projectionId, e);
            return;
        }
        if (appendId.isEmpty()) {
            log.debug("Projection '{}' handled an event with no appendid extension, skipping recording for it. Either the event " +
                    "predates this feature, or it arrived on a path that never stamped one (a push feed whose producer supplied " +
                    "no appendid, or a metadata-less fold).", projectionId);
            return;
        }
        AppendId id = appendId.get();
        if (id.equals(lastRecorded)) {
            return;
        }
        store.recordApplied(projectionId, id);
        lastRecorded = id;
    }

    /**
     * The one hook {@code AppliedAppendRecorder.replayObserved()} forwards to: mark a clear as needed and attempt it
     * on the calling thread now. Used by the Spring Boot registrars' poll, which always calls this from a thread
     * that tolerates blocking. Not called by the view-DSL replay lifecycle forwarding below, which must stay
     * non-blocking; see {@link #replayStarted()}.
     */
    public void replayObserved() {
        pendingClear = true;
        attemptClear();
    }

    /**
     * A catch-up replay has started delivering to this projection (the view-DSL replay lifecycle). Marks a clear as
     * needed but, deliberately, does not attempt it here: {@code ReplayAware.replayStarted()} and
     * {@code ReactiveReplayAware.replayStarted()} are void signals the driving engine calls inline and never waits
     * on, so attempting a blocking clear from here could stall that engine's own thread. The attempt happens lazily,
     * the next time {@link #readyToRecord()} or {@link #replayObserved()} runs on a thread that can afford to block.
     */
    public void replayStarted() {
        lifecycleReplaying = true;
        pendingClear = true;
    }

    /**
     * The replay lifecycle ended, whether it finished or was abandoned. Either way nothing more is buffered to
     * apply, so recording may resume once the pending clear (marked by {@link #replayStarted()}) succeeds.
     */
    public void replayEnded() {
        lifecycleReplaying = false;
    }

    private void attemptClear() {
        synchronized (clearLock) {
            if (!pendingClear) {
                return;
            }
            try {
                store.clear(projectionId);
                pendingClear = false;
                lastRecorded = null;
                clearFailureLogged = false;
            } catch (RuntimeException e) {
                if (clearFailureLogged) {
                    log.debug("Projection '{}' retried clearing its previously recorded appends and it is still failing. Recording stays off until a clear succeeds.", projectionId, e);
                } else {
                    log.error("Projection '{}' observed a replay and could not clear its previously recorded appends. Recording stays off, and a wait for an append recorded before this replay may keep answering true about a read model this rebuild is discarding, until a clear succeeds.", projectionId, e);
                    clearFailureLogged = true;
                }
            }
        }
    }
}
