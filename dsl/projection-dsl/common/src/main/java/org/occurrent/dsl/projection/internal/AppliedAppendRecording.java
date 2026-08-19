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
 * reactor wrapper is responsible for calling {@link #recordIfReady(EventMetadata)} and {@link #replayCompleted()}
 * only after hopping to a blocking-safe scheduler, and {@link #replayStarted()}/{@link #replayAbandoned()} are
 * deliberately I/O-free so a reactive lifecycle signal that is never awaited can call them inline without blocking.
 */
@NullMarked
public final class AppliedAppendRecording {

    private static final Logger log = LoggerFactory.getLogger(AppliedAppendRecording.class);

    private final String projectionId;
    private final AppliedAppendStore store;
    private final ReplayPhase phase;

    // Guards every check-and-write against a concurrent clear attempt, from whichever of readiness check, record,
    // or clear runs first: without one lock spanning both halves, a live delivery that read "ready" just before a
    // poll-driven clear could still write its append back in immediately after that clear finished, reinstating a
    // record the clear was supposed to remove. Reentrant, so a method already holding it can still call another
    // that also declares it, on the same thread.
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
     * Records {@code metadata}'s append id if the projection is ready to record right now, atomically with a
     * concurrent clear: a write already about to happen when a clear runs can never land after it and reinstate
     * what the clear just removed. Not recording is never an error. It means the projection is currently replaying
     * (lifecycle or phase, whichever says so), which also attempts the clear that implies, or a previously owed
     * clear has not yet succeeded, which this also retries. An append with no identifier (predates this feature, or
     * arrived through a push feed whose producer supplied none), a malformed one, or a repeat of the one just
     * recorded for this instance, is skipped quietly either way. May block on {@link AppliedAppendStore#clear(String)}
     * or {@link AppliedAppendStore#recordApplied}, so a reactive caller must already be off the event loop.
     */
    public void recordIfReady(EventMetadata metadata) {
        requireNonNull(metadata, "metadata cannot be null");
        synchronized (clearLock) {
            if (lifecycleReplaying || phase.isReplaying()) {
                pendingClear = true;
                attemptClear();
                return;
            }
            if (pendingClear) {
                attemptClear();
                if (pendingClear) {
                    return;
                }
            }
            doRecord(metadata);
        }
    }

    private void doRecord(EventMetadata metadata) {
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
     * that tolerates blocking.
     */
    public void replayObserved() {
        synchronized (clearLock) {
            pendingClear = true;
            attemptClear();
        }
    }

    /**
     * The one hook {@code AppliedAppendRecorder.pollReplayPhase()} forwards to: re-checks {@code phase} and reacts,
     * both inside the same {@code clearLock} acquisition, and returns what the check found. A caller that read
     * {@code phase} itself first and dispatched to {@link #replayObserved()} or {@link #retryPendingClear()} based
     * on that earlier read races a live delivery landing between the read and the call: this projection recording a
     * genuinely live append in between, only to have it wiped by a clear this method then runs for a replay that
     * ended before the call arrived. Re-checking here, under the same lock the clear itself runs under, closes that
     * window instead of narrowing it.
     */
    public boolean pollReplayPhase() {
        synchronized (clearLock) {
            boolean replaying = lifecycleReplaying || phase.isReplaying();
            if (replaying) {
                pendingClear = true;
                attemptClear();
            } else if (pendingClear) {
                attemptClear();
            }
            return replaying;
        }
    }

    /**
     * The one hook {@code AppliedAppendRecorder.retryPendingClear()} forwards to: retry a clear already marked as
     * owed, doing nothing if none is. Lets a poll tick that finds the phase back to live still retry a clear a
     * replay observed earlier and left failing, since the phase no longer reporting a replay is not the same as the
     * clear it caused having succeeded.
     */
    public void retryPendingClear() {
        synchronized (clearLock) {
            if (pendingClear) {
                attemptClear();
            }
        }
    }

    /**
     * A catch-up replay has started delivering to this projection (the view-DSL replay lifecycle). Marks a clear as
     * needed but, deliberately, does not attempt it here: {@code ReplayAware.replayStarted()} and
     * {@code ReactiveReplayAware.replayStarted()} are void signals the driving engine calls inline and never waits
     * on, so attempting a blocking clear from here could stall that engine's own thread. The attempt happens lazily,
     * the next time {@link #recordIfReady(EventMetadata)}, {@link #replayCompleted()}, {@link #replayObserved()}, or
     * {@link #retryPendingClear()} runs on a thread that can afford to block.
     */
    public void replayStarted() {
        lifecycleReplaying = true;
        pendingClear = true;
    }

    /**
     * The replay lifecycle finished delivering everything it had. Unlike {@link #replayStarted()} this is safe to
     * attempt the clear from directly: {@code ReplayAware.replayCompleted()} is a plain synchronous call already
     * tolerant of blocking work, and {@code ReactiveReplayAware.replayCompleted()} returns the one lifecycle
     * {@code Mono} its driving engine actually awaits, so the reactor wrapper hops this call to a blocking-safe
     * scheduler rather than needing to defer it further. Closes the window a replay that delivers nothing matching
     * would otherwise leave open until a live event, or the poll (for a composition that has one), got to it.
     */
    public void replayCompleted() {
        lifecycleReplaying = false;
        synchronized (clearLock) {
            attemptClear();
        }
    }

    /**
     * The replay lifecycle was abandoned before it finished. Deliberately does not attempt the clear here, for the
     * same reason {@link #replayStarted()} does not: {@code replayAbandoned()} is a void signal on both stacks that
     * its driving engine never awaits, so blocking here could stall it. {@link #replayStarted()} already marked the
     * clear as owed, and a later delivery, {@link #replayCompleted()} on the replay that follows, or the poll,
     * retries it.
     */
    public void replayAbandoned() {
        lifecycleReplaying = false;
    }

    // Assumes clearLock is already held by the caller.
    private void attemptClear() {
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
