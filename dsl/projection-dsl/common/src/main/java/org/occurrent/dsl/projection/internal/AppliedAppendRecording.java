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
import org.occurrent.eventstore.api.AppendId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashSet;
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

    // Guards every check-and-write against a concurrent clear attempt, from whichever of readiness check, record,
    // or clear runs first: without one lock spanning both halves, a live delivery that read "ready" just before a
    // poll-driven clear could still write its append back in immediately after that clear finished, reinstating a
    // record the clear was supposed to remove. Reentrant, so a method already holding it can still call another
    // that also declares it, on the same thread.
    private final Object clearLock = new Object();
    private volatile boolean pendingClear = false;
    // Tracks whether the current run of clear failures has already logged once at ERROR, so a clear stuck failing
    // for a while logs loudly exactly once and DEBUG on every retry after that, per ADR 132 decision 7.
    private volatile boolean clearFailureLogged = false;
    // One-slot dedup: a single append usually delivers several events and only the first needs a write. Reset
    // whenever a clear succeeds, since the store no longer has anything recorded to deduplicate against.
    private volatile @Nullable AppendId lastRecorded = null;
    // The catch-up this recorder currently belongs to, or null when none does. Written only by catchupStarted and
    // compared by identity, so a signal from a catch-up that has since lost its subscription is told apart from one
    // from the catch-up that took it over.
    private @Nullable Object episode = null;
    // True between a catch-up's start and its history boundary. Nothing is recorded while it is set.
    private boolean readingHistory = false;
    // Appends handled during a reconciliation while a clear was owed. Recording them then would be pointless, since
    // the pending clear deletes every record for this projection, so they wait here and are written once it lands.
    // Bounded because a reconciliation under a clear that keeps failing has no other limit, and an append evicted
    // from here is never recorded, unlike an id evicted from a delivery dedup cache.
    private final LinkedHashSet<AppendId> awaitingClear = new LinkedHashSet<>();
    private boolean awaitingClearOverflowLogged = false;

    // Holds one append id per append, so a reconciliation of a hundred thousand events costs far fewer entries than
    // that. Chosen to be large enough that reaching it means a clear has been failing for a long time.
    private static final int MAX_AWAITING_CLEAR = 1000;

    public AppliedAppendRecording(String projectionId, AppliedAppendStore store) {
        this.projectionId = requireNonNull(projectionId, "projectionId cannot be null");
        this.store = requireNonNull(store, "store cannot be null");
    }

    /**
     * Records {@code metadata}'s append id if the projection is ready to record right now, atomically with a
     * concurrent clear, so a write already about to happen when a clear runs can never land after it and reinstate
     * what the clear just removed. Not recording is never an error.
     * <p>
     * What happens depends on what the model last said. Between {@link #catchupStarted(Object)} and
     * {@link #historyRead(Object)} nothing is recorded, and the clear a catch-up implies is attempted once rather
     * than once per delivery. After that boundary, and when no catch-up is running at all, the append is recorded,
     * because a catch-up delivering events written since it started is the only delivery some of them get.
     * <p>
     * An append handled during a reconciliation while a clear is still owed waits until that clear lands, since
     * recording it any earlier would only give the pending clear something more to delete. Up to a thousand such
     * appends wait, and past that the oldest are dropped with a warning. An append with no identifier (it predates
     * this feature, or arrived through a push feed whose producer supplied none), a malformed one, or a repeat of the
     * one just recorded for this instance, is skipped quietly in every phase.
     * <p>
     * May block on {@link AppliedAppendStore#clear(String)} or {@link AppliedAppendStore#recordApplied}, so a
     * reactive caller must already be off the event loop.
     */
    public void recordIfReady(EventMetadata metadata) {
        requireNonNull(metadata, "metadata cannot be null");
        synchronized (clearLock) {
            if (readingHistory) {
                // The clear this catch-up owes, and nothing else. A history of N events clears once rather than
                // running N deleteMany calls, because a clear that succeeds is no longer owed.
                if (pendingClear) {
                    attemptClear();
                }
                return;
            }
            if (pendingClear) {
                attemptClear();
            }
            flushAwaitingClear();
            if (pendingClear) {
                bufferUntilClear(metadata);
                return;
            }
            doRecord(metadata);
        }
    }

    /**
     * Writes what the pending clear held back, gated on the buffer rather than on whichever call happened to run the
     * clear that succeeded. A clear can succeed inside the history branch of a poll tick, which flushes nothing, and
     * a flush placed inside a {@code pendingClear} guard would then never run again because no clear is owed any
     * more. Runs under the same lock acquisition as the delivery it precedes, so nothing interleaves between the
     * clear and these writes.
     */
    private void flushAwaitingClear() {
        if (pendingClear || awaitingClear.isEmpty()) {
            return;
        }
        for (AppendId appendId : awaitingClear) {
            store.recordApplied(projectionId, appendId);
            lastRecorded = appendId;
        }
        dropAwaitingClear();
    }

    // Assumes clearLock is already held by the caller.
    private void dropAwaitingClear() {
        awaitingClear.clear();
        awaitingClearOverflowLogged = false;
    }

    // Assumes clearLock is already held by the caller. Only reached when a clear is owed and the history is not
    // being read, so what is held here is always an append this projection applied and would otherwise lose: the
    // pending clear would delete it if it were written now, and nothing would write it again afterwards.
    private void bufferUntilClear(EventMetadata metadata) {
        AppendId appendId;
        try {
            appendId = AppendId.from(metadata).orElse(null);
        } catch (IllegalArgumentException e) {
            return;
        }
        if (appendId == null || awaitingClear.contains(appendId)) {
            return;
        }
        if (awaitingClear.size() >= MAX_AWAITING_CLEAR) {
            if (!awaitingClearOverflowLogged) {
                log.warn("Projection '{}' has {} appends waiting for a clear that keeps failing, so the oldest are being dropped. A wait for a dropped append answers false until it times out.", projectionId, MAX_AWAITING_CLEAR);
                awaitingClearOverflowLogged = true;
            }
            Iterator<AppendId> oldest = awaitingClear.iterator();
            oldest.next();
            oldest.remove();
        }
        awaitingClear.add(appendId);
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
     * A catch-up has begun. Marks the clear it implies as owed, drops what a previous catch-up was still holding, and
     * suppresses recording until {@link #historyRead(Object)} for this same {@code episode}.
     * <p>
     * Deliberately performs no store call. It is sent from the thread that registers the catch-up, before whatever
     * produces the deliveries exists, and blocking that thread on a store round trip would hold up the subscription
     * itself. The clear happens on the next call that can afford it, a delivery or a poll tick.
     */
    public void catchupStarted(Object episode) {
        requireNonNull(episode, "episode cannot be null");
        synchronized (clearLock) {
            this.episode = episode;
            readingHistory = true;
            pendingClear = true;
            // What a previous catch-up was holding describes a read model this one is rebuilding.
            dropAwaitingClear();
        }
    }

    /**
     * The history {@code episode} set out to read has been read, so what follows was written since it started and is
     * recorded. Ignored for any other catch-up, which is what stops one that has lost its subscription from moving
     * its replacement past a history the replacement has not read.
     * <p>
     * Performs no store call, for the same reason {@link #catchupStarted(Object)} does not.
     */
    public void historyRead(Object episode) {
        requireNonNull(episode, "episode cannot be null");
        synchronized (clearLock) {
            if (this.episode == episode) {
                readingHistory = false;
            }
        }
    }

    /**
     * The one hook {@code AppliedAppendRecorder.retryPendingClear()} forwards to. Retries a clear already marked as
     * owed, and does nothing if none is. Lets a poll tick retry a clear a catch-up left failing, since a catch-up
     * having moved on is not the same as the clear it owed having succeeded.
     * <p>
     * Writes nothing that is waiting for that clear even when it succeeds here. This hook is not told which catch-up
     * the projection is in, so it cannot tell a wait that belongs to the current one from a wait a lost signal left
     * behind. {@link #recordIfReady(EventMetadata)} and {@link #pollForClear()} both know, and write them.
     */
    public void retryPendingClear() {
        synchronized (clearLock) {
            if (pendingClear) {
                attemptClear();
            }
        }
    }

    /**
     * The one hook {@code AppliedAppendRecorder.pollForClear()} forwards to: retry an owed clear, write whatever was
     * waiting for it, and report whether one is still owed so the poll can pace on that.
     * <p>
     * This is what keeps a clear moving for a projection that has gone quiet. Without it, a clear that failed while
     * a catch-up ran would only be retried by the next delivery, and a projection that receives none would never
     * record again.
     */
    public boolean pollForClear() {
        synchronized (clearLock) {
            if (pendingClear) {
                attemptClear();
            }
            // Nothing is ever waiting while the history is being read, since a catch-up start drops what the
            // previous one held and a delivery during the history read buffers nothing.
            flushAwaitingClear();
            return pendingClear;
        }
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
