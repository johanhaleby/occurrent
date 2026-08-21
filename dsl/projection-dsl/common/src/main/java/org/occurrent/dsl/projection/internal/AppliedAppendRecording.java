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
import org.occurrent.dsl.projection.CatchupPhase;
import org.occurrent.dsl.projection.ReplayPhase;
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
    private final ReplayPhase phase;

    // Guards every check-and-write against a concurrent clear attempt, from whichever of readiness check, record,
    // or clear runs first: without one lock spanning both halves, a live delivery that read "ready" just before a
    // poll-driven clear could still write its append back in immediately after that clear finished, reinstating a
    // record the clear was supposed to remove. Reentrant, so a method already holding it can still call another
    // that also declares it, on the same thread.
    private final Object clearLock = new Object();
    private volatile boolean pendingClear = false;
    // True once a clear has already succeeded for the replay episode currently in progress. Decision 7 mandates a
    // clear attempt on every delivery seen while replaying, not a repeat store.clear() call on every one of them.
    // Without this, a replay of N events that all hit the per-delivery check runs N deleteMany round trips against
    // an already-empty result instead of one. Reset whenever a live observation ends the episode, or an explicit
    // replay-start signal begins one, so the next episode still gets its own clear.
    private volatile boolean episodeCleared = false;
    // Set by the view-DSL replay lifecycle (CatchupProjectionFeed/DomainEventFeed), independent of what phase says.
    // A composition fed that way has no ReplayPhase to ask (it is neverReplays()), so this is its only replay signal.
    private volatile boolean lifecycleReplaying = false;
    // Tracks whether the current run of clear failures has already logged once at ERROR, so a clear stuck failing
    // for a while logs loudly exactly once and DEBUG on every retry after that, per ADR 132 decision 7.
    private volatile boolean clearFailureLogged = false;
    // One-slot dedup: a single append usually delivers several events and only the first needs a write. Reset
    // whenever a clear succeeds, since the store no longer has anything recorded to deduplicate against.
    private volatile @Nullable AppendId lastRecorded = null;
    // The phase seen by the previous observation, so the reconciliation-to-history edge can be told from staying in
    // history. Only that edge starts a new episode without a live delivery in between, which is what a relaunched
    // replay does.
    private CatchupPhase lastPhase = CatchupPhase.LIVE;
    // The catch-up the previous observation belonged to, so a second one is noticed even when every phase between
    // them went unsampled. Zero while live.
    private long lastGeneration = 0L;
    // Appends handled during a reconciliation while a clear was owed. Recording them then would be pointless, since
    // the pending clear deletes every record for this projection, so they wait here and are written once it lands.
    // Bounded because a reconciliation under a clear that keeps failing has no other limit, and an append evicted
    // from here is never recorded, unlike an id evicted from a delivery dedup cache.
    private final LinkedHashSet<AppendId> awaitingClear = new LinkedHashSet<>();
    private boolean awaitingClearOverflowLogged = false;

    // Holds one append id per append, so a reconciliation of a hundred thousand events costs far fewer entries than
    // that. Chosen to be large enough that reaching it means a clear has been failing for a long time.
    private static final int MAX_AWAITING_CLEAR = 1000;

    public AppliedAppendRecording(String projectionId, AppliedAppendStore store, ReplayPhase phase) {
        this.projectionId = requireNonNull(projectionId, "projectionId cannot be null");
        this.store = requireNonNull(store, "store cannot be null");
        this.phase = requireNonNull(phase, "phase cannot be null");
    }

    /**
     * Records {@code metadata}'s append id if the projection is ready to record right now, atomically with a
     * concurrent clear, so a write already about to happen when a clear runs can never land after it and reinstate
     * what the clear just removed. Not recording is never an error.
     * <p>
     * What happens depends on which part of a catch-up the projection is in, which the lifecycle answers when it has
     * been told and {@link ReplayPhase} answers otherwise.
     * {@link CatchupPhase#REPLAYING_HISTORY} records nothing and attempts the clear that a replay implies, once per
     * episode rather than once per delivery. {@link CatchupPhase#RECONCILING} attempts the same clear and then
     * records, because a catch-up delivering events written since it started is the only delivery some of them get.
     * {@link CatchupPhase#LIVE} ends the episode, retries a clear still owed, and records.
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
            CatchupPhase current = observePhase();
            if (current == CatchupPhase.REPLAYING_HISTORY) {
                ensureEpisodeCleared();
                return;
            }
            if (current == CatchupPhase.RECONCILING) {
                ensureEpisodeCleared();
            } else {
                episodeCleared = false;
                if (pendingClear) {
                    attemptClear();
                }
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
     * Reads the phase once and applies what changing into it means, so every caller acts on the same answer under the
     * same lock. History is where the two carried-over pieces of state are dropped, and they are dropped on different
     * triggers on purpose. {@code awaitingClear} goes on any history observation, because a live observation under a
     * failing clear returns with it intact and a poll between two episodes routinely reads live, so the edge alone
     * would let a previous episode's appends outlive it and be written into a read model that is being rebuilt.
     * {@code episodeCleared} goes only on the reconciliation-to-history edge, because resetting it on every history
     * observation would run one delete per replayed event instead of one per episode.
     */
    private CatchupPhase observePhase() {
        CatchupPhase current = lifecycleReplaying ? CatchupPhase.REPLAYING_HISTORY : phase.currentPhase();
        long generation = lifecycleReplaying ? lastGeneration : phase.currentGeneration();
        if (generation != lastGeneration) {
            // A different catch-up than the one the last observation saw, so nothing this recorder learned during
            // that one applies here. This is what makes the clear happen even when the handover, the live gap and
            // the next history read all fell between two observations, which a poll routinely misses for a catch-up
            // whose history read matches nothing.
            episodeCleared = false;
            dropAwaitingClear();
        }
        lastGeneration = generation;
        if (current == CatchupPhase.REPLAYING_HISTORY) {
            dropAwaitingClear();
            if (lastPhase == CatchupPhase.RECONCILING) {
                episodeCleared = false;
            }
        } else if (current == CatchupPhase.RECONCILING && lastPhase == CatchupPhase.LIVE) {
            // A catch-up never goes live and then reconciles again, so this is a second catch-up whose history read
            // was never seen, which happens when it matches nothing and the poll interval steps over it. Whatever the
            // previous one was still holding belongs to a read model this one is rebuilding.
            dropAwaitingClear();
        }
        lastPhase = current;
        return current;
    }

    // Assumes clearLock is already held by the caller.
    private void dropAwaitingClear() {
        awaitingClear.clear();
        awaitingClearOverflowLogged = false;
    }

    // Assumes clearLock is already held by the caller.
    private void ensureEpisodeCleared() {
        if (!episodeCleared) {
            pendingClear = true;
            attemptClear();
            if (!pendingClear) {
                episodeCleared = true;
            }
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
    private void bufferUntilClear(EventMetadata metadata) {
        if (lastPhase != CatchupPhase.RECONCILING) {
            return;
        }
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
     * The one hook {@code AppliedAppendRecorder.replayObserved()} forwards to: mark a clear as needed and attempt it
     * on the calling thread now. Used by the Spring Boot registrars' poll, which always calls this from a thread
     * that tolerates blocking.
     */
    public void replayObserved() {
        synchronized (clearLock) {
            dropAwaitingClear();
            if (lastPhase == CatchupPhase.RECONCILING) {
                episodeCleared = false;
            }
            lastPhase = CatchupPhase.REPLAYING_HISTORY;
            ensureEpisodeCleared();
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
            CatchupPhase current = observePhase();
            if (current == CatchupPhase.REPLAYING_HISTORY) {
                ensureEpisodeCleared();
                return true;
            }
            if (current == CatchupPhase.RECONCILING) {
                ensureEpisodeCleared();
            } else {
                episodeCleared = false;
                if (pendingClear) {
                    attemptClear();
                }
            }
            flushAwaitingClear();
            return current != CatchupPhase.LIVE;
        }
    }

    /**
     * The one hook {@code AppliedAppendRecorder.retryPendingClear()} forwards to. Retries a clear already marked as
     * owed, and does nothing if none is. Lets a poll tick that finds the phase back to live still retry a clear a
     * replay observed earlier and left failing, since the phase no longer reporting a replay is not the same as the
     * clear it caused having succeeded.
     * <p>
     * Deliberately does not write the appends that are waiting for that clear, even when it succeeds here. This hook
     * is not told which part of a catch-up the projection is in, and a new replay may already have started before
     * anything observed it, so writing them here could put a previous episode's appends into a read model that is
     * being rebuilt. {@link #recordIfReady(EventMetadata)} and {@link #pollReplayPhase()} both read the phase first
     * and write them when it is safe.
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
        // Appends waiting for a clear are dropped by the next call that takes clearLock, not here. Setting
        // lifecycleReplaying makes every one of those read the phase as history, and dropping them is the first
        // thing that branch does, so they cannot be written into the read model this replay is rebuilding. Taking
        // the lock here instead would make this method block, which is exactly what its callers cannot afford.
        lifecycleReplaying = true;
        pendingClear = true;
        episodeCleared = false;
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
            // Dropped here as well as on a history observation, because a lifecycle replay that handled nothing
            // matching never reaches one, and what a previous episode was still holding belongs to a read model this
            // one has just rebuilt.
            dropAwaitingClear();
            attemptClear();
            episodeCleared = false;
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
        episodeCleared = false;
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
