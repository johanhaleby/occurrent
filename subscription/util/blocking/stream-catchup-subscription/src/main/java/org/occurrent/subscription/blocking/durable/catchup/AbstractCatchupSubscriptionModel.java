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

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointWriteVersionSource;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Shared plumbing for the mode-specific catch-up subscription models ({@link StreamCatchupSubscriptionModel} and the
 * DCB catch-up model): the live delegate, config, running-catch-up bookkeeping, shutdown flag, and lifecycle
 * delegation. Replay and {@code subscribe(...)} routing stay in each subclass. DCB-free so it can live in the
 * stream module both modes build against.
 */
@NullMarked
abstract class AbstractCatchupSubscriptionModel implements SubscriptionModel, SubscriptionModelWrapper, ReplayAwareSubscriptions {

    protected final CheckpointAwareSubscriptionModel subscriptionModel;
    protected final CatchupSubscriptionModelConfig config;
    protected final Class<?> subscriptionModelContextType;
    protected final ConcurrentMap<String, Boolean> runningCatchupSubscriptions;
    // Pause requested for a subscriptionId while its replay is still in-flight, before the delegate knows the id.
    // Applied via applyPendingPauseIfAny once the live delegate subscription exists.
    protected final ConcurrentMap<String, Boolean> pauseRequestedDuringCatchup = new ConcurrentHashMap<>();
    protected volatile boolean shuttingDown = false;
    // Set by stop(), cleared by start(...). Checked by the replay loops so stop() interrupts an in-flight
    // replay, not just the delegate the replay has not registered with yet.
    protected volatile boolean stopped = false;
    // Identifies which attempt currently owns a subscriptionId, kept separately from runningCatchupSubscriptions
    // (which stays a plain presence marker, its shipped shape) so a cancelled attempt's replay thread, resuming
    // after a later attempt has taken the id over, can tell it is no longer current instead of clobbering the
    // later attempt's bookkeeping. CURRENT_ATTEMPT carries the calling attempt's identity across this same call
    // without threading it through every method signature; safe because startCatchupAsync gives each attempt its
    // own dedicated virtual thread, never reused, and clears the value in a finally block.
    private final ConcurrentMap<String, CatchupAttempt> currentAttempt;
    private static final ThreadLocal<@Nullable CatchupAttempt> CURRENT_ATTEMPT = new ThreadLocal<>();
    // Who to tell about each id's catch-up boundaries, registered before the subscription that produces them.
    // Kept until this model shuts down, since the registration outlives any one catch-up: a stop and start, a
    // resume, or a cancel and re-subscribe all run another catch-up for the same id, and a recorder that stopped
    // being told would record that catch-up's history as though it were live.
    private final ConcurrentMap<String, CatchupListener> catchupListeners = new ConcurrentHashMap<>();
    // One lock per subscriptionId, guarding a fresh attempt's registration (startCatchupAsync), a finishing
    // attempt's checkpoint cleanup and delegate subscribe (or its cancelled-cleanup branch), and
    // cancelRunningCatchup. The identity check above only made the ownership decision itself atomic, not what
    // followed it, so a cancellation or a fresh registration could land in the gap after an attempt decided it was
    // still current but before it finished acting on that. Held only across those short transitions, never across
    // an in-flight replay, so a long catch-up is never serialized by this lock. Entries are never removed for an id
    // this instance has actually run a catch-up for, since a subscriptionId is application-defined and
    // low-cardinality here, unlike a per-event or per-request key; cancelRunningCatchup never creates one for an id
    // it has not seen, so an arbitrary or unknown id passed to cancelSubscription costs nothing.
    private final ConcurrentMap<String, ReentrantLock> handoverLocks;

    protected AbstractCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this(subscriptionModel, config, subscriptionModelContextType, new SharedCatchupState());
    }

    /**
     * @param sharedState The per-id registries ({@link #lockHandover}'s locks, {@link #currentAttempt},
     *                     {@link #runningCatchupSubscriptions}) this instance draws from. A dispatcher over several
     *                     children that route the same id to a different one of them on different calls passes the
     *                     same state to every child, so a handover on one child and a fresh registration on another
     *                     still serialize and still see the same current owner for that id, both of which a
     *                     registry private to each child cannot give. Every other caller passes a fresh one,
     *                     private to this instance.
     */
    protected AbstractCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType, SharedCatchupState sharedState) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscriptionModelContextType = Objects.requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
        Objects.requireNonNull(sharedState, "sharedState cannot be null");
        this.handoverLocks = sharedState.handoverLocks;
        this.currentAttempt = sharedState.currentAttempt;
        this.runningCatchupSubscriptions = sharedState.runningCatchupSubscriptions;
    }

    /**
     * The per-id registries {@link #handoverLocks}, {@link #currentAttempt}, and {@link #runningCatchupSubscriptions}
     * bundled into one unit, so a dispatcher sharing them across its children shares all three together rather than
     * risking one shared and another forgotten. All three need to move together: the lock alone only keeps two
     * children's handovers from running at the same instant, it does not stop a stale one, once it finally runs,
     * from still finding itself "current" in a registry only it can see.
     */
    static final class SharedCatchupState {
        private final ConcurrentMap<String, ReentrantLock> handoverLocks = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CatchupAttempt> currentAttempt = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
    }

    // Reports subscriptionModelContextType (the dispatcher's type when wrapped) so a caller's StartAt.dynamic
    // pattern-matching on the public dispatcher type keeps working regardless of which subclass runs underneath.
    protected SubscriptionModelContext generateSubscriptionModelContext() {
        return new SubscriptionModelContext(subscriptionModelContextType);
    }

    @Override
    public void stop() {
        stopped = true;
        getWrappedSubscriptionModel().stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return !runningCatchupSubscriptions.isEmpty() || getWrappedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return runningCatchupSubscriptions.containsKey(subscriptionId) || getWrappedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isCatchingUp(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        return runningCatchupSubscriptions.containsKey(subscriptionId);
    }

    @Override
    public boolean listenForCatchup(String subscriptionId, CatchupListener listener) {
        Objects.requireNonNull(subscriptionId, "subscriptionId cannot be null");
        Objects.requireNonNull(listener, "listener cannot be null");
        catchupListeners.put(subscriptionId, listener);
        return true;
    }

    /**
     * Tells a listener that this attempt has read the history it set out to read, so what follows was written since
     * it started. Called by a subclass once its history read has delivered everything, and not at all when a stop
     * truncated it. The attempt itself is the episode, so a listener that has since been started by a later attempt
     * for the same id ignores this, and no lock is needed to keep a stale attempt from speaking. Only meaningful on
     * the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected void historyRead(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        CatchupListener listener = catchupListeners.get(subscriptionId);
        if (listener != null) {
            listener.historyRead(attempt);
        }
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringCatchup.containsKey(subscriptionId) || getWrappedSubscriptionModel().isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        pauseRequestedDuringCatchup.remove(subscriptionId);
        return getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        if (runningCatchupSubscriptions.containsKey(subscriptionId)) {
            // Delegate does not know this id yet, so record the request and apply it in applyPendingPauseIfAny
            // once the live subscription exists. The replay itself keeps running until the handover since
            // interrupting and resuming it would require persisting the exact replay cursor, which this class does not do.
            pauseRequestedDuringCatchup.put(subscriptionId, true);
        } else {
            getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
        }
    }

    /**
     * Applies a pause requested via {@link #pauseSubscription(String)} while {@code subscriptionId}'s replay was
     * still in-flight, now that the live delegate subscription for it exists. A no-op if no pause was requested.
     */
    protected void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringCatchup.remove(subscriptionId) != null) {
            getWrappedSubscriptionModel().pauseSubscription(subscriptionId);
        }
    }

    /**
     * Identifies one catch-up attempt for a subscription id, so a cancelled attempt whose replay thread has not
     * noticed yet can be told apart from a later attempt for the same id. Identity is the point of the class itself,
     * reference equality is exactly what every check here wants, but {@link #cancelled} also needs to reach this
     * exact attempt's own thread without touching {@link #currentAttempt}'s entry for it: flagging the attempt
     * object in place, instead of swapping in a sentinel, keeps the map entry itself untouched by cancellation, so
     * {@link #endReplayIfStillCurrent} remains the only remover and a cancelled id cannot linger in the map for the
     * rest of the model's lifetime the way a shared sentinel value would.
     */
    private static final class CatchupAttempt {
        private volatile boolean cancelled = false;
    }

    /**
     * Whether persisting a checkpoint for an event this call's own attempt already delivered is still safe: this
     * attempt was not itself explicitly cancelled, and nobody else's, specifically not a different, {@code non-null}
     * attempt for the same {@code subscriptionId}, has taken over since. {@code null} (nobody registered, for
     * example {@link #markShuttingDown} clearing this same attempt's own entry) counts as safe, because a shutdown
     * triggered by the very event being persisted does not put a newer attempt's position at risk and deletes
     * nothing itself, only an actually different attempt taking the id over, or an explicit cancellation of it,
     * does. This is deliberately looser than {@link #shouldKeepReplaying}, which needs exact identity to decide
     * whether to keep replaying at all, not whether one already-delivered event's position is still safe to
     * persist. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected boolean isSafeToPersistFor(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (attempt.cancelled) {
            return false;
        }
        CatchupAttempt owner = currentAttempt.get(subscriptionId);
        return owner == null || owner == attempt;
    }

    /**
     * Whether the calling replay loop should keep going: not shutting down, not stopped, this call's own attempt was
     * not itself explicitly cancelled, and it is still the current one registered for {@code subscriptionId}. Checks
     * exact identity, not mere presence and not {@link #isSafeToPersistFor}'s looser null-is-fine rule, so an
     * attempt superseded by a later one for the same id (cancelled, then resubscribed before this attempt's thread
     * noticed), or simply cancelled outright with nothing yet taking its place, correctly stops instead of running
     * to completion or clobbering the later attempt's bookkeeping. Only meaningful on the virtual thread
     * {@link #startCatchupAsync} started for this attempt.
     */
    protected boolean shouldKeepReplaying(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        return !shuttingDown && !stopped && !attempt.cancelled && currentAttempt.get(subscriptionId) == attempt;
    }

    /**
     * Ends the calling attempt's ownership of {@code subscriptionId} and reports whether it completed normally, that
     * is {@link #shouldKeepReplaying} was still true for it right before this ran. A replay calls this exactly once,
     * at the point where it decides whether it completed normally or was superseded, cancelled, or stopped, closing
     * the same identity race {@link #shouldKeepReplaying} closes for the loop checks, for the one-time final
     * decision. The map entry is atomically removed whenever it is still this attempt's own, whether ending
     * normally, cancelled, or stopped, so an id does not linger in {@link #currentAttempt} for the rest of the
     * model's lifetime once this attempt is done with it. Left alone when a later attempt has already taken the id
     * over, since only that later attempt may remove its own entry.
     */
    protected boolean endReplayIfStillCurrent(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (shuttingDown || stopped) {
            return false;
        }
        if (currentAttempt.remove(subscriptionId, attempt)) {
            runningCatchupSubscriptions.remove(subscriptionId);
            return !attempt.cancelled;
        }
        return false;
    }

    /**
     * A held {@link #lockHandover} lock. {@code close()} declares no checked exception (unlike plain
     * {@link AutoCloseable}), so a try-with-resources releasing one needs no catch clause; {@link ReentrantLock#unlock()}
     * never throws one.
     */
    protected interface HandoverLock extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Acquires {@code subscriptionId}'s handover lock for the duration of a try-with-resources block, held by
     * {@link #startCatchupAsync}'s registration, a subclass's replay-completion code from
     * {@link #endReplayIfStillCurrent} through its checkpoint cleanup and delegate {@code subscribe} call (or the
     * cancelled-cleanup branch), and {@link #cancelRunningCatchup}. A {@link ReentrantLock}, not
     * {@code synchronized}, because every caller here runs on a {@link #startCatchupAsync virtual thread} and a
     * handover span can block on storage or delegate I/O. Blocking inside a {@code synchronized} block would pin
     * the carrier thread for that whole span, a plain lock does not.
     */
    protected HandoverLock lockHandover(String subscriptionId) {
        ReentrantLock lock = handoverLocks.computeIfAbsent(subscriptionId, id -> new ReentrantLock());
        lock.lock();
        return lock::unlock;
    }

    /**
     * Acquires {@code subscriptionId}'s handover lock only if one already exists, {@code null} otherwise, without
     * creating one. Registration ({@link #startCatchupAsync}) is the only place a lock is created for an id, and it
     * creates the lock before it creates that id's {@link #currentAttempt} entry, so a missing lock here means no
     * attempt has ever been registered for this id and there is nothing for {@link #cancelRunningCatchup} to
     * coordinate with. Lets a cancellation for an id this instance has never run a catch-up for, including an
     * arbitrary or unknown one a caller passes to {@code cancelSubscription} defensively, stay free of the registry
     * instead of permanently reserving a lock for it.
     */
    protected @Nullable HandoverLock tryLockHandover(String subscriptionId) {
        ReentrantLock lock = handoverLocks.get(subscriptionId);
        if (lock == null) {
            return null;
        }
        lock.lock();
        return lock::unlock;
    }

    /**
     * Captures the live resume checkpoint handed over to live delivery. Callers choose when: the position path in
     * {@link StreamCatchupSubscriptionModel} captures it before the bulk replay so no in-flight event is missed;
     * the time-based path captures it after, to keep the token fresh (avoids oplog ageing).
     * Returns null when the delegate must not run ({@code delegatedStartAt} null, catch-up owns the position
     * entirely). Fails loudly if the delegate has no checkpoint rather than silently resuming at "now" and
     * dropping events committed during replay.
     */
    protected @Nullable Checkpoint captureLiveResumeCheckpoint(@Nullable StartAt delegatedStartAt) {
        if (delegatedStartAt == null) {
            return null;
        }
        Checkpoint checkpoint = subscriptionModel.globalCheckpoint();
        if (checkpoint == null) {
            throw new IllegalStateException("Cannot run a catch-up subscription because the subscription model reported no resume token to hand over to live delivery. The change stream history may be unavailable, for example an empty oplog or a restricted cluster.");
        }
        return checkpoint;
    }

    /**
     * Cancel a catch-up running for {@code subscriptionId}. A no-op if this class has no catch-up running for that id
     * (for example because it belongs to the other path in a dual-mode dispatcher). Does not touch the shared live
     * delegate or position storage; the dispatcher owns those since both paths share the same delegate.
     */
    public void cancelRunningCatchup(String subscriptionId) {
        // Locked, when a lock already exists for this id, so this call lands either strictly before or strictly
        // after a handover attempt's own lockHandover span for the same id, never inside it. Unlocked, it could run
        // in the gap after that attempt decided it was still current but before it acted on that, finding nothing
        // left to flag and losing the cancellation. tryLockHandover deliberately does not create a lock for an id
        // that has none yet, since that means no attempt has ever been registered for it and the operations below
        // are then no-ops with or without a lock, including for an arbitrary or unknown id a caller passes here.
        HandoverLock lock = tryLockHandover(subscriptionId);
        try {
            runningCatchupSubscriptions.remove(subscriptionId);
            // Flags whichever attempt is currently registered, atomically with respect to a concurrent resubscribe
            // for the same id, instead of removing or replacing the entry: a dual-mode dispatcher calls this on both
            // inner models for every cancellation, and the one with nothing running for this id must stay a no-op
            // rather than start tracking an id that is not its concern. The entry itself is left for the flagged
            // attempt's own endReplayIfStillCurrent to remove, so this call can never race a newer attempt's map
            // removal.
            currentAttempt.computeIfPresent(subscriptionId, (id, attempt) -> {
                attempt.cancelled = true;
                return attempt;
            });
            pauseRequestedDuringCatchup.remove(subscriptionId);
        } finally {
            if (lock != null) {
                lock.close();
            }
        }
    }

    /**
     * Whether this call's own attempt was the one {@link #cancelRunningCatchup} flagged. Reads the calling thread's
     * own {@link CatchupAttempt}, not {@link #currentAttempt}'s entry for the id, so a later attempt superseding a
     * stale one for the same id is not a cancellation from that stale attempt's own point of view, even though the
     * stale attempt is itself no longer current. Only meaningful on the virtual thread {@link #startCatchupAsync}
     * started for this attempt.
     */
    protected boolean wasCancelled() {
        return CURRENT_ATTEMPT.get().cancelled;
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
        catchupListeners.clear();
        currentAttempt.clear();
        pauseRequestedDuringCatchup.clear();
    }

    /**
     * Interrupts an in-flight or future replay on this model, without touching the shared live delegate. Lets the
     * dispatcher stop every inner model's replay while calling the delegate's {@code stop()} exactly once.
     */
    public void stopReplay() {
        stopped = true;
    }

    /**
     * Allows the next replay on this model to run, without touching the shared live delegate. Does not restart a
     * replay already interrupted by {@link #stopReplay()}, it only permits the next one.
     */
    public void resumeReplay() {
        stopped = false;
    }

    /**
     * Delete {@code subscriptionId}'s position from the configured position storage, if any. Exposed so the
     * dispatcher can delete it exactly once when cancelling a subscription that could belong to either mode, since
     * the position storage config (and the storage instance it wraps) is shared, not owned per mode.
     */
    public void deletePositionFromStorage(String subscriptionId) {
        doIfCheckpointStorageConfigIs(UseCheckpointInStorage.class, cfg -> cfg.storage().delete(subscriptionId));
    }

    @Override
    public SubscriptionModel getWrappedSubscriptionModel() {
        return subscriptionModel;
    }

    /**
     * The {@link CheckpointWriteCondition} to stamp a checkpoint write triggered by {@code cfg} with. A version from
     * {@link UseCheckpointInStorage#checkpointWriteVersionSource()} becomes
     * {@link CheckpointWriteCondition#notOlderThan(long)}, and an empty answer or no source at all becomes
     * {@link CheckpointWriteCondition#any()}. Both {@link StreamCatchupSubscriptionModel} and the DCB catch-up model
     * call this for every checkpoint write, whichever config subtype triggered it, always through the 3-arg
     * {@code CheckpointStorage.save} rather than choosing between that and the 2-arg one.
     */
    protected CheckpointWriteCondition writeConditionFor(UseCheckpointInStorage cfg, String subscriptionId) {
        CheckpointWriteVersionSource source = cfg.checkpointWriteVersionSource();
        if (source == null) {
            return CheckpointWriteCondition.any();
        }
        OptionalLong version = source.writeVersion(subscriptionId);
        return version.isPresent() ? CheckpointWriteCondition.notOlderThan(version.getAsLong()) : CheckpointWriteCondition.any();
    }

    protected <T, C extends CheckpointStorageConfig> Optional<T> returnIfCheckpointStorageConfigIs(Class<C> cls, Function<C, @Nullable T> fn) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            return Optional.ofNullable(fn.apply(cls.cast(config.subscriptionStorageConfig)));
        }
        return Optional.empty();
    }

    protected <C extends CheckpointStorageConfig> void doIfCheckpointStorageConfigIs(Class<C> cls, Consumer<C> consumer) {
        if (cls.isInstance(config.subscriptionStorageConfig)) {
            consumer.accept(cls.cast(config.subscriptionStorageConfig));
        }
    }

    /**
     * Registers a fresh attempt as the running catch-up for {@code subscriptionId} and runs {@code catchup} on its
     * own dedicated virtual thread, never reused, which is what lets {@link #shouldKeepReplaying} and
     * {@link #endReplayIfStillCurrent} read the attempt's identity from {@link #CURRENT_ATTEMPT} instead of a
     * parameter. This is the only place that puts into {@link #runningCatchupSubscriptions}.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, Callable<Subscription> catchup) {
        CatchupAttempt attempt = new CatchupAttempt();
        // Locked so this registration cannot land inside a still-finishing earlier attempt's own lockHandover span
        // for the same id. Unlocked, this attempt could start, and its replay could reach a checkpoint save, before
        // the earlier attempt's late checkpoint delete runs, wiping out what this attempt just wrote instead of
        // its own.
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            runningCatchupSubscriptions.put(subscriptionId, true);
            currentAttempt.put(subscriptionId, attempt);
            // Sent here, inside the same lock that takes ownership of the id and before the thread below starts, so
            // it always precedes anything this attempt delivers.
            CatchupListener listener = catchupListeners.get(subscriptionId);
            if (listener != null) {
                listener.catchupStarted(attempt);
            }
        }
        // catchup itself ends its attempt's ownership on normal completion (via endReplayIfStillCurrent), and
        // deliberately leaves it in place when shouldKeepReplaying already turned false so a cancellation can
        // still be told apart from a completion (see the comment on subscriptionsWasCancelledOrShutdown in the
        // mode-specific classes). Neither path throws, so catching here only ever means the replay itself failed,
        // and is the one place both modes share to stop such a failure from leaving the subscription looking like
        // it is still running or catching up forever.
        FutureTask<Subscription> task = new FutureTask<>(() -> {
            CURRENT_ATTEMPT.set(attempt);
            try {
                return catchup.call();
            } catch (Throwable failure) {
                // Conditional on this attempt still being the current one: an attempt already superseded by a later
                // resubscribe for the same id must not remove the later attempt's running marker, and by the same
                // reasoning must not clear a pause request the later attempt's caller may have just made either.
                if (currentAttempt.remove(subscriptionId, attempt)) {
                    runningCatchupSubscriptions.remove(subscriptionId);
                    pauseRequestedDuringCatchup.remove(subscriptionId);
                }
                throw failure;
            } finally {
                CURRENT_ATTEMPT.remove();
            }
        });
        Thread.ofVirtual().name("occurrent-catchup-" + subscriptionId).start(task);
        return task;
    }
}
