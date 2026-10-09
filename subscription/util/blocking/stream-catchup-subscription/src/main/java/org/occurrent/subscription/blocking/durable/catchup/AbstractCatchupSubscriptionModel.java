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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.SubscriptionFilter;
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
import java.util.concurrent.atomic.AtomicReference;
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
    // Set by stop(), and by a start whose live delegate failed on a stopped model. Cleared by start(...), and by a
    // resume of the live delegate after such a failed start. Checked by the replay loops so stop() interrupts an
    // in-flight replay, not just the delegate the replay has not registered with yet. Written only under lifecycleLock.
    protected volatile boolean stopped = false;
    // A leaf lock, held only to read or write stopped and the fields below, so no method is called and no other lock
    // is taken while it is held
    private final Object lifecycleLock = new Object();
    // Moves on with every start and stop, so a start whose live delegate failed can tell whether another came after it
    private long lifecycleGeneration = 0;
    // Moves on with every resume of the live delegate that returned, so a start whose live delegate failed can tell
    // whether one came after it
    private long liveDelegateResumes = 0;
    // Whether stopped was set by a start whose live delegate failed, rather than by a stop
    private boolean stoppedByFailedStart = false;
    // Replays subscribed while this model was stopped, and replays a stop() cut short. The live delegate knows none of
    // them until start(true) or a resume runs the replay again.
    private final ConcurrentMap<String, ParkedReplay> parkedReplays = new ConcurrentHashMap<>();
    // Identifies which attempt currently owns a subscriptionId, kept separately from runningCatchupSubscriptions
    // (which stays a plain presence marker, its shipped shape) so a cancelled attempt's replay thread, resuming
    // after a later attempt has taken the id over, can tell it is no longer current instead of clobbering the
    // later attempt's bookkeeping. CURRENT_ATTEMPT carries the calling attempt's identity across this same call
    // without threading it through every method signature; safe because startCatchupAsync gives each attempt its
    // own dedicated virtual thread, never reused, and clears the value in a finally block.
    private final ConcurrentMap<String, CatchupAttempt> currentAttempt;
    private static final ThreadLocal<@Nullable CatchupAttempt> CURRENT_ATTEMPT = new ThreadLocal<>();
    // How often a replay run again checks, while an earlier attempt's action still runs, whether it should still wait
    private static final long EARLIER_ATTEMPT_POLL_MILLIS = 100;
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
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, startAt, action, false);
    }

    /**
     * Keeps a replay waiting to run as a stopped model does, and has the live delegate hold a subscription with no
     * replay paused, so nothing is read or delivered until {@link #resumeSubscription(String)} or {@link #start(boolean) start(true)}.
     */
    @Override
    public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return subscribe(subscriptionId, filter, startAt, action, true);
    }

    /**
     * Subscribes, holding the subscription paused when {@code holdPaused} is set, as {@link #subscribePaused} describes.
     */
    protected abstract Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused);

    /**
     * Hands {@code subscriptionId} to the live delegate, held paused there when {@code holdPaused} is set.
     */
    protected Subscription subscribeInTheWrappedModel(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, boolean holdPaused) {
        return holdPaused
                ? getWrappedSubscriptionModel().subscribePaused(subscriptionId, filter, startAt, action)
                : getWrappedSubscriptionModel().subscribe(subscriptionId, filter, startAt, action);
    }

    @Override
    public void stop() {
        stopReplay();
        getWrappedSubscriptionModel().stop();
    }

    /**
     * Starts the live delegate and, with {@code resumeSubscriptionsAutomatically}, runs every parked replay again, as
     * the delegate resumes what it holds paused. Without it a parked replay waits for {@link #resumeSubscription(String)}.
     * <p>
     * When starting the live delegate throws, a model that was stopped before the call stops again and parks a replay
     * that ran in the meantime, as {@link #stop()} does. It doesn't stop again when the delegate runs afterwards, or
     * when another start or stop of this model, or a resume of the delegate, came while the delegate was starting.
     * Once a later resume of the delegate returns, the model is started again, unless a {@link #stop()} came first.
     * When the delegate throws on being asked whether it runs, the model stops again, and what the delegate threw is
     * added to the start's failure as suppressed.
     */
    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        StartAttempt attempt = beginStart();
        try {
            getWrappedSubscriptionModel().start(resumeSubscriptionsAutomatically);
        } catch (Throwable e) {
            undoStart(attempt, attempt.wasStopped() && runsAfterFailedStart(getWrappedSubscriptionModel(), e));
            throw e;
        }
        if (resumeSubscriptionsAutomatically) {
            relaunchParkedReplays();
        }
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
        return pauseRequestedDuringCatchup.containsKey(subscriptionId) || parkedReplays.containsKey(subscriptionId) || getWrappedSubscriptionModel().isPaused(subscriptionId);
    }

    /**
     * Runs a parked replay again and passes any other subscription to the live delegate. When this model is stopped and
     * the subscription is paused, this first starts the model without resuming anything else, as resuming a
     * subscription starts the live delegate, so a subscription made afterwards replays at once.
     */
    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        if (stopped && isPaused(subscriptionId)) {
            start(false);
        }
        pauseRequestedDuringCatchup.remove(subscriptionId);
        if (hasParkedReplay(subscriptionId)) {
            Subscription relaunched = relaunchParkedReplay(subscriptionId);
            if (relaunched != null) {
                return relaunched;
            }
        }
        Subscription resumed = getWrappedSubscriptionModel().resumeSubscription(subscriptionId);
        liveDelegateResumed();
        return resumed;
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
        // Set by parkIfStillCurrent, under the handover lock, once a stop() has taken this attempt's replay away from
        // it. It stores the position of an event whose action it completes, and does not complete the caller's
        // handle, which the parked replay completes.
        private volatile boolean parked = false;
        private final AbstractCatchupSubscriptionModel owner;
        private final ReplayState replay;
        // Done once the thread of every earlier attempt at this replay has returned, so none of them is still inside
        // the subscriber's action
        private final CompletableFuture<Void> earlierAttemptsDone;
        // Done once this attempt's thread and every earlier attempt's thread have returned
        private final CompletableFuture<Void> done = new CompletableFuture<>();

        private CatchupAttempt(AbstractCatchupSubscriptionModel owner, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone) {
            this.owner = owner;
            this.replay = replay;
            this.earlierAttemptsDone = earlierAttemptsDone;
        }

        private boolean abandoned() {
            return replay.cancelled || parked;
        }
    }

    /**
     * One replay, shared by every attempt at it, the first one and each one that runs it again after a stop.
     */
    private static final class ReplayState {
        private final CatchupReplay catchup;
        // The handle its subscriber holds, completed by the attempt that hands the replay over
        private final CompletableFuture<Subscription> result;
        // Set by cancelRunningCatchup, and when a new subscription for the id drops the replay, so no attempt at it
        // stores a position any more, also one that stop() took the replay from
        private volatile boolean cancelled = false;
        // The last position an attempt at this replay stored, which the next attempt replays from
        private final AtomicReference<@Nullable Checkpoint> lastStored = new AtomicReference<>();

        private ReplayState(CatchupReplay catchup, CompletableFuture<Subscription> result) {
            this.catchup = catchup;
            this.result = result;
        }
    }

    /**
     * A replay waiting for this model to run again, so running it again completes the handle its subscriber holds.
     * {@code earlierAttemptsDone} completes once no earlier attempt at it is still running the subscriber's action,
     * which a replay that {@code stop()} cut short can still be doing when it is run again.
     */
    private record ParkedReplay(ReplayState replay, CompletableFuture<Void> earlierAttemptsDone) {
    }

    /**
     * Replays the history of one subscription and hands it over to the live subscription model.
     */
    @FunctionalInterface
    protected interface CatchupReplay {
        /**
         * @param lastStored The last position an earlier attempt at this replay stored through
         *                   {@link #saveCatchupCheckpoint}, which this attempt replays from, or {@code null} to replay
         *                   from where the subscription asked to start.
         */
        Subscription replayFrom(@Nullable Checkpoint lastStored) throws Exception;
    }

    /**
     * Whether persisting a checkpoint for an event this call's own attempt already delivered is still safe, which it is
     * when its replay was not cancelled and no attempt at another replay has taken the id over since. {@code null} (nobody
     * registered, for example {@link #markShuttingDown} clearing this same attempt's own entry, or a stop() taking the
     * replay from it) counts as safe, because a shutdown or a stop triggered by the very event being persisted does
     * not put a newer attempt's position at risk and deletes nothing itself. An attempt that runs this same replay again
     * counts as safe too, since it waits for this attempt's thread to return and then replays from the position this
     * attempt stored. This is deliberately looser than {@link #shouldKeepReplaying}, which needs exact identity to
     * decide whether to keep replaying at all, not whether one already-delivered event's position is still safe to
     * persist. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this attempt.
     */
    protected boolean isSafeToPersistFor(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (attempt.replay.cancelled) {
            return false;
        }
        CatchupAttempt owner = currentAttempt.get(subscriptionId);
        return owner == null || owner.replay == attempt.replay;
    }

    /**
     * Stores {@code checkpoint} as the position the calling attempt's replay has delivered through, with the write
     * condition {@link #writeConditionFor} gives, and remembers it, so an attempt that runs this replay again after a
     * stop replays from there. Only meaningful on the virtual thread {@link #startCatchupAsync} started for this
     * attempt.
     */
    protected void saveCatchupCheckpoint(String subscriptionId, UseCheckpointInStorage cfg, Checkpoint checkpoint) {
        cfg.storage().save(subscriptionId, checkpoint, writeConditionFor(cfg, subscriptionId));
        CURRENT_ATTEMPT.get().replay.lastStored.set(checkpoint);
    }

    /**
     * Whether the calling replay loop should keep going: not shutting down, not stopped, this call's own attempt was
     * not itself explicitly cancelled, and it is still the current one registered for {@code subscriptionId}. Checks
     * exact identity, not mere presence and not {@link #isSafeToPersistFor}'s looser null-is-fine rule, so an
     * attempt superseded by a later one for the same id (cancelled, then resubscribed before this attempt's thread
     * noticed), or simply cancelled outright with nothing yet taking its place, correctly stops instead of running
     * to completion or clobbering the later attempt's bookkeeping. Only meaningful on the virtual thread
     * {@link #startCatchupAsync} started for this attempt.
     * <p>
     * A replay that finds this model stopped parks itself before this returns, for the case where it registered after
     * {@link #stopReplay()} had parked the others.
     */
    protected boolean shouldKeepReplaying(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (stopped && !shuttingDown) {
            try (HandoverLock ignored = lockHandover(subscriptionId)) {
                parkIfStillCurrent(subscriptionId, attempt);
            }
            return false;
        }
        return !shuttingDown && !attempt.abandoned() && currentAttempt.get(subscriptionId) == attempt;
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
     * <p>
     * A stopped model parks the attempt instead of ending it, so the replay runs again once this model is started or
     * the subscription resumed. Called with {@code subscriptionId}'s handover lock held.
     */
    protected boolean endReplayIfStillCurrent(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (shuttingDown) {
            return false;
        }
        if (stopped) {
            parkIfStillCurrent(subscriptionId, attempt);
            return false;
        }
        if (currentAttempt.remove(subscriptionId, attempt)) {
            runningCatchupSubscriptions.remove(subscriptionId);
            return !attempt.abandoned();
        }
        return false;
    }

    /**
     * Takes {@code subscriptionId}'s replay away from {@code attempt} and parks it, if {@code attempt} is still the
     * current one. The subscription then counts as paused, not running, until the parked replay runs again. A
     * cancelled attempt is only ended, since nothing is to run it again. Called with the handover lock held, so a
     * finishing attempt either hands over before this or finds itself parked.
     * <p>
     * A start can allow replays to run again between the stop this parks for and the park itself, and start(true) then
     * runs what was parked before this replay was. So this asks whether the model is stopped once more after parking,
     * and runs the replay again when it is not, without resuming a pause asked for while it ran. Anything else that
     * allows replays in that window runs it again too, start(false) included.
     */
    private void parkIfStillCurrent(String subscriptionId, CatchupAttempt attempt) {
        if (currentAttempt.remove(subscriptionId, attempt)) {
            runningCatchupSubscriptions.remove(subscriptionId);
            if (!attempt.replay.cancelled) {
                attempt.parked = true;
                AbstractCatchupSubscriptionModel owner = attempt.owner;
                owner.parkedReplays.put(subscriptionId, new ParkedReplay(attempt.replay, attempt.done));
                // Read after the put, so a start that allows replays from here on finds this replay parked
                if (!owner.stopped && !owner.shuttingDown) {
                    owner.relaunchParkedReplay(subscriptionId, false);
                }
            }
        }
    }

    boolean hasParkedReplay(String subscriptionId) {
        return parkedReplays.containsKey(subscriptionId);
    }
    /**
     * Runs every replay this model has parked again, or parks it once more if this model is stopped by then.
     */
    public void relaunchParkedReplays() {
        for (String subscriptionId : parkedReplays.keySet()) {
            relaunchParkedReplay(subscriptionId);
        }
    }

    /**
     * Runs {@code subscriptionId}'s parked replay again and returns the handle its subscriber already holds, or
     * {@code null} when this model has no parked replay for it.
     */
    @Nullable Subscription relaunchParkedReplay(String subscriptionId) {
        return relaunchParkedReplay(subscriptionId, true);
    }

    /**
     * Runs {@code subscriptionId}'s parked replay again as {@link #relaunchParkedReplay(String)} does, and with
     * {@code resuming} also undoes a pause asked for before it was parked.
     */
    private @Nullable Subscription relaunchParkedReplay(String subscriptionId, boolean resuming) {
        final CatchupAttempt attempt;
        final ParkedReplay parked;
        // Locked, so a cancelRunningCatchup either finds the replay still parked or finds the attempt that runs it
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            parked = parkedReplays.remove(subscriptionId);
            if (parked == null) {
                return null;
            }
            if (resuming) {
                pauseRequestedDuringCatchup.remove(subscriptionId);
            }
            attempt = registerOrPark(subscriptionId, parked.replay(), parked.earlierAttemptsDone(), false);
        }
        if (attempt != null) {
            runOnItsOwnThread(subscriptionId, attempt);
        }
        return new CatchupSubscription(subscriptionId, parked.replay().result);
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
                attempt.replay.cancelled = true;
                return attempt;
            });
            pauseRequestedDuringCatchup.remove(subscriptionId);
            // A parked replay never runs again, and waitUntilStarted on its subscriber's handle returns false. Flagged
            // too, since the attempt that stop() took it from can still be in the subscriber's action and must not
            // store a position once that returns.
            ParkedReplay parked = parkedReplays.remove(subscriptionId);
            if (parked != null) {
                parked.replay().cancelled = true;
                parked.replay().result.cancel(false);
            }
        } finally {
            if (lock != null) {
                lock.close();
            }
        }
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
        parkedReplays.values().forEach(parked -> parked.replay().result.cancel(false));
        parkedReplays.clear();
    }

    /**
     * Parks the replays in flight on this model, and every replay subscribed until the next start, without touching
     * the shared live delegate. A parked subscription is paused, not running, and runs its replay again once
     * {@link #relaunchParkedReplays()} or {@link #resumeSubscription(String)} runs it, from the last position the replay
     * stored, or from where it started when it stored none.
     */
    public void stopReplay() {
        synchronized (lifecycleLock) {
            stopped = true;
            stoppedByFailedStart = false;
            lifecycleGeneration++;
        }
        parkReplaysInFlight();
    }

    private void parkReplaysInFlight() {
        if (shuttingDown) {
            return;
        }
        for (var entry : currentAttempt.entrySet()) {
            CatchupAttempt attempt = entry.getValue();
            if (attempt.owner == this) {
                try (HandoverLock ignored = lockHandover(entry.getKey())) {
                    parkIfStillCurrent(entry.getKey(), attempt);
                }
            }
        }
    }

    /**
     * Allows the next replay on this model to run, without touching the shared live delegate. Does not run a replay
     * {@link #stopReplay()} parked, which {@link #relaunchParkedReplays()} does, apart from one whose replay thread is
     * still parking it.
     */
    public void resumeReplay() {
        synchronized (lifecycleLock) {
            stopped = false;
            stoppedByFailedStart = false;
            lifecycleGeneration++;
        }
    }

    /**
     * Whether a start found this model stopped, and where the lifecycle and the resumes of the live delegate stood
     * once it allowed replays to run again.
     */
    record StartAttempt(boolean wasStopped, long generation, long liveDelegateResumes) {
    }

    /**
     * Allows the next replay on this model to run, as {@link #resumeReplay()} does, before the live delegate is
     * started. Hand the result to {@link #undoStart(StartAttempt, boolean)} when that start throws.
     */
    StartAttempt beginStart() {
        synchronized (lifecycleLock) {
            StartAttempt attempt = new StartAttempt(stopped, ++lifecycleGeneration, liveDelegateResumes);
            stopped = false;
            stoppedByFailedStart = false;
            return attempt;
        }
    }

    /**
     * Whether {@code liveDelegate} runs after its start threw {@code startFailure}. A failure to tell counts as not
     * running and is added to {@code startFailure} as suppressed, so the caller still gets the start's own failure.
     */
    static boolean runsAfterFailedStart(SubscriptionModel liveDelegate, Throwable startFailure) {
        try {
            return liveDelegate.isRunning();
        } catch (Throwable e) {
            startFailure.addSuppressed(e);
            return false;
        }
    }

    /**
     * Stops this model again after starting the live delegate threw, when the model was stopped before, the delegate
     * is not running, and no other start or stop of this model and no resume of the delegate came since. A delegate
     * that runs after all keeps this model started, so a later subscription replays and hands over to it.
     *
     * @param liveDelegateRuns What {@link #runsAfterFailedStart} answered, asked before this is called, so the
     *                         lifecycle lock is never held while the delegate is called
     */
    void undoStart(StartAttempt attempt, boolean liveDelegateRuns) {
        if (!attempt.wasStopped() || liveDelegateRuns) {
            return;
        }
        synchronized (lifecycleLock) {
            // A resume of the delegate that returned after it was asked has moved liveDelegateResumes by now, and one
            // still under way clears this stop in liveDelegateResumed() once it returns
            if (attempt.generation() != lifecycleGeneration || attempt.liveDelegateResumes() != liveDelegateResumes) {
                return;
            }
            stopped = true;
            stoppedByFailedStart = true;
            lifecycleGeneration++;
        }
        parkReplaysInFlight();
    }

    /**
     * Records that a resume of the live delegate returned, and allows replays to run again when a start whose live
     * delegate failed stopped this model again, as resuming a subscription starts the delegate.
     */
    void liveDelegateResumed() {
        synchronized (lifecycleLock) {
            liveDelegateResumes++;
            if (stopped && stoppedByFailedStart) {
                stopped = false;
                stoppedByFailedStart = false;
                lifecycleGeneration++;
            }
        }
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
     * Starts {@code catchup} as {@link #startCatchupAsync(String, CatchupReplay, boolean)} does with {@code holdPaused}
     * unset. A replay that stop() cut short runs {@code catchup} again from the start.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, Callable<Subscription> catchup) {
        return startCatchupAsync(subscriptionId, lastStored -> catchup.call(), false);
    }

    /**
     * Registers a fresh attempt as the running catch-up for {@code subscriptionId} and runs {@code catchup} on its
     * own dedicated virtual thread, never reused, which is what lets {@link #shouldKeepReplaying} and
     * {@link #endReplayIfStillCurrent} read the attempt's identity from {@link #CURRENT_ATTEMPT} instead of a
     * parameter. This and {@link #relaunchParkedReplay} are the only places that put into
     * {@link #runningCatchupSubscriptions}.
     * <p>
     * A stopped model keeps {@code catchup} waiting to run instead, without reading anything, and so does
     * {@code holdPaused}. The returned future then completes once the replay has run and handed over.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupReplay catchup, boolean holdPaused) {
        CompletableFuture<Subscription> result = new CompletableFuture<>();
        ReplayState replay = new ReplayState(catchup, result);
        final CatchupAttempt attempt;
        // Locked so this registration cannot land inside a still-finishing earlier attempt's own lockHandover span
        // for the same id. Unlocked, this attempt could start, and its replay could reach a checkpoint save, before
        // the earlier attempt's late checkpoint delete runs, wiping out what this attempt just wrote instead of
        // its own.
        try (HandoverLock ignored = lockHandover(subscriptionId)) {
            attempt = registerOrPark(subscriptionId, replay, CompletableFuture.completedFuture(null), holdPaused);
        }
        if (attempt != null) {
            runOnItsOwnThread(subscriptionId, attempt);
        }
        return result;
    }

    /**
     * Registers a fresh attempt for {@code catchup} as the running catch-up for {@code subscriptionId}, or parks
     * {@code catchup} and returns {@code null} while this model is stopped or {@code holdPaused} is set. Called with the
     * handover lock held. A replay still parked for the id is dropped, and waitUntilStarted on its handle returns false.
     */
    private @Nullable CatchupAttempt registerOrPark(String subscriptionId, ReplayState replay, CompletableFuture<Void> earlierAttemptsDone, boolean holdPaused) {
        ParkedReplay superseded = parkedReplays.remove(subscriptionId);
        if (superseded != null && superseded.replay() != replay) {
            superseded.replay().cancelled = true;
            superseded.replay().result.cancel(false);
        }
        if ((stopped || holdPaused) && !shuttingDown) {
            parkedReplays.put(subscriptionId, new ParkedReplay(replay, earlierAttemptsDone));
            // Asked again after the put, as parkIfStillCurrent does, so a start that allowed replays to run again
            // before it does not leave this replay parked
            if (holdPaused || stopped) {
                return null;
            }
            parkedReplays.remove(subscriptionId);
        }
        CatchupAttempt attempt = new CatchupAttempt(this, replay, earlierAttemptsDone);
        runningCatchupSubscriptions.put(subscriptionId, true);
        currentAttempt.put(subscriptionId, attempt);
        // Sent here, inside the same lock that takes ownership of the id and before the attempt's thread starts, so
        // it always precedes anything this attempt delivers.
        CatchupListener listener = catchupListeners.get(subscriptionId);
        if (listener != null) {
            listener.catchupStarted(attempt);
        }
        return attempt;
    }

    private void runOnItsOwnThread(String subscriptionId, CatchupAttempt attempt) {
        // catchup itself ends its attempt's ownership on normal completion (via endReplayIfStillCurrent), and
        // deliberately leaves it in place when shouldKeepReplaying already turned false so a cancellation can
        // still be told apart from a completion (see the comment on subscriptionsWasCancelledOrShutdown in the
        // mode-specific classes). Neither path throws, so catching here only ever means the replay itself failed,
        // and is the one place both modes share to stop such a failure from leaving the subscription looking like
        // it is still running or catching up forever.
        //
        // A parked attempt does not complete the handle. The parked replay completes it once it runs again, and the
        // flag is final by the time it is read, since parking takes the attempt out of currentAttempt under the
        // handover lock and a finishing attempt takes that lock too.
        Thread.ofVirtual().name("occurrent-catchup-" + subscriptionId).start(() -> {
            CURRENT_ATTEMPT.set(attempt);
            try {
                awaitEarlierAttempts(subscriptionId, attempt);
                // Read once every earlier attempt has returned, so it holds the last position any of them stored
                Subscription subscription = attempt.replay.catchup.replayFrom(attempt.replay.lastStored.get());
                if (!attempt.parked) {
                    attempt.replay.result.complete(subscription);
                }
            } catch (Throwable failure) {
                // Conditional on this attempt still being the current one: an attempt already superseded by a later
                // resubscribe for the same id must not remove the later attempt's running marker, and by the same
                // reasoning must not clear a pause request the later attempt's caller may have just made either.
                try (HandoverLock ignored = lockHandover(subscriptionId)) {
                    if (currentAttempt.remove(subscriptionId, attempt)) {
                        runningCatchupSubscriptions.remove(subscriptionId);
                        pauseRequestedDuringCatchup.remove(subscriptionId);
                    }
                }
                if (!attempt.parked) {
                    attempt.replay.result.completeExceptionally(failure);
                }
            } finally {
                CURRENT_ATTEMPT.remove();
                attempt.earlierAttemptsDone.whenComplete((ignored, alsoIgnored) -> attempt.done.complete(null));
            }
        });
    }

    /**
     * Waits until no earlier attempt at this replay is still running the subscriber's action, so the action never runs
     * alongside itself. The wait is on this attempt's own thread, so {@code start()} and {@code resumeSubscription(..)}
     * return without it. It ends early once this attempt should no longer replay, so a cancel, a stop or a shutdown
     * is not held up by an action that does not return. The replay then runs as it would for a stop that came before
     * its first event, delivering nothing. An action that never returns keeps the subscription from replaying, as it
     * would keep the live subscription model from delivering its next event.
     */
    private void awaitEarlierAttempts(String subscriptionId, CatchupAttempt attempt) throws InterruptedException {
        while (!attempt.earlierAttemptsDone.isDone() && shouldKeepReplaying(subscriptionId)) {
            try {
                attempt.earlierAttemptsDone.get(EARLIER_ATTEMPT_POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | ExecutionException ignored) {
                // Checked again on the next round
            }
        }
    }
}
