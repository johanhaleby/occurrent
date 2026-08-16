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
    protected final ConcurrentMap<String, Boolean> runningCatchupSubscriptions = new ConcurrentHashMap<>();
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
    private final ConcurrentMap<String, CatchupAttempt> currentAttempt = new ConcurrentHashMap<>();
    private static final ThreadLocal<@Nullable CatchupAttempt> CURRENT_ATTEMPT = new ThreadLocal<>();

    protected AbstractCatchupSubscriptionModel(CheckpointAwareSubscriptionModel subscriptionModel, CatchupSubscriptionModelConfig config, Class<?> subscriptionModelContextType) {
        this.subscriptionModel = Objects.requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscriptionModelContextType = Objects.requireNonNull(subscriptionModelContextType, "subscriptionModelContextType cannot be null");
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
     * noticed yet can be told apart from a later attempt for the same id. Identity is the entire point: no fields,
     * no {@code equals}/{@code hashCode} override, reference equality is exactly what every check here wants.
     */
    private static final class CatchupAttempt {
    }

    /**
     * Whether persisting a checkpoint for an event this call's own attempt already delivered is still safe: nobody
     * else's, specifically not a different, {@code non-null} attempt for the same {@code subscriptionId}, has taken
     * over since. {@code null} (nobody registered, for example {@link #cancelRunningCatchup} or
     * {@link #markShuttingDown} clearing this same attempt's own entry) counts as safe too, because a stop or
     * shutdown triggered by the very event being persisted does not put a newer attempt's position at risk, only an
     * actually different attempt taking the id over does. This is deliberately looser than
     * {@link #shouldKeepReplaying}, which needs exact identity to decide whether to keep replaying at all, not
     * whether one already-delivered event's position is still safe to persist. Only meaningful on the virtual
     * thread {@link #startCatchupAsync} started for this attempt.
     */
    protected boolean isSafeToPersistFor(String subscriptionId) {
        CatchupAttempt owner = currentAttempt.get(subscriptionId);
        return owner == null || owner == CURRENT_ATTEMPT.get();
    }

    /**
     * Whether the calling replay loop should keep going: not shutting down, not stopped, and this call's own
     * attempt is still the current one registered for {@code subscriptionId}. Checks exact identity, not mere
     * presence and not {@link #isSafeToPersistFor}'s looser null-is-fine rule, so an attempt superseded by a later
     * one for the same id (cancelled, then resubscribed before this attempt's thread noticed), or simply cancelled
     * outright with nothing yet taking its place, correctly stops instead of running to completion or clobbering
     * the later attempt's bookkeeping. Only meaningful on the virtual thread {@link #startCatchupAsync} started for
     * this attempt.
     */
    protected boolean shouldKeepReplaying(String subscriptionId) {
        return !shuttingDown && !stopped && currentAttempt.get(subscriptionId) == CURRENT_ATTEMPT.get();
    }

    /**
     * Atomically ends the calling attempt's ownership of {@code subscriptionId} if {@link #shouldKeepReplaying} is
     * still true for it, and reports whether it did. A replay calls this exactly once, at the point where it
     * decides whether it completed normally or was superseded, cancelled, or stopped: only an attempt that is
     * still current when this runs may claim a normal completion, closing the same identity race
     * {@link #shouldKeepReplaying} closes for the loop checks, for the one-time final decision.
     */
    protected boolean endReplayIfStillCurrent(String subscriptionId) {
        CatchupAttempt attempt = CURRENT_ATTEMPT.get();
        if (!shouldKeepReplaying(subscriptionId)) {
            return false;
        }
        if (currentAttempt.remove(subscriptionId, attempt)) {
            runningCatchupSubscriptions.remove(subscriptionId);
            return true;
        }
        return false;
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
        runningCatchupSubscriptions.remove(subscriptionId);
        currentAttempt.remove(subscriptionId);
        pauseRequestedDuringCatchup.remove(subscriptionId);
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
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
        runningCatchupSubscriptions.put(subscriptionId, true);
        currentAttempt.put(subscriptionId, attempt);
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
