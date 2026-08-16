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
    protected final ConcurrentMap<String, CatchupAttempt> runningCatchupSubscriptions = new ConcurrentHashMap<>();
    // Pause requested for a subscriptionId while its replay is still in-flight, before the delegate knows the id.
    // Applied via applyPendingPauseIfAny once the live delegate subscription exists.
    protected final ConcurrentMap<String, Boolean> pauseRequestedDuringCatchup = new ConcurrentHashMap<>();
    protected volatile boolean shuttingDown = false;
    // Set by stop(), cleared by start(...). Checked by the replay loops so stop() interrupts an in-flight
    // replay, not just the delegate the replay has not registered with yet.
    protected volatile boolean stopped = false;

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
    protected static final class CatchupAttempt {
    }

    /**
     * Whether {@code attempt}'s replay loop should keep going: not shutting down, not stopped, and still the
     * current running catch-up for {@code subscriptionId}. Checks identity, not mere presence, so an attempt
     * superseded by a later one for the same id (cancelled, then resubscribed before this attempt's thread noticed)
     * correctly stops instead of running to completion or clobbering the later attempt's bookkeeping.
     */
    protected boolean shouldKeepReplaying(String subscriptionId, CatchupAttempt attempt) {
        return !shuttingDown && !stopped && runningCatchupSubscriptions.get(subscriptionId) == attempt;
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
        pauseRequestedDuringCatchup.remove(subscriptionId);
    }

    /**
     * Mark this model as shutting down so any in-flight catch-up stops as soon as possible. Does not touch the shared
     * live delegate; the dispatcher owns that.
     */
    public void markShuttingDown() {
        shuttingDown = true;
        runningCatchupSubscriptions.clear();
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
     * Registers {@code attempt} as the running catch-up for {@code subscriptionId} and runs {@code catchup} on its
     * own virtual thread. This is the only place that puts into {@link #runningCatchupSubscriptions}; the caller
     * creates {@code attempt} once and passes the same instance into {@code catchup} for its own identity-checked
     * removal, so the marker is written exactly once per attempt instead of once here and again inside the callable.
     */
    protected Future<Subscription> startCatchupAsync(String subscriptionId, CatchupAttempt attempt, Callable<Subscription> catchup) {
        runningCatchupSubscriptions.put(subscriptionId, attempt);
        // catchup itself removes the running marker on normal completion, and deliberately leaves it in place when
        // shouldKeepReplaying already turned false so a cancellation can still be told apart from a completion (see
        // the comment on subscriptionsWasCancelledOrShutdown in the mode-specific classes). Neither path throws, so
        // catching here only ever means the replay itself failed, and is the one place both modes share to stop such
        // a failure from leaving the subscription looking like it is still running or catching up forever.
        FutureTask<Subscription> task = new FutureTask<>(() -> {
            try {
                return catchup.call();
            } catch (Throwable failure) {
                // Conditional on this attempt still being the current one: an attempt already superseded by a later
                // resubscribe for the same id must not remove the later attempt's running marker, and by the same
                // reasoning must not clear a pause request the later attempt's caller may have just made either.
                if (runningCatchupSubscriptions.remove(subscriptionId, attempt)) {
                    pauseRequestedDuringCatchup.remove(subscriptionId);
                }
                throw failure;
            }
        });
        Thread.ofVirtual().name("occurrent-catchup-" + subscriptionId).start(task);
        return task;
    }
}
