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
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.api.blocking.DelegatingSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.CheckpointStorageConfig.UseCheckpointInStorage;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Shared plumbing for the mode-specific catch-up subscription models ({@link StreamCatchupSubscriptionModel} and the
 * DCB catch-up model): the live delegate, config, running-catch-up bookkeeping, shutdown flag, and lifecycle
 * delegation. Replay and {@code subscribe(...)} routing stay in each subclass. DCB-free so it can live in the
 * stream module both modes build against.
 */
@NullMarked
abstract class AbstractCatchupSubscriptionModel implements SubscriptionModel, DelegatingSubscriptionModel {

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
        getDelegatedSubscriptionModel().stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        stopped = false;
        getDelegatedSubscriptionModel().start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return !runningCatchupSubscriptions.isEmpty() || getDelegatedSubscriptionModel().isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return runningCatchupSubscriptions.containsKey(subscriptionId) || getDelegatedSubscriptionModel().isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return pauseRequestedDuringCatchup.containsKey(subscriptionId) || getDelegatedSubscriptionModel().isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        pauseRequestedDuringCatchup.remove(subscriptionId);
        return getDelegatedSubscriptionModel().resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        if (runningCatchupSubscriptions.containsKey(subscriptionId)) {
            // Delegate does not know this id yet, so record the request and apply it in applyPendingPauseIfAny
            // once the live subscription exists. The replay itself keeps running until the handover since
            // interrupting and resuming it would require persisting the exact replay cursor, which this class does not do.
            pauseRequestedDuringCatchup.put(subscriptionId, true);
        } else {
            getDelegatedSubscriptionModel().pauseSubscription(subscriptionId);
        }
    }

    /**
     * Applies a pause requested via {@link #pauseSubscription(String)} while {@code subscriptionId}'s replay was
     * still in-flight, now that the live delegate subscription for it exists. A no-op if no pause was requested.
     */
    protected void applyPendingPauseIfAny(String subscriptionId) {
        if (pauseRequestedDuringCatchup.remove(subscriptionId) != null) {
            getDelegatedSubscriptionModel().pauseSubscription(subscriptionId);
        }
    }

    /**
     * Whether the replay loop for {@code subscriptionId} should keep going: not shutting down, not stopped, and
     * still registered as a running catch-up (removed on cancellation).
     */
    protected boolean shouldKeepReplaying(String subscriptionId) {
        return !shuttingDown && !stopped && runningCatchupSubscriptions.containsKey(subscriptionId);
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
    public SubscriptionModel getDelegatedSubscriptionModel() {
        return subscriptionModel;
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

    protected Future<Subscription> startCatchupAsync(String subscriptionId, Callable<Subscription> catchup) {
        runningCatchupSubscriptions.put(subscriptionId, true);
        FutureTask<Subscription> task = new FutureTask<>(catchup);
        Thread.ofVirtual().name("occurrent-catchup-" + subscriptionId).start(task);
        return task;
    }
}
