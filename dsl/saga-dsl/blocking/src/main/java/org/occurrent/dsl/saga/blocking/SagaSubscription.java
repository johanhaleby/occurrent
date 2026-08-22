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

package org.occurrent.dsl.saga.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelLifeCycle;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A running saga: the underlying event {@link Subscription} plus the timer poller. Closing it stops the poller and, when
 * the poller was lease-gated, releases the timer lease so another instance can take over. The event subscription is
 * cancelled through the subscription model the way any subscription is (this handle only owns the poller it started).
 * <p>
 * It is also where a quarantined instance is released, because release is the one saga operation that has to touch both
 * an instance and the subscription at once. {@link #instances()} stays read-only, as ADR 70 intended.
 */
public final class SagaSubscription implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SagaSubscription.class);

    private final Subscription subscription;
    private final ExecutorService timerPoller;
    private final SagaInstances instances;
    private final Subscribable subscriptionModel;
    private final QuarantinedInstances<?> quarantinedInstances;
    private final @Nullable CompetingConsumerStrategy competingConsumerStrategy;
    private final @Nullable String leaseKey;
    private final @Nullable String holderId;

    SagaSubscription(Subscription subscription, ExecutorService timerPoller, SagaInstances instances,
                     Subscribable subscriptionModel, QuarantinedInstances<?> quarantinedInstances,
                     @Nullable CompetingConsumerStrategy competingConsumerStrategy,
                     @Nullable String leaseKey, @Nullable String holderId) {
        this.subscription = requireNonNull(subscription, "subscription cannot be null");
        this.timerPoller = requireNonNull(timerPoller, "timerPoller cannot be null");
        this.instances = requireNonNull(instances, "instances cannot be null");
        this.subscriptionModel = requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.quarantinedInstances = requireNonNull(quarantinedInstances, "quarantinedInstances cannot be null");
        this.competingConsumerStrategy = competingConsumerStrategy;
        this.leaseKey = leaseKey;
        this.holderId = holderId;
    }

    /**
     * Bring a {@link SagaStatus#QUARANTINED} instance back, by clearing its record and replaying this saga's
     * subscription from the position it stopped at.
     * <p>
     * Both halves are needed and neither is enough on its own. Clear the record alone and the instance handles new
     * events against state with a gap in it. Replay alone and the instance is still quarantined when the events arrive,
     * so it skips them and quarantines again. So the instance is marked released first and stays inert until the
     * replay reaches its recorded position, at which point it handles that event and becomes {@link SagaStatus#ACTIVE}
     * again.
     * <p>
     * <strong>This pauses the saga's whole subscription while the replay catches up.</strong> Every other instance of
     * the same saga stops receiving events until it finishes, and each of them re-reads the events it already handled,
     * recognising them as redeliveries through its own watermarks so no command is dispatched twice. That is a real
     * pause of the shared channel, deliberately chosen by an operator and finite, rather than the indefinite one that
     * quarantine exists to remove. Do not call it in a loop over every quarantined instance.
     * <p>
     * <strong>It acts on this node only.</strong> The subscription it repositions is the one this JVM runs. On a
     * competing-consumer deployment call it on the node currently delivering the saga's events, which is the one
     * holding the subscription's lock. It refuses on any other node rather than repositioning a subscription that is
     * delivering nothing.
     *
     * @param sagaId the instance to release
     * @throws IllegalStateException         if the instance does not exist, is not quarantined, or this node is not the
     *                                       one delivering this saga's events
     * @throws UnsupportedOperationException if this saga's subscription model cannot be resumed at a chosen position,
     *                                       which is the capability a replay needs. The instance stays quarantined, and
     *                                       {@code SagaStateStore.delete(sagaId)} remains the way to abandon it
     * @throws SagaConcurrencyException      if the instance was written concurrently while being released
     */
    public void release(String sagaId) {
        requireNonNull(sagaId, "sagaId cannot be null");
        RepositionableSubscriptions repositionable = RepositionableSubscriptions.findIn(subscriptionModel)
                .orElseThrow(() -> new UnsupportedOperationException("Cannot release saga instance '" + sagaId + "': the subscription model behind saga subscription '" + subscription.id() + "' does not implement RepositionableSubscriptions, so it cannot be resumed at the position the instance stopped at. The instance stays quarantined. Use SagaStateStore.delete(sagaId) to abandon it instead."));
        // Paused and resumed on the model that owns the position, not through whatever wraps it. A wrapper's own
        // bookkeeping is then untouched, because it believes the subscription is running throughout, which it is either side of
        // the reposition. Going through an outer wrapper instead would leave it believing the subscription is paused
        // while the model underneath it runs.
        SubscriptionModelLifeCycle lifeCycle = repositionable.capability(SubscriptionModelLifeCycle.class)
                .orElseThrow(() -> new UnsupportedOperationException("Cannot release saga instance '" + sagaId + "': the subscription model behind saga subscription '" + subscription.id() + "' can be repositioned but not paused, and a reposition needs both. The instance stays quarantined."));
        if (!lifeCycle.isRunning(subscription.id())) {
            throw new IllegalStateException("Cannot release saga instance '" + sagaId + "': saga subscription '" + subscription.id() + "' is not running on this node, so a replay started here would deliver nothing. On a competing-consumer deployment, release from the node holding this subscription's lock.");
        }

        OptionalLong stoppedAt = quarantinedInstances.markReleased(sagaId);
        if (stoppedAt.isEmpty()) {
            throw new IllegalStateException("Saga instance '" + sagaId + "' is not quarantined, so there is nothing to release.");
        }
        // GlobalCheckpoint.of(p) means resume AFTER p, so replaying from the recorded position itself would skip the
        // one event the release exists to reprocess. The predecessor is the inclusive form of it, and position 1 gives
        // 0, which is the defined beginning of the sequence rather than an accident.
        StartAt replayFrom = StartAt.checkpoint(GlobalCheckpoint.of(Math.max(0, stoppedAt.getAsLong() - 1)));
        try {
            lifeCycle.pauseSubscription(subscription.id());
            repositionable.resumeSubscription(subscription.id(), replayFrom);
        } catch (RuntimeException e) {
            // Leave the instance exactly as it was found rather than released and waiting for a replay that never
            // started, which would look like a release that worked and quietly never finish.
            try {
                quarantinedInstances.undoRelease(sagaId);
            } catch (RuntimeException undoFailure) {
                e.addSuppressed(undoFailure);
            }
            throw e;
        }
        log.info("Released saga instance '{}' on saga subscription '{}' and restarted it from position {}. The subscription replays from there, so this saga's other instances re-read events they already handled and skip them as redeliveries.",
                sagaId, subscription.id(), stoppedAt.getAsLong());
    }

    /** The id of the underlying event subscription. */
    public String id() {
        return subscription.id();
    }

    /**
     * Read-only access to this saga's instances, for observing their lifecycle. Backed by the same
     * {@code SagaStateStore} the saga runs against, so it stays usable after {@link #close()}: closing stops this
     * instance's poller, it does not close the store.
     */
    public SagaInstances instances() {
        return instances;
    }

    /** The underlying event subscription. */
    public Subscription subscription() {
        return subscription;
    }

    /** Block until the underlying subscription has started. */
    public void waitUntilStarted() {
        subscription.waitUntilStarted();
    }

    /** Block until the underlying subscription has started, up to {@code timeout}. */
    public boolean waitUntilStarted(Duration timeout) {
        return subscription.waitUntilStarted(timeout);
    }

    /** Release the timer lease (if gated) and stop the poller, letting an in-flight poll finish before interrupting. */
    @Override
    public void close() {
        if (competingConsumerStrategy != null && leaseKey != null && holderId != null) {
            // Best-effort release so another instance takes over promptly. A failure here only delays the handover until
            // the lease expires on its own, so it must not stop the poller from shutting down.
            try {
                competingConsumerStrategy.unregisterCompetingConsumer(leaseKey, holderId);
            } catch (RuntimeException e) {
                log.warn("Failed to release the timer lease '{}' for saga subscription '{}'", leaseKey, subscription.id(), e);
            }
        }
        ExecutorShutdown.shutdownSafely(timerPoller, 5, TimeUnit.SECONDS);
    }
}
