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
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A running saga: the underlying event {@link Subscription} plus the timer poller. Closing it stops the poller and, when
 * the poller was lease-gated, releases the timer lease so another instance can take over. The event subscription is
 * cancelled through the subscription model the way any subscription is (this handle only owns the poller it started).
 * <p>
 * {@link #instances()} is read-only, as ADR 70 intended.
 */
public final class SagaSubscription implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SagaSubscription.class);

    private final Subscription subscription;
    private final ExecutorService timerPoller;
    private final SagaInstances instances;
    private final @Nullable CompetingConsumerStrategy competingConsumerStrategy;
    private final @Nullable String leaseKey;
    private final @Nullable String holderId;

    SagaSubscription(Subscription subscription, ExecutorService timerPoller, SagaInstances instances,
                     @Nullable CompetingConsumerStrategy competingConsumerStrategy,
                     @Nullable String leaseKey, @Nullable String holderId) {
        this.subscription = requireNonNull(subscription, "subscription cannot be null");
        this.timerPoller = requireNonNull(timerPoller, "timerPoller cannot be null");
        this.instances = requireNonNull(instances, "instances cannot be null");
        this.competingConsumerStrategy = competingConsumerStrategy;
        this.leaseKey = leaseKey;
        this.holderId = holderId;
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
