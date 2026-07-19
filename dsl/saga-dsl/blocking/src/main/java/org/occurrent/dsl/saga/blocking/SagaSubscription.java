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

import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.internal.ExecutorShutdown;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A running saga: the underlying event {@link Subscription} plus the timer poller. Closing it stops the poller. The event
 * subscription is cancelled through the subscription model the way any subscription is (this handle only owns the poller
 * it started).
 */
public final class SagaSubscription implements AutoCloseable {

    private final Subscription subscription;
    private final ExecutorService timerPoller;

    SagaSubscription(Subscription subscription, ExecutorService timerPoller) {
        this.subscription = requireNonNull(subscription, "subscription cannot be null");
        this.timerPoller = requireNonNull(timerPoller, "timerPoller cannot be null");
    }

    /** The id of the underlying event subscription. */
    public String id() {
        return subscription.id();
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

    /** Stop the timer poller, letting an in-flight poll finish before interrupting. */
    @Override
    public void close() {
        ExecutorShutdown.shutdownSafely(timerPoller, 5, TimeUnit.SECONDS);
    }
}
