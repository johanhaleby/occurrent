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
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A live delegate over the in-memory model that runs a hook of the test before each of its {@code start(..)},
 * {@code stop()}, {@code shutdown()} and {@code resumeSubscription(id)}, and counts them, so a test can hold, fail or look at a call from inside it. It
 * records each id passed to {@code resumeSubscription(..)}.
 */
class HookedSubscriptionModel implements CheckpointAwareSubscriptionModel {
    private final InMemorySubscriptionModel delegate;
    final AtomicInteger starts = new AtomicInteger();
    final AtomicInteger stops = new AtomicInteger();
    final AtomicInteger shutdowns = new AtomicInteger();
    final List<String> resumed = new CopyOnWriteArrayList<>();
    volatile Runnable beforeStart = () -> {
    };
    volatile Runnable beforeStop = () -> {
    };
    volatile Runnable beforeShutdown = () -> {
    };
    volatile Runnable beforeResume = () -> {
    };
    volatile Consumer<String> beforeSubscribe = id -> {
    };
    // Off, a shutdown() is counted and hooked but leaves the in-memory delegate running. Its own shutdown() waits for an
    // action that is still running
    volatile boolean shutsTheDelegateDown = true;

    HookedSubscriptionModel(InMemorySubscriptionModel delegate) {
        this.delegate = delegate;
    }

    static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch not released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        beforeSubscribe.accept(subscriptionId);
        return delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        return new StringBasedCheckpoint("in-memory-global-position");
    }

    @Override
    public void stop() {
        stops.incrementAndGet();
        beforeStop.run();
        delegate.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        starts.incrementAndGet();
        beforeStart.run();
        delegate.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return delegate.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return delegate.isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        resumed.add(subscriptionId);
        beforeResume.run();
        return delegate.resumeSubscription(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        delegate.pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        delegate.cancelSubscription(subscriptionId);
    }

    @Override
    public void shutdown() {
        shutdowns.incrementAndGet();
        beforeShutdown.run();
        if (shutsTheDelegateDown) {
            delegate.shutdown();
        }
    }
}
