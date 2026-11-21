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
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
    // The name of the thread of each shutdown(), recorded before the monitor of a serialized shutdown() is taken
    final List<String> shutdownAttempts = new CopyOnWriteArrayList<>();
    private final Object shutdownMonitor = new Object();
    volatile Runnable beforeStart = () -> {
    };
    volatile Runnable beforeStop = () -> {
    };
    volatile Runnable beforeShutdown = () -> {
    };
    volatile Runnable beforeResume = () -> {
    };
    // Runs once the delegate's shutdown() has returned, so a test can hold a shutdown() after it removed the subscriptions
    volatile Runnable afterShutdown = () -> {
    };
    volatile Consumer<String> beforeSubscribe = id -> {
    };
    // Off, a shutdown() is counted and hooked but leaves the in-memory delegate running. Its own shutdown() waits for an
    // action that is still running
    volatile boolean shutsTheDelegateDown = true;
    // On, shutdown() runs one at a time on a monitor, as a wrapped model does whose shutdown() is synchronized. A shutdown()
    // held inside it, by afterShutdown, then holds every other shutdown() at the monitor
    volatile boolean serializesShutdown = false;
    // Off, a subscribe is given StartAt.now(). On, the in-memory delegate asks the start-at it got from inside its own
    // subscribe, after it has checked that it is not shut down and before it registers the subscription, as a live
    // delegate that evaluates a StartAt.dynamic function does. The catch-up model hands that function to a delegate only
    // for the context type of a wrapper, which AsksAsAWrapper is
    volatile boolean asksTheStartAtInsideTheDelegate = false;
    // On, start(..), stop(), shutdown(), subscribe, subscribePaused, resumeSubscription, pauseSubscription and
    // cancelSubscription all run one at a time on one monitor, as the native Mongo model does, so a shutdown() held
    // inside it holds every other call of those at the monitor. isRunning and isPaused answer without it
    volatile boolean sharesOneMonitor = false;
    // The calls made on the shared monitor, as call@thread, recorded before the monitor is taken
    final List<String> monitoredCalls = new CopyOnWriteArrayList<>();
    private final Object sharedMonitor = new Object();

    /**
     * The context type the start-at is asked with when {@code asksTheStartAtInsideTheDelegate} is on.
     */
    interface AsksAsAWrapper extends SubscriptionModelWrapper {
    }

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
        return onTheMonitor("subscribe", () -> {
            beforeSubscribe.accept(subscriptionId);
            return delegate.subscribe(subscriptionId, filter, startAtForTheDelegate(startAt), action);
        });
    }

    @Override
    public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        return onTheMonitor("subscribePaused", () -> {
            beforeSubscribe.accept(subscriptionId);
            return delegate.subscribePaused(subscriptionId, filter, startAtForTheDelegate(startAt), action);
        });
    }

    private StartAt startAtForTheDelegate(StartAt startAt) {
        return asksTheStartAtInsideTheDelegate ? StartAt.dynamic(context -> {
            startAt.get(new SubscriptionModelContext(AsksAsAWrapper.class));
            return StartAt.now();
        }) : StartAt.now();
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        return new StringBasedCheckpoint("in-memory-global-position");
    }

    @Override
    public void stop() {
        onTheMonitor("stop", () -> {
            stops.incrementAndGet();
            beforeStop.run();
            delegate.stop();
        });
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        onTheMonitor("start", () -> {
            starts.incrementAndGet();
            beforeStart.run();
            delegate.start(resumeSubscriptionsAutomatically);
        });
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
        return onTheMonitor("resumeSubscription", () -> {
            resumed.add(subscriptionId);
            beforeResume.run();
            return delegate.resumeSubscription(subscriptionId);
        });
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        onTheMonitor("pauseSubscription", () -> delegate.pauseSubscription(subscriptionId));
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        onTheMonitor("cancelSubscription", () -> delegate.cancelSubscription(subscriptionId));
    }

    @Override
    public void shutdown() {
        shutdownAttempts.add(Thread.currentThread().getName());
        if (sharesOneMonitor) {
            onTheMonitor("shutdown", this::shutDownHooked);
        } else if (serializesShutdown) {
            synchronized (shutdownMonitor) {
                shutDownHooked();
            }
        } else {
            shutDownHooked();
        }
    }

    private void onTheMonitor(String call, Runnable body) {
        onTheMonitor(call, () -> {
            body.run();
            return null;
        });
    }

    private <T> T onTheMonitor(String call, Supplier<T> body) {
        if (!sharesOneMonitor) {
            return body.get();
        }
        monitoredCalls.add(call + "@" + Thread.currentThread().getName());
        synchronized (sharedMonitor) {
            return body.get();
        }
    }

    private void shutDownHooked() {
        shutdowns.incrementAndGet();
        beforeShutdown.run();
        if (shutsTheDelegateDown) {
            delegate.shutdown();
        }
        afterShutdown.run();
    }
}
