package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.catchup.TimeBasedCheckpoint;
import org.occurrent.subscription.blocking.durable.catchup.lifecycle.LifecycleCall.StartFailure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A change-stream-like live model. A subscribe while the model is stopped is held, a resume starts the model and that
 * subscription, start(true) resumes every held one. A start can fail before it takes effect or after it, and can wait.
 * Pausing a subscription that is not running throws, as {@code SubscriptionModelLifeCycle#pauseSubscription} says.
 */
final class ControllableLiveModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {

    static final class InjectedStartFailure extends RuntimeException {
        InjectedStartFailure(String message) {
            super(message);
        }
    }

    volatile boolean running = true;
    volatile long startDelayMicros = 0;
    final ThreadLocal<StartFailure> failStartOnThisThread = ThreadLocal.withInitial(() -> StartFailure.NONE);
    // Answers the live start check, false when the feed no longer has the history back to the checkpoint
    volatile Predicate<Checkpoint> canResume = checkpoint -> true;
    final Map<String, FakeSubscription> subscriptions = new ConcurrentHashMap<>();

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        return TimeBasedCheckpoint.beginningOfTime();
    }

    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return canResume.test(checkpoint);
    }

    @Override
    public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        FakeSubscription subscription = new FakeSubscription(subscriptionId, action);
        subscription.started = running;
        subscriptions.put(subscriptionId, subscription);
        return subscription;
    }

    @Override
    public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        FakeSubscription subscription = new FakeSubscription(subscriptionId, action);
        subscriptions.put(subscriptionId, subscription);
        return subscription;
    }

    synchronized void publish(CloudEvent event) {
        subscriptions.values().forEach(subscription -> subscription.receive(event));
    }

    @Override
    public synchronized void stop() {
        running = false;
        subscriptions.values().forEach(subscription -> subscription.started = false);
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        StartFailure mode = failStartOnThisThread.get();
        long delay = startDelayMicros;
        if (delay > 0) {
            LockSupport.parkNanos(delay * 1000);
        }
        if (mode == StartFailure.BEFORE_EFFECT) {
            throw new InjectedStartFailure("start failed before taking effect");
        }
        synchronized (this) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                new ArrayList<>(subscriptions.values()).forEach(FakeSubscription::go);
            }
        }
        if (mode == StartFailure.AFTER_EFFECT) {
            throw new InjectedStartFailure("start failed after taking effect");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        return resumeSubscription(subscriptionId);
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        FakeSubscription subscription = subscriptions.get(subscriptionId);
        return subscription != null && subscription.started;
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        FakeSubscription subscription = subscriptions.get(subscriptionId);
        return subscription != null && !subscription.started;
    }

    @Override
    public synchronized Subscription resumeSubscription(String subscriptionId) {
        FakeSubscription subscription = subscriptions.get(subscriptionId);
        if (subscription == null) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        if (subscription.started) {
            throw new SubscriptionAlreadyRunningException(subscriptionId);
        }
        running = true;
        subscription.go();
        return subscription;
    }

    @Override
    public synchronized void pauseSubscription(String subscriptionId) {
        FakeSubscription subscription = subscriptions.get(subscriptionId);
        if (subscription == null) {
            throw new UnknownSubscriptionException(subscriptionId);
        }
        if (!subscription.started) {
            throw new SubscriptionNotRunningException(subscriptionId, "Subscription " + subscriptionId + " is already paused.");
        }
        subscription.started = false;
    }

    @Override
    public synchronized void cancelSubscription(String subscriptionId) {
        subscriptions.remove(subscriptionId);
    }

    static final class FakeSubscription implements Subscription {
        private final String id;
        private final Consumer<CloudEvent> action;
        private final List<CloudEvent> held = new ArrayList<>();
        volatile boolean started = false;

        FakeSubscription(String id, Consumer<CloudEvent> action) {
            this.id = id;
            this.action = action;
        }

        synchronized void receive(CloudEvent event) {
            if (started) {
                action.accept(event);
            } else {
                held.add(event);
            }
        }

        synchronized void go() {
            if (!started) {
                started = true;
                List<CloudEvent> toDeliver = new ArrayList<>(held);
                held.clear();
                toDeliver.forEach(action);
            }
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            long until = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < until) {
                if (started) {
                    return true;
                }
                LockSupport.parkNanos(1_000_000);
            }
            return started;
        }
    }
}
