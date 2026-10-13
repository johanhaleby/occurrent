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
import org.occurrent.subscription.HistoryLossHandling;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.StartAtCheckpoint;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.HistoryLossReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Opens asynchronously like a MongoDB subscription model, and loses the first live start it opens after running
 * {@code writtenInTheGap}, or every one when asked to. The handle of an open that loses the history never reads as
 * started, and a resume returns the handle of the open it makes. A listener that takes the subscription over gets it
 * paused, otherwise it restarts from the present or is forgotten. Counts the resumes it is asked for, and can hold the
 * first open and the first pause until the test lets them go. It can also hold the first takeover between its pause
 * and what it runs after the pause, and resume a subscription without a position where it lost the history, as a
 * MongoDB subscription model does.
 */
final class LiveStartLosingSubscriptionModel implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {

    private final InMemorySubscriptionModel delegate;
    private final boolean restartSubscriptionsOnHistoryLost;
    private final boolean opensOnTheCallingThread;
    private final Runnable writtenInTheGap;
    private final AtomicBoolean losesTheNextOpen = new AtomicBoolean(true);
    private final AtomicBoolean firstOpen = new AtomicBoolean(true);
    private final AtomicBoolean firstPause = new AtomicBoolean(true);
    private volatile boolean losesEveryOpen;
    private volatile Runnable beforeTheFirstOpen = () -> {
    };
    private volatile Runnable beforeTheFirstPause = () -> {
    };
    private final AtomicBoolean firstTakeOver = new AtomicBoolean(true);
    private volatile Runnable betweenTheFirstTakeOverPauseAndWhatFollows = () -> {
    };
    private volatile @Nullable Runnable resumesWhereItWasHeldOnceThisReturns;
    private final AtomicInteger liveStartsRead = new AtomicInteger();
    private final Set<Checkpoint> lost = ConcurrentHashMap.newKeySet();
    private final List<HistoryLossListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, Held> paused = new ConcurrentHashMap<>();

    final List<Checkpoint> readLiveStarts = new CopyOnWriteArrayList<>();
    final List<Checkpoint> openedAt = new CopyOnWriteArrayList<>();
    final List<Checkpoint> lostLiveStarts = new CopyOnWriteArrayList<>();
    final List<Checkpoint> restartedFromThePresent = new CopyOnWriteArrayList<>();
    final List<Checkpoint> resumeProbes = new CopyOnWriteArrayList<>();
    final AtomicInteger resumeCalls = new AtomicInteger();
    final AtomicInteger takeOversAsked = new AtomicInteger();

    LiveStartLosingSubscriptionModel(InMemorySubscriptionModel delegate, boolean restartSubscriptionsOnHistoryLost, boolean opensOnTheCallingThread, Runnable writtenInTheGap) {
        this.delegate = delegate;
        this.restartSubscriptionsOnHistoryLost = restartSubscriptionsOnHistoryLost;
        this.opensOnTheCallingThread = opensOnTheCallingThread;
        this.writtenInTheGap = writtenInTheGap;
    }

    LiveStartLosingSubscriptionModel losingEveryOpen() {
        losesEveryOpen = true;
        return this;
    }

    LiveStartLosingSubscriptionModel beforeTheFirstOpen(Runnable beforeTheFirstOpen) {
        this.beforeTheFirstOpen = beforeTheFirstOpen;
        return this;
    }

    LiveStartLosingSubscriptionModel beforeTheFirstPause(Runnable beforeTheFirstPause) {
        this.beforeTheFirstPause = beforeTheFirstPause;
        return this;
    }

    LiveStartLosingSubscriptionModel betweenTheFirstTakeOverPauseAndWhatFollows(Runnable betweenTheFirstTakeOverPauseAndWhatFollows) {
        this.betweenTheFirstTakeOverPauseAndWhatFollows = betweenTheFirstTakeOverPauseAndWhatFollows;
        return this;
    }

    LiveStartLosingSubscriptionModel resumingWhereItWasHeldOnceThisReturns(Runnable beforeTheResumedOpen) {
        this.resumesWhereItWasHeldOnceThisReturns = beforeTheResumedOpen;
        return this;
    }

    private record Held(@Nullable SubscriptionFilter filter, Consumer<CloudEvent> action, StartAt heldAt) {
    }

    private static final class OpenHandle implements Subscription {
        private final String id;
        private final CountDownLatch opened = new CountDownLatch(1);

        private OpenHandle(String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            try {
                return opened.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        OpenHandle handle = new OpenHandle(subscriptionId);
        if (opensOnTheCallingThread) {
            open(subscriptionId, filter, startAt, action, handle);
        } else {
            Thread.ofVirtual().name("live-start-losing-open-" + subscriptionId).start(() -> open(subscriptionId, filter, startAt, action, handle));
        }
        return handle;
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        return resume(subscriptionId, held -> startAt, () -> {
        });
    }

    private Subscription resume(String subscriptionId, Function<Held, StartAt> startAt, Runnable beforeTheOpen) {
        resumeCalls.incrementAndGet();
        Held held = paused.remove(subscriptionId);
        if (held == null) {
            throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
        }
        OpenHandle resumed = new OpenHandle(subscriptionId);
        Runnable open = () -> {
            beforeTheOpen.run();
            open(subscriptionId, held.filter(), startAt.apply(held), held.action(), resumed);
        };
        if (opensOnTheCallingThread) {
            open.run();
        } else {
            Thread.ofVirtual().name("live-start-losing-open-" + subscriptionId).start(open);
        }
        return resumed;
    }

    private void open(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action, OpenHandle handle) {
        if (firstOpen.compareAndSet(true, false)) {
            beforeTheFirstOpen.run();
        }
        StartAt resolved = startAt.get(new SubscriptionModelContext(InMemorySubscriptionModel.class));
        if (resolved instanceof StartAtCheckpoint position) {
            Checkpoint opening = position.checkpoint;
            if (losesTheNextOpen.compareAndSet(true, false)) {
                writtenInTheGap.run();
                lost.add(opening);
            } else if (losesEveryOpen) {
                lost.add(opening);
            }
            if (lost.contains(opening)) {
                lostLiveStarts.add(opening);
                historyLost(subscriptionId, filter, opening, action, handle);
                return;
            }
            openedAt.add(opening);
        }
        delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
        handle.opened.countDown();
    }

    private void historyLost(String subscriptionId, @Nullable SubscriptionFilter filter, Checkpoint lostFrom, Consumer<CloudEvent> action, OpenHandle handle) {
        for (HistoryLossListener listener : listeners) {
            if (listener.handleHistoryLoss(subscriptionId, lostFrom) instanceof HistoryLossHandling.TakeOver takeOver) {
                paused.put(subscriptionId, new Held(filter, action, StartAt.checkpoint(lostFrom)));
                if (firstTakeOver.compareAndSet(true, false)) {
                    betweenTheFirstTakeOverPauseAndWhatFollows.run();
                }
                takeOver.afterPause().run();
                takeOversAsked.incrementAndGet();
                return;
            }
        }
        if (restartSubscriptionsOnHistoryLost) {
            Checkpoint present = new StringBasedCheckpoint("present-" + liveStartsRead.incrementAndGet());
            restartedFromThePresent.add(present);
            for (HistoryLossListener listener : listeners) {
                listener.restartingAfterHistoryLoss(subscriptionId, present, () -> true);
            }
            delegate.subscribe(subscriptionId, filter, StartAt.now(), action);
            handle.opened.countDown();
        }
    }

    @Override
    public void addHistoryLossListener(HistoryLossListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeHistoryLossListener(HistoryLossListener listener) {
        listeners.remove(listener);
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        Checkpoint liveStart = new StringBasedCheckpoint("live-start-" + liveStartsRead.incrementAndGet());
        readLiveStarts.add(liveStart);
        return liveStart;
    }

    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        resumeProbes.add(checkpoint);
        return !lost.contains(checkpoint);
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        delegate.start(resumeSubscriptionsAutomatically);
    }

    @Override
    public boolean isRunning() {
        return delegate.isRunning();
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return !paused.containsKey(subscriptionId) && delegate.isRunning(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return paused.containsKey(subscriptionId) || delegate.isPaused(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        Runnable beforeTheResumedOpen = resumesWhereItWasHeldOnceThisReturns;
        return beforeTheResumedOpen == null ? resumeSubscription(subscriptionId, StartAt.now()) : resume(subscriptionId, Held::heldAt, beforeTheResumedOpen);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        if (firstPause.compareAndSet(true, false)) {
            beforeTheFirstPause.run();
        }
        if (paused.containsKey(subscriptionId)) {
            return;
        }
        delegate.pauseSubscription(subscriptionId);
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        if (paused.remove(subscriptionId) != null) {
            return;
        }
        delegate.cancelSubscription(subscriptionId);
    }
}
