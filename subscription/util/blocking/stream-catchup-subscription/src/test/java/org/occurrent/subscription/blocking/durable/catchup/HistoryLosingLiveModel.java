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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A live model whose every open, pause, resume and loss of history happens on the calling thread, so a test decides
 * exactly when the history is lost. It stands in for a MongoDB subscription model with restart-on-history-lost on. A
 * loss that no listener takes over restarts the subscription from now and is recorded in {@link #restartedFromNow}.
 * A loss a listener takes over leaves the subscription paused where it lost the history, and a resume then opens it
 * there again, where it loses the history again if the position has since been aged out.
 */
final class HistoryLosingLiveModel implements CheckpointAwareSubscriptionModel, HistoryLossReportingSubscriptions, RepositionableSubscriptions {

    private record Open(@Nullable SubscriptionFilter filter, Consumer<CloudEvent> action, StartAt startAt) {
    }

    private final Map<String, Open> running = new ConcurrentHashMap<>();
    private final Map<String, Open> paused = new ConcurrentHashMap<>();
    private final Set<Checkpoint> agedOut = ConcurrentHashMap.newKeySet();
    private final List<HistoryLossListener> listeners = new CopyOnWriteArrayList<>();
    private final AtomicInteger liveStartsRead = new AtomicInteger();
    private record Committed(long sequence, CloudEvent cloudEvent) {
    }

    // What the live stream holds, in commit order, and the point in it each position answered by globalCheckpoint stands for
    private final List<Committed> committed = new CopyOnWriteArrayList<>();
    private final Map<String, Long> positionsRead = new ConcurrentHashMap<>();
    private final AtomicLong commits = new AtomicLong();
    private volatile boolean running_ = true;
    private volatile boolean answersNothingForTheCurrentPosition;
    private volatile @Nullable Checkpoint lossReportedInsideSubscribe;
    private volatile @Nullable Throwable nextSubscribeThrows;
    private volatile @Nullable RuntimeException nextRepositioningResumeThrows;
    private volatile Runnable betweenTheNextTakeOverPauseAndWhatFollows = () -> {
    };
    private volatile Runnable beforeTheNextResumeAt = () -> {
    };
    private volatile Runnable beforeTheNextResume = () -> {
    };
    private volatile Runnable beforeTheNextSubscribe = () -> {
    };
    private volatile boolean defersTheNextAfterPause;
    private volatile @Nullable Runnable deferredAfterPause;

    final List<String> subscribeCalls = new CopyOnWriteArrayList<>();
    final List<String> subscribePausedCalls = new CopyOnWriteArrayList<>();
    final List<StartAt> subscribePausedStartAts = new CopyOnWriteArrayList<>();
    final List<String> resumeCalls = new CopyOnWriteArrayList<>();
    final List<String> restartedFromNow = new CopyOnWriteArrayList<>();
    final List<Checkpoint> lossesReported = new CopyOnWriteArrayList<>();

    // Positions the live model has no history back to, so a subscription opened or resumed there reports the loss
    HistoryLosingLiveModel withoutHistoryBack(Checkpoint checkpoint) {
        agedOut.add(checkpoint);
        return this;
    }

    HistoryLosingLiveModel reportingTheLossInsideSubscribe(Checkpoint lostFrom) {
        lossReportedInsideSubscribe = lostFrom;
        return this;
    }

    HistoryLosingLiveModel failingTheNextSubscribeWith(Throwable failure) {
        nextSubscribeThrows = failure;
        return this;
    }

    HistoryLosingLiveModel failingTheNextRepositioningResumeWith(RuntimeException failure) {
        nextRepositioningResumeThrows = failure;
        return this;
    }

    // Runs once, after the next takeover has paused the subscription and before what follows the pause
    HistoryLosingLiveModel betweenTheNextTakeOverPauseAndWhatFollows(Runnable betweenTheNextTakeOverPauseAndWhatFollows) {
        this.betweenTheNextTakeOverPauseAndWhatFollows = betweenTheNextTakeOverPauseAndWhatFollows;
        return this;
    }

    // Runs once, at the start of the next resume at a given position and before the subscription is resumed
    HistoryLosingLiveModel beforeTheNextResumeAt(Runnable beforeTheNextResumeAt) {
        this.beforeTheNextResumeAt = beforeTheNextResumeAt;
        return this;
    }

    // Runs once, at the start of the next subscribe and before it fails or opens the subscription
    HistoryLosingLiveModel beforeTheNextSubscribe(Runnable beforeTheNextSubscribe) {
        this.beforeTheNextSubscribe = beforeTheNextSubscribe;
        return this;
    }

    // Runs once, at the start of the next plain resume and before the subscription is resumed
    HistoryLosingLiveModel beforeTheNextResume(Runnable beforeTheNextResume) {
        this.beforeTheNextResume = beforeTheNextResume;
        return this;
    }

    // Once, the next takeover pauses the subscription as usual and keeps what follows the pause until runsTheDeferredAfterPause
    HistoryLosingLiveModel defersTheNextAfterPause() {
        defersTheNextAfterPause = true;
        return this;
    }

    void runsTheDeferredAfterPause() {
        Runnable deferred = deferredAfterPause;
        if (deferred == null) {
            throw new IllegalStateException("No takeover has deferred what follows its pause");
        }
        deferredAfterPause = null;
        deferred.run();
    }

    // What the listeners answer for a loss, without pausing the subscription or running what follows the pause
    HistoryLossHandling asksTheListenersAbout(String subscriptionId, Checkpoint lostFrom) {
        for (HistoryLossListener listener : listeners) {
            HistoryLossHandling handling = listener.handleHistoryLoss(subscriptionId, lostFrom);
            if (handling instanceof HistoryLossHandling.TakeOver) {
                return handling;
            }
        }
        return HistoryLossHandling.leaveToSubscriptionModel();
    }

    // The change stream of a running subscription fails because its position is gone from the oplog
    void losesTheHistoryOf(String subscriptionId, Checkpoint lostFrom) {
        Open open = running.remove(subscriptionId);
        if (open == null) {
            throw new IllegalStateException("Subscription " + subscriptionId + " does not run");
        }
        reportLoss(subscriptionId, open, lostFrom);
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        subscribeCalls.add(subscriptionId);
        Runnable beforeThisSubscribe = beforeTheNextSubscribe;
        beforeTheNextSubscribe = () -> {
        };
        beforeThisSubscribe.run();
        Throwable failure = nextSubscribeThrows;
        if (failure != null) {
            nextSubscribeThrows = null;
            throw sneaky(failure);
        }
        Open open = new Open(filter, action, startAt);
        Checkpoint insideSubscribe = lossReportedInsideSubscribe;
        if (insideSubscribe != null) {
            lossReportedInsideSubscribe = null;
            reportLoss(subscriptionId, open, insideSubscribe);
        } else {
            open(subscriptionId, open);
        }
        return handle(subscriptionId);
    }

    @Override
    public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        subscribePausedCalls.add(subscriptionId);
        subscribePausedStartAts.add(startAt);
        paused.put(subscriptionId, new Open(filter, action, startAt));
        return handle(subscriptionId);
    }

    // Commits an event to the live stream. A subscription opened from now never gets it, and one opened from a position
    // read by globalCheckpoint before the commit gets it when it opens, as a stream resumed from a position does
    void commits(CloudEvent cloudEvent) {
        committed.add(new Committed(commits.incrementAndGet(), cloudEvent));
    }

    // The start a running subscription was opened at
    StartAt startOf(String subscriptionId) {
        Open open = running.get(subscriptionId);
        if (open == null) {
            throw new IllegalStateException("Subscription " + subscriptionId + " does not run");
        }
        return open.startAt();
    }

    // Hands an event to the action of a running subscription, as the live model does for an event it reads
    void deliver(String subscriptionId, CloudEvent cloudEvent) {
        Open open = running.get(subscriptionId);
        if (open == null) {
            throw new IllegalStateException("Subscription " + subscriptionId + " does not run");
        }
        open.action().accept(cloudEvent);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        Runnable beforeThisResume = beforeTheNextResume;
        beforeTheNextResume = () -> {
        };
        beforeThisResume.run();
        Open held = paused.remove(subscriptionId);
        if (held == null) {
            throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
        }
        resumeCalls.add(subscriptionId);
        open(subscriptionId, held);
        return handle(subscriptionId);
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
        Runnable beforeThisResume = beforeTheNextResumeAt;
        beforeTheNextResumeAt = () -> {
        };
        beforeThisResume.run();
        RuntimeException failure = nextRepositioningResumeThrows;
        if (failure != null) {
            nextRepositioningResumeThrows = null;
            throw failure;
        }
        Open held = paused.remove(subscriptionId);
        if (held == null) {
            throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
        }
        resumeCalls.add(subscriptionId);
        open(subscriptionId, new Open(held.filter(), held.action(), startAt));
        return handle(subscriptionId);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable throwable) throws E {
        throw (E) throwable;
    }

    private void open(String subscriptionId, Open open) {
        StartAt resolved = open.startAt().get(new SubscriptionModelContext(HistoryLosingLiveModel.class));
        if (resolved instanceof StartAtCheckpoint position && agedOut.contains(position.checkpoint)) {
            reportLoss(subscriptionId, open, position.checkpoint);
            return;
        }
        running.put(subscriptionId, open);
        if (resolved instanceof StartAtCheckpoint position && positionsRead.containsKey(position.checkpoint.asString())) {
            long after = positionsRead.get(position.checkpoint.asString());
            committed.stream().filter(event -> event.sequence() > after).forEach(event -> open.action().accept(event.cloudEvent()));
        }
    }

    private void reportLoss(String subscriptionId, Open open, Checkpoint lostFrom) {
        lossesReported.add(lostFrom);
        for (HistoryLossListener listener : listeners) {
            if (listener.handleHistoryLoss(subscriptionId, lostFrom) instanceof HistoryLossHandling.TakeOver takeOver) {
                paused.put(subscriptionId, new Open(open.filter(), open.action(), StartAt.checkpoint(lostFrom)));
                Runnable betweenThePauseAndWhatFollows = betweenTheNextTakeOverPauseAndWhatFollows;
                betweenTheNextTakeOverPauseAndWhatFollows = () -> {
                };
                betweenThePauseAndWhatFollows.run();
                if (defersTheNextAfterPause) {
                    defersTheNextAfterPause = false;
                    deferredAfterPause = takeOver.afterPause();
                } else {
                    takeOver.afterPause().run();
                }
                return;
            }
        }
        restartedFromNow.add(subscriptionId);
        Checkpoint present = new StringBasedCheckpoint("present-" + liveStartsRead.incrementAndGet());
        for (HistoryLossListener listener : listeners) {
            listener.restartingAfterHistoryLoss(subscriptionId, present, () -> true);
        }
        running.put(subscriptionId, new Open(open.filter(), open.action(), StartAt.now()));
    }

    private Subscription handle(String subscriptionId) {
        return new Subscription() {
            @Override
            public String id() {
                return subscriptionId;
            }

            @Override
            public boolean waitUntilStarted(Duration timeout) {
                return running.containsKey(subscriptionId);
            }
        };
    }

    @Override
    public void addHistoryLossListener(HistoryLossListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeHistoryLossListener(HistoryLossListener listener) {
        listeners.remove(listener);
    }

    // As a live model does when the call that reads its current position was refused
    HistoryLosingLiveModel answeringNothingForTheCurrentPosition() {
        answersNothingForTheCurrentPosition = true;
        return this;
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        if (answersNothingForTheCurrentPosition) {
            return null;
        }
        String position = "live-start-" + liveStartsRead.incrementAndGet();
        positionsRead.put(position, commits.get());
        return new StringBasedCheckpoint(position);
    }

    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return !agedOut.contains(checkpoint);
    }

    // As the MongoDB models answer: it opens at any checkpoint but a catch-up's position, which it doesn't recognize
    @Override
    public boolean restartsAt(Checkpoint checkpoint) {
        return !AbstractCatchupSubscriptionModel.isCatchupCheckpoint(checkpoint);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
        Open open = running.remove(subscriptionId);
        if (open != null) {
            paused.put(subscriptionId, open);
        }
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        running.remove(subscriptionId);
        paused.remove(subscriptionId);
    }

    @Override
    public void stop() {
        running_ = false;
        running.forEach((subscriptionId, open) -> pauseSubscription(subscriptionId));
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        running_ = true;
        if (resumeSubscriptionsAutomatically) {
            Set.copyOf(paused.keySet()).forEach(this::resumeSubscription);
        }
    }

    @Override
    public boolean isRunning() {
        return running_;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return running.containsKey(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return paused.containsKey(subscriptionId);
    }
}
