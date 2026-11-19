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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A live delegate that keeps the action it was given and calls it when told to, on the calling thread, as a delegate
 * does that delivers on a thread of its own. A test can hold its start, stop and shutdown on a gate, and it records
 * each call that returned, in order. Its {@code stop()} waits, for a bounded time, for every other thread that is inside
 * a call of a kept action, as a live delegate does that waits for its deliveries to end. When told to, its {@code stop()}
 * and {@code shutdown()} wait for the calling thread's own call of a kept action too, which a call made from inside the
 * action can never see end, and either can be told to throw.
 */
final class GatedLiveSubscriptionModel implements CheckpointAwareSubscriptionModel {
    static final String START_RETURNED = "start-returned";
    static final String STOP_RETURNED = "stop-returned";
    static final String SHUTDOWN = "shutdown";
    static final String RESUMED_PREFIX = "resumed:";

    private final Map<String, Consumer<CloudEvent>> kept = new ConcurrentHashMap<>();
    private final Set<String> running = ConcurrentHashMap.newKeySet();
    private final Set<Thread> inAKeptAction = ConcurrentHashMap.newKeySet();
    private final List<String> events = new CopyOnWriteArrayList<>();
    private final Duration stopWaitsForActionsFor;
    private final AtomicBoolean stopTimedOut = new AtomicBoolean();
    private volatile boolean runs = true;
    volatile @Nullable Gate startGate;
    volatile @Nullable Gate stopGate;
    volatile @Nullable Gate shutdownGate;
    volatile boolean stopAndShutdownWaitForTheCallersOwnAction;
    volatile @Nullable RuntimeException stopThrows;
    volatile @Nullable RuntimeException shutdownThrows;

    GatedLiveSubscriptionModel(Duration stopWaitsForActionsFor) {
        this.stopWaitsForActionsFor = stopWaitsForActionsFor;
    }

    List<String> events() {
        return events;
    }

    boolean stopGaveUpWaitingForAnAction() {
        return stopTimedOut.get();
    }

    void callTheKeptAction(String subscriptionId) {
        CloudEvent event = CloudEventBuilder.v1().withId("live1").withSource(URI.create("urn:test")).withType("Tested").build();
        Thread caller = Thread.currentThread();
        inAKeptAction.add(caller);
        try {
            kept.get(subscriptionId).accept(event);
        } finally {
            inAKeptAction.remove(caller);
        }
    }

    @Override
    public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
        kept.put(subscriptionId, action);
        running.add(subscriptionId);
        return subscriptionOf(subscriptionId);
    }

    private static Subscription subscriptionOf(String subscriptionId) {
        return new Subscription() {
            @Override
            public String id() {
                return subscriptionId;
            }

            @Override
            public boolean waitUntilStarted(Duration timeout) {
                return true;
            }
        };
    }

    @Override
    public @Nullable Checkpoint globalCheckpoint() {
        return new StringBasedCheckpoint("live start read now");
    }

    @Override
    public boolean canResumeFrom(Checkpoint checkpoint) {
        return true;
    }

    @Override
    public void stop() {
        pass(stopGate);
        throwIfTold(stopThrows);
        waitForTheKeptActions();
        runs = false;
        events.add(STOP_RETURNED);
    }

    private void waitForTheKeptActions() {
        long deadline = System.nanoTime() + stopWaitsForActionsFor.toNanos();
        while (aThreadIsInAKeptAction()) {
            if (System.nanoTime() >= deadline) {
                stopTimedOut.set(true);
                break;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private boolean aThreadIsInAKeptAction() {
        return inAKeptAction.stream().anyMatch(thread -> stopAndShutdownWaitForTheCallersOwnAction || thread != Thread.currentThread());
    }

    private static void throwIfTold(@Nullable RuntimeException failure) {
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void start(boolean resumeSubscriptionsAutomatically) {
        pass(startGate);
        runs = true;
        events.add(START_RETURNED);
    }

    @Override
    public boolean isRunning() {
        return runs;
    }

    @Override
    public boolean isRunning(String subscriptionId) {
        return running.contains(subscriptionId);
    }

    @Override
    public boolean isPaused(String subscriptionId) {
        return false;
    }

    @Override
    public Subscription resumeSubscription(String subscriptionId) {
        events.add(RESUMED_PREFIX + subscriptionId);
        return subscriptionOf(subscriptionId);
    }

    @Override
    public void pauseSubscription(String subscriptionId) {
    }

    @Override
    public void cancelSubscription(String subscriptionId) {
        running.remove(subscriptionId);
    }

    @Override
    public void shutdown() {
        pass(shutdownGate);
        throwIfTold(shutdownThrows);
        if (stopAndShutdownWaitForTheCallersOwnAction) {
            waitForTheKeptActions();
        }
        events.add(SHUTDOWN);
    }

    private static void pass(@Nullable Gate gate) {
        if (gate != null) {
            gate.pass();
        }
    }

    // A call that passes it waits until it is released, which a test does in a finally
    static final class Gate {
        final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void pass() {
            entered.countDown();
            try {
                if (!released.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("gate not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        void release() {
            released.countDown();
        }
    }
}
