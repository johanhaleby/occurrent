/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.mongodb.blocking.ccs.internal;


import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.subscription.internal.ExecutorShutdown;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Schedules a periodic refresh, and tells the listeners what a refresh changed on a thread of its own.
 * <p>
 * A listener runs straight into the subscription model, and pausing or resuming a subscription there can block for as
 * long as the database takes to answer. On the refresh thread that would hold up every other lease on the instance
 * until each one expired, so {@link #auto()} and {@link #every(Duration)} hand notifications to a notifier instead.
 * Each subscription id gets its notifications one at a time, in the order the refresh decided them, and a
 * notification that blocks holds up later ones for its own subscription id only. Each id whose notification blocks
 * takes a thread of the notifier's, a virtual one from Java 24 and a platform one before that, since a virtual thread
 * blocked inside {@code synchronized} keeps the platform thread it runs on before Java 24.
 *
 * @see #auto()
 */
@NullMarked
class ScheduledRefresh {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final BiConsumer<Duration, Scheduler> scheduleIt;
    // Its threads are daemon threads, since close() does not wait for them, and a listener that ignores the interrupt
    // close() sends can go on running after it. close() is what ends the refresh thread.
    private final @Nullable ExecutorService notifier;
    // The notifications not yet delivered per subscription id, the first one being delivered or about to be. An id is
    // here only while a task on the notifier delivers its notifications, so a new notification for it is queued behind
    // them and one for an id not here starts a task of its own. Read and written under its own monitor only.
    private final Map<String, Deque<Runnable>> undelivered = new HashMap<>();

    /**
     * Runs every notification on the thread that runs the refresh, so a test that holds the refresh and runs it
     * itself sees what the round changed by the time the round returns.
     *
     * @param scheduleIt A function which configures the schedule. Accepts two arguments. One is a
     *                   {@link Scheduler} which is what must be configured with the desired schedule.
     *                   The other argument is a lease {@link Duration} which may be used to inform
     *                   the schedule.
     */
    ScheduledRefresh(BiConsumer<Duration, Scheduler> scheduleIt) {
        this(scheduleIt, null);
    }

    /**
     * Runs every notification on {@code notifier}, or on the thread that runs the refresh when it is {@code null}.
     */
    ScheduledRefresh(BiConsumer<Duration, Scheduler> scheduleIt, @Nullable ExecutorService notifier) {
        this.scheduleIt = scheduleIt;
        this.notifier = notifier;
    }

    static ScheduledRefresh every(Duration period) {
        if (period.isNegative() || period.isZero()) {
            throw new IllegalArgumentException("Period must be > 0 but got " + period);
        }

        return new ScheduledRefresh((lease, scheduler) -> scheduler.fixedRate(Duration.ZERO, period), newNotifier());
    }

    /**
     * @return A {@link ScheduledRefresh} which automatically refreshes at a reasonable interval based
     * on the lease time of the lock.
     */
    static ScheduledRefresh auto() {
        return new ScheduledRefresh((lease, scheduler) -> {
            if (lease.isNegative()) {
                throw new IllegalArgumentException("Lease time must not be negative but got " + lease);
            }

            scheduler.fixedRate(Duration.ZERO, lease.dividedBy(2));
        }, newNotifier());
    }

    // A thread for each subscription id whose notification blocks, so none of them holds up another. A listener can
    // block inside synchronized, and before Java 24 a virtual thread blocked there keeps the platform thread it runs on,
    // so as many such listeners as there are processors would hold up every other notification. Before 24 each
    // therefore gets a platform thread, and from 24 a virtual one, so the number of platform threads no longer grows
    // with the ids that block.
    private static ExecutorService newNotifier() {
        if (Runtime.version().feature() >= 24) {
            return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("occurrent-lease-notifier-", 0).factory());
        }
        return Executors.newCachedThreadPool(Thread.ofPlatform().daemon().name("occurrent-lease-notifier-", 0).factory());
    }

    void scheduleInBackground(Runnable refresh, Duration leaseTime) {
        scheduleIt.accept(leaseTime, new Scheduler(executor, refresh));
    }

    /**
     * Hands {@code notification} to the notifier, or runs it right here when there is none. It runs once every
     * notification handed over before it for the same {@code subscriptionId} has run, and never waits for one for
     * another subscription id, as long as the notifier has a thread free, which the one {@link #auto()} and
     * {@link #every(Duration)} make always has. A notification handed over after {@link #close()} is dropped, since
     * nothing is left to act on it.
     */
    void notifyInBackground(String subscriptionId, Runnable notification) {
        if (notifier == null) {
            notification.run();
            return;
        }
        synchronized (undelivered) {
            Deque<Runnable> queued = undelivered.get(subscriptionId);
            if (queued != null) {
                queued.add(notification);
                return;
            }
            undelivered.put(subscriptionId, new ArrayDeque<>(List.of(notification)));
        }
        deliverNextInBackground(notifier, subscriptionId);
    }

    private void deliverNextInBackground(ExecutorService notifier, String subscriptionId) {
        try {
            notifier.execute(() -> deliverNext(notifier, subscriptionId));
        } catch (RejectedExecutionException closed) {
            // close() ran, and the listeners are shutting down with this instance
            synchronized (undelivered) {
                undelivered.remove(subscriptionId);
            }
        }
    }

    // One notification per task, so one that throws leaves the rest to the next task instead of to nobody
    private void deliverNext(ExecutorService notifier, String subscriptionId) {
        Runnable next;
        synchronized (undelivered) {
            next = undelivered.get(subscriptionId).getFirst();
        }
        try {
            next.run();
        } finally {
            boolean more;
            synchronized (undelivered) {
                Deque<Runnable> queued = undelivered.get(subscriptionId);
                queued.removeFirst();
                more = !queued.isEmpty();
                if (!more) {
                    undelivered.remove(subscriptionId);
                }
            }
            if (more) {
                deliverNextInBackground(notifier, subscriptionId);
            }
        }
    }

    /**
     * Stops the notifier without waiting for it. A listener it is calling may be waiting for the database, such as a
     * grant that resumes a subscription and opens its change stream, and waiting for it here would stall the shutdown
     * of the subscription model that shuts this down for the full timeout.
     */
    void close() {
        if (notifier != null) {
            notifier.shutdownNow();
        }
        ExecutorShutdown.shutdownSafely(executor, 5, SECONDS);
    }

    record Scheduler(ScheduledExecutorService executor, Runnable refresh) {

        void fixedRate(Duration initialDelay, Duration period) {
            executor.scheduleAtFixedRate(refresh,
                    initialDelay.toMillis(),
                    period.toMillis(),
                    TimeUnit.MILLISECONDS);
        }

        void fixedDelay(Duration initialDelay, Duration delay) {
            executor.scheduleWithFixedDelay(refresh,
                    initialDelay.toMillis(),
                    delay.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }
}
