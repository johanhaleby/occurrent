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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.occurrent.subscription.mongodb.blocking.ccs.internal.FailureLogging.logIgnoringWhatLoggingThrows;

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
 * <p>
 * A refresh that throws is logged at ERROR and the next one runs as scheduled. That includes an {@link Error}, a
 * {@link VirtualMachineError} too, since the executor would otherwise cancel every later refresh.
 *
 * @see #auto()
 */
@NullMarked
class ScheduledRefresh {
    private static final Logger log = LoggerFactory.getLogger(ScheduledRefresh.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final BiConsumer<Duration, Scheduler> scheduleIt;
    // Its threads are daemon threads, since close() does not wait for them, and a listener that ignores the interrupt
    // close() sends can go on running after it. close() is what ends the refresh thread.
    private final @Nullable ExecutorService notifier;
    // The notifications not yet delivered per subscription id. An id is here while a task on the notifier delivers its
    // notifications, or while they wait for a refresh to hand them over again, so a new notification for it is queued
    // behind them and one for an id not here starts a task of its own. Read and written under its own monitor only.
    private final Map<String, Undelivered> undelivered = new HashMap<>();

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
        scheduleIt.accept(leaseTime, new Scheduler(executor, () -> {
            handOverWhatTheNotifierDidNotTake();
            refresh.run();
        }));
    }

    /**
     * Hands {@code notification} to the notifier, or runs it right here when there is none. It runs once every
     * notification handed over before it for the same {@code subscriptionId} has run or thrown, and never waits for one
     * for another subscription id, as long as the notifier has a thread free, which the one {@link #auto()} and
     * {@link #every(Duration)} make always has. A notification handed over after {@link #close()} is dropped, since
     * nothing is left to act on it.
     * <p>
     * When the notifier throws while it isn't shut down, such as an {@link OutOfMemoryError} when it can't start a
     * thread, this method logs it at ERROR and returns without throwing, also when logging it throws. That includes a
     * {@link RejectedExecutionException}, since the contract of {@code execute} lets a notifier reject a task for
     * other reasons than {@link #close()}. A later scheduled refresh hands the notifications for {@code subscriptionId}
     * over again, in the same order. Each refresh hands the waiting subscription ids over one at a time and stops at the
     * first one the notifier throws on, so the ones after it wait for the next refresh.
     */
    void notifyInBackground(String subscriptionId, Runnable notification) {
        if (notifier == null) {
            notification.run();
            return;
        }
        Undelivered queued;
        synchronized (undelivered) {
            queued = undelivered.get(subscriptionId);
            if (queued != null) {
                queued.notifications.add(notification);
                return;
            }
            queued = new Undelivered();
            queued.notifications.add(notification);
            undelivered.put(subscriptionId, queued);
        }
        handOver(notifier, subscriptionId, queued);
    }

    // One subscription id at a time, so the ids already waiting add one ERROR line per refresh, not one each. A
    // notification a refresh decides is handed over on its own and logs its own line when the notifier throws. Throws
    // nothing, so the refresh after it always runs.
    private void handOverWhatTheNotifierDidNotTake() {
        if (notifier == null) {
            return;
        }
        try {
            while (true) {
                String subscriptionId = null;
                Undelivered queued = null;
                synchronized (undelivered) {
                    for (Map.Entry<String, Undelivered> entry : undelivered.entrySet()) {
                        if (entry.getValue().waitingForARefresh) {
                            subscriptionId = entry.getKey();
                            queued = entry.getValue();
                            queued.waitingForARefresh = false;
                            break;
                        }
                    }
                }
                if (queued == null || !handOver(notifier, subscriptionId, queued)) {
                    return;
                }
            }
        } catch (Throwable t) {
            logIgnoringWhatLoggingThrows(log, Level.ERROR, "Handing the waiting notifications over to the notifier failed due to {} - {}. The refresh runs as usual.", t);
        }
    }

    // False when the notifier threw, and then the notifications wait for a refresh to hand them over again
    private boolean handOver(ExecutorService notifier, String subscriptionId, Undelivered queued) {
        try {
            notifier.execute(() -> deliverUntilNoneIsLeft(notifier, subscriptionId, queued));
            return true;
        } catch (Throwable t) {
            if (notifier.isShutdown()) {
                // close() ran, and the listeners are shutting down with this instance
                synchronized (undelivered) {
                    undelivered.remove(subscriptionId, queued);
                }
                return true;
            }
            // Kept, also for a RejectedExecutionException, since a refresh decides each notification only once. Setting
            // a flag allocates nothing, so a second OutOfMemoryError can't throw here.
            synchronized (undelivered) {
                queued.waitingForARefresh = true;
            }
            logIgnoringWhatLoggingThrows(log, Level.ERROR, "The notifier did not take the notifications for subscription {} due to {} - {}. A later scheduled refresh hands them over again.",
                    subscriptionId, t);
            return false;
        }
    }

    // One task delivers every notification for the id, also those queued while it runs, so only handing over the
    // first one can find the notifier unable to start a thread
    private void deliverUntilNoneIsLeft(ExecutorService notifier, String subscriptionId, Undelivered queued) {
        while (true) {
            Runnable next;
            synchronized (undelivered) {
                // Only close() is meant to interrupt this thread. An interrupt that a notification sets and doesn't
                // clear would make the first blocking call of the next one throw. Cleared before the isShutdown()
                // check, so an interrupt close() sends after that check stays set.
                Thread.interrupted();
                // Dropped after close(), the same as a notification handed over after it
                next = notifier.isShutdown() ? null : queued.notifications.poll();
                if (next == null) {
                    undelivered.remove(subscriptionId, queued);
                    return;
                }
            }
            try {
                next.run();
            } catch (Throwable t) {
                // Not rethrown, since that would leave the notifications after it to nobody
                logIgnoringWhatLoggingThrows(log, Level.ERROR, "A notification for subscription {} failed due to {} - {}. The notifications after it are delivered as usual.",
                        subscriptionId, t);
            }
        }
    }

    private static final class Undelivered {
        private final Deque<Runnable> notifications = new ArrayDeque<>();
        // Set when the notifier threw on taking them, so no task delivers them until a refresh hands them over again
        private boolean waitingForARefresh;
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
            executor.scheduleAtFixedRate(logEveryThrowable(refresh),
                    initialDelay.toMillis(),
                    period.toMillis(),
                    TimeUnit.MILLISECONDS);
        }

        void fixedDelay(Duration initialDelay, Duration delay) {
            executor.scheduleWithFixedDelay(logEveryThrowable(refresh),
                    initialDelay.toMillis(),
                    delay.toMillis(),
                    TimeUnit.MILLISECONDS);
        }

        // An execution that throws cancels every later one, and nothing reads the ScheduledFuture that holds what it
        // threw. So nothing is rethrown here, not even a VirtualMachineError, and the next refresh still runs.
        private static Runnable logEveryThrowable(Runnable refresh) {
            return () -> {
                try {
                    refresh.run();
                } catch (Throwable t) {
                    logIgnoringWhatLoggingThrows(log, Level.ERROR, "Lease refresh failed due to {} - {}. The next scheduled refresh will run as usual.", t);
                }
            };
        }
    }
}
