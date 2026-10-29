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

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Schedules a periodic refresh, and tells the listeners what a refresh changed on a thread of its own.
 * <p>
 * A listener runs straight into the subscription model, and pausing or resuming a subscription there can block for as
 * long as the database takes to answer. On the refresh thread that would hold up every other lease on the instance
 * until each one expired, so {@link #auto()} and {@link #every(Duration)} hand each tell to a notifier instead. The
 * strategy makes sure a consumer has one tell running at a time, so a tell that blocks holds up later ones for its own
 * consumer only. Each consumer whose tell blocks takes a thread of the notifier's, a virtual one from Java 24 and a
 * platform one before that, since a virtual thread blocked inside {@code synchronized} keeps the platform thread it
 * runs on before Java 24.
 * <p>
 * A refresh that throws is logged at ERROR and the next one runs as scheduled. That includes an {@link Error}, a
 * {@link VirtualMachineError} too, since the executor would otherwise cancel every later refresh.
 *
 * @see #auto()
 */
@NullMarked
class ScheduledRefresh {
    private static final Logger log = LoggerFactory.getLogger(ScheduledRefresh.class);
    private static final FailureLogging refreshFailed = new FailureLogging(log, "Lease refresh failed due to {} - {}. The next scheduled refresh will run as usual.");
    private static final FailureLogging notifierTookNothing = new FailureLogging(log, "The notifier did not take the tell for subscription {} due to {} - {}. The next scheduled refresh hands it over again.");

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final BiConsumer<Duration, Scheduler> scheduleIt;
    // Its threads are daemon threads, since close() does not wait for them, and a listener that ignores the interrupt
    // close() sends can go on running after it. close() is what ends the refresh thread.
    private final @Nullable ExecutorService notifier;
    // Set once the notifier throws, and cleared when the next refresh starts, so a notifier that cannot start threads
    // costs one ERROR line per refresh rather than one per tell
    private volatile boolean notifierFailedThisRound;

    /**
     * Runs every tell on the thread that hands it over, so a test that holds the refresh and runs it itself sees what
     * the round changed by the time the round returns.
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
     * Runs every tell on {@code notifier}, or on the thread that hands it over when it is {@code null}.
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

    // A thread for each consumer whose tell blocks, so none of them holds up another. A listener can block inside
    // synchronized, and before Java 24 a virtual thread blocked there keeps the platform thread it runs on, so as many
    // such listeners as there are processors would hold up every other tell. Before 24 each therefore gets a platform
    // thread, and from 24 a virtual one, so the number of platform threads no longer grows with the consumers that block.
    private static ExecutorService newNotifier() {
        if (Runtime.version().feature() >= 24) {
            return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("occurrent-lease-notifier-", 0).factory());
        }
        return Executors.newCachedThreadPool(Thread.ofPlatform().daemon().name("occurrent-lease-notifier-", 0).factory());
    }

    void scheduleInBackground(Runnable refresh, Duration leaseTime) {
        scheduleIt.accept(leaseTime, new Scheduler(executor, () -> {
            notifierFailedThisRound = false;
            refresh.run();
        }));
    }

    /**
     * Hands {@code tell} to the notifier, or runs it right here when there is none. Keeps nothing, so a tell the
     * notifier does not take is gone, and the strategy's next refresh round compares again and hands over what is
     * still due. A tell handed over after {@link #close()} is dropped, since nothing is left to act on it.
     * <p>
     * When the notifier throws while it isn't shut down, such as an {@link OutOfMemoryError} when it can't start a
     * thread or a {@link java.util.concurrent.RejectedExecutionException}, this method logs it at ERROR and returns
     * without throwing, also when logging it throws. Every later tell handed over before the next refresh starts is
     * then dropped without asking the notifier again.
     */
    void notifyInBackground(String subscriptionId, Runnable tell) {
        if (notifier == null) {
            tell.run();
            return;
        }
        if (notifierFailedThisRound) {
            return;
        }
        try {
            notifier.execute(tell);
        } catch (Throwable t) {
            if (!notifier.isShutdown()) {
                notifierFailedThisRound = true;
                notifierTookNothing.error(subscriptionId, t);
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
                    refreshFailed.error(t);
                }
            };
        }
    }
}
