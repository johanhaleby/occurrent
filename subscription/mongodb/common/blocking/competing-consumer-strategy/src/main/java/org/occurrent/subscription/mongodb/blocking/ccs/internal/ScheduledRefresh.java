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
 * A listener runs straight into the subscription model, and pausing a subscription there can block for as long as
 * its change stream takes to open. On the refresh thread that would hold up every other lease on the instance
 * until each one expired, so {@link #auto()} and {@link #every(Duration)} hand notifications to a single notifier
 * thread instead, which delivers them in the order the refresh decided them.
 *
 * @see #auto()
 */
@NullMarked
class ScheduledRefresh {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final BiConsumer<Duration, Scheduler> scheduleIt;
    private final @Nullable ExecutorService notifier;

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

    private ScheduledRefresh(BiConsumer<Duration, Scheduler> scheduleIt, @Nullable ExecutorService notifier) {
        this.scheduleIt = scheduleIt;
        this.notifier = notifier;
    }

    static ScheduledRefresh every(Duration period) {
        if (period.isNegative() || period.isZero()) {
            throw new IllegalArgumentException("Period must be > 0 but got " + period);
        }

        return new ScheduledRefresh((lease, scheduler) -> scheduler.fixedRate(Duration.ZERO, period), Executors.newSingleThreadExecutor());
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
        }, Executors.newSingleThreadExecutor());
    }

    void scheduleInBackground(Runnable refresh, Duration leaseTime) {
        scheduleIt.accept(leaseTime, new Scheduler(executor, refresh));
    }

    /**
     * Hands {@code notification} to the notifier thread, or runs it right here when there is none. A notification
     * handed over after {@link #close()} is dropped, since nothing is left to act on it.
     */
    void notifyInBackground(Runnable notification) {
        if (notifier == null) {
            notification.run();
            return;
        }
        try {
            notifier.execute(notification);
        } catch (RejectedExecutionException closed) {
            // close() ran, and the listeners are shutting down with this instance
        }
    }

    /**
     * Stops the notifier without waiting for it. A notification it is delivering may be waiting for the monitor
     * of the very subscription model that is shutting this down, and waiting for it here would stall that shutdown
     * for the full timeout.
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
