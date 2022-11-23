/*
 *
 *  Copyright 2022 Johan Haleby
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

package org.occurrent.deadline.inmemory;

import org.occurrent.deadline.api.blocking.Deadline;
import org.occurrent.deadline.api.blocking.DeadlineScheduler;
import org.occurrent.deadline.inmemory.internal.DeadlineData;

import java.util.Objects;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Hello world!
 */
public class InMemoryDeadlineScheduler implements DeadlineScheduler {

    private final ConcurrentMap<String, ScheduledFuture<?>> scheduledDeadlines = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

    private final BlockingDeque<DeadlineData> queue;

    public InMemoryDeadlineScheduler(BlockingDeque<DeadlineData> queue) {
        Objects.requireNonNull(queue, "Queue cannot be null");
        this.queue = queue;
    }

    @Override
    public void schedule(String id, String category, Deadline deadline, Object data) {
        long millisToDeadline = deadline.toEpochMilli() - System.currentTimeMillis();
        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(
                () -> queue.addLast(new DeadlineData(id, category, deadline, data)),
                millisToDeadline, TimeUnit.MILLISECONDS
        );
        scheduledDeadlines.put(id, scheduledFuture);
    }

    @Override
    public void cancel(String id) {
        ScheduledFuture<?> scheduledFuture = scheduledDeadlines.remove(id);
        if (scheduledFuture == null) {
            return;
        }
        scheduledFuture.cancel(true);
    }

    public void shutdown() {
        boolean interrupted = false;
        try {
            long remainingNanos = TimeUnit.SECONDS.toNanos(2);
            long end = System.nanoTime() + remainingNanos;
            scheduledExecutorService.shutdown();

            while (true) {
                try {
                    if (!scheduledExecutorService.awaitTermination(remainingNanos, NANOSECONDS)) {
                        scheduledExecutorService.shutdownNow();
                    }
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                    scheduledExecutorService.shutdownNow();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}