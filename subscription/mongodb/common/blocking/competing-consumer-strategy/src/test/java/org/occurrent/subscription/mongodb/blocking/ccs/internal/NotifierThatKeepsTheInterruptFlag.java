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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A notifier that runs every task on one thread and never clears that thread's interrupt flag between two tasks, which
 * a {@link java.util.concurrent.ThreadPoolExecutor} does. It waits for the next task by parking, since a blocking
 * queue would throw on a thread whose flag is set. Once {@link #awaitEveryTask()} returns, a test can assert on
 * everything the tasks delivered.
 */
class NotifierThatKeepsTheInterruptFlag extends AbstractExecutorService {
    private static final Duration TASK_TIMEOUT = Duration.ofSeconds(10);

    private final AtomicInteger unfinished = new AtomicInteger();
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final Thread thread = Thread.ofPlatform().daemon().unstarted(this::runTasks);
    private volatile boolean shutdown;

    NotifierThatKeepsTheInterruptFlag() {
        thread.start();
    }

    private void runTasks() {
        while (!shutdown) {
            Runnable task = tasks.poll();
            if (task == null) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            } else {
                try {
                    task.run();
                } finally {
                    unfinished.decrementAndGet();
                }
            }
        }
    }

    // Also waits for a task handed over while it waits
    void awaitEveryTask() throws InterruptedException {
        long deadline = System.nanoTime() + TASK_TIMEOUT.toNanos();
        while (unfinished.get() > 0) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("A task of the notifier still runs after " + TASK_TIMEOUT);
            }
            Thread.sleep(1);
        }
    }

    @Override
    public void execute(Runnable task) {
        unfinished.incrementAndGet();
        tasks.add(task);
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return !thread.isAlive();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        thread.join(unit.toMillis(timeout));
        return !thread.isAlive();
    }
}
