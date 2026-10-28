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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A notifier that runs each task on a thread of its own, and that throws from {@code execute} for as long as it was
 * told to, as a cached thread pool throws an {@link OutOfMemoryError} when it can't start a thread. Once
 * {@link #awaitEveryTask()} returns, a test can assert on everything the tasks delivered.
 */
class NotifierThatThrowsWhenTold extends AbstractExecutorService {
    private static final Duration TASK_TIMEOUT = Duration.ofSeconds(10);

    private final List<Thread> tasks = new CopyOnWriteArrayList<>();
    private volatile Error thrown;

    void throwOnEveryTask(Error thrown) {
        this.thrown = thrown;
    }

    void takeEveryTask() {
        this.thrown = null;
    }

    // Also waits for a task started while it waits
    void awaitEveryTask() throws InterruptedException {
        for (int i = 0; i < tasks.size(); i++) {
            if (!tasks.get(i).join(TASK_TIMEOUT)) {
                throw new IllegalStateException("A task of the notifier still runs after " + TASK_TIMEOUT);
            }
        }
    }

    @Override
    public void execute(Runnable task) {
        Error toThrow = thrown;
        if (toThrow != null) {
            throw toThrow;
        }
        Thread thread = Thread.ofPlatform().daemon().unstarted(task);
        tasks.add(thread);
        thread.start();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }
}
