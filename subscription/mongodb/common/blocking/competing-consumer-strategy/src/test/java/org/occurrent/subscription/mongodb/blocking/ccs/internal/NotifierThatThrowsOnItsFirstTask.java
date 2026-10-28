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

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A notifier that throws {@code thrown} from the first {@code execute}, as a cached thread pool throws an
 * {@link OutOfMemoryError} when it can't start a thread, and runs every later task on the calling thread. A test can
 * then assert on what a refresh delivered as soon as the refresh returns.
 */
class NotifierThatThrowsOnItsFirstTask extends AbstractExecutorService {
    private final AtomicBoolean first = new AtomicBoolean(true);
    private final Error thrown;

    NotifierThatThrowsOnItsFirstTask(Error thrown) {
        this.thrown = thrown;
    }

    @Override
    public void execute(Runnable task) {
        if (first.getAndSet(false)) {
            throw thrown;
        }
        task.run();
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
