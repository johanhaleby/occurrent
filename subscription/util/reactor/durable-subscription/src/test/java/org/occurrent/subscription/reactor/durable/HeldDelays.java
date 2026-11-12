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

package org.occurrent.subscription.reactor.durable;

import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The parallel scheduler of Reactor, which {@code Mono.delay} runs on, with each task it is asked to run after a
 * delay held until {@link #letGo()} runs it on the calling thread. A held task that is disposed never runs.
 */
final class HeldDelays implements Schedulers.Factory {
    private final List<HeldDelay> held = new CopyOnWriteArrayList<>();
    private final List<Duration> asked = new CopyOnWriteArrayList<>();

    @Override
    public Scheduler newParallel(int parallelism, ThreadFactory threadFactory) {
        return new Holding(Schedulers.Factory.super.newParallel(parallelism, threadFactory));
    }

    int waiting() {
        return (int) held.stream().filter(delay -> !delay.isDisposed()).count();
    }

    // The delay each held task was asked to wait, in the order they were asked for
    List<Duration> asked() {
        return List.copyOf(asked);
    }

    // Runs each task waiting now. One that such a task schedules waits for the next call
    void letGo() {
        for (HeldDelay delay : List.copyOf(held)) {
            if (delay.state.compareAndSet(HeldDelay.WAITING, HeldDelay.RAN)) {
                delay.task.run();
            }
        }
    }

    private final class Holding implements Scheduler {
        private final Scheduler scheduler;

        private Holding(Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public Disposable schedule(Runnable task) {
            return scheduler.schedule(task);
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            if (delay <= 0) {
                return scheduler.schedule(task);
            }
            HeldDelay heldDelay = new HeldDelay(task);
            asked.add(Duration.ofNanos(unit.toNanos(delay)));
            held.add(heldDelay);
            return heldDelay;
        }

        @Override
        public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            return scheduler.schedulePeriodically(task, initialDelay, period, unit);
        }

        @Override
        public Worker createWorker() {
            return scheduler.createWorker();
        }

        @Override
        public void init() {
            scheduler.init();
        }

        @Override
        public void dispose() {
            scheduler.dispose();
        }

        @Override
        public boolean isDisposed() {
            return scheduler.isDisposed();
        }
    }

    private static final class HeldDelay implements Disposable {
        private static final int WAITING = 0;
        private static final int RAN = 1;
        private static final int DISPOSED = 2;
        private final Runnable task;
        private final AtomicInteger state = new AtomicInteger(WAITING);

        private HeldDelay(Runnable task) {
            this.task = task;
        }

        @Override
        public void dispose() {
            state.compareAndSet(WAITING, DISPOSED);
        }

        @Override
        public boolean isDisposed() {
            return state.get() != WAITING;
        }
    }
}
