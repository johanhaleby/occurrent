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

package org.occurrent.tck;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.IntFunction;

import static java.util.Objects.requireNonNull;

/**
 * Drives N tasks into a genuine collision on a {@link CyclicBarrier} and reports what each one did, rather than what
 * a shared mutable field last happened to see.
 * <p>
 * This exists because the five macOS-only tests it replaces (issue 467) wrote their outcome into a single shared
 * {@code AtomicReference}, so whichever thread wrote last decided what the test saw. Two threads racing is exactly
 * the situation in which "last write" is itself a race, so that field was never trustworthy. Each task's outcome
 * here is returned as a value, in the order the tasks were submitted, so a suite can count winners and losers
 * instead of inspecting shared state that the race itself corrupts.
 * <p>
 * Every wait here has a timeout, and {@linkplain #collide every task is joined before this method returns or
 * throws}, so a hung task cannot leave a sibling still running in the background after the assertion that was
 * supposed to observe it has already happened. That ordering is precisely what was fixed in
 * {@code MongoEventStoreDcbConcurrencyTest} shortly before this class was written. The same bug is easy to
 * reintroduce by returning as soon as one task's result is known.
 */
@NullMarked
public final class ConcurrentRendezvous {

    /**
     * How long a task may wait at the barrier for its siblings. Generous relative to submitting a handful of tasks to
     * a thread pool, so a wedged task is reported by this wait's own timeout rather than by a test's
     * {@code @Timeout} killing the whole run without saying why.
     */
    public static final Duration DEFAULT_BARRIER_TIMEOUT = Duration.ofSeconds(10);

    /**
     * How long this class waits for one task to finish once released from the barrier.
     */
    public static final Duration DEFAULT_TASK_TIMEOUT = Duration.ofSeconds(20);

    private ConcurrentRendezvous() {
    }

    /**
     * Runs {@code taskCount} tasks, each built by {@code taskFactory} from its index, on their own threads. Every
     * task waits at a shared barrier before doing anything else, so all of them are released together and collide on
     * whatever they do next.
     * <p>
     * Every task is joined, with {@link #DEFAULT_TASK_TIMEOUT}, before this method returns, and the pool is only
     * {@linkplain ExecutorService#shutdownNow() interrupted} afterwards. An {@link ExecutionException} is unwrapped so
     * the returned {@link Outcome} carries the task's own exception, not the executor's wrapper.
     *
     * @return one {@link Outcome} per task, in submission order (task 0 first)
     */
    public static <T> List<Outcome<T>> collide(int taskCount, IntFunction<Callable<T>> taskFactory) {
        return collide(taskCount, DEFAULT_BARRIER_TIMEOUT, DEFAULT_TASK_TIMEOUT, taskFactory);
    }

    /**
     * As {@link #collide(int, IntFunction)}, with explicit bounds on the barrier wait and the per-task join.
     */
    public static <T> List<Outcome<T>> collide(int taskCount, Duration barrierTimeout, Duration taskTimeout,
                                                IntFunction<Callable<T>> taskFactory) {
        if (taskCount < 2) {
            throw new IllegalArgumentException("taskCount must be at least 2 to collide, was " + taskCount);
        }
        requireNonNull(barrierTimeout, "barrierTimeout cannot be null");
        requireNonNull(taskTimeout, "taskTimeout cannot be null");
        requireNonNull(taskFactory, "taskFactory cannot be null");

        CyclicBarrier barrier = new CyclicBarrier(taskCount);
        ExecutorService pool = Executors.newFixedThreadPool(taskCount);
        try {
            List<Future<T>> futures = new ArrayList<>(taskCount);
            for (int i = 0; i < taskCount; i++) {
                Callable<T> task = requireNonNull(taskFactory.apply(i), "taskFactory returned null for index " + i);
                futures.add(pool.submit(() -> {
                    barrier.await(barrierTimeout.toMillis(), TimeUnit.MILLISECONDS);
                    return task.call();
                }));
            }

            // Every future is joined here, one at a time, before this method can return or throw. A task that threw
            // cannot short-circuit the join of the tasks after it, which is the exact bug this class exists to avoid.
            List<Outcome<T>> outcomes = new ArrayList<>(taskCount);
            for (Future<T> future : futures) {
                outcomes.add(join(future, taskTimeout));
            }
            return outcomes;
        } finally {
            // Only interrupted after every join above has already been attempted, so shutdownNow() can never cut off
            // a task this method is still waiting to observe.
            pool.shutdownNow();
        }
    }

    private static <T> Outcome<T> join(Future<T> future, Duration timeout) {
        try {
            return Outcome.success(future.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            return Outcome.failure(cause != null ? cause : e);
        } catch (Exception e) {
            // TimeoutException, InterruptedException, CancellationException: reported as the task's outcome rather
            // than escaping, so one wedged task still lets every other task's join in the same collide(..) call run.
            return Outcome.failure(e);
        }
    }

    /**
     * What one task did, either the value it returned or the throwable it raised. Exactly one of {@link #value()} or
     * {@link #failure()} is non-null, which is why both are exposed as accessors rather than as a class a caller
     * would need to instanceof against.
     */
    public static final class Outcome<T> {

        private final @Nullable T value;
        private final @Nullable Throwable failure;

        private Outcome(@Nullable T value, @Nullable Throwable failure) {
            this.value = value;
            this.failure = failure;
        }

        private static <T> Outcome<T> success(T value) {
            return new Outcome<>(value, null);
        }

        private static <T> Outcome<T> failure(Throwable failure) {
            return new Outcome<>(null, failure);
        }

        public boolean succeeded() {
            return failure == null;
        }

        /**
         * The task's return value. Only call this after checking {@link #succeeded()}.
         */
        public T value() {
            if (failure != null) {
                throw new IllegalStateException("This task failed, so it has no value", failure);
            }
            @SuppressWarnings("nullness") // succeeded() being true is exactly the invariant that value is non-null
            T nonNullValue = value;
            return nonNullValue;
        }

        /**
         * The throwable the task raised. Only call this after checking that {@link #succeeded()} is {@code false}.
         */
        public Throwable failure() {
            if (failure == null) {
                throw new IllegalStateException("This task succeeded, so it has no failure");
            }
            return failure;
        }
    }
}
