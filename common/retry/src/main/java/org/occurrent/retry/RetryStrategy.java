/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.*;

import static org.occurrent.retry.MaxAttempts.Infinite.infinite;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * Retry strategy to use if the action throws an exception.
 * <p>
 * A {@code RetryStrategy} is thread-safe and immutable, so you can change the settings at any time without impacting the original instance.
 * For example this is perfectly valid:
 * <p>
 * <pre>
 * RetryStrategy retryStrategy = RetryStrategy.retry().fixed(200).maxAttempts(5);
 * // 200 ms fixed delay
 * retryStrategy.execute(() -> Something.something());
 * // 600 ms fixed delay
 * retryStrategy.backoff(fixed(600)).execute(() -> SomethingElse.somethingElse());
 * // 200 ms fixed delay
 * retryStrategy.execute(() -> Thing.thing());
 * </pre>
 * </p>
 */
public abstract class RetryStrategy {

    private RetryStrategy() {
    }

    private static final Predicate<Throwable> ALWAYS_RETRY = __ -> true;

    /**
     * Create a retry strategy that performs retries if exceptions are caught.
     *
     * @return {@link Retry}
     * @see Retry
     */
    public static Retry retry() {
        return new Retry();
    }

    /**
     * Create a retry strategy that doesn't perform retries (i.e. retries are disabled).
     *
     * @return {@link DontRetry}
     * @see DontRetry
     */
    public static DontRetry none() {
        return DontRetry.INSTANCE;
    }

    /**
     * Shortcut to create a retry strategy with exponential backoff. This is the same as doing:
     *
     * <pre>
     * RetryStrategy.retry().backoff(Backoff.exponential(..));
     * </pre>
     *
     * @param initial    The initial wait time before retrying the first time
     * @param max        Max wait time
     * @param multiplier Multiplier between retries
     * @return A retry strategy with exponential backoff
     */
    public static Retry exponentialBackoff(Duration initial, Duration max, double multiplier) {
        return RetryStrategy.retry().backoff(Backoff.exponential(initial, max, multiplier));
    }

    /**
     * Shortcut to create a retry strategy with fixed backoff. This is the same as doing:
     *
     * <pre>
     * RetryStrategy.retry().backoff(Backoff.fixed(..));
     * </pre>
     *
     * @param duration The duration to wait before retry
     * @return A retry strategy with fixed backoff
     */
    public static Retry fixed(Duration duration) {
        return RetryStrategy.retry().backoff(Backoff.fixed(duration));
    }

    /**
     * Shortcut to create a retry strategy with fixed backoff. This is the same as doing:
     *
     * <pre>
     * RetryStrategy.retry().backoff(Backoff.fixed(..));
     * </pre>
     *
     * @param millis The number of millis to wait before retry
     * @return A retry strategy with fixed backoff
     */
    public static Retry fixed(long millis) {
        return RetryStrategy.retry().backoff(Backoff.fixed(millis));
    }

    /**
     * Execute a {@link Supplier} with the configured retry settings.
     * Rethrows the exception from the supplier if retry strategy is exhausted.
     *
     * @param supplier The supplier to execute
     * @return The result of the supplier, if successful.
     */
    public <T> T execute(Supplier<T> supplier) {
        Objects.requireNonNull(supplier, Supplier.class.getSimpleName() + " cannot be null");
        return executeWithRetry(supplier, ALWAYS_RETRY, this).get();
    }

    /**
     * Execute a {@link Runnable} with the configured retry settings.
     * Rethrows the exception from the runnable if retry strategy is exhausted.
     *
     * @param runnable The runnable to execute
     */
    public void execute(Runnable runnable) {
        Objects.requireNonNull(runnable, Runnable.class.getSimpleName() + " cannot be null");
        executeWithRetry(runnable, ALWAYS_RETRY, this).run();
    }

    /**
     * A retry strategy that doesn't retry at all. Just rethrows the exception.
     */
    public static class DontRetry extends RetryStrategy {
        private static final DontRetry INSTANCE = new DontRetry();

        private DontRetry() {
        }

        @Override
        public String toString() {
            return DontRetry.class.getSimpleName();
        }
    }

    /**
     * A retry strategy that does retry. By default the following settings are used:
     *
     * <ul>
     *     <li>No backoff</li>
     *     <li>Infinite number of retries</li>
     *     <li>Retries all exceptions</li>
     *     <li>No error listener (will retry silently)</li>
     * </ul>
     */
    public static class Retry extends RetryStrategy {
        private static final BiConsumer<RetryInfo, Throwable> NOOP_ERROR_LISTENER = (__, ___) -> {
        };

        public final Backoff backoff;
        public final MaxAttempts maxAttempts;
        public final Predicate<Throwable> retryPredicate;
        public final BiConsumer<RetryInfo, Throwable> errorListener;

        private Retry(Backoff backoff, MaxAttempts maxAttempts, Predicate<Throwable> retryPredicate, BiConsumer<RetryInfo, Throwable> errorListener) {
            Objects.requireNonNull(backoff, Backoff.class.getSimpleName() + " cannot be null");
            Objects.requireNonNull(maxAttempts, MaxAttempts.class.getSimpleName() + " cannot be null");
            Objects.requireNonNull(retryPredicate, "Retry predicate cannot be null");
            this.backoff = backoff;
            this.maxAttempts = maxAttempts;
            this.retryPredicate = retryPredicate;
            this.errorListener = errorListener == null ? NOOP_ERROR_LISTENER : errorListener;
        }

        private Retry() {
            this(Backoff.none(), infinite(), ALWAYS_RETRY, NOOP_ERROR_LISTENER);
        }

        /**
         * Configure the backoff settings for the retry strategy.
         *
         * @param backoff The backoff to use.
         * @return A new instance of {@link Retry} with the backoff settings applied.
         * @see Backoff
         */
        public Retry backoff(Backoff backoff) {
            Objects.requireNonNull(backoff, Backoff.class.getSimpleName() + " cannot be null");
            return new Retry(backoff, maxAttempts, retryPredicate, errorListener);
        }

        /**
         * Retry an infinite number of times (this is default).
         *
         * @return A new instance of {@link Retry} with infinite number of retry attempts.
         * @see #maxAttempts(int)
         */
        public Retry infiniteAttempts() {
            return new Retry(backoff, infinite(), retryPredicate, errorListener);
        }

        /**
         * Specify the max number of attempts the runnable/supplier should be invoked before failing.
         *
         * @return A new instance of {@link Retry} with the max number of attempts configured.
         */
        public Retry maxAttempts(int maxAttempts) {
            return new Retry(backoff, new MaxAttempts.Limit(maxAttempts), retryPredicate, errorListener);
        }

        /**
         * Only retry if the specified predicate is {@code true}. Will override previous retry predicate.
         *
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        public Retry retryIf(Predicate<Throwable> retryPredicate) {
            Objects.requireNonNull(retryPredicate, "Retry predicate cannot be null");
            return new Retry(backoff, maxAttempts, retryPredicate, errorListener);
        }

        /**
         * Allows you specify a retry predicate by basing it on the current retry predicate.
         *
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        public Retry mapRetryPredicate(Function<Predicate<Throwable>, Predicate<Throwable>> retryPredicateFn) {
            Objects.requireNonNull(retryPredicateFn, "Retry predicate function cannot be null");
            return new Retry(backoff, maxAttempts, retryPredicateFn.apply(retryPredicate), errorListener);
        }

        /**
         * Only retry if the specified predicate is {@code true}.
         *
         * @return A new instance of {@link Retry} with the given error listener
         * @see #onError(Consumer)
         */
        public Retry onError(BiConsumer<RetryInfo, Throwable> errorListener) {
            return new Retry(backoff, maxAttempts, retryPredicate, errorListener);
        }

        /**
         * Only retry if the specified predicate is {@code true}. Also includes a {@link RetryInfo}
         * instance which contains useful information on the state of the retry.
         *
         * @return A new instance of {@link Retry} with the given error listener
         * @see #onError(BiConsumer)
         */
        public Retry onError(Consumer<Throwable> errorListener) {
            return onError((__, throwable) -> errorListener.accept(throwable));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Retry)) return false;
            Retry retry = (Retry) o;
            return Objects.equals(backoff, retry.backoff) && Objects.equals(maxAttempts, retry.maxAttempts) && Objects.equals(retryPredicate, retry.retryPredicate) && Objects.equals(errorListener, retry.errorListener);
        }

        @Override
        public int hashCode() {
            return Objects.hash(backoff, maxAttempts, retryPredicate, errorListener);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Retry.class.getSimpleName() + "[", "]")
                    .add("backoff=" + backoff)
                    .add("maxAttempts=" + maxAttempts)
                    .add("retryPredicate=" + retryPredicate)
                    .add("errorListener=" + errorListener)
                    .toString();
        }
    }
}