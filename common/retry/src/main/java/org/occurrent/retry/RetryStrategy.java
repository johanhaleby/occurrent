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
public sealed interface RetryStrategy {
    /**
     * Create a retry strategy that performs retries if exceptions are caught.
     *
     * @return {@link Retry}
     * @see Retry
     */
    static Retry retry() {
        return new Retry();
    }

    /**
     * Create a retry strategy that doesn't perform retries (i.e. retries are disabled).
     *
     * @return {@link DontRetry}
     * @see DontRetry
     */
    static DontRetry none() {
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
    static Retry exponentialBackoff(Duration initial, Duration max, double multiplier) {
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
    static Retry fixed(Duration duration) {
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
    static Retry fixed(long millis) {
        return RetryStrategy.retry().backoff(Backoff.fixed(millis));
    }

    /**
     * Execute a {@link Supplier} with the configured retry settings.
     * Rethrows the exception from the supplier if retry strategy is exhausted.
     *
     * @param function A function that takes {@link RetryInfo} and returns the result
     * @return The result of the supplier, if successful.
     */
    default <T> T execute(Function<RetryInfo, T> function) {
        Objects.requireNonNull(function, Supplier.class.getSimpleName() + " cannot be null");
        return executeWithRetry(function, __ -> true, this).apply(null);
    }

    /**
     * Execute a {@link Supplier} with the configured retry settings.
     * Rethrows the exception from the supplier if retry strategy is exhausted.
     *
     * @param supplier The supplier to execute
     * @return The result of the supplier, if successful.
     */
    default <T> T execute(Supplier<T> supplier) {
        Objects.requireNonNull(supplier, Supplier.class.getSimpleName() + " cannot be null");
        return executeWithRetry(supplier, __ -> true, this).get();
    }

    /**
     * Execute a {@link Runnable} with the configured retry settings.
     * Rethrows the exception from the runnable if retry strategy is exhausted.
     *
     * @param runnable The runnable to execute
     */
    default void execute(Runnable runnable) {
        Objects.requireNonNull(runnable, Runnable.class.getSimpleName() + " cannot be null");
        executeWithRetry(runnable, __ -> true, this).run();
    }

    /**
     * A retry strategy that doesn't retry at all. Just rethrows the exception.
     */
    final class DontRetry implements RetryStrategy {
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
    final class Retry implements RetryStrategy {
        // @formatter:off
        private static final Consumer<Throwable> NOOP_ERROR_LISTENER = (__) -> {};
        private static final BiConsumer<BeforeRetryInfo, Throwable> NOOP_BEFORE_RETRY_LISTENER = (__, ___) -> {};
        private static final BiConsumer<AfterRetryInfo, Throwable> NOOP_AFTER_RETRY_LISTENER = (__, ___) -> {};
    // @formatter:on

        public final Backoff backoff;
        public final MaxAttempts maxAttempts;
        public final Predicate<Throwable> retryPredicate;
        public final Consumer<Throwable> errorListener;
        public final BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener;
        public final BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener;

        public final Function<Throwable, Throwable> errorMapper;

        private Retry(Backoff backoff, MaxAttempts maxAttempts, Function<Throwable, Throwable> errorMapper, Predicate<Throwable> retryPredicate, Consumer<Throwable> errorListener,
                      BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener, BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener) {
            Objects.requireNonNull(backoff, Backoff.class.getSimpleName() + " cannot be null");
            Objects.requireNonNull(maxAttempts, MaxAttempts.class.getSimpleName() + " cannot be null");
            Objects.requireNonNull(retryPredicate, "Retry predicate cannot be null");
            Objects.requireNonNull(errorMapper, "Error mapper cannot be null");
            this.backoff = backoff;
            this.maxAttempts = maxAttempts;
            this.retryPredicate = retryPredicate;
            this.errorMapper = errorMapper;
            this.errorListener = errorListener == null ? NOOP_ERROR_LISTENER : errorListener;
            this.onBeforeRetryListener = onBeforeRetryListener == null ? NOOP_BEFORE_RETRY_LISTENER : onBeforeRetryListener;
            this.onAfterRetryListener = onAfterRetryListener == null ? NOOP_AFTER_RETRY_LISTENER : onAfterRetryListener;
        }

        private Retry() {
            this(Backoff.none(), infinite(), Function.identity(), __ -> true, NOOP_ERROR_LISTENER, NOOP_BEFORE_RETRY_LISTENER, NOOP_AFTER_RETRY_LISTENER);
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
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Retry an infinite number of times (this is default).
         *
         * @return A new instance of {@link Retry} with infinite number of retry attempts.
         * @see #maxAttempts(int)
         */
        public Retry infiniteAttempts() {
            return new Retry(backoff, infinite(), errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Specify the max number of attempts the runnable/supplier should be invoked before failing.
         *
         * @return A new instance of {@link Retry} with the max number of attempts configured.
         */
        public Retry maxAttempts(int maxAttempts) {
            return new Retry(backoff, new MaxAttempts.Limit(maxAttempts), errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Only retry if the specified predicate is {@code true}. Will override previous retry predicate.
         *
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        public Retry retryIf(Predicate<Throwable> retryPredicate) {
            Objects.requireNonNull(retryPredicate, "Retry predicate cannot be null");
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Allows you to specify a retry predicate by basing it on the current retry predicate.
         *
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        public Retry mapRetryPredicate(Function<Predicate<Throwable>, Predicate<Throwable>> retryPredicateFn) {
            Objects.requireNonNull(retryPredicateFn, "Retry predicate function cannot be null");
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicateFn.apply(retryPredicate), errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Maps any thrown throwable to another throwable
         *
         * @param errorMapper The function that maps throwable to another throwable
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        public Retry mapError(Function<Throwable, Throwable> errorMapper) {
            Objects.requireNonNull(errorMapper, "Mapping function cannot be null");
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Maps a throwable that is an <i>instance of</i> {@code E}.
         *
         * @param <E>    The type of throwable that should trigger the {@code mapper} function
         * @param type   The type of the throwable
         * @param mapper The mapper function that will be invoked if an exception is thrown by an {@code execute} function that is instance of {@code type}
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        @SuppressWarnings("unchecked")
        public <E extends Throwable> Retry mapError(Class<E> type, Function<? super E, ? extends Throwable> mapper) {
            Objects.requireNonNull(type, "Exception type cannot be null");
            Objects.requireNonNull(mapper, "Mapper function cannot be null");
            Function<Throwable, Throwable> matchingError = errorMapper.andThen(e -> {
                if (type.isAssignableFrom(e.getClass())) {
                    return mapper.apply((E) e);
                } else {
                    return e;
                }
            });
            return new Retry(backoff, maxAttempts, errorMapper.andThen(matchingError), retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * If the {@code RetryStrategy} records an error (exception) even after all retry attempts have been exhausted,
         * then the supplied {@code errorListener} will be invoked before the exception is thrown.
         *
         * @param errorListener The consumer to invoke
         * @return A new instance of {@link Retry} with the given error listener
         */
        public Retry onError(Consumer<Throwable> errorListener) {
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Specify a bi-consumer that accepts retry information ({@link RetryInfo}) as well the exception that caused the {@code RetryStrategy} to retry.
         * The bi-consumer will be invoked <i>before</i> the retry takes place.
         *
         * @param onBeforeRetryListener The bi-consumer to invoke
         * @return A new instance of {@link Retry} with the given onBeforeRetryListener
         */
        public Retry onBeforeRetry(BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener) {
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        /**
         * Specify a bi-consumer that accepts retry information ({@link RetryInfo}) as well the exception that caused the {@code RetryStrategy} to retry.
         * The bi-consumer will be invoked <i>after</i> the retry takes place.
         *
         * @param onAfterRetryListener The bi-consumer to invoke
         * @return A new instance of {@link Retry} with the given onAfterRetryListener
         */
        public Retry onAfterRetry(BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener) {
            return new Retry(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Retry retry)) return false;
            return Objects.equals(backoff, retry.backoff) && Objects.equals(maxAttempts, retry.maxAttempts) && Objects.equals(retryPredicate, retry.retryPredicate) && Objects.equals(errorListener, retry.errorListener) && Objects.equals(errorMapper, retry.errorMapper);
        }

        @Override
        public int hashCode() {
            return Objects.hash(backoff, maxAttempts, retryPredicate, errorListener, errorMapper);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Retry.class.getSimpleName() + "[", "]")
                    .add("backoff=" + backoff)
                    .add("maxAttempts=" + maxAttempts)
                    .add("retryPredicate=" + retryPredicate)
                    .add("errorListener=" + errorListener)
                    .add("errorMapper=" + errorMapper)
                    .toString();
        }
    }
}