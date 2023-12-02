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

import org.occurrent.retry.internal.RetryImpl;

import java.time.Duration;
import java.util.Objects;
import java.util.function.*;

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
public interface RetryStrategy {
    /**
     * Create a retry strategy that performs retries if exceptions are caught.
     *
     * @return {@link RetryImpl}
     * @see RetryImpl
     */
    static Retry retry() {
        return new RetryImpl();
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

    interface Retry extends RetryStrategy {
        /**
         * Configure the backoff settings for the retry strategy.
         *
         * @param backoff The backoff to use.
         * @return A new instance of {@link Retry} with the backoff settings applied.
         * @see Backoff
         */
        Retry backoff(Backoff backoff);

        /**
         * Retry an infinite number of times (this is default).
         *
         * @return A new instance of {@link Retry} with infinite number of retry attempts.
         * @see #maxAttempts(int)
         */
        Retry infiniteAttempts();

        /**
         * Specify the max number of attempts the runnable/supplier should be invoked before failing.
         *
         * @return A new instance of {@link Retry} with the max number of attempts configured.
         */
        Retry maxAttempts(int maxAttempts);

        /**
         * Only retry if the specified predicate is {@code true}. Will override previous retry predicate.
         *
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        Retry retryIf(Predicate<Throwable> retryPredicate);

        /**
         * Allows you to specify a retry predicate by basing it on the current retry predicate.
         *
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        Retry mapRetryPredicate(Function<Predicate<Throwable>, Predicate<Throwable>> retryPredicateFn);

        /**
         * Maps any thrown throwable to another throwable
         *
         * @param errorMapper The function that maps throwable to another throwable
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        Retry mapError(Function<Throwable, Throwable> errorMapper);

        /**
         * Maps a throwable that is an <i>instance of</i> {@code E}.
         *
         * @param <E>    The type of throwable that should trigger the {@code mapper} function
         * @param type   The type of the throwable
         * @param mapper The mapper function that will be invoked if an exception is thrown by an {@code execute} function that is instance of {@code type}
         * @return A new instance of {@link Retry} with the given retry predicate
         */
        <E extends Throwable> Retry mapError(Class<E> type, Function<? super E, ? extends Throwable> mapper);

        /**
         * Add an error listener that will be invoked for every error (throwable) that happens during the execution.
         * You can use {@link ErrorInfo#isRetryable()} to check if the error matches what's specified by the {@link #retryIf(Predicate)},
         * or if number of retries have been exhausted.
         *
         * @param errorListener The consumer to invoke
         * @return A new instance of {@link Retry} with the given error listener
         */
        Retry onError(BiConsumer<ErrorInfo, Throwable> errorListener);

        /**
         * Add an error listener that will be invoked for every error (throwable) that happens during the execution.
         *
         * @param errorListener The consumer to invoke
         * @return A new instance of {@link Retry} with the given error listener
         * @see #onError(BiConsumer)
         */
        Retry onError(Consumer<Throwable> errorListener);

        /**
         * Add a so called retryable error listener that will be invoked for error (throwable's) that are retryable, i.e. those that match the {@code retry predicate}
         * defined in {@link #retryIf(Predicate)}. If such a predicate is not specified, it'll behave the same way as {@link #onError(Consumer)}.
         *
         * @param retryableErrorListener The consumer to invoke
         * @return A new instance of {@link Retry} with the given retryable error listener
         * @see #onError(BiConsumer)
         */
        Retry onRetryableError(Consumer<Throwable> retryableErrorListener);

        /**
         * Add a so called retryable error listener that will be invoked for error (throwable's) that are retryable, i.e. those that match the {@code retry predicate}
         * defined in {@link #retryIf(Predicate)}. If such a predicate is not specified, it'll behave the same way as {@link #onError(BiConsumer)}.
         *
         * @param retryableErrorListener The consumer to invoke
         * @return A new instance of {@link Retry} with the given retryable error listener
         * @see #onError(BiConsumer)
         */
        Retry onRetryableError(BiConsumer<RetryableErrorInfo, Throwable> retryableErrorListener);

        /**
         * Specify a listener that accepts retry information ({@link BeforeRetryInfo}) as well the exception that caused the {@code RetryStrategy} to retry.
         * The listener will be invoked <i>before</i> each retry takes place.
         *
         * @param onBeforeRetryListener The bi-consumer to invoke
         * @return A new instance of {@link Retry} with the given onBeforeRetryListener
         */
        Retry onBeforeRetry(BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener);

        /**
         * Specify a listener that will be invoked <i>before</i> each retry takes place.
         *
         * @param onBeforeRetryListener The bi-consumer to invoke
         * @return A new instance of {@link Retry} with the given onBeforeRetryListener
         * @see #onBeforeRetry(BiConsumer)
         */
        Retry onBeforeRetry(Consumer<Throwable> onBeforeRetryListener);

        /**
         * Specify a listener that accepts retry information ({@link AfterRetryInfo}) as well the exception that caused the {@code RetryStrategy} to retry.
         * The listener will be invoked <i>after</i> each retry attempt.
         *
         * @param onAfterRetryListener The bi-consumer to invoke
         * @return A new instance of {@link Retry} with the given onAfterRetryListener
         */
        Retry onAfterRetry(BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener);

        /**
         * Specify a listener that will be invoked <i>after</i> each retry attempt.
         *
         * @param onAfterRetryListener The consumer to invoke
         * @return A new instance of {@link Retry} with the given onAfterRetryListener
         * @see #onAfterRetry(BiConsumer)
         */
        Retry onAfterRetry(Consumer<Throwable> onAfterRetryListener);
    }
}