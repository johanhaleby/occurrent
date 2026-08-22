/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.retry.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.AfterRetryInfo.ResultOfRetryAttempt;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.MaxAttempts;
import org.occurrent.retry.RetryInfo;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.DontRetry;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Internal class for executing functions with retry capability. Never use this class directly from your own code!
 */
public class RetryExecution {

    /**
     * Upper bound, in milliseconds, on how long a backoff sleep runs before the shutdown predicate is re-checked.
     * A shutdown signaled through the predicate is therefore observed within this many milliseconds of being
     * raised, no matter how long the remaining backoff is, rather than only after the full backoff has elapsed.
     */
    private static final long SHUTDOWN_POLL_INTERVAL_MILLIS = 50;

    /**
     * Seam for injecting a fake backoff sleep in tests. Production code always uses {@link #DEFAULT_SLEEPER}.
     */
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private static final Sleeper DEFAULT_SLEEPER = TimeUnit.MILLISECONDS::sleep;

    private static RetryInfo firstAttemptRetryInfo() {
        return new RetryInfoImpl(1, 0, new MaxAttempts.Limit(1), Duration.ZERO);
    }

    public static <T1 extends @Nullable Object> Supplier<T1> executeWithRetry(@NonNull Supplier<T1> supplier, @NonNull Predicate<Throwable> shutdownPredicate, @NonNull RetryStrategy retryStrategy) {
        return () -> executeWithRetry((Function<RetryInfo, T1>) __ -> supplier.get(), shutdownPredicate, retryStrategy).apply(firstAttemptRetryInfo());
    }

    public static <T1 extends @Nullable Object> Function<RetryInfo, T1> executeWithRetry(@NonNull Function<RetryInfo, T1> function, @NonNull Predicate<Throwable> shutdownPredicate, @NonNull RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return function;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(function, retry, convertToDelayStream(retry.backoff), DEFAULT_SLEEPER);
    }

    public static Runnable executeWithRetry(Runnable runnable, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return runnable;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(runnable, retry, convertToDelayStream(retry.backoff), DEFAULT_SLEEPER);
    }

    public static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return fn;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(fn, retry, convertToDelayStream(retry.backoff), DEFAULT_SLEEPER);
    }

    /**
     * Test-only seam: same as the {@code Runnable} overload above but lets a test swap in a fake backoff sleep, so a
     * mid-sleep shutdown can be proven deterministically instead of racing a wall-clock sleep.
     */
    static Runnable executeWithRetry(Runnable runnable, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy, Sleeper sleeper) {
        if (retryStrategy instanceof DontRetry) {
            return runnable;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(runnable, retry, convertToDelayStream(retry.backoff), sleeper);
    }

    private static RetryImpl applyShutdownPredicate(Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        RetryImpl retry = (RetryImpl) retryStrategy;
        return retry.retryIf(shutdownPredicate.and(retry.retryPredicate));
    }

    private static Runnable executeWithRetry(Runnable runnable, RetryImpl retry, Iterator<Long> delay, Sleeper sleeper) {
        return () -> executeWithRetry(__ -> {
            runnable.run();
            return null;
        }, retry, delay, sleeper).apply(firstAttemptRetryInfo());
    }

    private static <T1> Consumer<T1> executeWithRetry(@NonNull Consumer<T1> fn, @NonNull RetryImpl retry, @NonNull Iterator<Long> delay, Sleeper sleeper) {
        return t1 -> executeWithRetry(retryInfo -> {
            fn.accept(t1);
            return null;
        }, retry, delay, sleeper).apply(firstAttemptRetryInfo());
    }

    private static <T1 extends @Nullable Object> Function<RetryInfo, T1> executeWithRetry(
            Function<RetryInfo, T1> fn,
            RetryImpl retry,
            Iterator<Long> delay,
            Sleeper sleeper
    ) {
        return (ignored) -> {
            int currentAttempt = 1;
            Throwable lastErr = null;
            Duration prevBackoff = Duration.ZERO;

            for (; ; ) {
                var nextRetryInfo = evolveRetryInfo(retry, delay, currentAttempt);
                var retryInfoWithPrevBackoff = nextRetryInfo.withBackoff(prevBackoff);
                boolean isRetryAttempt = lastErr != null;

                if (isRetryAttempt) {
                    retry.onBeforeRetryListener.accept(new BeforeRetryInfoImpl(retryInfoWithPrevBackoff), lastErr);
                }

                try {
                    T1 result = fn.apply(retryInfoWithPrevBackoff);
                    if (isRetryAttempt) {
                        retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPrevBackoff, new ResultOfRetryAttempt.Success(), null), lastErr);
                    }
                    return result;
                } catch (Throwable e) {
                    var currentBackoff = nextRetryInfo.getBackoff();
                    boolean shouldRetryAgain = !isExhausted(currentAttempt, retry.maxAttempts) && retry.retryPredicate.test(e);

                    retry.errorListener.accept(new ErrorInfoImpl(retryInfoWithPrevBackoff, shouldRetryAgain ? currentBackoff : null, shouldRetryAgain), e);

                    if (!shouldRetryAgain) {
                        if (isRetryAttempt) {
                            retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPrevBackoff, new ResultOfRetryAttempt.Failed(e), null), lastErr);
                        }
                        return SafeExceptionRethrower.safeRethrow(retry.errorMapper.apply(e));
                    }
                    retry.onRetryableErrorListener.accept(new RetryableErrorInfoImpl(retryInfoWithPrevBackoff, currentBackoff), e);

                    if (isRetryAttempt) {
                        retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPrevBackoff, new ResultOfRetryAttempt.Failed(e), currentBackoff), lastErr);
                    }

                    long backoffMillis = currentBackoff.toMillis();
                    if (backoffMillis > 0 && !sleepObservingShutdown(sleeper, backoffMillis, retry.retryPredicate, e)) {
                        // Shutdown was observed partway through the backoff: stop now, the same way exhaustion or a
                        // non-retryable error would, instead of sleeping out the rest of a backoff nobody wants.
                        if (isRetryAttempt) {
                            retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPrevBackoff, new ResultOfRetryAttempt.Failed(e), null), lastErr);
                        }
                        return SafeExceptionRethrower.safeRethrow(retry.errorMapper.apply(e));
                    }

                    // advance state and continue
                    currentAttempt++;
                    lastErr = e;
                    prevBackoff = currentBackoff;
                }
            }
        };
    }

    /**
     * Sleeps up to {@code totalMillis}, polling {@code shutdownPredicate} every {@link #SHUTDOWN_POLL_INTERVAL_MILLIS}
     * so a shutdown signaled during the sleep is observed within that bound instead of only after the full backoff
     * has elapsed. Returns {@code false} the moment shutdown is observed, leaving any remaining backoff unslept, or
     * {@code true} once the full duration has elapsed without shutdown being observed.
     * <p>
     * An interrupted sleep restores the thread's interrupt status before rethrowing, so a caller's own interrupt
     * handling (e.g. an executor shutting down its worker threads) is preserved rather than swallowed here.
     */
    private static boolean sleepObservingShutdown(Sleeper sleeper, long totalMillis, Predicate<Throwable> shutdownPredicate, Throwable lastError) {
        long remainingMillis = totalMillis;
        while (remainingMillis > 0) {
            long chunkMillis = Math.min(SHUTDOWN_POLL_INTERVAL_MILLIS, remainingMillis);
            try {
                sleeper.sleep(chunkMillis);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(lastError);
            }
            remainingMillis -= chunkMillis;
            if (!shutdownPredicate.test(lastError)) {
                return false;
            }
        }
        return true;
    }

    private static RetryInfoImpl evolveRetryInfo(RetryImpl retry, Iterator<Long> delay, int attempt) {
        long backoffMillis = delay.next();
        Duration backoffDuration = backoffMillis == 0 ? Duration.ZERO : Duration.ofMillis(backoffMillis);
        return new RetryInfoImpl(attempt, attempt - 1, retry.maxAttempts, backoffDuration);
    }

    private static boolean isExhausted(int attempt, MaxAttempts maxAttempts) {
        if (maxAttempts instanceof MaxAttempts.Infinite) {
            return false;
        }
        return attempt >= ((MaxAttempts.Limit) maxAttempts).limit();
    }

    private static Iterator<Long> convertToDelayStream(Backoff backoff) {
        final Stream<Long> delay;
        if (backoff instanceof Backoff.None) {
            delay = Stream.iterate(0L, __ -> 0L);
        } else if (backoff instanceof Backoff.Fixed) {
            long millis = ((Backoff.Fixed) backoff).millis;
            delay = Stream.iterate(millis, __ -> millis);
        } else if (backoff instanceof Backoff.Exponential strategy) {
            long initialMillis = strategy.initial.toMillis();
            long maxMillis = strategy.max.toMillis();
            double multiplier = strategy.multiplier;
            delay = Stream.iterate(initialMillis, current -> Math.min(maxMillis, Math.round(current * multiplier)));
        } else {
            throw new IllegalStateException("Invalid retry strategy: " + backoff.getClass().getName());
        }
        return delay.iterator();
    }
}
