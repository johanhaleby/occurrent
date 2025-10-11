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

    public static <T1> Supplier<T1> executeWithRetry(@NonNull Supplier<T1> supplier, @NonNull Predicate<Throwable> shutdownPredicate, @NonNull RetryStrategy retryStrategy) {
        return () -> executeWithRetry((Function<RetryInfo, T1>) __ -> supplier.get(), shutdownPredicate, retryStrategy).apply(null);
    }

    public static <T1> Function<RetryInfo, T1> executeWithRetry(@NonNull Function<RetryInfo, T1> function, @NonNull Predicate<Throwable> shutdownPredicate, @NonNull RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return function;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(function, retry, convertToDelayStream(retry.backoff));
    }

    public static Runnable executeWithRetry(Runnable runnable, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return runnable;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(runnable, retry, convertToDelayStream(retry.backoff));
    }

    public static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return fn;
        }
        RetryImpl retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(fn, retry, convertToDelayStream(retry.backoff));
    }

    private static RetryImpl applyShutdownPredicate(Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        RetryImpl retry = (RetryImpl) retryStrategy;
        return retry.retryIf(shutdownPredicate.and(retry.retryPredicate));
    }

    private static Runnable executeWithRetry(Runnable runnable, RetryImpl retry, Iterator<Long> delay) {
        Consumer<Void> runnableConsumer = __ -> runnable.run();
        return () -> executeWithRetry(runnableConsumer, retry, delay).accept(null);
    }

    private static <T1> Consumer<T1> executeWithRetry(@NonNull Consumer<T1> fn, @NonNull RetryImpl retry, @NonNull Iterator<Long> delay) {
        return t1 -> executeWithRetry(retryInfo -> {
            fn.accept(t1);
            return null;
        }, retry, delay).apply(null);
    }

    private static <T1> Function<RetryInfo, T1> executeWithRetry(
            Function<RetryInfo, T1> fn,
            RetryImpl retry,
            Iterator<Long> delay
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
                    if (backoffMillis > 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(backoffMillis);
                        } catch (InterruptedException ie) {
                            throw new RuntimeException(e);
                        }
                    }

                    // advance state and continue
                    currentAttempt++;
                    lastErr = e;
                    prevBackoff = currentBackoff;
                }
            }
        };
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