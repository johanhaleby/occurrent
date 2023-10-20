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

import org.occurrent.retry.AfterRetryInfo.ResultOfRetryAttempt;
import org.occurrent.retry.Backoff;
import org.occurrent.retry.MaxAttempts;
import org.occurrent.retry.RetryInfo;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.DontRetry;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Internal class for executing functions with retry capability. Never use this class directly from your own code!
 */
public class RetryExecution {

    public static <T1> Supplier<T1> executeWithRetry(Supplier<T1> supplier, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        return () -> executeWithRetry((Function<RetryInfo, T1>) __ -> supplier.get(), shutdownPredicate, retryStrategy).apply(null);
    }

    public static <T1> Function<RetryInfo, T1> executeWithRetry(Function<RetryInfo, T1> function, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return function;
        }
        Retry retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(function, retry, convertToDelayStream(retry.backoff), 1, null, Duration.ZERO);
    }

    public static Runnable executeWithRetry(Runnable runnable, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return runnable;
        }
        Retry retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(runnable, retry, convertToDelayStream(retry.backoff));
    }

    public static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return fn;
        }
        Retry retry = applyShutdownPredicate(shutdownPredicate, retryStrategy);
        return executeWithRetry(fn, retry, convertToDelayStream(retry.backoff));
    }

    private static Retry applyShutdownPredicate(Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        Retry retry = (Retry) retryStrategy;
        return retry.retryIf(shutdownPredicate.and(retry.retryPredicate));
    }

    private static Runnable executeWithRetry(Runnable runnable, Retry retry, Iterator<Long> delay) {
        Consumer<Void> runnableConsumer = __ -> runnable.run();
        return () -> executeWithRetry(runnableConsumer, retry, delay).accept(null);
    }

    private static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Retry retry, Iterator<Long> delay) {
        return t1 -> executeWithRetry(retryInfo -> {
            fn.accept(t1);
            return null;
        }, retry, delay, 1, null, Duration.ZERO).apply(null);
    }

    private static <T1> Function<RetryInfo, T1> executeWithRetry(Function<RetryInfo, T1> fn, Retry retry, Iterator<Long> delay, int attempt, Throwable lastError, Duration previousBackoff) {
        return (RetryInfo) -> {
            var nextRetryInfo = newRetryInfo(retry, delay, attempt);
            var retryInfoWithPreviousBackoff = nextRetryInfo.withBackoff(previousBackoff);
            boolean currentAttemptIsARetryAttempt = lastError != null;

            if (currentAttemptIsARetryAttempt) {
                retry.onBeforeRetryListener.accept(new BeforeRetryInfoImpl(retryInfoWithPreviousBackoff), lastError);
            }

            try {
                T1 result = fn.apply(retryInfoWithPreviousBackoff);
                if (currentAttemptIsARetryAttempt) {
                    retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPreviousBackoff, new ResultOfRetryAttempt.Success(), null), lastError);
                }
                return result;
            } catch (Throwable e) {
                Duration currentBackoff = nextRetryInfo.getBackoff();
                boolean shouldRetryAgain = !isExhausted(attempt, retry.maxAttempts) && retry.retryPredicate.test(e);
                if (shouldRetryAgain) {
                    if (currentAttemptIsARetryAttempt) {
                        retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPreviousBackoff, new ResultOfRetryAttempt.Failed(e), currentBackoff), lastError);
                    }
                    try {
                        long backoffMillis = ((RetryInfo) nextRetryInfo).getBackoff().toMillis();
                        if (backoffMillis > 0) {
                            Thread.sleep(backoffMillis);
                        }
                    } catch (InterruptedException interruptedException) {
                        throw new RuntimeException(e);
                    }
                    return executeWithRetry(fn, retry, delay, attempt + 1, e, currentBackoff).apply(nextRetryInfo);
                } else {
                    if (currentAttemptIsARetryAttempt) {
                        retry.onAfterRetryListener.accept(new AfterRetryInfoImpl(retryInfoWithPreviousBackoff, new ResultOfRetryAttempt.Failed(e), null), lastError);
                    }
                    retry.errorListener.accept(e);
                    return SafeExceptionRethrower.safeRethrow(retry.errorMapper.apply(e));
                }
            }
        };
    }

    private static RetryInfoImpl newRetryInfo(Retry retry, Iterator<Long> delay, int attempt) {
        Long backoffMillis = delay.next();
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