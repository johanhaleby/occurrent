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

import org.occurrent.retry.Backoff;
import org.occurrent.retry.MaxAttempts;
import org.occurrent.retry.RetryInfo;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.DontRetry;
import org.occurrent.retry.RetryStrategy.Retry;

import java.time.Duration;
import java.util.Iterator;
import java.util.Objects;
import java.util.StringJoiner;
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
        return executeWithRetry(function, retry, convertToDelayStream(retry.backoff), 1);
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
        return executeWithRetry(fn, retry, convertToDelayStream(retry.backoff), 1);
    }

    private static Retry applyShutdownPredicate(Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        Retry retry = (Retry) retryStrategy;
        return retry.retryIf(shutdownPredicate.and(retry.retryPredicate));
    }

    private static Runnable executeWithRetry(Runnable runnable, Retry retry, Iterator<Long> delay) {
        Consumer<Void> runnableConsumer = __ -> runnable.run();
        return () -> executeWithRetry(runnableConsumer, retry, delay, 1).accept(null);
    }

    private static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Retry retry, Iterator<Long> delay, int attempt) {
        return t1 -> {
            try {
                fn.accept(t1);
            } catch (Throwable e) {
                var retryInfo = newRetryInfo(retry, delay, attempt);
                if (handleError(retry, retryInfo, attempt, e)) {
                    retry.onBeforeRetryListener.accept(retryInfo.increaseAttemptsAndRetryCountByOne(), e);
                    executeWithRetry(fn, retry, delay, attempt + 1).accept(t1);
                } else {
                    retry.errorListener.accept(e);
                    //noinspection ResultOfMethodCallIgnored
                    SafeExceptionRethrower.safeRethrow(retry.errorMapper.apply(e));
                }
            }
        };
    }

    private static <T1> Function<RetryInfo, T1> executeWithRetry(Function<RetryInfo, T1> fn, Retry retry, Iterator<Long> delay, int attempt) {
        return (RetryInfo) -> {
            var retryInfo = newRetryInfo(retry, delay, attempt);
            try {
                return fn.apply(retryInfo);
            } catch (Throwable e) {
                if (handleError(retry, retryInfo, attempt, e)) {
                    retry.onBeforeRetryListener.accept(retryInfo.increaseAttemptsAndRetryCountByOne(), e);
                    return executeWithRetry(fn, retry, delay, attempt + 1).apply(retryInfo);
                } else {
                    retry.errorListener.accept(e);
                    return SafeExceptionRethrower.safeRethrow(retry.errorMapper.apply(e));
                }
            }
        };
    }

    private static RetryInfoImpl newRetryInfo(Retry retry, Iterator<Long> delay, int attempt) {
        Long backoffMillis = delay.next();
        Duration backoffDuration = backoffMillis == 0 ? Duration.ZERO : Duration.ofMillis(backoffMillis);
        return new RetryInfoImpl(attempt, retry.maxAttempts, backoffDuration);
    }

    /**
     * @return {@code true} if retry should be made, {@code false} otherwise.
     */
    private static boolean handleError(Retry retry, RetryInfo retryInfo, int attempt, Throwable e) {
        if (!isExhausted(attempt, retry.maxAttempts) && retry.retryPredicate.test(e)) {
            try {
                long backoffMillis = retryInfo.getBackoff().toMillis();
                if (backoffMillis > 0) {
                    Thread.sleep(backoffMillis);
                }
            } catch (InterruptedException interruptedException) {
                throw new RuntimeException(e);
            }
            return true;
        } else {
            return false;
        }
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

    static class RetryInfoImpl implements RetryInfo {

        private final int attemptNumber;
        private final MaxAttempts maxAttempts;
        private final Duration backoff;

        public RetryInfoImpl(int attemptNumber, MaxAttempts maxAttempts, Duration backoff) {
            this.attemptNumber = attemptNumber;
            this.maxAttempts = maxAttempts;
            this.backoff = backoff;
        }

        @Override
        public int getRetryCount() {
            return attemptNumber - 1;
        }

        @Override
        public int getAttemptNumber() {
            return attemptNumber;
        }

        @Override
        public int getMaxAttempts() {
            if (isInfiniteRetriesLeft()) {
                return Integer.MAX_VALUE;
            }
            return ((MaxAttempts.Limit) maxAttempts).limit();
        }

        @Override
        public int getAttemptsLeft() {
            if (isInfiniteRetriesLeft()) {
                return Integer.MAX_VALUE;
            }
            return getMaxAttempts() - getAttemptNumber() + 1;
        }

        @Override
        public boolean isInfiniteRetriesLeft() {
            return maxAttempts instanceof MaxAttempts.Infinite;
        }

        @Override
        public Duration getBackoff() {
            return backoff;
        }

        @Override
        public boolean isLastAttempt() {
            if (isInfiniteRetriesLeft()) {
                return false;
            }
            return getAttemptNumber() == getMaxAttempts();
        }

        @Override
        public boolean isFirstAttempt() {
            return attemptNumber == 1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RetryInfoImpl retryInfo)) return false;
            return attemptNumber == retryInfo.attemptNumber && Objects.equals(maxAttempts, retryInfo.maxAttempts) && Objects.equals(backoff, retryInfo.backoff);
        }

        @Override
        public int hashCode() {
            return Objects.hash(attemptNumber, maxAttempts, backoff);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", RetryInfo.class.getSimpleName() + "[", "]")
                    .add("attempt=" + attemptNumber)
                    .add("maxAttempts=" + maxAttempts)
                    .add("backoff=" + backoff)
                    .toString();
        }

        RetryInfoImpl increaseAttemptsAndRetryCountByOne() {
            return new RetryInfoImpl(attemptNumber + 1, maxAttempts, backoff);
        }
    }
}