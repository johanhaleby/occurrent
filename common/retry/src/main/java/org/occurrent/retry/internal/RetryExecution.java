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
import org.occurrent.retry.RetryStrategy;
import org.occurrent.retry.RetryStrategy.DontRetry;
import org.occurrent.retry.RetryStrategy.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Internal class for executing functions with retry capability. Never use this class directly from your own code!
 */
public class RetryExecution {

    private static final Logger log = LoggerFactory.getLogger(RetryExecution.class);

    public static <T1> Supplier<T1> executeWithRetry(Supplier<T1> supplier, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return supplier;
        }
        Retry retry = (Retry) retryStrategy;
        return executeWithRetry(supplier, shutdownPredicate.and(retry.retryPredicate), convertToDelayStream(retry.backoff));
    }

    public static Runnable executeWithRetry(Runnable runnable, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return runnable;
        }
        Retry retry = (Retry) retryStrategy;
        return executeWithRetry(runnable, shutdownPredicate.and(retry.retryPredicate), convertToDelayStream(retry.backoff));
    }

    public static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Predicate<Throwable> shutdownPredicate, RetryStrategy retryStrategy) {
        if (retryStrategy instanceof DontRetry) {
            return fn;
        }
        Retry retry = (Retry) retryStrategy;
        return executeWithRetry(fn, shutdownPredicate.and(retry.retryPredicate), convertToDelayStream(retry.backoff));
    }

    private static Runnable executeWithRetry(Runnable runnable, Predicate<Throwable> retryPredicate, Iterator<Long> delay) {
        Consumer<Void> runnableConsumer = __ -> runnable.run();
        return () -> executeWithRetry(runnableConsumer, retryPredicate, delay).accept(null);
    }

    private static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Predicate<Throwable> retryPredicate, Iterator<Long> delay) {
        return t1 -> {
            try {
                fn.accept(t1);
            } catch (Exception e) {
                if (retryPredicate.test(e) && delay != null) {
                    Long retryAfterMillis = delay.next();
                    log.error("Caught {} with message \"{}\", will retry in {} milliseconds.", e.getClass().getName(), e.getMessage(), retryAfterMillis, e);
                    try {
                        Thread.sleep(retryAfterMillis);
                    } catch (InterruptedException interruptedException) {
                        throw new RuntimeException(e);
                    }
                    executeWithRetry(fn, retryPredicate, delay).accept(t1);
                } else {
                    throw e;
                }
            }
        };
    }

    private static <T1> Supplier<T1> executeWithRetry(Supplier<T1> supplier, Predicate<Throwable> retryPredicate, Iterator<Long> delay) {
        return () -> {
            try {
                return supplier.get();
            } catch (Exception e) {
                if (retryPredicate.test(e) && delay != null) {
                    Long retryAfterMillis = delay.next();
                    log.error("Caught {} with message \"{}\", will retry in {} milliseconds.", e.getClass().getName(), e.getMessage(), retryAfterMillis, e);
                    try {
                        Thread.sleep(retryAfterMillis);
                    } catch (InterruptedException interruptedException) {
                        throw new RuntimeException(e);
                    }
                    return executeWithRetry(supplier, retryPredicate, delay).get();
                } else {
                    throw e;
                }
            }
        };
    }

    private static Iterator<Long> convertToDelayStream(Backoff backoff) {
        final Stream<Long> delay;
        if (backoff instanceof Backoff.None) {
            delay = null;
        } else if (backoff instanceof Backoff.Fixed) {
            long millis = ((Backoff.Fixed) backoff).millis;
            delay = Stream.iterate(millis, __ -> millis);
        } else if (backoff instanceof Backoff.Exponential) {
            Backoff.Exponential strategy = (Backoff.Exponential) backoff;
            long initialMillis = strategy.initial.toMillis();
            long maxMillis = strategy.max.toMillis();
            double multiplier = strategy.multiplier;
            delay = Stream.iterate(initialMillis, current -> Math.min(maxMillis, Math.round(current * multiplier)));
        } else {
            throw new IllegalStateException("Invalid retry strategy: " + backoff.getClass().getName());
        }
        return delay == null ? null : delay.iterator();
    }
}
