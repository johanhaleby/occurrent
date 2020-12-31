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

package org.occurrent.retry.internal;

import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Internal class for executing functions with retry capability. Never use this class directly from your own code!
 */
public class RetryExecution {

    private static final Logger log = LoggerFactory.getLogger(RetryExecution.class);

    public static Runnable executeWithRetry(Runnable runnable, Predicate<Exception> retryPredicate, Iterator<Long> delay) {
        Consumer<Void> runnableConsumer = __ -> runnable.run();
        return () -> executeWithRetry(runnableConsumer, retryPredicate, delay).accept(null);
    }

    public static <T1> Consumer<T1> executeWithRetry(Consumer<T1> fn, Predicate<Exception> retryPredicate, Iterator<Long> delay) {
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

    public static Iterator<Long> convertToDelayStream(RetryStrategy retryStrategy) {
        final Stream<Long> delay;
        if (retryStrategy instanceof RetryStrategy.None) {
            delay = null;
        } else if (retryStrategy instanceof RetryStrategy.Fixed) {
            long millis = ((RetryStrategy.Fixed) retryStrategy).millis;
            delay = Stream.iterate(millis, __ -> millis);
        } else if (retryStrategy instanceof RetryStrategy.Backoff) {
            RetryStrategy.Backoff strategy = (RetryStrategy.Backoff) retryStrategy;
            long initialMillis = strategy.initial.toMillis();
            long maxMillis = strategy.max.toMillis();
            double multiplier = strategy.multiplier;
            delay = Stream.iterate(initialMillis, current -> Math.min(maxMillis, Math.round(current * multiplier)));
        } else {
            throw new IllegalStateException("Invalid retry strategy: " + retryStrategy.getClass().getName());
        }
        return delay == null ? null : delay.iterator();
    }
}
