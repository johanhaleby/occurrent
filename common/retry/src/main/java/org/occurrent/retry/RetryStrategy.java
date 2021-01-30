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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.occurrent.retry.MaxAttempts.Infinite.infinite;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * Retry strategy to use if the action throws an exception.
 * <p>
 * A {@code RetryStrategy} is thread-safe and immutable, so you can change the settings at any time without impacting the original instance.
 * For example this is perfectly valid:
 * <p>
 * <pre>
 * RetryStrategy retryStrategy = RetryStrategy.fixed(200).maxAttempts(5);
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

    public static Retry retry() {
        return new Retry();
    }

    public static DontRetry none() {
        return DontRetry.INSTANCE;
    }

    public static Retry exponentialBackoff(Duration initial, Duration max, double multiplier) {
        return RetryStrategy.retry().backoff(Backoff.exponential(initial, max, multiplier));
    }

    public static Retry fixed(Duration duration) {
        return RetryStrategy.retry().backoff(Backoff.fixed(duration));
    }

    public static Retry fixed(long millis) {
        return RetryStrategy.retry().backoff(Backoff.fixed(millis));
    }

    public <T> T execute(Supplier<T> supplier) {
        return executeWithRetry(supplier, ALWAYS_RETRY, this).get();
    }

    public void execute(Runnable runnable) {
        executeWithRetry(runnable, ALWAYS_RETRY, this).run();
    }

    public static class DontRetry extends RetryStrategy {
        private static final DontRetry INSTANCE = new DontRetry();

        private DontRetry() {
        }

        @Override
        public String toString() {
            return DontRetry.class.getSimpleName();
        }
    }

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

        public Retry backoff(Backoff backoff) {
            return new Retry(backoff, maxAttempts, retryPredicate, errorListener);
        }

        public Retry infiniteAttempts() {
            return new Retry(backoff, infinite(), retryPredicate, errorListener);
        }

        public Retry maxAttempts(int maxAttempts) {
            return new Retry(backoff, new MaxAttempts.Limit(maxAttempts), retryPredicate, errorListener);
        }

        public Retry retryIf(Predicate<Throwable> retryPredicate) {
            return new Retry(backoff, maxAttempts, retryPredicate, errorListener);
        }

        public Retry errorListener(BiConsumer<RetryInfo, Throwable> errorListener) {
            return new Retry(backoff, maxAttempts, retryPredicate, errorListener);
        }

        public Retry errorListener(Consumer<Throwable> errorListener) {
            return errorListener((__, throwable) -> errorListener.accept(throwable));
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