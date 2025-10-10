/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.retry.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.retry.*;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.occurrent.retry.MaxAttempts.Infinite.infinite;

/**
 *
 * A retry strategy that does retry. By default, the following settings are used:
 *
 * <ul>
 *     <li>No backoff</li>
 *     <li>Infinite number of retries</li>
 *     <li>Retries all exceptions</li>
 *     <li>No error listener (will retry silently)</li>
 * </ul>
 */
@NullMarked
public final class RetryImpl implements RetryStrategy.Retry {
    // @formatter:off
    private static final BiConsumer<ErrorInfo, Throwable> NOOP_ERROR_LISTENER = (__, ___) -> {};
    private static final BiConsumer<BeforeRetryInfo, Throwable> NOOP_BEFORE_RETRY_LISTENER = (__, ___) -> {};
    private static final BiConsumer<AfterRetryInfo, Throwable> NOOP_AFTER_RETRY_LISTENER = (__, ___) -> {};
    private static final BiConsumer<RetryableErrorInfo, Throwable> NOOP_RETRYABLE_ERROR_LISTENER = (__, ___) -> {};
// @formatter:on

    final Backoff backoff;
    final MaxAttempts maxAttempts;
    final Predicate<Throwable> retryPredicate;
    final BiConsumer<ErrorInfo, Throwable> errorListener;
    final BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener;
    final BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener;
    final BiConsumer<RetryableErrorInfo, Throwable> onRetryableErrorListener;
    final Function<Throwable, Throwable> errorMapper;

    private RetryImpl(Backoff backoff, MaxAttempts maxAttempts, Function<Throwable, Throwable> errorMapper, Predicate<Throwable> retryPredicate, @Nullable BiConsumer<ErrorInfo, Throwable> errorListener,
                      @Nullable BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener, @Nullable BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener,
                      @Nullable BiConsumer<RetryableErrorInfo, Throwable> onRetryableErrorListener) {
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
        this.onRetryableErrorListener = onRetryableErrorListener == null ? NOOP_RETRYABLE_ERROR_LISTENER : onRetryableErrorListener;
    }

    public RetryImpl() {
        this(Backoff.none(), infinite(), Function.identity(), __ -> true, NOOP_ERROR_LISTENER, NOOP_BEFORE_RETRY_LISTENER, NOOP_AFTER_RETRY_LISTENER, NOOP_RETRYABLE_ERROR_LISTENER);
    }

    @Override
    public Retry backoff(Backoff backoff) {
        Objects.requireNonNull(backoff, Backoff.class.getSimpleName() + " cannot be null");
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry infiniteAttempts() {
        return new RetryImpl(backoff, infinite(), errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry maxAttempts(int maxAttempts) {
        return new RetryImpl(backoff, new MaxAttempts.Limit(maxAttempts), errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public RetryImpl retryIf(Predicate<Throwable> retryPredicate) {
        Objects.requireNonNull(retryPredicate, "Retry predicate cannot be null");
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry mapRetryPredicate(Function<Predicate<Throwable>, Predicate<Throwable>> retryPredicateFn) {
        Objects.requireNonNull(retryPredicateFn, "Retry predicate function cannot be null");
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicateFn.apply(retryPredicate), errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry mapError(Function<Throwable, Throwable> errorMapper) {
        Objects.requireNonNull(errorMapper, "Mapping function cannot be null");
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @NullMarked
    @Override
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
        return new RetryImpl(backoff, maxAttempts, errorMapper.andThen(matchingError), retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry onError(BiConsumer<ErrorInfo, Throwable> errorListener) {
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry onError(Consumer<Throwable> errorListener) {
        return onError((__, throwable) -> errorListener.accept(throwable));
    }

    @Override
    public Retry onRetryableError(Consumer<Throwable> retryableErrorListener) {
        return onRetryableError((__, throwable) -> retryableErrorListener.accept(throwable));
    }

    @Override
    public Retry onRetryableError(BiConsumer<RetryableErrorInfo, Throwable> retryableErrorListener) {
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, retryableErrorListener);
    }


    @Override
    public Retry onBeforeRetry(BiConsumer<BeforeRetryInfo, Throwable> onBeforeRetryListener) {
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry onBeforeRetry(Consumer<Throwable> onBeforeRetryListener) {
        return onBeforeRetry(((__, throwable) -> onBeforeRetryListener.accept(throwable)));
    }

    @Override
    public Retry onAfterRetry(BiConsumer<AfterRetryInfo, Throwable> onAfterRetryListener) {
        return new RetryImpl(backoff, maxAttempts, errorMapper, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener);
    }

    @Override
    public Retry onAfterRetry(Consumer<Throwable> onAfterRetryListener) {
        return onAfterRetry(((__, throwable) -> onAfterRetryListener.accept(throwable)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetryImpl that)) return false;
        return Objects.equals(backoff, that.backoff) && Objects.equals(maxAttempts, that.maxAttempts) && Objects.equals(retryPredicate, that.retryPredicate) && Objects.equals(errorListener, that.errorListener) && Objects.equals(onBeforeRetryListener, that.onBeforeRetryListener) && Objects.equals(onAfterRetryListener, that.onAfterRetryListener) && Objects.equals(onRetryableErrorListener, that.onRetryableErrorListener) && Objects.equals(errorMapper, that.errorMapper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backoff, maxAttempts, retryPredicate, errorListener, onBeforeRetryListener, onAfterRetryListener, onRetryableErrorListener, errorMapper);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RetryImpl.class.getSimpleName() + "[", "]")
                .add("backoff=" + backoff)
                .add("maxAttempts=" + maxAttempts)
                .add("retryPredicate=" + retryPredicate)
                .add("errorListener=" + errorListener)
                .add("onBeforeRetryListener=" + onBeforeRetryListener)
                .add("onAfterRetryListener=" + onAfterRetryListener)
                .add("onRetryableErrorListener=" + onRetryableErrorListener)
                .add("errorMapper=" + errorMapper)
                .toString();
    }
}
