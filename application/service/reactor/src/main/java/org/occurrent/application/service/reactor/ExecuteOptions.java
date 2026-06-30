/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.application.service.reactor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.eventstore.api.StreamReadFilter;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Options used when executing a command through a reactive {@link ApplicationService}.
 * <p>
 * It carries an optional {@link StreamReadFilter} (or an {@link ExecuteFilter} resolved at execution time) that limits
 * which events are read, and an optional reactive side-effect that is invoked after the events have been written. The
 * side-effect returns a {@link Mono} so it can do non-blocking work, and it runs once on success, outside the retry.
 *
 * @param <E> The application service event type.
 */
@NullMarked
public final class ExecuteOptions<E> {
    private final @Nullable StreamReadFilter filter;
    private final @Nullable ExecuteFilter<? extends E> executeFilter;
    private final @Nullable Function<Stream<E>, Mono<Void>> sideEffect;

    private ExecuteOptions(@Nullable StreamReadFilter filter, @Nullable ExecuteFilter<? extends E> executeFilter, @Nullable Function<Stream<E>, Mono<Void>> sideEffect) {
        this.filter = filter;
        this.executeFilter = executeFilter;
        this.sideEffect = sideEffect;
    }

    /**
     * Create empty options, i.e. no read filter and no side-effect.
     */
    public static <E> ExecuteOptions<E> empty() {
        return new ExecuteOptions<>(null, null, null);
    }

    /**
     * Alias for {@link #empty()} intended to read naturally in fluent call sites.
     */
    public static <E> ExecuteOptions<E> options() {
        return empty();
    }

    /**
     * Create options that contain only the supplied {@link ExecuteFilter}.
     */
    public static <E> ExecuteOptions<E> withExecuteFilter(ExecuteFilter<? extends E> executeFilter) {
        return new ExecuteOptions<>(null, Objects.requireNonNull(executeFilter, "executeFilter cannot be null"), null);
    }

    /**
     * Set the stream read filter.
     */
    public ExecuteOptions<E> filter(StreamReadFilter filter) {
        return new ExecuteOptions<>(Objects.requireNonNull(filter, "filter cannot be null"), null, null);
    }

    /**
     * Set an application-service-level execute filter that resolves domain event classes to CloudEvent types at
     * execution time.
     */
    public ExecuteOptions<E> filter(ExecuteFilter<? extends E> executeFilter) {
        return withExecuteFilter(executeFilter);
    }

    /**
     * Set the reactive side-effect to invoke after a successful write. It receives the events produced by the current
     * execution and returns a {@link Mono} that completes when the side-effect is done.
     */
    @SuppressWarnings("unchecked")
    public <E_SPECIFIC extends E> ExecuteOptions<E_SPECIFIC> sideEffect(Function<Stream<E_SPECIFIC>, Mono<Void>> sideEffect) {
        return new ExecuteOptions<>(filter, (ExecuteFilter<? extends E_SPECIFIC>) executeFilter, Objects.requireNonNull(sideEffect, "sideEffect cannot be null"));
    }

    /**
     * Return the configured stream read filter, or {@code null} if none has been configured.
     */
    public @Nullable StreamReadFilter filter() {
        return filter;
    }

    /**
     * Return the configured application-service execute filter, or {@code null} if none has been configured.
     */
    public @Nullable ExecuteFilter<? extends E> executeFilter() {
        return executeFilter;
    }

    /**
     * Return the configured post-write side-effect, or {@code null} if none has been configured.
     */
    public @Nullable Function<Stream<E>, Mono<Void>> sideEffect() {
        return sideEffect;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        ExecuteOptions<?> that = (ExecuteOptions<?>) obj;
        return Objects.equals(this.filter, that.filter) &&
                Objects.equals(this.executeFilter, that.executeFilter) &&
                Objects.equals(this.sideEffect, that.sideEffect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, executeFilter, sideEffect);
    }

    @Override
    public String toString() {
        return "ExecuteOptions[" +
                "filter=" + filter + ", " +
                "executeFilter=" + executeFilter + ", " +
                "sideEffect=" + sideEffect + ']';
    }
}
