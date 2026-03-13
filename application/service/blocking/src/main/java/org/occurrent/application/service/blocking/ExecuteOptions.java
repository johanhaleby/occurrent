/*
 *
 *  Copyright 2026 Johan Haleby
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

package org.occurrent.application.service.blocking;


import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.StreamReadFilter;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Options used when executing a command in {@link ApplicationService}.
 * <p>
 * Use this record to configure:
 * <ul>
 *     <li>a {@link StreamReadFilter} that limits which events are read before command execution</li>
 *     <li>an optional side-effect that is invoked after events have been written</li>
 * </ul>
 * <p>
 * A typical usage pattern is:
 * <pre>{@code
 * applicationService.execute(streamId,
 *         filter(myFilter).sideEffect(mySideEffect),
 *         functionThatCallsDomainModel);
 * }</pre>
 *
 */
public final class ExecuteOptions<E> {
    private final @Nullable StreamReadFilter filter;
    private final @Nullable Consumer<Stream<E>> sideEffect;

    private ExecuteOptions(@Nullable StreamReadFilter filter, @Nullable Consumer<Stream<E>> sideEffect) {
        this.filter = filter;
        this.sideEffect = sideEffect;
    }

    /**
     * Create empty options, i.e. no read filter and no side-effect.
     *
     * @param <E> The application service event type.
     * @return Empty execute options.
     */
    public static <E> ExecuteOptions<E> empty() {
        return new ExecuteOptions<>(null, null);
    }

    /**
     * Alias for {@link #empty()} intended to read naturally in fluent call sites.
     *
     * @param <E> The application service event type.
     * @return Empty execute options.
     */
    public static <E> ExecuteOptions<E> options() {
        return empty();
    }

    /**
     * Set stream read filter.
     * <p>
     * Note that the generic type parameter may change when chaining since the side-effect type is established
     * once {@link #sideEffect(Consumer)} is provided.
     *
     * @param filter       The filter to use when reading the stream.
     * @param <E_SPECIFIC> The side-effect event type for the returned options.
     * @return New options with filter applied.
     */
    public <E_SPECIFIC extends E> ExecuteOptions<E_SPECIFIC> filter(StreamReadFilter filter) {
        return new ExecuteOptions<>(Objects.requireNonNull(filter, "filter cannot be null"), null);
    }

    /**
     * Set side-effect to invoke after successful writes.
     * <p>
     * The side-effect is invoked with the events produced by the current
     * execution after those events have been written successfully.
     *
     * @param sideEffect   Side-effect that receives the newly produced domain events.
     * @param <E_SPECIFIC> The side-effect event type for the returned options.
     * @return New options with side-effect applied.
     */
    public <E_SPECIFIC extends E> ExecuteOptions<E_SPECIFIC> sideEffect(Consumer<Stream<E_SPECIFIC>> sideEffect) {
        return new ExecuteOptions<>(filter, Objects.requireNonNull(sideEffect, "sideEffect cannot be null"));
    }

    /**
     * Return the configured stream read filter, or {@code null} if none has been configured.
     */
    public @Nullable StreamReadFilter filter() {
        return filter;
    }

    /**
     * Return the configured post-write side-effect, or {@code null} if none has been configured.
     */
    public @Nullable Consumer<Stream<E>> sideEffect() {
        return sideEffect;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ExecuteOptions) obj;
        return Objects.equals(this.filter, that.filter) &&
                Objects.equals(this.sideEffect, that.sideEffect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, sideEffect);
    }

    @Override
    public String toString() {
        return "ExecuteOptions[" +
                "filter=" + filter + ", " +
                "sideEffect=" + sideEffect + ']';
    }

}
