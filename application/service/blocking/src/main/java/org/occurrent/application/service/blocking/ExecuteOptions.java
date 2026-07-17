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

import org.occurrent.application.service.ExecuteFilter;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.StreamReadFilter;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

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
    private final @Nullable ExecuteFilter<? extends E> executeFilter;
    private final @Nullable Consumer<List<E>> sideEffect;
    private final @Nullable Long fromStreamVersion;

    private ExecuteOptions(@Nullable StreamReadFilter filter, @Nullable ExecuteFilter<? extends E> executeFilter, @Nullable Consumer<List<E>> sideEffect, @Nullable Long fromStreamVersion) {
        this.filter = filter;
        this.executeFilter = executeFilter;
        this.sideEffect = sideEffect;
        this.fromStreamVersion = fromStreamVersion;
    }

    /**
     * Create empty options, i.e. no read filter and no side-effect.
     *
     * @param <E> The application service event type.
     * @return Empty execute options.
     */
    public static <E> ExecuteOptions<E> empty() {
        return new ExecuteOptions<>(null, null, null, null);
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
     * Create options that contain only the supplied {@link ExecuteFilter}.
     *
     * @param executeFilter The execute filter to configure.
     * @param <E>           The application service event type.
     * @return Execute options configured with the supplied execute filter.
     */
    public static <E> ExecuteOptions<E> withExecuteFilter(ExecuteFilter<? extends E> executeFilter) {
        return new ExecuteOptions<>(null, Objects.requireNonNull(executeFilter, "executeFilter cannot be null"), null, null);
    }

    /**
     * Set stream read filter.
     * <p>
     * Note that the generic type parameter may change when chaining since the side-effect type is established
     * once {@link #sideEffect(Consumer)} is provided.
     *
     * @param filter The filter to use when reading the stream.
     * @return New options with filter applied.
     */
    public ExecuteOptions<E> filter(StreamReadFilter filter) {
        return new ExecuteOptions<>(Objects.requireNonNull(filter, "filter cannot be null"), null, null, fromStreamVersion);
    }

    /**
     * Set an application-service-level execute filter that resolves domain event classes to CloudEvent types
     * at execution time.
     * <p>
     * This is useful when the filter should be expressed in terms of domain event classes instead of raw
     * CloudEvent type strings.
     *
     * @param executeFilter The execute filter to use when reading the stream.
     * @return New options with execute filter applied.
     */
    public ExecuteOptions<E> filter(ExecuteFilter<? extends E> executeFilter) {
        return withExecuteFilter(executeFilter);
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
    @SuppressWarnings("unchecked")
    public <E_SPECIFIC extends E> ExecuteOptions<E_SPECIFIC> sideEffect(Consumer<List<E_SPECIFIC>> sideEffect) {
        return new ExecuteOptions<>(filter, (ExecuteFilter<? extends E_SPECIFIC>) executeFilter, Objects.requireNonNull(sideEffect, "sideEffect cannot be null"), fromStreamVersion);
    }

    /**
     * Start reading the stream <em>after</em> the given stream version instead of from the beginning, so that only the
     * events written after that version are handed to the domain function.
     * <p>
     * This is an advanced option intended for snapshot-based execution: the caller has already folded the stream up to
     * {@code fromStreamVersion} into a known state (a snapshot) and only needs the events after it. The domain function
     * must therefore fold the events it receives onto that snapshot state rather than onto the initial state. The
     * optimistic write still happens at the stream's true current version, so concurrency control is unaffected.
     *
     * @param fromStreamVersion The exclusive stream version to start reading after (0 reads the whole stream).
     * @return New options that read the stream from the given version.
     */
    public ExecuteOptions<E> fromStreamVersion(long fromStreamVersion) {
        if (fromStreamVersion < 0) {
            throw new IllegalArgumentException("fromStreamVersion cannot be negative");
        }
        if (fromStreamVersion > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("fromStreamVersion cannot exceed Integer.MAX_VALUE (" + Integer.MAX_VALUE + "), because the stream tail is read with EventStore.read(streamId, skip, limit) where skip is an int, so a snapshot base version beyond that is not supported");
        }
        return new ExecuteOptions<>(filter, executeFilter, sideEffect, fromStreamVersion);
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
    public @Nullable Consumer<List<E>> sideEffect() {
        return sideEffect;
    }

    /**
     * Return the configured stream version to start reading after, or {@code null} if the whole stream should be read.
     */
    public @Nullable Long fromStreamVersion() {
        return fromStreamVersion;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ExecuteOptions) obj;
        return Objects.equals(this.filter, that.filter) &&
                Objects.equals(this.executeFilter, that.executeFilter) &&
                Objects.equals(this.sideEffect, that.sideEffect) &&
                Objects.equals(this.fromStreamVersion, that.fromStreamVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, executeFilter, sideEffect, fromStreamVersion);
    }

    @Override
    public String toString() {
        return "ExecuteOptions[" +
                "filter=" + filter + ", " +
                "executeFilter=" + executeFilter + ", " +
                "sideEffect=" + sideEffect + ", " +
                "fromStreamVersion=" + fromStreamVersion + ']';
    }

}
