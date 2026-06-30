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

package org.occurrent.application.service.reactor.dcb;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Options used when executing a command through a reactive {@link DcbApplicationService}.
 * <p>
 * It carries an optional side-effect that is invoked after the produced events have been appended. The side-effect
 * returns a {@link Mono} so it can do non-blocking work, and it is composed into the returned {@code Mono} after the
 * append, outside the retry, so it runs once on success rather than once per attempt.
 * <p>
 * Unlike the stream execute options, it has no read-filter option on purpose. In DCB the
 * {@link org.occurrent.eventstore.api.dcb.DcbQuery} passed to {@code execute} is both the read filter and the
 * consistency boundary, so a separate filter here would be redundant and misleading.
 *
 * @param <E> The application service event type.
 */
@NullMarked
public final class DcbExecuteOptions<E> {
    private final @Nullable Function<Stream<E>, Mono<Void>> sideEffect;

    private DcbExecuteOptions(@Nullable Function<Stream<E>, Mono<Void>> sideEffect) {
        this.sideEffect = sideEffect;
    }

    /**
     * Create empty options, i.e. no side-effect.
     *
     * @param <E> The application service event type.
     * @return Empty execute options.
     */
    public static <E> DcbExecuteOptions<E> empty() {
        return new DcbExecuteOptions<>(null);
    }

    /**
     * Alias for {@link #empty()} intended to read naturally in fluent call sites.
     *
     * @param <E> The application service event type.
     * @return Empty execute options.
     */
    public static <E> DcbExecuteOptions<E> options() {
        return empty();
    }

    /**
     * Set the side-effect to invoke after a successful append.
     * <p>
     * The side-effect is invoked once with the events produced by the current execution after those events have been
     * appended successfully. It is not invoked when the domain function produced no new events.
     *
     * @param sideEffect   Side-effect that receives the newly produced domain events and returns a {@link Mono} that
     *                     completes when the side-effect is done.
     * @param <E_SPECIFIC> The side-effect event type for the returned options.
     * @return New options with the side-effect applied.
     */
    public <E_SPECIFIC extends E> DcbExecuteOptions<E_SPECIFIC> sideEffect(Function<Stream<E_SPECIFIC>, Mono<Void>> sideEffect) {
        return new DcbExecuteOptions<>(Objects.requireNonNull(sideEffect, "sideEffect cannot be null"));
    }

    /**
     * Return the configured post-append side-effect, or {@code null} if none has been configured.
     */
    public @Nullable Function<Stream<E>, Mono<Void>> sideEffect() {
        return sideEffect;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        DcbExecuteOptions<?> that = (DcbExecuteOptions<?>) obj;
        return Objects.equals(this.sideEffect, that.sideEffect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sideEffect);
    }

    @Override
    public String toString() {
        return "DcbExecuteOptions[sideEffect=" + sideEffect + ']';
    }
}
