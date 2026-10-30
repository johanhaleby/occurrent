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
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.DcbMarkerVersions;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Options used when executing a command through a reactive {@link DcbApplicationService}.
 * <p>
 * It carries an optional side-effect that is invoked after the produced events have been appended. The side-effect
 * returns a {@link Mono} so it can do non-blocking work, and it is composed into the returned {@code Mono} after the
 * append, outside the retry, so it runs once on success rather than once per attempt.
 * <p>
 * Unlike the stream execute options, it has no read-filter option on purpose. In DCB the
 * {@link org.occurrent.eventstore.api.dcb.DcbCriteria} passed to {@code execute} is both the read filter and the
 * consistency boundary, so a separate filter here would be redundant and misleading.
 *
 * @param <E> The application service event type.
 */
@NullMarked
public final class DcbExecuteOptions<E> {
    private final @Nullable Function<List<E>, Mono<Void>> sideEffect;
    private final @Nullable TagGenerator<E> tagGenerator;
    private final @Nullable Long fromPosition;
    private final boolean versionsRequested;
    private final @Nullable DcbMarkerVersions fromVersions;

    private DcbExecuteOptions(@Nullable Function<List<E>, Mono<Void>> sideEffect, @Nullable TagGenerator<E> tagGenerator, @Nullable Long fromPosition,
                              boolean versionsRequested, @Nullable DcbMarkerVersions fromVersions) {
        this.sideEffect = sideEffect;
        this.tagGenerator = tagGenerator;
        this.fromPosition = fromPosition;
        this.versionsRequested = versionsRequested;
        this.fromVersions = fromVersions;
    }

    /**
     * Create empty options, i.e. no side-effect and no per-execute {@link TagGenerator}.
     *
     * @param <E> The application service event type.
     * @return Empty execute options.
     */
    public static <E> DcbExecuteOptions<E> empty() {
        return new DcbExecuteOptions<>(null, null, null, false, null);
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
     * @apiNote Widens the existing {@code tagGenerator} with an unchecked cast, independently of this call's own type
     * parameter. Narrowing {@code sideEffect} to {@code E_SPECIFIC} while a {@code tagGenerator} for an unrelated,
     * non-supertype event type is already configured can therefore throw a {@link ClassCastException} later, at the
     * point the tag generator is invoked, not here.
     */
    @SuppressWarnings("unchecked")
    public <E_SPECIFIC extends E> DcbExecuteOptions<E_SPECIFIC> sideEffect(Function<List<E_SPECIFIC>, Mono<Void>> sideEffect) {
        return new DcbExecuteOptions<>(Objects.requireNonNull(sideEffect, "sideEffect cannot be null"), (TagGenerator<E_SPECIFIC>) this.tagGenerator, this.fromPosition, this.versionsRequested, this.fromVersions);
    }

    /**
     * Set the {@link TagGenerator} to use for this execution, overriding any global tagger configured on the
     * application service.
     *
     * @param tagGenerator The per-execute {@link TagGenerator}.
     * @param <E_SPECIFIC> The tag generator event type for the returned options.
     * @return New options with the tag generator applied.
     * @apiNote Widens the existing {@code sideEffect} with an unchecked cast, independently of this call's own type
     * parameter. Narrowing {@code tagGenerator} to {@code E_SPECIFIC} while a {@code sideEffect} for an unrelated,
     * non-supertype event type is already configured can therefore throw a {@link ClassCastException} later, at the
     * point the side-effect is invoked, not here.
     */
    @SuppressWarnings("unchecked")
    public <E_SPECIFIC extends E> DcbExecuteOptions<E_SPECIFIC> tagGenerator(TagGenerator<E_SPECIFIC> tagGenerator) {
        return new DcbExecuteOptions<>((Function<List<E_SPECIFIC>, Mono<Void>>) (Function<?, ?>) this.sideEffect, Objects.requireNonNull(tagGenerator, "tagGenerator cannot be null"), this.fromPosition, this.versionsRequested, this.fromVersions);
    }

    /**
     * Start reading the DCB boundary <em>after</em> the given global position instead of from the beginning, so that
     * only the events appended after that position are handed to the domain function.
     * <p>
     * This is an advanced option intended for snapshot-based execution: the caller has already folded the boundary up to
     * {@code fromPosition} into a known state (a snapshot) and only needs the events after it. The domain function must
     * therefore fold the events it receives onto that snapshot state rather than onto the initial state. The read still
     * captures the whole boundary's consistency token, so the append condition is unaffected.
     *
     * @param fromPosition The exclusive global DCB position to start reading after (0 reads the whole boundary).
     * @return New options that read the boundary from the given position.
     * @throws IllegalStateException if these options already ask for marker versions
     * @deprecated A position is no safe place to resume from on MongoDB. The store reserves a position before the
     * append commits, so an append that reserved a lower position can commit after a read that saw a higher one, and a
     * read after that higher position never returns it. Use {@link #fromVersions(DcbMarkerVersions)} instead.
     */
    @Deprecated(forRemoval = true)
    public DcbExecuteOptions<E> fromPosition(long fromPosition) {
        if (fromPosition < 0) {
            throw new IllegalArgumentException("fromPosition cannot be negative");
        }
        if (versionsRequested) {
            throw new IllegalStateException("fromPosition cannot be combined with withVersions or fromVersions");
        }
        return new DcbExecuteOptions<>(this.sideEffect, this.tagGenerator, fromPosition, false, null);
    }

    /**
     * Ask the store for the marker versions of the boundary on the read, and read the whole boundary. When the store
     * reports them, and the read and the append report the same epoch for every marker key, the returned
     * {@link org.occurrent.eventstore.api.dcb.DcbAppendResult#markerVersions()} holds the versions after this append.
     * Otherwise they are empty. A snapshot saves them and passes them to {@link #fromVersions(DcbMarkerVersions)} on
     * the next execute.
     *
     * @return New options that ask for marker versions.
     * @throws IllegalStateException if these options read from a position
     * @see DcbReadOptions#withVersions()
     */
    public DcbExecuteOptions<E> withVersions() {
        requireNoPosition();
        return new DcbExecuteOptions<>(this.sideEffect, this.tagGenerator, null, true, this.fromVersions);
    }

    /**
     * Read only the events of the boundary appended after {@code markerVersions}, the versions a previous append or read
     * reported, and ask for marker versions as {@link #withVersions()} does. The domain function receives only those
     * events, so it folds them onto the state the caller saved at {@code markerVersions}, not onto the initial state.
     * The read still takes the whole boundary's consistency token, so the append condition doesn't change.
     * <p>
     * The read throws {@link org.occurrent.eventstore.api.dcb.DcbSnapshotInvalidException} when the store can't compare
     * {@code markerVersions} with its own, for example after its markers were pruned. Execute again with
     * {@link #withVersions()} and fold from the initial state then.
     *
     * @param markerVersions the versions to read after
     * @return New options that read after the given marker versions.
     * @throws IllegalStateException if these options read from a position
     * @see DcbReadOptions#afterVersions(DcbMarkerVersions)
     */
    public DcbExecuteOptions<E> fromVersions(DcbMarkerVersions markerVersions) {
        Objects.requireNonNull(markerVersions, "markerVersions cannot be null");
        requireNoPosition();
        return new DcbExecuteOptions<>(this.sideEffect, this.tagGenerator, null, true, markerVersions);
    }

    private void requireNoPosition() {
        if (fromPosition != null) {
            throw new IllegalStateException("withVersions and fromVersions cannot be combined with fromPosition");
        }
    }

    /**
     * Return the configured post-append side-effect, or {@code null} if none has been configured.
     */
    public @Nullable Function<List<E>, Mono<Void>> sideEffect() {
        return sideEffect;
    }

    /**
     * Return the configured per-execute {@link TagGenerator}, or {@code null} if none has been configured.
     */
    public @Nullable TagGenerator<E> tagGenerator() {
        return tagGenerator;
    }

    /**
     * Return the configured global position to start reading after, or {@code null} if the whole boundary should be read.
     *
     * @deprecated See {@link #fromPosition(long)}.
     */
    @Deprecated(forRemoval = true)
    public @Nullable Long fromPosition() {
        return fromPosition;
    }

    /**
     * Return whether the read asks the store for marker versions, set by {@link #withVersions()} and
     * {@link #fromVersions(DcbMarkerVersions)}.
     */
    public boolean versionsRequested() {
        return versionsRequested;
    }

    /**
     * Return the marker versions to read after, or {@code null} if the whole boundary should be read.
     */
    public @Nullable DcbMarkerVersions fromVersions() {
        return fromVersions;
    }

    // sideEffect and tagGenerator are lambda-typed fields, so they are compared by identity here.
    // Two options built with separately written but source-identical lambdas are therefore not equal.

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        DcbExecuteOptions<?> that = (DcbExecuteOptions<?>) obj;
        return Objects.equals(this.sideEffect, that.sideEffect) &&
                Objects.equals(this.tagGenerator, that.tagGenerator) &&
                Objects.equals(this.fromPosition, that.fromPosition) &&
                this.versionsRequested == that.versionsRequested &&
                Objects.equals(this.fromVersions, that.fromVersions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sideEffect, tagGenerator, fromPosition, versionsRequested, fromVersions);
    }

    @Override
    public String toString() {
        return "DcbExecuteOptions[sideEffect=" + sideEffect + ", tagGenerator=" + tagGenerator + ", fromPosition=" + fromPosition
                + ", versionsRequested=" + versionsRequested + ", fromVersions=" + fromVersions + ']';
    }
}
