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

package org.occurrent.dsl.snapshot.internal;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.snapshot.Snapshot;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The reusable, storage-neutral load/resume steps shared by the blocking and reactor snapshot executors, so the
 * schema-check logic exists once rather than per stack and per stream/DCB path. The store-touching save steps live
 * alongside {@code SnapshotStore} in the blocking module instead (a reactive store cannot use them).
 */
public final class SnapshotSupport {

    private SnapshotSupport() {
    }

    /**
     * The state to resume folding from: the loaded snapshot's state and version when it is present and its schema
     * matches, otherwise the initial state at version {@code 0}. A schema mismatch is treated as no snapshot so a
     * changed state shape falls back to a full replay rather than being read into the new shape.
     *
     * @param state   the state to start folding the tail onto
     * @param version the version the state is folded up to, {@code 0} when starting from the initial state
     * @param <S>     the state type
     */
    public record Base<S extends @Nullable Object>(S state, long version) {
    }

    /**
     * Resolves the {@link Base} to resume from, treating the snapshot as trustworthy no matter how high its version is.
     * Equivalent to {@link #resolveBase(Optional, int, long, Supplier)} with an unbounded observed head, so a caller that
     * already knows the snapshot cannot be ahead of the true head (the append still happens at the real version, so a
     * stale snapshot only lengthens the tail) keeps the original behavior.
     */
    public static <S extends @Nullable Object> Base<S> resolveBase(Optional<Snapshot<S>> loaded, int expectedSchemaVersion, Supplier<? extends S> initialState) {
        return resolveBase(loaded, expectedSchemaVersion, Long.MAX_VALUE, initialState);
    }

    /**
     * Resolves the {@link Base} to resume from. Returns the snapshot's state and version when {@code loaded} is present,
     * its {@link Snapshot#schemaVersion()} equals {@code expectedSchemaVersion}, and its version is at or below
     * {@code observedHead}, otherwise the {@code initialState} at version {@code 0}. A schema mismatch is treated as no
     * snapshot so a changed state shape falls back to a full replay rather than being read into the new shape.
     * <p>
     * The {@code observedHead} guard is the safety net for a reset stream: if the stream was truncated below the snapshot,
     * the snapshot's version now exceeds the true head, so resuming from it would fold onto state that no longer exists.
     * Discarding it and folding from the initial state instead is the only correct choice. Pass {@link Long#MAX_VALUE} (or
     * use the 3-argument overload) when the head is not known and the snapshot is trusted unconditionally.
     *
     * @param observedHead the true current head the snapshot must not be ahead of (stream version, or DCB position)
     */
    public static <S extends @Nullable Object> Base<S> resolveBase(Optional<Snapshot<S>> loaded, int expectedSchemaVersion, long observedHead, Supplier<? extends S> initialState) {
        requireNonNull(loaded, "loaded cannot be null");
        requireNonNull(initialState, "initialState cannot be null");
        if (loaded.isPresent() && loaded.get().schemaVersion() == expectedSchemaVersion && loaded.get().version() <= observedHead) {
            Snapshot<S> snapshot = loaded.get();
            return new Base<>(snapshot.state(), snapshot.version());
        }
        return new Base<>(initialState.get(), 0L);
    }

    /**
     * Whether a maintained-snapshot delivery is a redelivery that should be skipped to keep folding idempotent. Equivalent
     * to {@link #isRedelivery(Optional, int, long, long)} with an unbounded observed head, for callers that trust the
     * snapshot's version unconditionally.
     *
     * @param deliveredVersion the version (stream version, or DCB position) of the event being delivered
     */
    public static <S extends @Nullable Object> boolean isRedelivery(Optional<Snapshot<S>> loaded, int schemaVersion, long deliveredVersion) {
        return isRedelivery(loaded, schemaVersion, deliveredVersion, Long.MAX_VALUE);
    }

    /**
     * Whether a maintained-snapshot delivery is a redelivery that should be skipped to keep folding idempotent. True when
     * {@code loaded} is present, its {@link Snapshot#schemaVersion()} matches, its version is at or beyond
     * {@code deliveredVersion}, and its version is at or below {@code observedHead}. A schema mismatch is not a redelivery,
     * so the caller rebuilds from the initial state.
     * <p>
     * The {@code observedHead} guard distinguishes a genuine redelivery from a reset: a snapshot version above
     * {@code deliveredVersion} normally means the event was already folded, but if that version is also above the true
     * head the stream was truncated and the snapshot is stale, so this is not a redelivery and the caller must rebuild.
     *
     * @param deliveredVersion the version (stream version, or DCB position) of the event being delivered
     * @param observedHead     the true current head the snapshot must not be ahead of
     */
    public static <S extends @Nullable Object> boolean isRedelivery(Optional<Snapshot<S>> loaded, int schemaVersion, long deliveredVersion, long observedHead) {
        requireNonNull(loaded, "loaded cannot be null");
        return loaded.isPresent() && loaded.get().schemaVersion() == schemaVersion
                && deliveredVersion <= loaded.get().version() && loaded.get().version() <= observedHead;
    }

    /**
     * Narrow a non-negative {@code long} version or count to an {@code int}, failing with a clear message instead of the
     * generic {@link ArithmeticException} that {@link Math#toIntExact(long)} throws. The snapshot machinery reads the
     * stream tail with {@code EventStore.read(streamId, skip, limit)} where {@code skip} is an {@code int}, and
     * {@code SnapshotDecision#eventsSinceSnapshot()} is an {@code int}, so a value beyond {@link Integer#MAX_VALUE}
     * cannot be represented.
     *
     * @param value       the non-negative value to narrow.
     * @param description what the value represents, used in the error message.
     * @return the value as an {@code int}.
     */
    public static int requireInt(long value, String description) {
        if (value < 0) {
            throw new IllegalArgumentException(description + " cannot be negative (was " + value + ")");
        }
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(description + " (" + value + ") exceeds Integer.MAX_VALUE, which the EventStore read skip and the eventsSinceSnapshot field cannot represent");
        }
        return (int) value;
    }
}
