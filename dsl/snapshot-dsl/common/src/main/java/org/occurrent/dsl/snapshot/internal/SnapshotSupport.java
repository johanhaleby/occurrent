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
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The reusable load/resume/persist steps shared by the blocking and reactor snapshot executors, so the schema-check and
 * policy-driven save logic exists once rather than per stack and per stream/DCB path.
 */
public final class SnapshotSupport {

    private static final Logger log = LoggerFactory.getLogger(SnapshotSupport.class);

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
     * Resolves the {@link Base} to resume from. Returns the snapshot's state and version when {@code loaded} is present
     * and its {@link Snapshot#schemaVersion()} equals {@code expectedSchemaVersion}; otherwise the {@code initialState}
     * at version {@code 0}.
     */
    public static <S extends @Nullable Object> Base<S> resolveBase(Optional<Snapshot<S>> loaded, int expectedSchemaVersion, Supplier<? extends S> initialState) {
        requireNonNull(loaded, "loaded cannot be null");
        requireNonNull(initialState, "initialState cannot be null");
        if (loaded.isPresent() && loaded.get().schemaVersion() == expectedSchemaVersion) {
            Snapshot<S> snapshot = loaded.get();
            return new Base<>(snapshot.state(), snapshot.version());
        }
        return new Base<>(initialState.get(), 0L);
    }

    /**
     * Whether a maintained-snapshot delivery is a redelivery that should be skipped to keep folding idempotent. True when
     * {@code loaded} is present, its {@link Snapshot#schemaVersion()} matches, and its version is at or beyond
     * {@code deliveredVersion}. A schema mismatch is not a redelivery, so the caller rebuilds from the initial state.
     *
     * @param deliveredVersion the version (stream version, or DCB position) of the event being delivered
     */
    public static <S extends @Nullable Object> boolean isRedelivery(Optional<Snapshot<S>> loaded, int schemaVersion, long deliveredVersion) {
        requireNonNull(loaded, "loaded cannot be null");
        return loaded.isPresent() && loaded.get().schemaVersion() == schemaVersion && deliveredVersion <= loaded.get().version();
    }

    /**
     * Writes a snapshot when {@code policy} fires for {@code decision}, tagging it with {@code schemaVersion}. The caller
     * decides whether the save is best-effort (wrap the call) or transactional (run it inside the write transaction).
     *
     * @return {@code true} if a snapshot was written
     */
    public static <S extends @Nullable Object, E> boolean maybeSave(SnapshotStore<S> store, String key, int schemaVersion,
                                                                    SnapshotPolicy<S, E> policy, SnapshotDecision<S, E> decision) {
        requireNonNull(store, "store cannot be null");
        requireNonNull(key, "key cannot be null");
        requireNonNull(policy, "policy cannot be null");
        requireNonNull(decision, "decision cannot be null");
        if (!policy.shouldSnapshot(decision)) {
            return false;
        }
        store.save(key, new Snapshot<>(decision.newState(), decision.newVersion(), schemaVersion));
        return true;
    }

    /**
     * Best-effort variant of {@link #maybeSave} for the DSL executors, which save the snapshot after the command's
     * events have already committed. A snapshot is a discardable optimization, so a save failure is logged and swallowed
     * rather than propagated: failing here would surface as a command failure even though the write succeeded, and a lost
     * snapshot only means the next replay folds a longer tail. The maintained {@code @Snapshot} path keeps using the
     * throwing {@link #maybeSave} so a durable subscription can retry.
     *
     * @return {@code true} if a snapshot was written, {@code false} if the policy declined it or the save failed
     */
    public static <S extends @Nullable Object, E> boolean maybeSaveBestEffort(SnapshotStore<S> store, String key, int schemaVersion,
                                                                              SnapshotPolicy<S, E> policy, SnapshotDecision<S, E> decision) {
        try {
            return maybeSave(store, key, schemaVersion, policy, decision);
        } catch (RuntimeException e) {
            log.warn("Best-effort snapshot save failed for key '{}'. The write is committed, the snapshot will be rebuilt from events on the next replay.", key, e);
            return false;
        }
    }

    /**
     * Narrow a non-negative {@code long} version or count to an {@code int}, failing with a clear message instead of the
     * generic {@link ArithmeticException} that {@link Math#toIntExact(long)} throws. The snapshot machinery reads the
     * stream tail with {@code EventStore.read(streamId, skip, limit)} where {@code skip} is an {@code int}, and
     * {@link SnapshotDecision#eventsSinceSnapshot()} is an {@code int}, so a value beyond {@link Integer#MAX_VALUE}
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
