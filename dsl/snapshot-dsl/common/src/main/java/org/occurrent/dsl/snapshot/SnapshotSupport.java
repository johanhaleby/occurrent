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

package org.occurrent.dsl.snapshot;

import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * The reusable load/resume/persist steps shared by the blocking and reactor snapshot executors, so the schema-check and
 * policy-driven save logic exists once rather than per stack and per stream/DCB path.
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
}
