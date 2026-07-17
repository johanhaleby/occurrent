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

import java.util.Objects;

/**
 * Bundles the two things a snapshot-accelerated execute needs beyond the decider and the store: the {@code schemaVersion}
 * to stamp on written snapshots (and to check loaded ones against), and the {@link SnapshotPolicy} that decides when a new
 * snapshot is written.
 * <p>
 * The {@code schemaVersion} guards against reading a stale state shape: a loaded {@link Snapshot} whose
 * {@link Snapshot#schemaVersion()} differs is ignored and the state is rebuilt by a full replay. Bump it whenever the
 * decider's state type changes shape.
 *
 * @param schemaVersion the version stamped on written snapshots and required of loaded ones
 * @param policy        decides when a new snapshot is written
 * @param <S>           the decider state type
 * @param <E>           the event type
 */
public record SnapshotOptions<S extends @Nullable Object, E>(int schemaVersion, SnapshotPolicy<S, E> policy) {

    public SnapshotOptions {
        if (schemaVersion < 0) {
            throw new IllegalArgumentException("schemaVersion cannot be negative");
        }
        Objects.requireNonNull(policy, "policy cannot be null");
    }

    /**
     * Options with the given schema version and policy.
     */
    public static <S extends @Nullable Object, E> SnapshotOptions<S, E> of(int schemaVersion, SnapshotPolicy<S, E> policy) {
        return new SnapshotOptions<>(schemaVersion, policy);
    }

    /**
     * Options that write a snapshot every {@code n} events, at the given schema version.
     */
    public static <S extends @Nullable Object, E> SnapshotOptions<S, E> everyNEvents(int schemaVersion, int n) {
        return new SnapshotOptions<>(schemaVersion, SnapshotPolicy.everyNEvents(n));
    }
}
