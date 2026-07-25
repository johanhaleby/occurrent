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
import org.occurrent.dsl.decider.Decider;

import java.util.Objects;

/**
 * Snapshot policies that need a {@link Decider}, complementing the storage-neutral built-ins on {@link SnapshotPolicy}.
 * <p>
 * Previously duplicated per stack (blocking and reactor, see ADR 0061) because it depends on {@link Decider}. The two
 * copies were byte-identical, so they were collapsed into this single copy here; {@code occurrent-decider} is now a
 * dependency of this module.
 */
public final class SnapshotPolicies {

    private SnapshotPolicies() {
    }

    /**
     * Snapshots whenever the decider's state becomes terminal, the natural "close the books" boundary. A terminal
     * state is the end of an entity's lifecycle (a closed fiscal period, a completed process), so capturing it lets
     * the next period resume from the closing state instead of replaying the closed one.
     */
    public static <S extends @Nullable Object, E> SnapshotPolicy<S, E> whenTerminal(Decider<?, S, E> decider) {
        Objects.requireNonNull(decider, "decider cannot be null");
        return SnapshotPolicy.whenState(decider::isTerminal);
    }
}
