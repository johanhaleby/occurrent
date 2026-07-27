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

package org.occurrent.dsl.snapshot.reactor;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.dcb.DcbDecider;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.Objects;
import java.util.function.Function;

/**
 * The reactive per-aggregate snapshot spec for the DCB decider path, the reactor counterpart to
 * {@link org.occurrent.dsl.snapshot.blocking.SnapshotDcbDecider}: a {@link DcbDecider} bundled with the
 * {@link ReactiveSnapshotStore} that holds its snapshots, the {@link SnapshotOptions} for its state, and the function
 * that turns the decider's resolved {@link DcbCriteria} into a snapshot key. Create one per aggregate with a
 * {@code from(...)} factory and pass it to a {@link ReactiveSnapshotDcbDeciderApplicationService}, built once and reused.
 * <p>
 * The default key function is the canonical rendering of the {@link DcbCriteria}
 * ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}), use
 * {@link #from(DcbDecider, ReactiveSnapshotStore, SnapshotOptions, Function)} to override it. The spec holds the
 * {@link ReactiveSnapshotStore}, so it is not a pure value. It stays inert and the executor performs all I/O.
 * <p>
 * The key is a function of the {@link DcbCriteria}, not of the decider. The criteria carries the per-instance identity
 * (the decider is a per-type constant reused across instances), and a change to it is the signal to rebuild a stale
 * snapshot, so a custom key function should stay keyed on the criteria. See ADR 0061 for the full rationale.
 *
 * @param dcbDecider  the decision logic paired with its DCB read boundary and write tags
 * @param store       the snapshot store for this aggregate's state
 * @param options     the schema version and the policy that decides when a new snapshot is written
 * @param keyFunction turns the resolved {@link DcbCriteria} into the snapshot key
 * @param <C>         the command type
 * @param <S>         the snapshot state type
 * @param <E>         the event type
 */
public record ReactiveSnapshotDcbDecider<C, S extends @Nullable Object, E>(
        DcbDecider<C, S, E> dcbDecider,
        ReactiveSnapshotStore<S> store,
        SnapshotOptions<S, E> options,
        Function<DcbCriteria, String> keyFunction
) {

    public ReactiveSnapshotDcbDecider {
        Objects.requireNonNull(dcbDecider, "dcbDecider cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
        Objects.requireNonNull(keyFunction, "keyFunction cannot be null");
    }

    /**
     * Creates a {@code ReactiveSnapshotDcbDecider} keying snapshots by the canonical form of the decider's
     * {@link DcbCriteria} ({@link DcbSnapshotKeys#canonicalKey(DcbCriteria)}).
     */
    public static <C, S extends @Nullable Object, E> ReactiveSnapshotDcbDecider<C, S, E> from(DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return new ReactiveSnapshotDcbDecider<>(dcbDecider, store, options, DcbSnapshotKeys::canonicalKey);
    }

    /**
     * Creates a {@code ReactiveSnapshotDcbDecider} with an explicit {@code keyFunction} for deriving the snapshot key
     * from the decider's {@link DcbCriteria}.
     */
    public static <C, S extends @Nullable Object, E> ReactiveSnapshotDcbDecider<C, S, E> from(DcbDecider<C, S, E> dcbDecider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options, Function<DcbCriteria, String> keyFunction) {
        return new ReactiveSnapshotDcbDecider<>(dcbDecider, store, options, keyFunction);
    }
}
