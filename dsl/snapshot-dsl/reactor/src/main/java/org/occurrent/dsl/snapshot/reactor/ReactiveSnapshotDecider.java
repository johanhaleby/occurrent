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
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.SnapshotOptions;

import java.util.Objects;

/**
 * The reactive per-aggregate snapshot spec for the stream-based decider path, the reactor counterpart to
 * {@link org.occurrent.dsl.snapshot.blocking.SnapshotDecider}: a {@link Decider} bundled with the
 * {@link ReactiveSnapshotStore} that holds its snapshots and the {@link SnapshotOptions} for its state. Create one per
 * aggregate with {@link #from(Decider, ReactiveSnapshotStore, SnapshotOptions)} and pass it to a
 * {@link ReactiveSnapshotDeciderApplicationService}, which is built once around the application service and reused.
 * <p>
 * The spec holds the {@link ReactiveSnapshotStore}, an I/O collaborator, so it is not a pure value, it stays inert and
 * the {@link ReactiveSnapshotDeciderApplicationService} performs all reads and writes.
 *
 * @param decider the decision logic and its fold
 * @param store   the snapshot store for this aggregate's state
 * @param options the schema version and the policy that decides when a new snapshot is written
 * @param <C>     the command type
 * @param <S>     the snapshot state type
 * @param <E>     the event type
 */
public record ReactiveSnapshotDecider<C, S extends @Nullable Object, E>(
        Decider<C, S, E> decider,
        ReactiveSnapshotStore<S> store,
        SnapshotOptions<S, E> options
) {

    public ReactiveSnapshotDecider {
        Objects.requireNonNull(decider, "decider cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
    }

    /**
     * Creates a {@code ReactiveSnapshotDecider} from a decider, its snapshot store, and its options.
     */
    public static <C, S extends @Nullable Object, E> ReactiveSnapshotDecider<C, S, E> from(Decider<C, S, E> decider, ReactiveSnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return new ReactiveSnapshotDecider<>(decider, store, options);
    }
}
