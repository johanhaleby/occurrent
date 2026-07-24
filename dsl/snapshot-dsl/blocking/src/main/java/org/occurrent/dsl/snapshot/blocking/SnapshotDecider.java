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

package org.occurrent.dsl.snapshot.blocking;

import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.decider.Decider;
import org.occurrent.dsl.snapshot.SnapshotOptions;
import org.occurrent.dsl.snapshot.SnapshotStore;

import java.util.Objects;

/**
 * The per-aggregate snapshot spec for the stream-based decider path: a {@link Decider} bundled with the
 * {@link SnapshotStore} that holds its snapshots and the {@link SnapshotOptions} (schema version plus write policy) for
 * its state. Create one per aggregate with {@link #from(Decider, SnapshotStore, SnapshotOptions)} and pass it to a
 * {@link SnapshotDeciderApplicationService}, which is built once around the application service and reused across every
 * aggregate. This mirrors how {@link org.occurrent.dsl.dcb.DcbDecider} pairs a decider with its DCB read boundary.
 * <p>
 * Unlike {@link Decider}, this is not a pure value: it holds the {@link SnapshotStore}, an I/O collaborator. The spec
 * itself stays inert, the {@link SnapshotDeciderApplicationService} performs all reads and writes, and the wrapped
 * {@link Decider} remains independently pure and testable.
 *
 * @param decider the decision logic and its fold
 * @param store   the snapshot store for this aggregate's state
 * @param options the schema version and the policy that decides when a new snapshot is written
 * @param <C>     the command type
 * @param <S>     the snapshot state type
 * @param <E>     the event type
 */
public record SnapshotDecider<C, S extends @Nullable Object, E>(
        Decider<C, S, E> decider,
        SnapshotStore<S> store,
        SnapshotOptions<S, E> options
) {

    public SnapshotDecider {
        Objects.requireNonNull(decider, "decider cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(options, "options cannot be null");
    }

    /**
     * Creates a {@code SnapshotDecider} from a decider, its snapshot store, and its options. Equivalent to the canonical
     * constructor, provided as a static factory for a more fluent call site (mirrors {@link org.occurrent.dsl.dcb.DcbDecider#from}).
     */
    public static <C, S extends @Nullable Object, E> SnapshotDecider<C, S, E> from(Decider<C, S, E> decider, SnapshotStore<S> store, SnapshotOptions<S, E> options) {
        return new SnapshotDecider<>(decider, store, options);
    }
}
