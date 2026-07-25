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
import org.occurrent.dsl.snapshot.SnapshotView;

import java.util.Objects;

/**
 * The per-aggregate snapshot spec for the deciders-free read path: a {@link SnapshotView} (the fold plus its schema
 * version) bundled with the {@link SnapshotStore} that holds its snapshots. Create one per aggregate with
 * {@link #from(SnapshotView, SnapshotStore)} and pass it to a {@link SnapshotViews} facade, which is built once around
 * the event store and the cloud event converter and reused across every view.
 * <p>
 * It is the read-side counterpart to {@link SnapshotDecider}: there is no command and nothing is decided, only a view
 * to fold and a store to resume from. Named {@code SnapshotViewSource} to stay distinct from {@link SnapshotView} (the
 * fold) and {@link SnapshotViews} (the facade).
 *
 * @param view  the fold and its schema version
 * @param store the snapshot store for this view's state
 * @param <S>   the snapshot state type
 * @param <E>   the event type
 */
public record SnapshotViewSource<S extends @Nullable Object, E>(
        SnapshotView<S, E> view,
        SnapshotStore<S> store
) {

    public SnapshotViewSource {
        Objects.requireNonNull(view, "view cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
    }

    /**
     * Creates a {@code SnapshotViewSource} from a view and its snapshot store.
     */
    public static <S extends @Nullable Object, E> SnapshotViewSource<S, E> from(SnapshotView<S, E> view, SnapshotStore<S> store) {
        return new SnapshotViewSource<>(view, store);
    }
}
