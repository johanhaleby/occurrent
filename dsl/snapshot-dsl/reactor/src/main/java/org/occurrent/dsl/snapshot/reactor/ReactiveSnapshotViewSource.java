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

import org.occurrent.dsl.snapshot.SnapshotView;

import java.util.Objects;

/**
 * The reactive per-aggregate snapshot spec for the deciders-free read path, the reactor counterpart to
 * {@link org.occurrent.dsl.snapshot.blocking.SnapshotViewSource}: a {@link SnapshotView} bundled with the
 * {@link ReactiveSnapshotStore} that holds its snapshots. Create one per aggregate with
 * {@link #from(SnapshotView, ReactiveSnapshotStore)} and pass it to a {@link ReactiveSnapshotViews} facade, built once
 * around the event store and the cloud event converter and reused across every view.
 * <p>
 * The state is bound to a non-null type because a {@link reactor.core.publisher.Mono} cannot carry a null value.
 *
 * @param view  the fold and its schema version
 * @param store the snapshot store for this view's state
 * @param <S>   the snapshot state type
 * @param <E>   the event type
 */
public record ReactiveSnapshotViewSource<S, E>(
        SnapshotView<S, E> view,
        ReactiveSnapshotStore<S> store
) {

    public ReactiveSnapshotViewSource {
        Objects.requireNonNull(view, "view cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
    }

    /**
     * Creates a {@code ReactiveSnapshotViewSource} from a view and its reactive snapshot store.
     */
    public static <S, E> ReactiveSnapshotViewSource<S, E> from(SnapshotView<S, E> view, ReactiveSnapshotStore<S> store) {
        return new ReactiveSnapshotViewSource<>(view, store);
    }
}
