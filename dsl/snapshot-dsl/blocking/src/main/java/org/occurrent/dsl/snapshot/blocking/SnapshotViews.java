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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.SnapshotStore;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;

import static java.util.Objects.requireNonNull;

/**
 * On-demand, deciders-free access to a {@link SnapshotView}'s state, backed by a {@link SnapshotStore}. Build one over
 * the event store, the cloud event converter, and a store, then reuse it. It is the read-side counterpart to
 * {@link SnapshotDeciderApplicationService}: there is no command and nothing is appended.
 * <p>
 * {@link #readState(String, SnapshotView)} is a plain read: it resumes the view from the stored snapshot, folds the
 * events written since, and returns the current state, without writing anything. {@link #refresh(String, SnapshotView)}
 * is the explicit maintenance write: it folds to the current head and persists a fresh snapshot. Snapshotting therefore
 * never happens as a hidden side effect of a read, and there is no {@code SnapshotPolicy} on this path (a policy is the
 * automatic write trigger used by {@code @Snapshot} and the decider executors).
 */
@NullMarked
public final class SnapshotViews<S extends @Nullable Object, E> {

    private final EventStore eventStore;
    private final CloudEventConverter<E> converter;
    private final SnapshotStore<S> store;

    private SnapshotViews(EventStore eventStore, CloudEventConverter<E> converter, SnapshotStore<S> store) {
        this.eventStore = requireNonNull(eventStore, "eventStore cannot be null");
        this.converter = requireNonNull(converter, "converter cannot be null");
        this.store = requireNonNull(store, "store cannot be null");
    }

    /** Creates a reader/refresher over {@code eventStore}, {@code converter}, and the snapshot {@code store}. */
    public static <S extends @Nullable Object, E> SnapshotViews<S, E> create(EventStore eventStore, CloudEventConverter<E> converter, SnapshotStore<S> store) {
        return new SnapshotViews<>(eventStore, converter, store);
    }

    /**
     * Returns the current state for {@code streamId}, resuming {@code snapshotView} from the snapshot in the store and
     * folding the events written after it. This is a pure read, it never writes a snapshot. A loaded snapshot whose
     * schema version does not match the view is ignored and the state is rebuilt from the whole stream.
     */
    public S readState(String streamId, SnapshotView<S, E> snapshotView) {
        requireNonNull(streamId, "streamId cannot be null");
        requireNonNull(snapshotView, "snapshotView cannot be null");
        return foldToHead(streamId, snapshotView).state();
    }

    /**
     * Folds {@code streamId} to its current head and persists a fresh snapshot for {@code snapshotView}. This is an
     * explicit maintenance call, so it always writes and lets a store failure surface, unlike the best-effort save on
     * the write path where there is a committed command to protect.
     */
    public void refresh(String streamId, SnapshotView<S, E> snapshotView) {
        requireNonNull(streamId, "streamId cannot be null");
        requireNonNull(snapshotView, "snapshotView cannot be null");
        Folded<S> folded = foldToHead(streamId, snapshotView);
        store.save(streamId, new Snapshot<>(folded.state(), folded.version(), snapshotView.schemaVersion()));
    }

    private Folded<S> foldToHead(String streamId, SnapshotView<S, E> snapshotView) {
        SnapshotSupport.Base<S> base = SnapshotSupport.resolveBase(store.findLatest(streamId), snapshotView.schemaVersion(), snapshotView.view()::initialState);
        EventStream<CloudEvent> eventStream = eventStore.read(streamId, SnapshotSupport.requireInt(base.version(), "the snapshot base stream version"), Integer.MAX_VALUE);
        S current = snapshotView.view().evolve(base.state(), converter.toDomainEvents(eventStream.events()));
        return new Folded<>(current, eventStream.version());
    }

    private record Folded<S extends @Nullable Object>(S state, long version) {
    }
}
