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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.snapshot.Snapshot;
import org.occurrent.dsl.snapshot.internal.SnapshotSupport;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.view.View;
import org.occurrent.eventstore.api.reactor.EventStore;
import reactor.core.publisher.Mono;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * On-demand, deciders-free access to a {@link SnapshotView}'s state, backed by a {@link ReactiveSnapshotStore}. Build
 * one over the event store and the cloud event converter, then reuse it for every view, passing a
 * {@link ReactiveSnapshotViewSource} (the per-aggregate spec that bundles the view with its store) per call. It is the
 * read-side counterpart to {@link ReactiveSnapshotDeciderApplicationService}: there is no command and nothing is
 * appended. The state is bound to a non-null type because a {@link Mono} cannot carry a null value.
 * <p>
 * {@link #readState(String, ReactiveSnapshotViewSource)} is a plain read: it resumes the view from the stored snapshot,
 * folds the events written since, and returns the current state, without writing anything.
 * {@link #refresh(String, ReactiveSnapshotViewSource)} is the explicit maintenance write: it folds to the current head
 * and persists a fresh snapshot. Snapshotting therefore never happens as a hidden side effect of a read, and there is no
 * {@code SnapshotPolicy} on this path.
 *
 * @param <E> the event type
 */
@NullMarked
public final class ReactiveSnapshotViews<E> {

    private final EventStore eventStore;
    private final CloudEventConverter<E> converter;

    private ReactiveSnapshotViews(EventStore eventStore, CloudEventConverter<E> converter) {
        this.eventStore = requireNonNull(eventStore, "eventStore cannot be null");
        this.converter = requireNonNull(converter, "converter cannot be null");
    }

    /** Creates a reader/refresher over {@code eventStore} and {@code converter}. */
    public static <E> ReactiveSnapshotViews<E> create(EventStore eventStore, CloudEventConverter<E> converter) {
        return new ReactiveSnapshotViews<>(eventStore, converter);
    }

    /**
     * Returns the current state for {@code streamId}, resuming {@code source}'s view from the snapshot in its store and
     * folding the events written after it. This is a pure read, it never writes a snapshot. A loaded snapshot whose
     * schema version does not match the view is ignored and the state is rebuilt from the whole stream.
     */
    public <S> Mono<S> readState(String streamId, ReactiveSnapshotViewSource<S, E> source) {
        requireNonNull(streamId, "streamId cannot be null");
        requireNonNull(source, "source cannot be null");
        return foldToHead(streamId, source).map(Folded::state);
    }

    /**
     * Folds {@code streamId} to its current head and persists a fresh snapshot for {@code source}'s view. This is an
     * explicit maintenance call, so it always writes and lets a store failure surface, unlike the best-effort save on
     * the write path where there is a committed command to protect.
     */
    public <S> Mono<Void> refresh(String streamId, ReactiveSnapshotViewSource<S, E> source) {
        requireNonNull(streamId, "streamId cannot be null");
        requireNonNull(source, "source cannot be null");
        return foldToHead(streamId, source)
                .flatMap(folded -> source.store().save(streamId, new Snapshot<>(folded.state(), folded.version(), source.view().schemaVersion())));
    }

    private <S> Mono<Folded<S>> foldToHead(String streamId, ReactiveSnapshotViewSource<S, E> source) {
        ReactiveSnapshotStore<S> store = source.store();
        SnapshotView<S, E> snapshotView = source.view();
        return Mono.defer(() -> ReactiveSnapshotSupport.resolveBase(store, streamId, snapshotView.schemaVersion(), snapshotView.view()::initialState).flatMap(base ->
                eventStore.read(streamId, SnapshotSupport.requireInt(base.version(), "the snapshot base stream version"), Integer.MAX_VALUE).flatMap(eventStream ->
                        eventStream.events()
                                .collectList()
                                .map(cloudEvents -> foldWithMetadata(snapshotView.view(), base.state(), cloudEvents, converter))
                                .map(current -> new Folded<>(current, eventStream.version())))));
    }

    // A Mono cannot carry null, so a view whose initial state is null or that evolves to a null state fails fast with
    // guidance instead of Reactor's bare "reducer returned a null value" NPE. Use the blocking SnapshotViews for a
    // nullable state.
    private static <S> S requireNonNullState(S state) {
        return requireNonNull(state, "The snapshot view folded to a null state, but a Mono cannot carry null. Use the blocking SnapshotViews for a nullable state.");
    }

    // Folds a range one CloudEvent at a time so each event keeps its own metadata. View.evolve(state, event) folds
    // through the two-argument evolve, which substitutes EventMetadata.empty(), and a metadata-reading fold cannot
    // tolerate that. Folded synchronously over an already-collected list, never blocking the reactive chain.
    private static <S, E> S foldWithMetadata(View<S, E> view, S state, List<CloudEvent> cloudEvents, CloudEventConverter<E> converter) {
        S result = requireNonNullState(state);
        for (CloudEvent cloudEvent : cloudEvents) {
            result = requireNonNullState(view.evolve(result, EventMetadata.from(cloudEvent), converter.toDomainEvent(cloudEvent)));
        }
        return result;
    }

    private record Folded<S>(S state, long version) {
    }
}
