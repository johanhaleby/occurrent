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

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.snapshot.SnapshotDecision;
import org.occurrent.dsl.snapshot.SnapshotPolicy;
import org.occurrent.dsl.snapshot.SnapshotSupport;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.eventstore.api.reactor.EventStore;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * Reads the current state of a {@link SnapshotView} on demand, folding only the events written after the stored snapshot.
 * This is the deciders-free, read-side counterpart to {@link ReactiveSnapshotDeciderApplicationService}: there is no
 * command and nothing is appended, the view state is simply rebuilt from the snapshot plus the tail and returned, and a
 * fresh snapshot is written when the policy fires.
 */
@NullMarked
public final class ReactiveSnapshotViews {

    private ReactiveSnapshotViews() {
    }

    /**
     * Read the current state for {@code streamId} by resuming {@code snapshotView} from the snapshot in {@code store} and
     * folding the events written after it. Writes a refreshed snapshot when {@code policy} fires. A loaded snapshot whose
     * schema version does not match the view is ignored and the state is rebuilt from the whole stream.
     */
    public static <S extends @Nullable Object, E> Mono<S> readState(EventStore eventStore, CloudEventConverter<E> converter, String streamId,
                                                                    SnapshotView<S, E> snapshotView, ReactiveSnapshotStore<S> store,
                                                                    SnapshotPolicy<S, E> policy) {
        Objects.requireNonNull(eventStore, "eventStore cannot be null");
        Objects.requireNonNull(converter, "converter cannot be null");
        Objects.requireNonNull(streamId, "streamId cannot be null");
        Objects.requireNonNull(snapshotView, "snapshotView cannot be null");
        Objects.requireNonNull(store, "store cannot be null");
        Objects.requireNonNull(policy, "policy cannot be null");

        return Mono.defer(() -> ReactiveSnapshotSupport.resolveBase(store, streamId, snapshotView.schemaVersion(), snapshotView.view()::initialState).flatMap(base ->
                eventStore.read(streamId, SnapshotSupport.requireInt(base.version(), "the snapshot base stream version"), Integer.MAX_VALUE).flatMap(eventStream ->
                        eventStream.events().collectList().flatMap(cloudEvents -> {
                            List<E> tail = converter.toDomainEvents(cloudEvents.stream()).toList();
                            S current = snapshotView.view().evolve(base.state(), tail);
                            long version = eventStream.version();
                            // On the read side the policy sees the tail it folded as the "new events", so always()/onEvent(...)
                            // stay meaningful and everyNEvents rides the version delta.
                            return ReactiveSnapshotSupport.maybeSaveBestEffort(store, streamId, snapshotView.schemaVersion(), policy,
                                            new SnapshotDecision<>(current, tail, version, SnapshotSupport.requireInt(version - base.version(), "the number of events since the snapshot")))
                                    .thenReturn(current);
                        }))));
    }
}
