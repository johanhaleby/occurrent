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

package org.occurrent.dsl.projection.blocking;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * The public factory for assembling blocking projection runners (the Kotlin {@code project} extensions and the Java
 * {@code *ProjectionRunner} facades): turns a {@link Projection} plus a {@link ViewStateRepository} into a
 * {@link MaterializedView} that skips events whose id resolves to {@code null}. The plain-{@code Filter} derivation
 * lives in {@code org.occurrent.dsl.projection.internal.ProjectionFilters}, shared with the reactor stack.
 */
@NullMarked
public final class Projections {

    private Projections() {
    }

    /**
     * A {@link MaterializedView} for a keyed projection: loads, evolves, and saves the view state through
     * {@code repository}, keyed by the projection's {@link Projection#id() id}. An event whose id resolves to
     * {@code null} is skipped (no load or save), so a projection can safely see events that map to no keyed instance.
     * Throws if the projection is single-instance (it has no id function); use
     * {@link #materializedView(Projection, ViewStateRepository, String)} with the single-instance key for those.
     * <p>
     * This does a plain read, fold, and save with no optimistic-locking retry of its own. A failed update is still
     * retried by the subscription model's retry strategy, which redelivers the event, but for concurrent writers to the
     * same instance supply a {@link MaterializedView} that re-reads and reapplies on conflict, such as the one the view
     * DSL's {@code materialized(...)} builds.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        Function<E, @Nullable ID> id = projection.id();
        requireNonNull(id, "projection is single-instance; use materializedView(projection, repository, singletonKey)");
        View<S, E> view = projection.view();
        return event -> {
            @Nullable ID instanceId = id.apply(event);
            if (instanceId == null) {
                return;
            }
            S currentState = repository.findById(instanceId).orElse(view.initialState());
            repository.save(instanceId, view.evolve(currentState, event));
        };
    }

    /**
     * A {@link MaterializedView} for a keyed or single-instance projection. A keyed projection behaves like
     * {@link #materializedView(Projection, ViewStateRepository)}. A single-instance projection (no id function) updates
     * one slot keyed by {@code singletonKey}, the projection's runtime identity (the subscription id, or the
     * {@code @Projection} id).
     */
    @SuppressWarnings("unchecked")
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> materializedView(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        if (projection.id() != null) {
            return materializedView(projection, repository);
        }
        requireNonNull(singletonKey, "singletonKey cannot be null");
        View<S, E> view = projection.view();
        ID key = (ID) singletonKey; // a single-instance projection is Projection<S, E, String>
        return event -> {
            S currentState = repository.findById(key).orElse(view.initialState());
            repository.save(key, view.evolve(currentState, event));
        };
    }

    /**
     * A {@link Consumer} that folds a <strong>domain event</strong> straight into {@code repository} for a keyed
     * projection, with no CloudEvent conversion. Use it to drive a projection from a source that already hands you
     * domain events (a RabbitMQ or Kafka listener with its own message converter), so a live event is folded directly
     * rather than round-tripped through {@code toCloudEvent}/{@code toDomainEvent}. An event whose id resolves to
     * {@code null} is skipped, and the fold no-ops on an event type the projection does not handle. This is the live-tail
     * feed only, with no catch-up: backfill a new or rebuilt projection from the event store first.
     */
    public static <S extends @Nullable Object, E, ID> Consumer<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return materializedView(projection, repository)::update;
    }

    /**
     * A {@link Consumer} domain-event feed for a keyed or single-instance projection, folding directly into
     * {@code repository} with no CloudEvent conversion. A single-instance projection (no id function) updates one slot
     * keyed by {@code singletonKey}. See {@link #domainEventFeed(Projection, ViewStateRepository)}.
     */
    public static <S extends @Nullable Object, E, ID> Consumer<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return materializedView(projection, repository, singletonKey)::update;
    }
}
