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
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionKeys;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.filter.Filter;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

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
     * Throws if the projection is single-instance (it has no id function), use
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
        BiFunction<EventMetadata, E, @Nullable ID> id = projection.idWithMetadata();
        requireNonNull(id, "projection is single-instance; use materializedView(projection, repository, singletonKey)");
        View<S, E> view = projection.view();
        return new MaterializedView<>() {
            @Override
            public void update(E event) {
                update(EventMetadata.empty(), event);
            }

            @Override
            public void update(EventMetadata metadata, E event) {
                @Nullable ID instanceId = id.apply(metadata, event);
                if (instanceId == null) {
                    ProjectionKeys.failIfKeyNeededMetadata(projection.metadataKeyed(), metadata);
                    return;
                }
                S currentState = repository.findById(instanceId).orElse(view.initialState());
                repository.save(instanceId, view.evolve(currentState, metadata, event));
            }
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
        return new MaterializedView<>() {
            @Override
            public void update(E event) {
                update(EventMetadata.empty(), event);
            }

            @Override
            public void update(EventMetadata metadata, E event) {
                S currentState = repository.findById(key).orElse(view.initialState());
                repository.save(key, view.evolve(currentState, metadata, event));
            }
        };
    }

    /**
     * A sink that folds a <strong>domain event</strong> straight into {@code repository} for a keyed projection, with no
     * CloudEvent conversion. Use it to drive a projection from a source that already hands you domain events (a RabbitMQ
     * or Kafka listener with its own message converter), so a live event is folded directly rather than round-tripped
     * through {@code toCloudEvent}/{@code toDomainEvent}. An event whose id resolves to {@code null} is skipped, and the
     * fold no-ops on an event type the projection does not handle. This is the live-tail feed only, with no catch-up:
     * backfill a new or rebuilt projection from the event store first.
     * <p>
     * Returns the {@link MaterializedView} rather than a bare {@code Consumer<E>} so the metadata channel survives. Call
     * {@link MaterializedView#update(Object)} when the source gives you only the event, and
     * {@link MaterializedView#update(EventMetadata, Object)} when it also gives you the stream id, version or position,
     * which is what a projection keyed on metadata needs to work on the live path.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return materializedView(projection, repository);
    }

    /**
     * A domain-event feed for a keyed or single-instance projection, folding directly into {@code repository} with no
     * CloudEvent conversion. A single-instance projection (no id function) updates one slot keyed by
     * {@code singletonKey}. See {@link #domainEventFeed(Projection, ViewStateRepository)}.
     */
    public static <S extends @Nullable Object, E, ID> MaterializedView<E> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return materializedView(projection, repository, singletonKey);
    }

    /**
     * Folds the events {@code projection} selects, read on demand, into its view state and returns it: the
     * strongly-consistent, query-driven counterpart to the subscription-fed {@code project(subscriptionId, projection,
     * ...)} runners. Uses the projection's explicit {@link Projection#filter() filter} if set, else its handled
     * {@link Projection#eventTypes() event types} (empty means "all events"). Only valid for a single-instance
     * (singleton) projection. A keyed projection throws, since folding every instance into one blended state on demand
     * would produce a nonsense result. Use {@link #project(Projection, DomainEventQueries, Object)} with an
     * {@code instanceId} for a keyed projection.
     */
    public static <S extends @Nullable Object, E, ID> S project(Projection<S, E, ID> projection, DomainEventQueries<E> queries) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        if (projection.id() != null) {
            throw new IllegalArgumentException("projection is keyed; folding every instance into one blended state on demand is not supported, "
                    + "use project(projection, queries, instanceId) to read a single instance, or a singleton projection for one shared state");
        }
        Stream<E> events = selectEvents(projection, queries);
        return projection.view().evolve(events);
    }

    /**
     * Folds the events {@code projection} selects for {@code instanceId}, read on demand, into that instance's view
     * state and returns it: the strongly-consistent, query-driven, single-instance counterpart to the unqualified
     * {@link #project(Projection, DomainEventQueries)}. Uses the same filter or handled event types as the unqualified
     * {@code project} to read candidate events, then keeps only the ones whose {@link Projection#id()} resolves to
     * {@code instanceId} before folding. A singleton projection (no id function) has a single instance regardless of
     * {@code instanceId}, so this folds all selected events, same as the unqualified {@code project}.
     */
    public static <S extends @Nullable Object, E, ID> S project(Projection<S, E, ID> projection, DomainEventQueries<E> queries, ID instanceId) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        requireNonNull(instanceId, "instanceId cannot be null");
        Stream<E> events = selectEvents(projection, queries);
        Function<E, @Nullable ID> id = projection.id();
        Stream<E> scopedEvents = id == null ? events : events.filter(event -> instanceId.equals(id.apply(event)));
        return projection.view().evolve(scopedEvents);
    }

    private static <S extends @Nullable Object, E, ID> Stream<E> selectEvents(Projection<S, E, ID> projection, DomainEventQueries<E> queries) {
        Filter explicitFilter = projection.filter();
        if (explicitFilter != null) {
            return queries.query(explicitFilter);
        } else if (projection.eventTypes().isEmpty()) {
            return queries.all();
        } else {
            return queries.query(projection.eventTypes());
        }
    }

    /**
     * Folds the events matching {@code dcbProjection}'s DCB criteria, read on demand, into its view state and returns
     * it: the strongly-consistent, query-driven counterpart to the subscription-fed {@code DcbProjectionRunner}. This is
     * the shape of a single-instance DCB projection such as "is this username claimed?", where the criteria itself
     * already scopes the read, so there is no keyed/singleton ambiguity to guard against.
     */
    public static <S extends @Nullable Object, E, ID> S project(DcbProjection<S, E, ID> dcbProjection, DcbDomainEventQueries<E> queries) {
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(queries, "queries cannot be null");
        return dcbProjection.projection().view().evolve(queries.query(dcbProjection.criteria()));
    }
}
