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

package org.occurrent.dsl.projection.reactor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Internal helpers shared by the reactor projection runners (the Kotlin {@code project} extensions and the Java
 * {@code Reactive*ProjectionRunner} facades): adapting a blocking view store into the reactive {@code (E) -> Mono<Void>}
 * update the runners consume. The plain-{@code Filter} derivation lives in
 * {@code org.occurrent.dsl.projection.internal.ProjectionFilters}, shared with the blocking stack.
 * <p>
 * There is no reactive {@link MaterializedView}/{@link ViewStateRepository} in the view DSL, so a genuinely reactive
 * store should be driven through a caller-supplied {@code (E) -> Mono<Void>} update (the runners' primitive overload).
 * The helpers here bridge the <em>blocking</em> view store into a reactive pipeline by scheduling its blocking work on
 * {@link Schedulers#boundedElastic()}. A reactive-store convenience (a reactive {@code ViewStateRepository}) is a
 * follow-up. Note that the {@code boundedElastic} bridge runs on a different thread than the writer, so it is <em>not</em>
 * suitable for synchronous, in-transaction (read-your-writes) dispatch. Use the reactive-primitive update there.
 */
@NullMarked
public final class Projections {

    private Projections() {
    }

    /**
     * A reactive {@code (E) -> Mono<Void>} update that loads, evolves, and saves the projection's view state through the
     * blocking {@code repository}, keyed by the projection's {@link Projection#id() id}, with the blocking work scheduled
     * on {@link Schedulers#boundedElastic()}. An event whose id resolves to {@code null} is skipped. This does a plain
     * read, fold, and save with no optimistic-locking retry of its own. A failed update is still retried by the
     * subscription model's retry strategy, which redelivers the event, but for concurrent writers to the same instance
     * supply a {@link MaterializedView} that re-reads and reapplies on conflict through
     * {@link #reactiveUpdate(MaterializedView)}.
     */
    public static <S extends @Nullable Object, E, ID> Function<E, Mono<Void>> reactiveUpdate(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        Function<E, @Nullable ID> id = projection.id();
        requireNonNull(id, "projection is single-instance; use reactiveUpdate(projection, repository, singletonKey)");
        View<S, E> view = projection.view();
        return event -> Mono.<Void>fromRunnable(() -> {
            @Nullable ID instanceId = id.apply(event);
            if (instanceId == null) {
                return;
            }
            S currentState = repository.findById(instanceId).orElse(view.initialState());
            repository.save(instanceId, view.evolve(currentState, event));
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * A reactive update for a keyed or single-instance projection. A single-instance projection (no id function) updates
     * one slot keyed by {@code singletonKey}, the projection's runtime identity (the subscription id).
     */
    @SuppressWarnings("unchecked")
    public static <S extends @Nullable Object, E, ID> Function<E, Mono<Void>> reactiveUpdate(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(repository, "repository cannot be null");
        if (projection.id() != null) {
            return reactiveUpdate(projection, repository);
        }
        requireNonNull(singletonKey, "singletonKey cannot be null");
        View<S, E> view = projection.view();
        ID key = (ID) singletonKey; // a single-instance projection is Projection<S, E, String>
        return event -> Mono.<Void>fromRunnable(() -> {
            S currentState = repository.findById(key).orElse(view.initialState());
            repository.save(key, view.evolve(currentState, event));
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * A reactive {@code (E) -> Mono<Void>} update that calls the blocking {@code materializedView.update(event)} on
     * {@link Schedulers#boundedElastic()}. Use this to drive a blocking {@link MaterializedView} (for example the one the
     * view DSL's {@code materialized(...)} builds, with its own retry/locking policy) from a reactive pipeline.
     */
    public static <E> Function<E, Mono<Void>> reactiveUpdate(MaterializedView<E> materializedView) {
        requireNonNull(materializedView, "materializedView cannot be null");
        return event -> Mono.<Void>fromRunnable(() -> materializedView.update(event)).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * A reactive {@code (E) -> Mono<Void>} feed that folds a <strong>domain event</strong> straight into the blocking
     * {@code repository} for a keyed projection (on {@link Schedulers#boundedElastic()}), with no CloudEvent conversion.
     * Use it to drive a projection from a source that already hands you domain events (a RabbitMQ or Kafka listener with
     * its own message converter), so a live event is folded directly rather than round-tripped through
     * {@code toCloudEvent}/{@code toDomainEvent}. The reactor counterpart of the blocking
     * {@code Projections.domainEventFeed(...)}, and the same shape as {@link #reactiveUpdate(Projection, ViewStateRepository)}.
     * This is the live-tail feed only, with no catch-up.
     */
    public static <S extends @Nullable Object, E, ID> Function<E, Mono<Void>> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return reactiveUpdate(projection, repository);
    }

    /**
     * A reactive domain-event feed for a keyed or single-instance projection, folding directly into {@code repository}
     * with no CloudEvent conversion. See {@link #domainEventFeed(Projection, ViewStateRepository)}.
     */
    public static <S extends @Nullable Object, E, ID> Function<E, Mono<Void>> domainEventFeed(Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, String singletonKey) {
        return reactiveUpdate(projection, repository, singletonKey);
    }
}
