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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.reactor.FluxSubscriptionModel;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Runs a {@link DcbProjection} as an asynchronous, subscription-fed read model over a DCB consistency boundary on the
 * reactor stack: it subscribes to the events matching the projection's {@link DcbProjection#criteria() criteria} and
 * updates the read model from each. The reactor counterpart to the blocking {@code DcbProjectionRunner}.
 * <p>
 * The criteria is used verbatim, not intersected with the handler event types (the fold ignores types it does not
 * handle, so a broader criteria is safe). The read model is updated through a reactive {@code (E) -> Mono<Void>}. The
 * {@link ViewStateRepository}/{@link MaterializedView} overloads drive a blocking view store from the reactive pipeline,
 * scheduled on {@code boundedElastic} (see {@link Projections}).
 * <p>
 * <strong>Whether this catches up and resumes durably, or is live-only, depends on the {@code FluxSubscriptionModel} passed
 * to {@link #create}</strong>, since it subscribes through {@link DcbSubscriptions}, which is only as capable as that
 * model. A catch-up-capable model (the Spring composite, or a hand-wired {@code CatchupSubscriptionModel}) replays
 * history and resumes across restarts, a plain live model does neither. For a strongly consistent read, fold on demand
 * with the pull {@link Projections#project(DcbProjection, org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries)}. For a
 * declarative read model use the {@code @Projection} annotation.
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class ReactiveDcbProjectionRunner<E> {

    private final DcbSubscriptions<E> dcbSubscriptions;

    /**
     * Creates a runner that subscribes through the given {@code subscriptionModel}, matching the factory style of
     * {@link ReactiveProjectionRunner}.
     */
    public static <E> ReactiveDcbProjectionRunner<E> create(FluxSubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new ReactiveDcbProjectionRunner<>(subscriptionModel, cloudEventConverter);
    }

    private ReactiveDcbProjectionRunner(FluxSubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        this.dcbSubscriptions = new DcbSubscriptions<>(
                requireNonNull(subscriptionModel, "subscriptionModel cannot be null"),
                requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null"));
    }

    /**
     * Subscribes with the given id and applies {@code update} for every event matching the projection's DCB criteria.
     * The primitive overload: {@code update} owns the reactive load-evolve-save against a reactive store.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, Function<E, Mono<Void>> update) {
        return project(subscriptionId, dcbProjection, update, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and applies {@code update} for every event matching the projection's DCB criteria.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, Function<E, Mono<Void>> update, @Nullable DcbStartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(update, "update cannot be null");
        return dcbSubscriptions.subscribe(subscriptionId, dcbProjection.criteria(), startAt, update::apply);
    }

    /**
     * Subscribes with the given id and applies {@code update} for every event matching the projection's DCB criteria.
     * The metadata-carrying sibling of {@link #project(String, DcbProjection, Function)}, for a caller that owns the
     * reactive load-evolve-save but still needs the delivering event's {@link EventMetadata}.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, BiFunction<EventMetadata, E, Mono<Void>> update) {
        return project(subscriptionId, dcbProjection, update, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and applies {@code update} for every event matching the projection's DCB criteria, exposing the event's
     * {@link EventMetadata}.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, BiFunction<EventMetadata, E, Mono<Void>> update, @Nullable DcbStartAt startAt) {
        return projectWithMetadata(subscriptionId, dcbProjection, update, startAt);
    }

    /**
     * Subscribes with the given id and materializes {@code dcbProjection} into the blocking {@code repository} (scheduled
     * on {@code boundedElastic}), skipping events whose id resolves to {@code null}.
     *
     * <p>A DCB projection with no id function is single-instance: it has one slot, and that slot is
     * stored under {@code subscriptionId} rather than under a key derived from the events, so read it back
     * with the same id. One that has an id function keys each instance by whatever that function returns
     * for an event.</p>
     *
     * <p>Those are two different nulls, which is worth keeping apart. No id function at all means
     * single-instance. An id function that returns {@code null} for one event means that event is skipped,
     * and the projection is still keyed.</p>
     */
    public <S extends @Nullable Object, ID> SubscriptionHandle project(String subscriptionId, DcbProjection<S, E, ID> dcbProjection, ViewStateRepository<S, ID> repository) {
        return project(subscriptionId, dcbProjection, repository, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and materializes {@code dcbProjection} into the blocking {@code repository} (scheduled on {@code boundedElastic}),
     * skipping events whose id resolves to {@code null}.
     *
     * <p>A DCB projection with no id function is single-instance: it has one slot, and that slot is
     * stored under {@code subscriptionId} rather than under a key derived from the events, so read it back
     * with the same id. One that has an id function keys each instance by whatever that function returns
     * for an event.</p>
     *
     * <p>Those are two different nulls, which is worth keeping apart. No id function at all means
     * single-instance. An id function that returns {@code null} for one event means that event is skipped,
     * and the projection is still keyed.</p>
     */
    public <S extends @Nullable Object, ID> SubscriptionHandle project(String subscriptionId, DcbProjection<S, E, ID> dcbProjection, ViewStateRepository<S, ID> repository, @Nullable DcbStartAt startAt) {
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        return projectWithMetadata(subscriptionId, dcbProjection, Projections.reactiveUpdateWithMetadata(dcbProjection.projection(), repository, subscriptionId), startAt);
    }

    /**
     * Subscribes with the given id and drives the blocking {@code materializedView} (scheduled on {@code boundedElastic})
     * for every event matching the projection's DCB criteria.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, MaterializedView<E> materializedView) {
        return project(subscriptionId, dcbProjection, materializedView, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and drives the blocking {@code materializedView} (scheduled on {@code boundedElastic}) for every event matching
     * the projection's DCB criteria.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, MaterializedView<E> materializedView, @Nullable DcbStartAt startAt) {
        return projectWithMetadata(subscriptionId, dcbProjection, Projections.reactiveUpdateWithMetadata(materializedView), startAt);
    }

    // Routes through subscribeWithMetadata so the ViewStateRepository/MaterializedView overloads above carry real DCB
    // delivery metadata into the fold, instead of the plain subscribe() path, which has none to give.
    private SubscriptionHandle projectWithMetadata(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, BiFunction<EventMetadata, E, Mono<Void>> update, @Nullable DcbStartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(update, "update cannot be null");
        return dcbSubscriptions.subscribeWithMetadata(subscriptionId, dcbProjection.criteria(), startAt,
                (dcbMetadata, event) -> update.apply(dcbMetadata.eventMetadata(), event));
    }
}
