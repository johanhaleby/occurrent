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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.internal.ProjectionFilters;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.occurrent.subscription.api.reactor.Subscription;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Runs a {@link Projection} as an asynchronous, subscription-fed read model on the reactor stack: it creates the
 * subscription and updates the read model from every matching event, in one call. The reactor counterpart to the
 * blocking {@code ProjectionRunner}, and the Java counterpart to the Kotlin {@code project(...)} extensions on the
 * reactor subscription DSL.
 * <p>
 * Choose the capability with the factory: {@link #agnostic(Subscribable, CloudEventConverter) agnostic} delivers both
 * stream-written and DCB-appended events (filtered only by the projection's selector), while
 * {@link #stream(Subscribable, CloudEventConverter) stream} scopes delivery to stream-written events. For a DCB
 * consistency-boundary read model use {@link ReactiveDcbProjectionRunner} instead.
 * <p>
 * The read model is updated through a reactive {@code (E) -> Mono<Void>} update. Supply one directly for a genuinely
 * reactive store, or use the {@link ViewStateRepository}/{@link MaterializedView} overloads to drive a blocking view
 * store from the reactive pipeline (scheduled on {@code boundedElastic}, see {@link Projections}).
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class ReactiveProjectionRunner<E> {

    private final Subscribable subscriptionModel;
    private final CloudEventConverter<E> cloudEventConverter;
    private final Function<Filter, SubscriptionFilter> toSubscriptionFilter;

    private ReactiveProjectionRunner(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter, Function<Filter, SubscriptionFilter> toSubscriptionFilter) {
        this.subscriptionModel = requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.cloudEventConverter = requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null");
        this.toSubscriptionFilter = requireNonNull(toSubscriptionFilter, "toSubscriptionFilter cannot be null");
    }

    /**
     * A runner whose subscriptions are capability-agnostic: on a store with both the {@code STREAM} and {@code DCB}
     * capabilities they deliver both stream-written and DCB-appended events, filtered only by the projection's selector.
     */
    public static <E> ReactiveProjectionRunner<E> agnostic(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new ReactiveProjectionRunner<>(subscriptionModel, cloudEventConverter, AgnosticSubscriptionFilter::filter);
    }

    /**
     * A runner whose subscriptions are scoped to the {@code STREAM} capability, excluding DCB-appended events.
     */
    public static <E> ReactiveProjectionRunner<E> stream(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new ReactiveProjectionRunner<>(subscriptionModel, cloudEventConverter, StreamSubscriptionFilter::filter);
    }

    /**
     * Subscribes with the given id and applies {@code update} for every matching event. This is the primitive overload:
     * {@code update} owns the reactive load-evolve-save against a reactive store, and is also the overload to use for
     * synchronous, in-transaction (read-your-writes) dispatch, since its work composes into the writer's reactive chain.
     */
    public Subscription project(String subscriptionId, Projection<?, E, ?> projection, Function<E, Mono<Void>> update) {
        return project(subscriptionId, projection, update, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and applies {@code update} for every matching event.
     */
    public Subscription project(String subscriptionId, Projection<?, E, ?> projection, Function<E, Mono<Void>> update, @Nullable StartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(update, "update cannot be null");
        SubscriptionFilter filter = toSubscriptionFilter.apply(ProjectionFilters.filterFor(cloudEventConverter, projection));
        Function<CloudEvent, Mono<Void>> action = cloudEvent -> update.apply(cloudEventConverter.toDomainEvent(cloudEvent));
        StartAt effectiveStartAt = startAt != null ? startAt : StartAt.subscriptionModelDefault();
        return subscriptionModel.subscribe(subscriptionId, filter, effectiveStartAt, action);
    }

    /**
     * Subscribes with the given id and applies {@code update} for every matching event. The metadata-carrying sibling of
     * {@link #project(String, Projection, Function)}, for a caller that owns the reactive load-evolve-save but still
     * needs the delivering event's {@link EventMetadata}.
     */
    public Subscription project(String subscriptionId, Projection<?, E, ?> projection, BiFunction<EventMetadata, E, Mono<Void>> update) {
        return project(subscriptionId, projection, update, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and applies {@code update} for every matching event, exposing the event's {@link EventMetadata}.
     */
    public Subscription project(String subscriptionId, Projection<?, E, ?> projection, BiFunction<EventMetadata, E, Mono<Void>> update, @Nullable StartAt startAt) {
        return projectWithMetadata(subscriptionId, projection, update, startAt);
    }

    /**
     * Subscribes with the given id and materializes {@code projection} into the blocking {@code repository} (scheduled on
     * {@code boundedElastic}), skipping events whose id resolves to {@code null}.
     *
     * <p>A projection with no id function is single-instance: it has one slot, and that slot is
     * stored under {@code subscriptionId} rather than under a key derived from the events, so read it back
     * with the same id. A projection that has an id function keys each instance by whatever that function
     * returns for an event.</p>
     *
     * <p>Those are two different nulls, which is worth keeping apart. No id function at all means
     * single-instance. An id function that returns {@code null} for one event means that event is skipped,
     * and the projection is still keyed.</p>
     */
    public <S extends @Nullable Object, ID> Subscription project(String subscriptionId, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return project(subscriptionId, projection, repository, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and materializes {@code projection} into the blocking {@code repository} (scheduled on {@code boundedElastic}),
     * skipping events whose id resolves to {@code null}.
     *
     * <p>A projection with no id function is single-instance: it has one slot, and that slot is
     * stored under {@code subscriptionId} rather than under a key derived from the events, so read it back
     * with the same id. A projection that has an id function keys each instance by whatever that function
     * returns for an event.</p>
     *
     * <p>Those are two different nulls, which is worth keeping apart. No id function at all means
     * single-instance. An id function that returns {@code null} for one event means that event is skipped,
     * and the projection is still keyed.</p>
     */
    public <S extends @Nullable Object, ID> Subscription project(String subscriptionId, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, @Nullable StartAt startAt) {
        return projectWithMetadata(subscriptionId, projection, Projections.reactiveUpdateWithMetadata(projection, repository, subscriptionId), startAt);
    }

    /**
     * Subscribes with the given id and drives the blocking {@code materializedView} (scheduled on {@code boundedElastic})
     * for every matching event. Use this to reuse a {@link MaterializedView} with its own retry/locking policy.
     */
    public Subscription project(String subscriptionId, Projection<?, E, ?> projection, MaterializedView<E> materializedView) {
        return project(subscriptionId, projection, materializedView, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and drives the blocking {@code materializedView} (scheduled on {@code boundedElastic}) for every matching event.
     */
    public Subscription project(String subscriptionId, Projection<?, E, ?> projection, MaterializedView<E> materializedView, @Nullable StartAt startAt) {
        return projectWithMetadata(subscriptionId, projection, Projections.reactiveUpdateWithMetadata(materializedView), startAt);
    }

    // Threads the delivered event's EventMetadata into the update. The public (E) -> Mono<Void> primitive overload stays
    // event-only, since a caller-supplied reactive update composes at the domain-event level; the BiFunction overload,
    // and the repository and MaterializedView overloads, route here so a metadata-keyed projection folds with real
    // metadata.
    private Subscription projectWithMetadata(String subscriptionId, Projection<?, E, ?> projection, BiFunction<EventMetadata, E, Mono<Void>> update, @Nullable StartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(update, "update cannot be null");
        SubscriptionFilter filter = toSubscriptionFilter.apply(ProjectionFilters.filterFor(cloudEventConverter, projection));
        Function<CloudEvent, Mono<Void>> action = cloudEvent -> update.apply(EventMetadata.from(cloudEvent), cloudEventConverter.toDomainEvent(cloudEvent));
        StartAt effectiveStartAt = startAt != null ? startAt : StartAt.subscriptionModelDefault();
        return subscriptionModel.subscribe(subscriptionId, filter, effectiveStartAt, action);
    }
}
