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
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;

import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Runs a {@link Projection} as an asynchronous, subscription-fed read model: it creates the subscription and updates the
 * materialized view from every matching event, in one call. The Java counterpart to the Kotlin {@code project(...)}
 * extensions on the subscription DSL, and the read-side mirror of {@code DeciderApplicationService} on the write side.
 * <p>
 * Pick the capability with the factory: {@link #agnostic(Subscribable, CloudEventConverter) agnostic} delivers both
 * stream-written and DCB-appended events, {@link #stream(Subscribable, CloudEventConverter) stream} only stream-written
 * ones. For a DCB consistency-boundary read model use {@link DcbProjectionRunner}.
 * <p>
 * The subscription filter comes from the projection (its explicit {@link Projection#filter() filter}, else a type filter
 * over its handled types). The returned {@link SubscriptionHandle} is already started.
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class ProjectionRunner<E> {

    private final Subscribable subscriptionModel;
    private final CloudEventConverter<E> cloudEventConverter;
    private final Function<Filter, SubscriptionFilter> toSubscriptionFilter;

    private ProjectionRunner(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter, Function<Filter, SubscriptionFilter> toSubscriptionFilter) {
        this.subscriptionModel = requireNonNull(subscriptionModel, "subscriptionModel cannot be null");
        this.cloudEventConverter = requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null");
        this.toSubscriptionFilter = requireNonNull(toSubscriptionFilter, "toSubscriptionFilter cannot be null");
    }

    /**
     * A runner whose subscriptions are capability-agnostic: on a store with both the {@code STREAM} and {@code DCB}
     * capabilities they deliver both stream-written and DCB-appended events, filtered only by the projection's selector.
     */
    public static <E> ProjectionRunner<E> agnostic(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new ProjectionRunner<>(subscriptionModel, cloudEventConverter, AgnosticSubscriptionFilter::filter);
    }

    /**
     * A runner whose subscriptions are scoped to the {@code STREAM} capability, excluding DCB-appended events.
     */
    public static <E> ProjectionRunner<E> stream(Subscribable subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new ProjectionRunner<>(subscriptionModel, cloudEventConverter, StreamSubscriptionFilter::filter);
    }

    /**
     * Subscribes with the given id and materializes {@code projection} into {@code repository}, skipping events whose id
     * resolves to {@code null}. A failed update is retried by the subscription model's retry strategy, which redelivers
     * the event. This overload adds no fine-grained optimistic-locking retry of its own, so for concurrent writers to the
     * same instance use {@link #project(String, Projection, MaterializedView)} with a store that re-reads and reapplies
     * on conflict, such as the view DSL's {@code materialized(...)}.
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
    public <S extends @Nullable Object, ID> SubscriptionHandle project(String subscriptionId, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository) {
        return project(subscriptionId, projection, repository, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and materializes {@code projection} into {@code repository}, skipping events whose id resolves to {@code null}. Pass
     * {@code StartAt.subscriptionModelDefault()} or a specific position to control catch-up.
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
    public <S extends @Nullable Object, ID> SubscriptionHandle project(String subscriptionId, Projection<S, E, ID> projection, ViewStateRepository<S, ID> repository, @Nullable StartAt startAt) {
        return project(subscriptionId, projection, Projections.materializedView(projection, repository, subscriptionId), startAt);
    }

    /**
     * Subscribes with the given id and calls {@code materializedView.update(event)} for every matching event. Use this
     * overload when you already have a {@link MaterializedView} (for example one built by the view DSL's
     * {@code materialized(...)} with its own retry/locking policy).
     */
    public SubscriptionHandle project(String subscriptionId, Projection<?, E, ?> projection, MaterializedView<E> materializedView) {
        return project(subscriptionId, projection, materializedView, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and calls {@code materializedView.update(event)} for every matching event.
     */
    public SubscriptionHandle project(String subscriptionId, Projection<?, E, ?> projection, MaterializedView<E> materializedView, @Nullable StartAt startAt) {
        return project(subscriptionId, projection, materializedView, startAt, true);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and calls {@code materializedView.update(event)} for every matching event, waiting for the subscription to start
     * only if {@code waitUntilStarted} says to.
     * <p>
     * Pass {@code false} to keep a catch-up replay off the startup path, which is what
     * {@code @Projection(startupMode = BACKGROUND)} does. The projection then starts folding history on its own thread
     * while the caller carries on, so a read model is briefly incomplete rather than an application being briefly
     * unavailable. A replay failure surfaces from the returned subscription's {@code waitUntilStarted} instead of from
     * here, so a caller that passes {@code false} and never waits will not see it.
     */
    public SubscriptionHandle project(String subscriptionId, Projection<?, E, ?> projection, MaterializedView<E> materializedView, @Nullable StartAt startAt, boolean waitUntilStarted) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(projection, "projection cannot be null");
        requireNonNull(materializedView, "materializedView cannot be null");
        SubscriptionFilter filter = toSubscriptionFilter.apply(ProjectionFilters.filterFor(cloudEventConverter, projection));
        Consumer<CloudEvent> action = cloudEvent -> materializedView.update(EventMetadata.from(cloudEvent), cloudEventConverter.toDomainEvent(cloudEvent));
        StartAt effectiveStartAt = startAt != null ? startAt : StartAt.subscriptionModelDefault();
        SubscriptionHandle subscription = subscriptionModel.subscribe(subscriptionId, filter, effectiveStartAt, action);
        if (waitUntilStarted) {
            subscription.waitUntilStarted();
        }
        return subscription;
    }
}
