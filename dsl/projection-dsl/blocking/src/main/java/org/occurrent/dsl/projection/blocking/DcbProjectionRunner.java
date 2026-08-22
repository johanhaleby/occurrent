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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.blocking.SubscriptionHandle;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import static java.util.Objects.requireNonNull;

/**
 * Runs a {@link DcbProjection} as an asynchronous, subscription-fed read model over a DCB consistency boundary: it
 * subscribes to the events matching the projection's {@link DcbProjection#criteria() criteria} and updates the
 * materialized view from each. The DCB counterpart to {@link ProjectionRunner}.
 * <p>
 * The criteria is used verbatim, not intersected with the handler event types: the fold already ignores types it does
 * not handle, so a broader criteria is safe and over-reading only costs a few no-op folds.
 * <p>
 * <strong>Whether this catches up and resumes durably, or is live-only, depends on the {@code SubscriptionModel} passed
 * to {@link #create}</strong>, since it subscribes through {@link DcbSubscriptions}, which is only as capable as that
 * model. A catch-up-capable model (the Spring composite, or a hand-wired {@code CatchupSubscriptionModel}) replays
 * history and resumes across restarts, a plain live model does neither. For a strongly consistent read, fold on demand
 * with the pull {@link Projections#project(DcbProjection, org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries)}. For a
 * declarative read model use the {@code @Projection} annotation.
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class DcbProjectionRunner<E> {

    private final DcbSubscriptions<E> dcbSubscriptions;

    /**
     * Creates a runner that subscribes through the given {@code subscriptionModel}, matching the factory style of
     * {@link ProjectionRunner}.
     */
    public static <E> DcbProjectionRunner<E> create(SubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        return new DcbProjectionRunner<>(subscriptionModel, cloudEventConverter);
    }

    private DcbProjectionRunner(SubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        this.dcbSubscriptions = new DcbSubscriptions<>(
                requireNonNull(subscriptionModel, "subscriptionModel cannot be null"),
                requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null"));
    }

    /**
     * Subscribes with the given id and materializes {@code dcbProjection} into {@code repository}, skipping events whose
     * id resolves to {@code null}.
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
     * and materializes {@code dcbProjection} into {@code repository}, skipping events whose id resolves to {@code null}.
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
        return project(subscriptionId, dcbProjection, Projections.materializedView(dcbProjection.projection(), repository, subscriptionId), startAt);
    }

    /**
     * Subscribes with the given id and calls {@code materializedView.update(event)} for every event matching the
     * projection's DCB criteria.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, MaterializedView<E> materializedView) {
        return project(subscriptionId, dcbProjection, materializedView, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and calls {@code materializedView.update(event)} for every event matching the projection's DCB criteria.
     */
    public SubscriptionHandle project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, MaterializedView<E> materializedView, @Nullable DcbStartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(materializedView, "materializedView cannot be null");
        SubscriptionHandle subscription = dcbSubscriptions.subscribeWithMetadata(subscriptionId, dcbProjection.criteria(), startAt,
                (dcbMetadata, event) -> materializedView.update(dcbMetadata.eventMetadata(), event));
        subscription.waitUntilStarted();
        return subscription;
    }
}
