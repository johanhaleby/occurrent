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
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.view.MaterializedView;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.subscription.DcbStartAt;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Runs a {@link DcbProjection} as an asynchronous, subscription-fed read model over a DCB consistency boundary on the
 * reactor stack: it subscribes to the events matching the projection's {@link DcbProjection#criteria() DCB criteria} and
 * updates the read model from each one, in a single call. The reactor counterpart to the blocking
 * {@code DcbProjectionRunner}.
 * <p>
 * The read boundary is the descriptor's criteria verbatim; handler-derived event types are not additionally intersected
 * into it (the fold already ignores unhandled types, so a broader criteria is safe, and over-reading only costs a few
 * no-op folds). The read model is updated through a reactive {@code (E) -> Mono<Void>} update; the
 * {@link ViewStateRepository}/{@link MaterializedView} overloads drive a blocking view store from the reactive pipeline
 * (scheduled on {@code boundedElastic}; see {@link Projections}).
 * <p>
 * <strong>Whether this is live-only or catches up and resumes durably depends on the {@code SubscriptionModel} given
 * to this runner's constructor</strong>, since it subscribes through {@link DcbSubscriptions}, which is only as
 * capable as that model. Given a plain live model with no catch-up support, this runner is live-only. It does not
 * replay history and does not resume durably across restarts. Given a catch-up-capable model (the Spring composite
 * subscription model, or a hand-wired {@code CatchupSubscriptionModel}), this runner catches up from history and
 * resumes durably across restarts, the same as {@link ReactiveProjectionRunner}. For a strongly consistent, complete
 * DCB read model, fold on demand instead with the pull {@code project(...)} (the Kotlin
 * {@code DcbDomainEventQueries.project} extension). For a persistent, declarative DCB read model use the
 * {@code @Projection} annotation. To wire the same catch-up-then-resume behavior by hand without the Spring starter,
 * use {@code ResumeStartPositions.replayThenResumeDcb(...)}.
 *
 * @param <E> the domain event type
 */
@NullMarked
public final class ReactiveDcbProjectionRunner<E> {

    private final DcbSubscriptions<E> dcbSubscriptions;

    public ReactiveDcbProjectionRunner(SubscriptionModel subscriptionModel, CloudEventConverter<E> cloudEventConverter) {
        this.dcbSubscriptions = new DcbSubscriptions<>(
                requireNonNull(subscriptionModel, "subscriptionModel cannot be null"),
                requireNonNull(cloudEventConverter, "cloudEventConverter cannot be null"));
    }

    /**
     * Subscribes with the given id and applies {@code update} for every event matching the projection's DCB criteria.
     * The primitive overload: {@code update} owns the reactive load-evolve-save against a reactive store.
     */
    public Subscription project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, Function<E, Mono<Void>> update) {
        return project(subscriptionId, dcbProjection, update, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and applies {@code update} for every event matching the projection's DCB criteria.
     */
    public Subscription project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, Function<E, Mono<Void>> update, @Nullable DcbStartAt startAt) {
        requireNonNull(subscriptionId, "subscriptionId cannot be null");
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        requireNonNull(update, "update cannot be null");
        return dcbSubscriptions.subscribe(subscriptionId, dcbProjection.criteria(), startAt, update::apply);
    }

    /**
     * Subscribes with the given id and materializes {@code dcbProjection} into the blocking {@code repository} (scheduled
     * on {@code boundedElastic}), skipping events whose id resolves to {@code null}.
     */
    public <S extends @Nullable Object, ID> Subscription project(String subscriptionId, DcbProjection<S, E, ID> dcbProjection, ViewStateRepository<S, ID> repository) {
        return project(subscriptionId, dcbProjection, repository, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and materializes {@code dcbProjection} into the blocking {@code repository} (scheduled on {@code boundedElastic}),
     * skipping events whose id resolves to {@code null}.
     */
    public <S extends @Nullable Object, ID> Subscription project(String subscriptionId, DcbProjection<S, E, ID> dcbProjection, ViewStateRepository<S, ID> repository, @Nullable DcbStartAt startAt) {
        requireNonNull(dcbProjection, "dcbProjection cannot be null");
        return project(subscriptionId, dcbProjection, Projections.reactiveUpdate(dcbProjection.projection(), repository), startAt);
    }

    /**
     * Subscribes with the given id and drives the blocking {@code materializedView} (scheduled on {@code boundedElastic})
     * for every event matching the projection's DCB criteria.
     */
    public Subscription project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, MaterializedView<E> materializedView) {
        return project(subscriptionId, dcbProjection, materializedView, null);
    }

    /**
     * Subscribes with the given id, starting at {@code startAt} ({@code null} means the subscription model's default),
     * and drives the blocking {@code materializedView} (scheduled on {@code boundedElastic}) for every event matching
     * the projection's DCB criteria.
     */
    public Subscription project(String subscriptionId, DcbProjection<?, E, ?> dcbProjection, MaterializedView<E> materializedView, @Nullable DcbStartAt startAt) {
        return project(subscriptionId, dcbProjection, Projections.reactiveUpdate(materializedView), startAt);
    }
}
