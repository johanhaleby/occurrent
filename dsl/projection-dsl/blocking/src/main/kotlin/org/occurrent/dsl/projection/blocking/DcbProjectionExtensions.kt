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

package org.occurrent.dsl.projection.blocking

import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions
import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.view.MaterializedView
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.subscription.DcbStartAt
import org.occurrent.subscription.api.blocking.Subscription

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed read model over its DCB consistency boundary: subscribes to
 * the events matching [DcbProjection.criteria] and updates [materializedView] from each. The returned [Subscription] is
 * already started.
 *
 * Whether this catches up and resumes durably, or is live-only, depends on the `SubscriptionModel` behind this
 * [DcbSubscriptions]. A catch-up-capable model (the Spring composite, or a hand-wired `CatchupSubscriptionModel`)
 * replays history and resumes across restarts, a plain live model does neither. For a strongly consistent read, fold on
 * demand with the pull [DcbDomainEventQueries.project]. For a declarative read model use the `@Projection` annotation.
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, materializedView: MaterializedView<E>, startAt: DcbStartAt? = null): Subscription =
    subscribeWithMetadata(subscriptionId, dcbProjection.criteria(), startAt) { dcbMetadata, e -> materializedView.update(dcbMetadata.eventMetadata(), e) }.also { it.waitUntilStarted() }

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed DCB read model materialized into [repository], skipping
 * events whose id resolves to `null`. See the [MaterializedView] overload above for the live-only versus catch-up
 * and durability details, which depend on the subscription model the same way.
 */
fun <S, E : Any, ID : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<S, E, ID>, repository: ViewStateRepository<S, ID>, startAt: DcbStartAt? = null): Subscription =
    project(subscriptionId, dcbProjection, Projections.materializedView(dcbProjection.projection(), repository, subscriptionId), startAt)

/**
 * Folds the events matching [dcbProjection]'s DCB criteria, read on demand, into its view state and returns it: the
 * strongly-consistent, query-driven counterpart to the subscription-fed [DcbSubscriptions.project]. This is the shape
 * of a single-instance DCB projection such as "is this username claimed?".
 *
 * The receiver form of [Projections.project], which does the work.
 */
fun <S, E : Any, ID : Any> DcbDomainEventQueries<E>.project(dcbProjection: DcbProjection<S, E, ID>): S =
    Projections.project(dcbProjection, this)
