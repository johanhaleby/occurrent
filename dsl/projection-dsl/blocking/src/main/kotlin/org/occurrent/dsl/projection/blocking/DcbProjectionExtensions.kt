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
 * the events matching the projection's [DcbProjection.criteria] and updates [materializedView] from each. The returned
 * [Subscription] has been waited on until started.
 *
 * This is a live-only read model. It is built on the ephemeral [DcbSubscriptions] live subscription, which post-filters
 * live events and provides no DCB catch-up read or durable checkpoint, so it does not replay history and does not resume
 * durably across restarts. For a strongly consistent, complete DCB read model, fold on demand with the pull [project]
 * ([DcbDomainEventQueries.project]). For a persistent DCB read model that catches up from history on startup, use the
 * `@DcbSubscription` annotation today (a future `@Projection` annotation is planned to integrate that with this DSL).
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, materializedView: MaterializedView<E>, startAt: DcbStartAt? = null): Subscription =
    subscribe(subscriptionId, dcbProjection.criteria(), startAt) { e -> materializedView.update(e) }.also { it.waitUntilStarted() }

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed DCB read model materialized into [repository], skipping
 * events whose id resolves to `null`. Live-only, see the [MaterializedView] overload for the catch-up and durability
 * caveat.
 */
fun <S, E : Any, ID : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<S, E, ID>, repository: ViewStateRepository<S, ID>, startAt: DcbStartAt? = null): Subscription =
    project(subscriptionId, dcbProjection, Projections.materializedView(dcbProjection.projection(), repository), startAt)

/**
 * Folds the events matching [dcbProjection]'s DCB criteria, read on demand, into its view state and returns it: the
 * strongly-consistent, query-driven counterpart to the subscription-fed [DcbSubscriptions.project]. This is the shape
 * of a single-instance DCB projection such as "is this username claimed?".
 */
fun <S, E : Any, ID : Any> DcbDomainEventQueries<E>.project(dcbProjection: DcbProjection<S, E, ID>): S =
    dcbProjection.projection().view().evolve(query(dcbProjection.criteria()))
