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

import org.occurrent.dsl.projection.Projection
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.dsl.view.MaterializedView
import org.occurrent.subscription.AgnosticSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.StreamSubscriptionFilter
import org.occurrent.subscription.api.blocking.Subscription

/**
 * Runs [projection] as a capability-agnostic, subscription-fed read model: creates the subscription (its selector
 * derived from the projection) and updates [materializedView] from every matching event. On a store with both the
 * `STREAM` and `DCB` capabilities this delivers both stream-written and DCB-appended events.
 */
fun <E : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, materializedView: MaterializedView<E>, startAt: StartAt? = null): Subscription {
    val explicitFilter = projection.filter()
    return if (explicitFilter != null) {
        subscribe(subscriptionId, AgnosticSubscriptionFilter.filter(explicitFilter), startAt) { metadata, e -> materializedView.update(metadata, e) }
    } else {
        subscribe(subscriptionId, projection.eventTypes().toList(), startAt) { metadata, e -> materializedView.update(metadata, e) }
    }
}

/**
 * Runs [projection] as a capability-agnostic, subscription-fed read model materialized into [repository]. Events whose
 * id resolves to `null` are skipped. A failed update is retried by the subscription model's retry strategy, which
 * redelivers the event. This adds no fine-grained optimistic-locking retry of its own, so for concurrent writers to the
 * same instance supply a [MaterializedView] that re-reads and reapplies on conflict, for example the view DSL's
 * `materialized(...)`.
 */
fun <S, E : Any, ID : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, repository: org.occurrent.dsl.view.ViewStateRepository<S, ID>, startAt: StartAt? = null): Subscription =
    project(subscriptionId, projection, Projections.materializedView(projection, repository, subscriptionId), startAt)

/**
 * Runs [projection] as a stream-scoped, subscription-fed read model, excluding DCB-appended events. See the
 * capability-agnostic [Subscriptions.project] for the cross-capability variant.
 */
fun <E : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, materializedView: MaterializedView<E>, startAt: StartAt? = null): Subscription {
    val explicitFilter = projection.filter()
    return if (explicitFilter != null) {
        subscribe(subscriptionId, StreamSubscriptionFilter.filter(explicitFilter), startAt) { metadata, e -> materializedView.update(metadata, e) }
    } else {
        subscribe(subscriptionId, projection.eventTypes().toList(), startAt) { metadata, e -> materializedView.update(metadata, e) }
    }
}

/**
 * Runs [projection] as a stream-scoped, subscription-fed read model materialized into [repository], skipping events
 * whose id resolves to `null`.
 */
fun <S, E : Any, ID : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, repository: org.occurrent.dsl.view.ViewStateRepository<S, ID>, startAt: StartAt? = null): Subscription =
    project(subscriptionId, projection, Projections.materializedView(projection, repository, subscriptionId), startAt)

/**
 * Folds the events [projection] selects, read on demand, into its view state and returns it: the strongly-consistent,
 * query-driven counterpart to the subscription-fed [Subscriptions.project]. Only valid for a single-instance
 * (singleton) projection. A keyed projection throws, since folding every instance into one blended state on demand
 * would produce a nonsense result. Use [project] with an `instanceId` for a keyed projection.
 *
 * The Kotlin receiver form of [Projections.project], which is the same fold and does the work, so which events get
 * selected and when a keyed projection is rejected are decided in one place for both languages.
 */
fun <S, E : Any, ID : Any> DomainEventQueries<E>.project(projection: Projection<S, E, ID>): S =
    Projections.project(projection, this)

/**
 * Folds the events [projection] selects for [instanceId], read on demand, into that instance's view state and returns
 * it: the strongly-consistent, query-driven, single-instance counterpart to the unqualified [project]. Reads the same
 * candidate events as the unqualified [project], then keeps only the ones whose [Projection.id] resolves to
 * [instanceId] before folding. A singleton projection (no id function) has a single instance regardless of
 * [instanceId], so this folds all selected events, same as the unqualified [project].
 *
 * The receiver form of [Projections.project], which does the work.
 */
fun <S, E : Any, ID : Any> DomainEventQueries<E>.project(projection: Projection<S, E, ID>, instanceId: ID): S =
    Projections.project(projection, this, instanceId)
