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

package org.occurrent.dsl.projection.reactor

import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions
import org.occurrent.dsl.dcb.reactor.subscribeDcb
import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.view.MaterializedView
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.subscription.DcbStartAt
import org.occurrent.subscription.api.reactor.Subscription
import reactor.core.publisher.Mono

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed read model over its DCB consistency boundary on the reactor
 * stack: subscribes to the events matching the projection's [DcbProjection.criteria] and applies [update] for each.
 * [update] owns the reactive load-evolve-save, and is also the overload for synchronous read-your-writes dispatch.
 *
 * This is a live-only read model. It is built on the ephemeral [DcbSubscriptions] live subscription, which post-filters
 * live events and provides no DCB catch-up read or durable checkpoint, so it does not replay history and does not resume
 * durably across restarts. For a strongly consistent, complete DCB read model, fold on demand with the pull [project]
 * ([DcbDomainEventQueries.project]). For a persistent DCB read model that catches up from history on startup, use the
 * `@DcbSubscription` annotation today (a future `@Projection` annotation is planned to integrate that with this DSL).
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, update: (E) -> Mono<Void>, startAt: DcbStartAt? = null): Subscription =
    subscribeDcb(subscriptionId, dcbProjection.criteria(), startAt) { e -> update(e) }

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed DCB read model materialized into the blocking [repository]
 * (scheduled on `boundedElastic`), skipping events whose id resolves to `null`.
 */
fun <S, E : Any, ID : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<S, E, ID>, repository: ViewStateRepository<S, ID>, startAt: DcbStartAt? = null): Subscription {
    val update = Projections.reactiveUpdate(dcbProjection.projection(), repository)
    return project(subscriptionId, dcbProjection, { e -> update.apply(e) }, startAt)
}

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed DCB read model driving the blocking [materializedView]
 * (scheduled on `boundedElastic`).
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, materializedView: MaterializedView<E>, startAt: DcbStartAt? = null): Subscription {
    val update = Projections.reactiveUpdate(materializedView)
    return project(subscriptionId, dcbProjection, { e -> update.apply(e) }, startAt)
}

/**
 * Folds the events matching [dcbProjection]'s DCB criteria, read on demand, into its view state: the strongly-consistent,
 * query-driven counterpart to the subscription-fed [DcbSubscriptions.project], and the shape of a single-instance DCB
 * projection such as "is this username claimed?". The returned [Mono] emits the folded state, or completes empty when
 * that state is `null` (Reactor cannot carry a `null` value, so the state type is constrained to be non-null).
 */
fun <S : Any, E : Any, ID : Any> DcbDomainEventQueries<E>.project(dcbProjection: DcbProjection<S, E, ID>): Mono<S> =
    query(dcbProjection.criteria()).collectList().flatMap { list -> Mono.justOrEmpty(dcbProjection.projection().view().evolve(list)) }
