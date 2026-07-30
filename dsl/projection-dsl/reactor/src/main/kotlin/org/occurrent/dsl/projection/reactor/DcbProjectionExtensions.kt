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

import org.occurrent.cloudevents.EventMetadata
import org.occurrent.dsl.dcb.reactor.DcbDomainEventQueries
import org.occurrent.dsl.dcb.reactor.DcbSubscriptions
import org.occurrent.dsl.dcb.reactor.subscribeDcb
import org.occurrent.dsl.dcb.reactor.subscribeDcbWithMetadata
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
 * Whether this catches up and resumes durably, or is live-only, depends on the `SubscriptionModel` behind this
 * [DcbSubscriptions]. A catch-up-capable model (the Spring composite, or a hand-wired `CatchupSubscriptionModel`)
 * replays history and resumes across restarts, a plain live model does neither. For a strongly consistent read, fold on
 * demand with the pull [DcbDomainEventQueries.project]. For a declarative read model use the `@Projection` annotation.
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, update: (E) -> Mono<Void>, startAt: DcbStartAt? = null): Subscription =
    subscribeDcb(subscriptionId, dcbProjection.criteria(), startAt) { e -> update(e) }

/**
 * As the [update] overload above, but [update] also sees the delivering event's [EventMetadata], for a caller that owns
 * the reactive load-evolve-save but still needs the stream id, stream version, or other metadata.
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, update: (EventMetadata, E) -> Mono<Void>, startAt: DcbStartAt? = null): Subscription =
    subscribeDcbWithMetadata(subscriptionId, dcbProjection.criteria(), startAt) { dcbMetadata, e -> update(dcbMetadata.eventMetadata(), e) }

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed DCB read model materialized into the blocking [repository]
 * (scheduled on `boundedElastic`), skipping events whose id resolves to `null`.
 */
fun <S, E : Any, ID : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<S, E, ID>, repository: ViewStateRepository<S, ID>, startAt: DcbStartAt? = null): Subscription {
    val update = Projections.reactiveUpdateWithMetadata(dcbProjection.projection(), repository, subscriptionId)
    return subscribeDcbWithMetadata(subscriptionId, dcbProjection.criteria(), startAt) { dcbMetadata, e -> update.apply(dcbMetadata.eventMetadata(), e) }
}

/**
 * Runs [dcbProjection] as an asynchronous, subscription-fed DCB read model driving the blocking [materializedView]
 * (scheduled on `boundedElastic`).
 */
fun <E : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<*, E, *>, materializedView: MaterializedView<E>, startAt: DcbStartAt? = null): Subscription {
    val update = Projections.reactiveUpdateWithMetadata(materializedView)
    return subscribeDcbWithMetadata(subscriptionId, dcbProjection.criteria(), startAt) { dcbMetadata, e -> update.apply(dcbMetadata.eventMetadata(), e) }
}

/**
 * Folds the events matching [dcbProjection]'s DCB criteria, read on demand, into its view state: the strongly-consistent,
 * query-driven counterpart to the subscription-fed [DcbSubscriptions.project], and the shape of a single-instance DCB
 * projection such as "is this username claimed?". The returned [Mono] emits the folded state, and completes empty when
 * the fold produced `null`, since a [Mono] cannot carry `null`. An empty completion therefore means the state is
 * `null`, not that the criteria matched nothing.
 *
 * The receiver form of [Projections.project], which does the work. It folds each event as the query emits it rather
 * than reading the whole boundary into a list first.
 */
// See the non-DCB pull: a null state becomes an empty completion, so the Mono never emits null, and Kotlin will not let
// a Mono be declared over a nullable type.
@Suppress("UNCHECKED_CAST")
fun <S, E : Any, ID : Any> DcbDomainEventQueries<E>.project(dcbProjection: DcbProjection<S, E, ID>): Mono<S & Any> =
    Projections.project(dcbProjection, this) as Mono<S & Any>
