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
import org.occurrent.dsl.projection.Projection
import org.occurrent.dsl.query.reactor.DomainEventQueries
import org.occurrent.dsl.subscription.reactor.StreamSubscriptions
import org.occurrent.dsl.subscription.reactor.Subscriptions
import org.occurrent.dsl.view.MaterializedView
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.subscription.AgnosticSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.StreamSubscriptionFilter
import org.occurrent.subscription.api.reactor.Subscription
import reactor.core.publisher.Mono

/**
 * Runs [projection] as a capability-agnostic, subscription-fed read model on the reactor stack: creates the subscription
 * (its selector derived from the projection) and applies [update] for every matching event. [update] owns the reactive
 * load-evolve-save against a reactive store, and is also the overload for synchronous, in-transaction
 * (read-your-writes) dispatch. On a store with both the `STREAM` and `DCB` capabilities this delivers both
 * stream-written and DCB-appended events.
 */
fun <E : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, update: (E) -> Mono<Void>, startAt: StartAt? = null): Subscription {
    val explicitFilter = projection.filter()
    return if (explicitFilter != null) {
        subscribe(subscriptionId, AgnosticSubscriptionFilter.filter(explicitFilter), startAt) { e -> update(e) }
    } else {
        subscribe(subscriptionId, *projection.eventTypes().map { it.kotlin }.toTypedArray(), startAt = startAt) { e -> update(e) }
    }
}

/**
 * As the [update] overload above, but [update] also sees the delivering event's [EventMetadata], for a caller that owns
 * the reactive load-evolve-save but still needs the stream id, stream version, or other metadata.
 */
fun <E : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, update: (EventMetadata, E) -> Mono<Void>, startAt: StartAt? = null): Subscription =
    projectWithMetadata(subscriptionId, projection, update, startAt)

/**
 * Runs [projection] as a capability-agnostic, subscription-fed read model materialized into the blocking [repository]
 * (scheduled on `boundedElastic`), skipping events whose id resolves to `null`.
 */
fun <S, E : Any, ID : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, repository: ViewStateRepository<S, ID>, startAt: StartAt? = null): Subscription {
    val update = Projections.reactiveUpdateWithMetadata(projection, repository, subscriptionId)
    return projectWithMetadata(subscriptionId, projection, { metadata, e -> update.apply(metadata, e) }, startAt)
}

/**
 * Runs [projection] as a capability-agnostic, subscription-fed read model driving the blocking [materializedView]
 * (scheduled on `boundedElastic`).
 */
fun <E : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, materializedView: MaterializedView<E>, startAt: StartAt? = null): Subscription {
    val update = Projections.reactiveUpdateWithMetadata(materializedView)
    return projectWithMetadata(subscriptionId, projection, { metadata, e -> update.apply(metadata, e) }, startAt)
}

// Threads the delivered event's EventMetadata into the update, using the metadata-carrying subscribe overloads. The
// public (E) -> Mono<Void> primitive project stays event-only, since a caller-supplied reactive update composes at the
// domain-event level; the repository and MaterializedView overloads route here so a metadata-keyed projection folds
// with real metadata.
private fun <E : Any> Subscriptions<E>.projectWithMetadata(subscriptionId: String, projection: Projection<*, E, *>, update: (EventMetadata, E) -> Mono<Void>, startAt: StartAt?): Subscription {
    val explicitFilter = projection.filter()
    return if (explicitFilter != null) {
        subscribe(subscriptionId, AgnosticSubscriptionFilter.filter(explicitFilter), startAt) { metadata, e -> update(metadata, e) }
    } else {
        subscribe(subscriptionId, *projection.eventTypes().map { it.kotlin }.toTypedArray(), startAt = startAt) { metadata, e -> update(metadata, e) }
    }
}

/**
 * Runs [projection] as a stream-scoped, subscription-fed read model, excluding DCB-appended events.
 */
fun <E : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, update: (E) -> Mono<Void>, startAt: StartAt? = null): Subscription {
    val explicitFilter = projection.filter()
    return if (explicitFilter != null) {
        subscribe(subscriptionId, StreamSubscriptionFilter.filter(explicitFilter), startAt) { e -> update(e) }
    } else {
        subscribe(subscriptionId, *projection.eventTypes().map { it.kotlin }.toTypedArray(), startAt = startAt) { e -> update(e) }
    }
}

/**
 * As the [update] overload above, but [update] also sees the delivering event's [EventMetadata], for a caller that owns
 * the reactive load-evolve-save but still needs the stream id, stream version, or other metadata.
 */
fun <E : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, update: (EventMetadata, E) -> Mono<Void>, startAt: StartAt? = null): Subscription =
    projectWithMetadata(subscriptionId, projection, update, startAt)

/**
 * Runs [projection] as a stream-scoped, subscription-fed read model materialized into the blocking [repository]
 * (scheduled on `boundedElastic`), skipping events whose id resolves to `null`.
 */
fun <S, E : Any, ID : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, repository: ViewStateRepository<S, ID>, startAt: StartAt? = null): Subscription {
    val update = Projections.reactiveUpdateWithMetadata(projection, repository, subscriptionId)
    return projectWithMetadata(subscriptionId, projection, { metadata, e -> update.apply(metadata, e) }, startAt)
}

/**
 * Runs [projection] as a stream-scoped, subscription-fed read model driving the blocking [materializedView]
 * (scheduled on `boundedElastic`).
 */
fun <E : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<*, E, *>, materializedView: MaterializedView<E>, startAt: StartAt? = null): Subscription {
    val update = Projections.reactiveUpdateWithMetadata(materializedView)
    return projectWithMetadata(subscriptionId, projection, { metadata, e -> update.apply(metadata, e) }, startAt)
}

// See the agnostic projectWithMetadata above: threads EventMetadata into the update, stream-scoped.
private fun <E : Any> StreamSubscriptions<E>.projectWithMetadata(subscriptionId: String, projection: Projection<*, E, *>, update: (EventMetadata, E) -> Mono<Void>, startAt: StartAt?): Subscription {
    val explicitFilter = projection.filter()
    return if (explicitFilter != null) {
        subscribe(subscriptionId, StreamSubscriptionFilter.filter(explicitFilter), startAt) { metadata, e -> update(metadata, e) }
    } else {
        subscribe(subscriptionId, *projection.eventTypes().map { it.kotlin }.toTypedArray(), startAt = startAt) { metadata, e -> update(metadata, e) }
    }
}

/**
 * Folds the events [projection] selects, read on demand, into its view state: the strongly-consistent, query-driven
 * counterpart to the subscription-fed [Subscriptions.project]. The returned [Mono] emits the folded state, and completes
 * empty when the fold produced `null`, since a [Mono] cannot carry `null`. An empty completion therefore means the state
 * is `null`, not that no events matched. Only valid for a single-instance (singleton) projection. A keyed projection
 * errors when subscribed to, since folding every instance into one blended state on demand would produce a nonsense
 * result. Use [project] with an `instanceId` for a keyed projection.
 *
 * The Kotlin receiver form of [Projections.project], which is the same fold and does the work, so which events get
 * selected and when a keyed projection is rejected are decided in one place for both languages. It folds each event as
 * the query emits it rather than reading the whole history into a list first.
 */
// The cast expresses what the Mono already guarantees: a null state becomes an empty completion, so nothing null is
// ever emitted. Kotlin will not let a Mono be declared over a nullable type, hence stating the non-null element here.
@Suppress("UNCHECKED_CAST")
fun <S, E : Any, ID : Any> DomainEventQueries<E>.project(projection: Projection<S, E, ID>): Mono<S & Any> =
    Projections.project(projection, this) as Mono<S & Any>

/**
 * Folds the events [projection] selects for [instanceId], read on demand, into that instance's view state: the
 * strongly-consistent, query-driven, single-instance counterpart to the unqualified [project]. Reads the same candidate
 * events as the unqualified [project], then keeps only the ones whose [Projection.id] resolves to
 * [instanceId] before folding. A singleton projection (no id function) has a single instance regardless of
 * [instanceId], so this folds all selected events, same as the unqualified [project]. The returned [Mono] emits the
 * folded state, and completes empty when the fold produced `null`.
 *
 * The receiver form of [Projections.project], which does the work.
 */
@Suppress("UNCHECKED_CAST")
fun <S, E : Any, ID : Any> DomainEventQueries<E>.project(projection: Projection<S, E, ID>, instanceId: ID): Mono<S & Any> =
    Projections.project(projection, this, instanceId) as Mono<S & Any>
