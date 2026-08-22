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

import org.occurrent.dsl.dcb.blocking.DcbSubscriptions
import org.occurrent.dsl.projection.DcbProjection
import org.occurrent.dsl.projection.Projection
import org.occurrent.dsl.subscription.blocking.StreamSubscriptions
import org.occurrent.dsl.subscription.blocking.Subscriptions
import org.occurrent.dsl.view.ViewStateRepository
import org.occurrent.dsl.view.internal.requireMatchingDocumentId
import org.occurrent.dsl.view.viewStateRepository
import org.occurrent.subscription.DcbStartAt
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.SubscriptionHandle
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.findById

/**
 * A [ViewStateRepository] that finds and saves the view state as a MongoDB document via [mongoOperations]. Shared by
 * the three `project(...)` overloads below so they all delegate to the [Projection]-and-repository `project(...)`
 * overload rather than resolving the id themselves, and so get the same id resolution, null-id skip, and
 * metadata-keyed guard as every other projection runner.
 */
inline fun <reified S : Any, ID : Any> mongoViewStateRepository(mongoOperations: MongoOperations): ViewStateRepository<S, ID> {
    val stateType = S::class.java
    return viewStateRepository(
        find = { id: ID -> mongoOperations.findById(id) },
        save = { id: ID, state: S ->
            requireMatchingDocumentId(mongoOperations, stateType, state, id)
            mongoOperations.save(state)
        }
    )
}

/**
 * Convenience over [Subscriptions.project] that materializes [projection] into MongoDB via [mongoOperations], routed
 * through the [Projection]-and-repository [Subscriptions.project] overload. A projection keyed by event metadata (for
 * example the stream id) is resolved and folded with that metadata, an id that resolves to `null` is skipped, and a
 * projection keyed by metadata that never arrived throws with an accurate message instead of silently dropping the event.
 */
inline fun <reified S : Any, E : Any, ID : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, mongoOperations: MongoOperations, startAt: StartAt? = null): SubscriptionHandle =
    project(subscriptionId, projection, mongoViewStateRepository<S, ID>(mongoOperations), startAt)

/**
 * Convenience over [StreamSubscriptions.project] that materializes [projection] into MongoDB via [mongoOperations].
 */
inline fun <reified S : Any, E : Any, ID : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, mongoOperations: MongoOperations, startAt: StartAt? = null): SubscriptionHandle =
    project(subscriptionId, projection, mongoViewStateRepository<S, ID>(mongoOperations), startAt)

/**
 * Convenience over [DcbSubscriptions.project] that materializes [dcbProjection] into MongoDB via [mongoOperations].
 */
inline fun <reified S : Any, E : Any, ID : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<S, E, ID>, mongoOperations: MongoOperations, startAt: DcbStartAt? = null): SubscriptionHandle =
    project(subscriptionId, dcbProjection, mongoViewStateRepository<S, ID>(mongoOperations), startAt)
