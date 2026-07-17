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
import org.occurrent.dsl.view.MaterializedView
import org.occurrent.dsl.view.materialized
import org.occurrent.subscription.DcbStartAt
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.Subscription
import org.springframework.data.mongodb.core.MongoOperations

/**
 * Convenience over [Subscriptions.project] that materializes [projection] into MongoDB via [mongoOperations], using the
 * view DSL's `materialized(...)` (with its default duplicate-key/optimistic-locking retry policy). The projection's id
 * becomes the MongoDB document id and so must not resolve to `null`. A single-instance projection has no id function, so
 * it uses the `subscriptionId` as the document id instead.
 */
inline fun <reified S, E : Any, ID : Any> Subscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, mongoOperations: MongoOperations, startAt: StartAt? = null): Subscription {
    val id = projection.id()
    @Suppress("UNCHECKED_CAST")
    val materializedView: MaterializedView<E> = projection.view().materialized(mongoOperations) { event ->
        if (id == null) subscriptionId as ID
        else requireNotNull(id.apply(event)) { "Projection id resolved to null for a MongoDB-materialized view; a document id cannot be null" }
    }
    return project(subscriptionId, projection, materializedView, startAt)
}

/**
 * Convenience over [StreamSubscriptions.project] that materializes [projection] into MongoDB via [mongoOperations].
 */
inline fun <reified S, E : Any, ID : Any> StreamSubscriptions<E>.project(subscriptionId: String, projection: Projection<S, E, ID>, mongoOperations: MongoOperations, startAt: StartAt? = null): Subscription {
    val id = projection.id()
    @Suppress("UNCHECKED_CAST")
    val materializedView: MaterializedView<E> = projection.view().materialized(mongoOperations) { event ->
        if (id == null) subscriptionId as ID
        else requireNotNull(id.apply(event)) { "Projection id resolved to null for a MongoDB-materialized view; a document id cannot be null" }
    }
    return project(subscriptionId, projection, materializedView, startAt)
}

/**
 * Convenience over [DcbSubscriptions.project] that materializes [dcbProjection] into MongoDB via [mongoOperations].
 */
inline fun <reified S, E : Any, ID : Any> DcbSubscriptions<E>.project(subscriptionId: String, dcbProjection: DcbProjection<S, E, ID>, mongoOperations: MongoOperations, startAt: DcbStartAt? = null): Subscription {
    val projection = dcbProjection.projection()
    val id = projection.id()
    @Suppress("UNCHECKED_CAST")
    val materializedView: MaterializedView<E> = projection.view().materialized(mongoOperations) { event ->
        if (id == null) subscriptionId as ID
        else requireNotNull(id.apply(event)) { "Projection id resolved to null for a MongoDB-materialized view; a document id cannot be null" }
    }
    return project(subscriptionId, dcbProjection, materializedView, startAt)
}
