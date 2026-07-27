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

package org.occurrent.example.projection.dsl.streamkotlin

import org.occurrent.dsl.projection.Projection
import org.occurrent.dsl.projection.projection
import org.occurrent.filter.Filter

/** Name events. Top-level data classes so the reflection CloudEvent type mapper resolves each from its simple name. */
sealed interface NameEvent {
    val userId: String
}

data class NameDefined(override val userId: String, val name: String) : NameEvent
data class NameChanged(override val userId: String, val name: String) : NameEvent

/** The materialized read model: a user's current name. */
data class CurrentName(val userId: String, val name: String)

/**
 * The Kotlin handler-builder DSL: initial state plus a fold per event type. The registered types become the
 * subscription's selector, so nothing has to restate "which events feed this view".
 */
fun currentNameProjection(): Projection<CurrentName?, NameEvent, String> =
    projection(initialState = null) {
        id { it.userId }
        on<NameDefined> { _, event -> CurrentName(event.userId, event.name) }
        on<NameChanged> { state, event -> state?.copy(name = event.name) }
    }

/**
 * The same fold, but with an explicit [Filter] selector. A projection can select on more than event type. Here it
 * subscribes only to events whose CloudEvent subject is [userId] (the demo maps each event's subject to its user id),
 * so a single-user read model ignores everyone else's events server-side rather than folding-then-discarding them.
 */
fun currentNameProjectionForUser(userId: String): Projection<CurrentName?, NameEvent, String> =
    projection(initialState = null) {
        id { it.userId }
        filter(Filter.subject(userId))
        on<NameDefined> { _, event -> CurrentName(event.userId, event.name) }
        on<NameChanged> { state, event -> state?.copy(name = event.name) }
    }
