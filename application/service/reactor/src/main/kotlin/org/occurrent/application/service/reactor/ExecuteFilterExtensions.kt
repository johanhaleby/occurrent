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

package org.occurrent.application.service.reactor

import org.occurrent.application.service.ExecuteFilter
import kotlin.reflect.KClass

/**
 * Kotlin namespace for application-service-level typed execute filters for the reactive application service.
 * Java callers should use the static factory methods on [ExecuteFilter].
 */
object ExecuteFilters {

    /** Create an [ExecuteFilter] that includes events of the reified domain event type [E]. */
    inline fun <reified E : Any> type(): ExecuteFilter<E> = ExecuteFilter.type(E::class.java)

    /** Create an [ExecuteFilter] that includes events whose domain event type matches any of [eventTypes]. */
    fun <E : Any> includeTypes(vararg eventTypes: KClass<out E>): ExecuteFilter<E> {
        require(eventTypes.isNotEmpty()) { "At least one event type must be supplied" }
        val first = eventTypes.first().java
        val remaining = eventTypes.drop(1).map { it.java }.toTypedArray()
        return ExecuteFilter.includeTypes(first, *remaining)
    }

    /** Create an [ExecuteFilter] that excludes events whose domain event type matches any of [eventTypes]. */
    fun <E : Any> excludeTypes(vararg eventTypes: KClass<out E>): ExecuteFilter<E> {
        require(eventTypes.isNotEmpty()) { "At least one event type must be supplied" }
        val first = eventTypes.first().java
        val remaining = eventTypes.drop(1).map { it.java }.toTypedArray()
        return ExecuteFilter.excludeTypes(first, *remaining)
    }
}
