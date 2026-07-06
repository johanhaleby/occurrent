/*
 * Copyright 2021 Johan Haleby
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

package org.occurrent.dsl.subscription

import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.get
import org.occurrent.condition.Condition
import org.occurrent.filter.Filter
import org.occurrent.subscription.AgnosticSubscriptionFilter
import org.occurrent.subscription.OccurrentSubscriptionFilter
import kotlin.reflect.KClass

/**
 * Build the plain [Filter] that matches the cloud event types derived from the given domain [eventTypes], using the
 * [cloudEventConverter] to map each [KClass] to its cloud event type. An empty [eventTypes] matches all events.
 * Shared by the blocking and reactive subscription DSLs, since the logic is neither blocking- nor reactive-specific.
 */
fun <E : Any> filterFromEventTypes(cloudEventConverter: CloudEventConverter<E>, eventTypes: Array<out KClass<out E>>): Filter {
    val condition = when {
        eventTypes.isEmpty() -> null
        eventTypes.size == 1 -> Condition.eq(cloudEventConverter[eventTypes[0]])
        else -> Condition.or(eventTypes.map { e -> Condition.eq(cloudEventConverter[e]) })
    }
    return if (condition == null) Filter.all() else Filter.type(condition)
}

/**
 * Build an [OccurrentSubscriptionFilter] (stream capability) that matches the cloud event types derived from the given
 * domain [eventTypes]. An empty [eventTypes] matches all events.
 */
fun <E : Any> subscriptionFilterFromEventTypes(cloudEventConverter: CloudEventConverter<E>, eventTypes: Array<out KClass<out E>>): OccurrentSubscriptionFilter =
    OccurrentSubscriptionFilter.filter(filterFromEventTypes(cloudEventConverter, eventTypes))

/**
 * Build an [AgnosticSubscriptionFilter] (capability-agnostic) that matches the cloud event types derived from the given
 * domain [eventTypes]. An empty [eventTypes] matches all events, of every capability.
 */
fun <E : Any> agnosticSubscriptionFilterFromEventTypes(cloudEventConverter: CloudEventConverter<E>, eventTypes: Array<out KClass<out E>>): AgnosticSubscriptionFilter =
    AgnosticSubscriptionFilter.filter(filterFromEventTypes(cloudEventConverter, eventTypes))
