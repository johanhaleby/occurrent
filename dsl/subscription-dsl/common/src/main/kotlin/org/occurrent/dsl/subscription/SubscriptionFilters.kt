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
import org.occurrent.filter.Filter
import org.occurrent.filter.internal.EventTypeExpansion
import org.occurrent.subscription.AgnosticSubscriptionFilter
import org.occurrent.subscription.StreamSubscriptionFilter
import kotlin.reflect.KClass

/**
 * Build the plain [Filter] that matches the cloud event types the given domain [eventTypes] expand into, using the
 * [cloudEventConverter] to map each expanded [KClass] to its cloud event type. A sealed [KClass] expands to the
 * concrete types it permits, the same expansion the saga DSL and the projection DSL apply, so a declaration keyed on a
 * sealed supertype asks for every concrete type it permits. An empty [eventTypes] matches all events. A declared type
 * whose concrete types cannot all be found is refused, naming the type and the remedy. Shared by the blocking and
 * reactive subscription DSLs, since the logic is neither blocking- nor reactive-specific.
 */
fun <E : Any> filterFromEventTypes(cloudEventConverter: CloudEventConverter<E>, eventTypes: Array<out KClass<out E>>): Filter {
    val declaredTypes: Set<Class<out E>> = eventTypes.mapTo(LinkedHashSet()) { it.java }
    return EventTypeExpansion.deriveFilter(declaredTypes, { type -> cloudEventTypeOf(cloudEventConverter, type) }, ::cannotExpand)
}

@Suppress("UNCHECKED_CAST")
private fun <E : Any> cloudEventTypeOf(cloudEventConverter: CloudEventConverter<E>, type: Class<*>): String =
    cloudEventConverter.getCloudEventType(type as Class<out E>)

private fun cannotExpand(eventType: Class<*>): IllegalArgumentException {
    if (eventType.isArray) {
        return IllegalArgumentException(
            "${eventType.typeName} cannot be a declared event type, since this expansion does not support an array. Declare the concrete event types instead."
        )
    }
    if (eventType.isPrimitive) {
        return IllegalArgumentException(
            "${eventType.typeName} cannot be a declared event type, since no event is ever an instance of a primitive type. Declare the concrete event types instead."
        )
    }
    return IllegalArgumentException(
        "the concrete event types dispatch would accept for ${eventType.name} cannot all be enumerated, so a filter " +
            "derived from it would miss some of them. Declare the concrete event types instead, or make ${eventType.simpleName} " +
            "and every level below it final or sealed."
    )
}

/**
 * Build a [StreamSubscriptionFilter] (stream capability) that matches the cloud event types derived from the given
 * domain [eventTypes]. An empty [eventTypes] matches all events.
 */
fun <E : Any> subscriptionFilterFromEventTypes(cloudEventConverter: CloudEventConverter<E>, eventTypes: Array<out KClass<out E>>): StreamSubscriptionFilter =
    StreamSubscriptionFilter.filter(filterFromEventTypes(cloudEventConverter, eventTypes))

/**
 * Build an [AgnosticSubscriptionFilter] (capability-agnostic) that matches the cloud event types derived from the given
 * domain [eventTypes]. An empty [eventTypes] matches all events, of every capability.
 */
fun <E : Any> agnosticSubscriptionFilterFromEventTypes(cloudEventConverter: CloudEventConverter<E>, eventTypes: Array<out KClass<out E>>): AgnosticSubscriptionFilter =
    AgnosticSubscriptionFilter.filter(filterFromEventTypes(cloudEventConverter, eventTypes))
