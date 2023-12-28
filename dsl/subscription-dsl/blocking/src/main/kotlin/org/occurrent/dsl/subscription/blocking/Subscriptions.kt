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

package org.occurrent.dsl.subscription.blocking

import io.cloudevents.CloudEvent
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.get
import org.occurrent.condition.Condition
import org.occurrent.filter.Filter
import org.occurrent.subscription.OccurrentSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.Subscribable
import org.occurrent.subscription.api.blocking.Subscription
import java.util.function.Consumer
import kotlin.reflect.KClass

/**
 * Subscription DSL entry-point. Usage example:
 *
 * ```
 * val mySubscriptionModel = ..
 * val myCloudEventConverter = ..
 * subscriptions(mySubscriptionModel, myCloudEventConverter) {
 *      subscribe<MyEvent>("subscriptionId") {
 *          ...
 *      }
 * }
 * ```
 *
 * This will create a subscription with id "subscriptionId" and subscribe to all events of type "MyEvent" (it uses the [cloudEventConverter] to derive the cloud event type from the domain event type).
 */
fun <E : Any> subscriptions(subscriptionModel: Subscribable, cloudEventConverter: CloudEventConverter<E>, subscriptions: Subscriptions<E>.() -> Unit) {
    Subscriptions(subscriptionModel, cloudEventConverter).apply(subscriptions)
}


class Subscriptions<E : Any>(private val subscriptionModel: Subscribable, private val cloudEventConverter: CloudEventConverter<E>) {

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, *emptyArray(), startAt = startAt) { e -> fn(e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = E1::class.simpleName!!, startAt: StartAt? = null, crossinline fn: (E1) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { e -> fn(e as E1) }
    }


    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { e -> fn(e) }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E>, startAt: StartAt? = null, fn: Consumer<E>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { e ->
            fn.accept(e)
        }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: Consumer<E>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { e -> fn.accept(e) }
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, fn: (E) -> Unit): Subscription {
        val condition = when {
            eventTypes.isEmpty() -> null
            eventTypes.size == 1 -> Condition.eq(cloudEventConverter[eventTypes[0]])
            else -> Condition.or(eventTypes.map { e -> Condition.eq(cloudEventConverter[e]) })
        }
        val filter = OccurrentSubscriptionFilter.filter(if (condition == null) Filter.all() else Filter.type(condition))
        return subscribe(subscriptionId, filter, startAt, fn)
    }

    fun subscribe(subscriptionId: String, filter: OccurrentSubscriptionFilter = OccurrentSubscriptionFilter.filter(Filter.all()), startAt: StartAt? = null, fn: (E) -> Unit): Subscription {
        val consumer: (CloudEvent) -> Unit = { cloudEvent ->
            val event = cloudEventConverter[cloudEvent]
            fn(event)
        }

        val subscription = if (startAt == null) {
            subscriptionModel.subscribe(subscriptionId, filter, consumer)
        } else {
            subscriptionModel.subscribe(subscriptionId, filter, startAt, consumer)
        }

        return subscription.apply {
            waitUntilStarted()
        }
    }
}