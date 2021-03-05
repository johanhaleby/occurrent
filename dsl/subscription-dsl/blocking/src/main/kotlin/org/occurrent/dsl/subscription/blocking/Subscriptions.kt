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
import org.occurrent.condition.Condition
import org.occurrent.filter.Filter
import org.occurrent.subscription.OccurrentSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.Subscribable
import org.occurrent.subscription.api.blocking.Subscription
import java.util.function.Consumer
import kotlin.reflect.KClass

/**
 * Subscription DSL
 */
fun <T : Any> subscriptions(subscriptionModel: Subscribable, cloudEventConverter: CloudEventConverter<T>, subscriptions: Subscriptions<T>.() -> Unit) {
    Subscriptions(subscriptionModel, cloudEventConverter).apply(subscriptions)
}

class Subscriptions<T : Any> @JvmOverloads constructor(
    private val subscriptionModel: Subscribable, private val cloudEventConverter: CloudEventConverter<T>,
    private val eventNameFromType: (KClass<out T>) -> String = { e -> e.simpleName!! }
) {

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (T) -> Unit): Subscription {
        return subscribe(subscriptionId, *emptyArray(), startAt = startAt) { e -> fn(e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E : T> subscribe(subscriptionId: String = E::class.simpleName!!, startAt: StartAt? = null, crossinline fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, E::class, startAt = startAt) { e -> fn(e as E) }
    }


    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : T, reified E2 : T> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (T) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { e -> fn(e) }
    }

    @JvmOverloads
    fun <E : T> subscribe(subscriptionId: String, eventType: Class<E>, startAt: StartAt? = null, fn: Consumer<E>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { e ->
            @Suppress("UNCHECKED_CAST")
            fn.accept(e as E)
        }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out T>>, startAt: StartAt? = null, fn: Consumer<T>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { e -> fn.accept(e) }
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out T>, startAt: StartAt? = null, fn: (T) -> Unit): Subscription {
        val condition = when {
            eventTypes.isEmpty() -> null
            eventTypes.size == 1 -> Condition.eq(eventNameFromType(eventTypes[0]))
            else -> Condition.or(eventTypes.map { e -> Condition.eq(eventNameFromType(e)) })
        }
        val filter = OccurrentSubscriptionFilter.filter(if (condition == null) Filter.all() else Filter.type(condition))
        return subscribe(subscriptionId, filter, startAt, fn)
    }

    fun subscribe(subscriptionId: String, filter: OccurrentSubscriptionFilter = OccurrentSubscriptionFilter.filter(Filter.all()), startAt: StartAt? = null, fn: (T) -> Unit): Subscription {
        val consumer: (CloudEvent) -> Unit = { cloudEvent ->
            val event = cloudEventConverter.toDomainEvent(cloudEvent)
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