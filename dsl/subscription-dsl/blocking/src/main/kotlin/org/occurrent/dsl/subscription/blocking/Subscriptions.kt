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
import org.occurrent.cloudevents.EventMetadata
import org.occurrent.dsl.subscription.agnosticSubscriptionFilterFromEventTypes
import org.occurrent.dsl.subscription.subscriptionFilterFromEventTypes
import org.occurrent.filter.Filter
import org.occurrent.subscription.AgnosticSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.StreamSubscriptionFilter
import org.occurrent.subscription.api.blocking.Subscribable
import org.occurrent.subscription.api.blocking.Subscription
import java.util.function.BiConsumer
import java.util.function.Consumer
import kotlin.reflect.KClass


/**
 * Subscription DSL entry-point. Usage example:
 *
 * ```
 * val mySubscriptionModel = ..
 * val myCloudEventConverter = ..
 * streamSubscriptions(mySubscriptionModel, myCloudEventConverter) {
 *      subscribe<MyEvent>("subscriptionId") {
 *          ...
 *      }
 * }
 * ```
 *
 * This will create a subscription with id "subscriptionId" and subscribe to all events of type "MyEvent" (it uses the [cloudEventConverter] to derive the cloud event type from the domain event type).
 */
fun <E : Any> streamSubscriptions(subscriptionModel: Subscribable, cloudEventConverter: CloudEventConverter<E>, block: StreamSubscriptions<E>.() -> Unit) {
    StreamSubscriptions(subscriptionModel, cloudEventConverter).apply(block)
}

/**
 * Capability-agnostic subscription DSL entry-point. On a store with both the `STREAM` and `DCB` capabilities it
 * delivers both stream-written and DCB-appended events, filtered only by event type. Usage example:
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
 * Use [streamSubscriptions] or the DCB counterpart when a subscription should be scoped to a single capability.
 */
fun <E : Any> subscriptions(subscriptionModel: Subscribable, cloudEventConverter: CloudEventConverter<E>, block: Subscriptions<E>.() -> Unit) {
    Subscriptions(subscriptionModel, cloudEventConverter).apply(block)
}

/**
 * @property subscriptionModel The model every `subscribe` call here runs on, exposed for a caller that needs to
 * check it for a capability, such as [org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions], rather than
 * resolving a possibly different bean of the same type from elsewhere.
 */
open class StreamSubscriptions<E : Any>(val subscriptionModel: Subscribable, private val cloudEventConverter: CloudEventConverter<E>) {

    /**
     * Derives a stable default subscription id from the cloud event type that [cloudEventConverter] maps [type] to.
     * This is a genuinely non-inline function so that changing the cloud event type mapping doesn't require recompiling callers.
     */
    fun defaultSubscriptionId(type: KClass<out E>): String = cloudEventConverter.getCloudEventType(type.java)

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, startAt) { _, e -> fn(e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (EventMetadata, E) -> Unit): Subscription {
        return subscribe(subscriptionId, *emptyArray(), startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (E1) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { _, e -> fn(e as E1) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (EventMetadata, E1) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { metadata, e -> fn(metadata, e as E1) }
    }


    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { _, e -> fn(e) }
    }

    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (EventMetadata, E) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: Consumer<E1>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { e: E ->
            @Suppress("UNCHECKED_CAST")
            fn.accept(e as E1)
        }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: BiConsumer<EventMetadata, E1>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { metadata, e ->
            @Suppress("UNCHECKED_CAST")
            fn.accept(metadata, e as E1)
        }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: Consumer<E>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { e -> fn.accept(e) }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: BiConsumer<EventMetadata, E>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { metadata, e -> fn.accept(metadata, e) }
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, waitUntilStarted: Boolean = true, fn: (E) -> Unit): Subscription {
        val filter = subscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, waitUntilStarted, fn)
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, waitUntilStarted: Boolean = true, fn: (EventMetadata, E) -> Unit): Subscription {
        val filter = subscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, waitUntilStarted, fn)
    }

    fun subscribe(subscriptionId: String, filter: StreamSubscriptionFilter = StreamSubscriptionFilter.filter(Filter.all()), startAt: StartAt? = null, waitUntilStarted: Boolean = true, fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, filter, startAt, waitUntilStarted) { _, e -> fn(e) }
    }

    @JvmOverloads
    fun subscribe(
        subscriptionId: String,
        filter: StreamSubscriptionFilter = StreamSubscriptionFilter.filter(Filter.all()),
        startAt: StartAt? = null,
        waitUntilStarted: Boolean = true,
        fn: (EventMetadata, E) -> Unit
    ): Subscription {
        val consumer: (CloudEvent) -> Unit = { cloudEvent ->
            val event = cloudEventConverter[cloudEvent]
            val eventMetadata = EventMetadata.from(cloudEvent)
            fn(eventMetadata, event)
        }

        val subscription = if (startAt == null) {
            subscriptionModel.subscribe(subscriptionId, filter, consumer)
        } else {
            subscriptionModel.subscribe(subscriptionId, filter, startAt, consumer)
        }

        return subscription.apply {
            if (waitUntilStarted) {
                waitUntilStarted()
            }
        }
    }
}

/**
 * The capability-agnostic subscription DSL. It mirrors the method surface of [StreamSubscriptions] but routes through an
 * [AgnosticSubscriptionFilter] instead of a [StreamSubscriptionFilter], so on a store with both the `STREAM` and
 * `DCB` capabilities it delivers both stream-written and DCB-appended events, filtered only by event type. Use
 * [StreamSubscriptions] or `DcbSubscriptions` when a subscription should be scoped to a single capability.
 *
 * @property subscriptionModel The model every `subscribe` call here runs on, exposed for a caller that needs to
 * check it for a capability, such as [org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions], rather than
 * resolving a possibly different bean of the same type from elsewhere.
 */
class Subscriptions<E : Any>(val subscriptionModel: Subscribable, private val cloudEventConverter: CloudEventConverter<E>) {

    /**
     * Derives a stable default subscription id from the cloud event type that [cloudEventConverter] maps [type] to.
     * This is a genuinely non-inline function so that changing the cloud event type mapping doesn't require recompiling callers.
     */
    fun defaultSubscriptionId(type: KClass<out E>): String = cloudEventConverter.getCloudEventType(type.java)

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, startAt) { _, e -> fn(e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (EventMetadata, E) -> Unit): Subscription {
        return subscribe(subscriptionId, *emptyArray(), startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (E1) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { _, e -> fn(e as E1) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (EventMetadata, E1) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { metadata, e -> fn(metadata, e as E1) }
    }


    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { _, e -> fn(e) }
    }

    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (EventMetadata, E) -> Unit): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: Consumer<E1>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { e: E ->
            @Suppress("UNCHECKED_CAST")
            fn.accept(e as E1)
        }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: BiConsumer<EventMetadata, E1>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { metadata, e ->
            @Suppress("UNCHECKED_CAST")
            fn.accept(metadata, e as E1)
        }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: Consumer<E>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { e -> fn.accept(e) }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: BiConsumer<EventMetadata, E>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { metadata, e -> fn.accept(metadata, e) }
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, waitUntilStarted: Boolean = true, fn: (E) -> Unit): Subscription {
        val filter = agnosticSubscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, waitUntilStarted, fn)
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, waitUntilStarted: Boolean = true, fn: (EventMetadata, E) -> Unit): Subscription {
        val filter = agnosticSubscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, waitUntilStarted, fn)
    }

    fun subscribe(subscriptionId: String, filter: AgnosticSubscriptionFilter = AgnosticSubscriptionFilter.filter(Filter.all()), startAt: StartAt? = null, waitUntilStarted: Boolean = true, fn: (E) -> Unit): Subscription {
        return subscribe(subscriptionId, filter, startAt, waitUntilStarted) { _, e -> fn(e) }
    }

    @JvmOverloads
    fun subscribe(
        subscriptionId: String,
        filter: AgnosticSubscriptionFilter = AgnosticSubscriptionFilter.filter(Filter.all()),
        startAt: StartAt? = null,
        waitUntilStarted: Boolean = true,
        fn: (EventMetadata, E) -> Unit
    ): Subscription {
        val consumer: (CloudEvent) -> Unit = { cloudEvent ->
            val event = cloudEventConverter[cloudEvent]
            val eventMetadata = EventMetadata.from(cloudEvent)
            fn(eventMetadata, event)
        }

        val subscription = if (startAt == null) {
            subscriptionModel.subscribe(subscriptionId, filter, consumer)
        } else {
            subscriptionModel.subscribe(subscriptionId, filter, startAt, consumer)
        }

        return subscription.apply {
            if (waitUntilStarted) {
                waitUntilStarted()
            }
        }
    }
}
