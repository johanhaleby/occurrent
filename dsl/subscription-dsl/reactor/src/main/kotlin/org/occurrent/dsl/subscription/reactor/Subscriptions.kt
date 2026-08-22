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

package org.occurrent.dsl.subscription.reactor

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
import org.occurrent.subscription.api.reactor.Subscribable
import org.occurrent.subscription.api.reactor.Subscription
import reactor.core.publisher.Mono
import java.util.function.BiFunction
import java.util.function.Function
import kotlin.reflect.KClass


/**
 * Subscription DSL entry-point. Usage example:
 *
 * ```
 * val mySubscriptionModel = ..
 * val myCloudEventConverter = ..
 * streamSubscriptions(mySubscriptionModel, myCloudEventConverter) {
 *      subscribe<MyEvent>("subscriptionId") { event ->
 *          ...
 *          Mono.empty()
 *      }
 * }
 * ```
 *
 * This will create a subscription with id "subscriptionId" and subscribe to all events of type "MyEvent" (it uses the [cloudEventConverter] to derive the cloud event type from the domain event type).
 */
fun <E : Any> streamSubscriptions(subscriptionModel: Subscribable, cloudEventConverter: CloudEventConverter<E>, block: StreamSubscriptions<E>.() -> Unit) {
    StreamSubscriptions(subscriptionModel, cloudEventConverter).apply(block)
}

class StreamSubscriptions<E : Any>(private val subscriptionModel: Subscribable, private val cloudEventConverter: CloudEventConverter<E>) {

    /**
     * Derives a stable default subscription id from the cloud event type that [cloudEventConverter] maps [type] to.
     * This is a genuinely non-inline function so that changing the cloud event type mapping doesn't require recompiling callers.
     */
    fun defaultSubscriptionId(type: KClass<out E>): String = cloudEventConverter.getCloudEventType(type.java)

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, startAt) { _, e -> fn(e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (EventMetadata, E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, *emptyArray(), startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (E1) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { _, e -> fn(e as E1) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (EventMetadata, E1) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { metadata, e -> fn(metadata, e as E1) }
    }


    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { _, e -> fn(e) }
    }

    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (EventMetadata, E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: Function<E1, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { e: E ->
            @Suppress("UNCHECKED_CAST")
            fn.apply(e as E1)
        }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: BiFunction<EventMetadata, E1, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { metadata, e ->
            @Suppress("UNCHECKED_CAST")
            fn.apply(metadata, e as E1)
        }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: Function<E, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { e -> fn.apply(e) }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: BiFunction<EventMetadata, E, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { metadata, e -> fn.apply(metadata, e) }
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, fn: (E) -> Mono<Void>): Subscription {
        val filter = subscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, fn)
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, fn: (EventMetadata, E) -> Mono<Void>): Subscription {
        val filter = subscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, fn)
    }

    fun subscribe(subscriptionId: String, filter: StreamSubscriptionFilter = StreamSubscriptionFilter.filter(Filter.all()), startAt: StartAt? = null, fn: (E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, filter, startAt) { _, e -> fn(e) }
    }

    /**
     * Unlike the blocking DSL this overload has no `waitUntilStarted` flag. The blocking DSL blocks the calling
     * thread until the subscription has started, which only makes sense for a synchronous API. Here [Subscription]
     * is returned immediately and exposes its own [Subscription.waitUntilStarted] returning a `Mono<Void>` that the
     * caller can compose into their own reactive chain if they need to wait, so no extra parameter is needed on `subscribe` itself.
     */
    @JvmOverloads
    fun subscribe(
        subscriptionId: String,
        filter: StreamSubscriptionFilter = StreamSubscriptionFilter.filter(Filter.all()),
        startAt: StartAt? = null,
        fn: (EventMetadata, E) -> Mono<Void>
    ): Subscription {
        val action: (CloudEvent) -> Mono<Void> = { cloudEvent ->
            val event = cloudEventConverter[cloudEvent]
            val eventMetadata = EventMetadata.from(cloudEvent)
            fn(eventMetadata, event)
        }

        return if (startAt == null) {
            subscriptionModel.subscribe(subscriptionId, filter, action)
        } else {
            subscriptionModel.subscribe(subscriptionId, filter, startAt, action)
        }
    }
}

/**
 * Capability-agnostic subscription DSL entry-point. On a store with both the `STREAM` and `DCB` capabilities it
 * delivers both stream-written and DCB-appended events, filtered only by event type. Usage example:
 *
 * ```
 * val mySubscriptionModel = ..
 * val myCloudEventConverter = ..
 * subscriptions(mySubscriptionModel, myCloudEventConverter) {
 *      subscribe<MyEvent>("subscriptionId") { event ->
 *          ...
 *          Mono.empty()
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
 * The capability-agnostic subscription DSL. It mirrors the method surface of [StreamSubscriptions] but routes through an
 * [AgnosticSubscriptionFilter] instead of a [StreamSubscriptionFilter], so on a store with both the `STREAM` and
 * `DCB` capabilities it delivers both stream-written and DCB-appended events, filtered only by event type. Use
 * [StreamSubscriptions] or `DcbSubscriptions` when a subscription should be scoped to a single capability.
 */
class Subscriptions<E : Any>(private val subscriptionModel: Subscribable, private val cloudEventConverter: CloudEventConverter<E>) {

    /**
     * Derives a stable default subscription id from the cloud event type that [cloudEventConverter] maps [type] to.
     * This is a genuinely non-inline function so that changing the cloud event type mapping doesn't require recompiling callers.
     */
    fun defaultSubscriptionId(type: KClass<out E>): String = cloudEventConverter.getCloudEventType(type.java)

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, startAt) { _, e -> fn(e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    @JvmName("subscribeAll")
    fun subscribe(subscriptionId: String, startAt: StartAt? = null, fn: (EventMetadata, E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, *emptyArray(), startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (E1) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { _, e -> fn(e as E1) }
    }

    /**
     * Create a new subscription that is invoked after a specific domain event is written to the event store
     */
    inline fun <reified E1 : E> subscribe(subscriptionId: String = defaultSubscriptionId(E1::class), startAt: StartAt? = null, crossinline fn: (EventMetadata, E1) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, startAt = startAt) { metadata, e -> fn(metadata, e as E1) }
    }


    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { _, e -> fn(e) }
    }

    @JvmName("subscribeAnyOf")
    inline fun <reified E1 : E, reified E2 : E> subscribe(subscriptionId: String, startAt: StartAt? = null, crossinline fn: (EventMetadata, E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, E1::class, E2::class, startAt = startAt) { metadata, e -> fn(metadata, e) }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: Function<E1, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { e: E ->
            @Suppress("UNCHECKED_CAST")
            fn.apply(e as E1)
        }
    }

    @JvmOverloads
    fun <E1 : E> subscribe(subscriptionId: String, eventType: Class<E1>, startAt: StartAt? = null, fn: BiFunction<EventMetadata, E1, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, listOf(eventType), startAt) { metadata, e ->
            @Suppress("UNCHECKED_CAST")
            fn.apply(metadata, e as E1)
        }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: Function<E, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { e -> fn.apply(e) }
    }

    @JvmOverloads
    fun subscribe(subscriptionId: String, eventTypes: List<Class<out E>>, startAt: StartAt? = null, fn: BiFunction<EventMetadata, E, Mono<Void>>): Subscription {
        return subscribe(subscriptionId, *eventTypes.map { c -> c.kotlin }.toTypedArray(), startAt = startAt) { metadata, e -> fn.apply(metadata, e) }
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, fn: (E) -> Mono<Void>): Subscription {
        val filter = agnosticSubscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, fn)
    }

    fun subscribe(subscriptionId: String, vararg eventTypes: KClass<out E>, startAt: StartAt? = null, fn: (EventMetadata, E) -> Mono<Void>): Subscription {
        val filter = agnosticSubscriptionFilterFromEventTypes(cloudEventConverter, eventTypes)
        return subscribe(subscriptionId, filter, startAt, fn)
    }

    fun subscribe(subscriptionId: String, filter: AgnosticSubscriptionFilter = AgnosticSubscriptionFilter.filter(Filter.all()), startAt: StartAt? = null, fn: (E) -> Mono<Void>): Subscription {
        return subscribe(subscriptionId, filter, startAt) { _, e -> fn(e) }
    }

    /**
     * Unlike the blocking DSL this overload has no `waitUntilStarted` flag. The blocking DSL blocks the calling
     * thread until the subscription has started, which only makes sense for a synchronous API. Here [Subscription]
     * is returned immediately and exposes its own [Subscription.waitUntilStarted] returning a `Mono<Void>` that the
     * caller can compose into their own reactive chain if they need to wait, so no extra parameter is needed on `subscribe` itself.
     */
    @JvmOverloads
    fun subscribe(
        subscriptionId: String,
        filter: AgnosticSubscriptionFilter = AgnosticSubscriptionFilter.filter(Filter.all()),
        startAt: StartAt? = null,
        fn: (EventMetadata, E) -> Mono<Void>
    ): Subscription {
        val action: (CloudEvent) -> Mono<Void> = { cloudEvent ->
            val event = cloudEventConverter[cloudEvent]
            val eventMetadata = EventMetadata.from(cloudEvent)
            fn(eventMetadata, event)
        }

        return if (startAt == null) {
            subscriptionModel.subscribe(subscriptionId, filter, action)
        } else {
            subscriptionModel.subscribe(subscriptionId, filter, startAt, action)
        }
    }
}
