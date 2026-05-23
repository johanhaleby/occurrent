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

package org.occurrent.dsl.dcb.blocking

import io.cloudevents.CloudEvent
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.get
import org.occurrent.cloudevents.OccurrentCloudEventExtension
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.filter.Filter
import org.occurrent.subscription.OccurrentSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.Subscribable
import org.occurrent.subscription.api.blocking.Subscription

/**
 * Metadata associated with a DCB-tagged event delivered through a live subscription.
 */
data class DcbEventMetadata internal constructor(val data: Map<String, Any?>) {
    /**
     * The Occurrent storage stream id of the event.
     */
    val streamId: String get() = data[OccurrentCloudEventExtension.STREAM_ID] as String

    /**
     * The version of the event in its Occurrent storage stream.
     */
    val streamVersion: Long get() = data[OccurrentCloudEventExtension.STREAM_VERSION] as Long

    /**
     * The DCB sequence position of the event.
     */
    val dcbPosition: Long get() = position(data[DcbCloudEvents.POSITION])

    /**
     * The canonical DCB tags of the event.
     */
    val dcbTags: Set<String> get() = tags(data[DcbCloudEvents.TAGS])

    inline operator fun <reified T : Any?> get(key: String) = data[key] as T
}

/**
 * Subscribes to live DCB-tagged events that match [query].
 *
 * This is a live CloudEvent subscription convenience that post-filters DCB-tagged
 * events by [DcbQuery]. It is not a DCB read and it does not provide DCB append-condition
 * or high-watermark guarantees.
 */
@JvmName("subscribeDcb")
fun <E : Any> Subscribable.subscribeDcb(
    subscriptionId: String,
    cloudEventConverter: CloudEventConverter<E>,
    query: DcbQuery = DcbQuery.all(),
    startAt: StartAt? = null,
    fn: (E) -> Unit
): Subscription =
    subscribeDcb(subscriptionId, cloudEventConverter, query, startAt) { _, event -> fn(event) }

/**
 * Subscribes to live DCB-tagged events that match [query], including DCB metadata in the callback.
 */
@JvmOverloads
@JvmName("subscribeDcbWithMetadata")
fun <E : Any> Subscribable.subscribeDcb(
    subscriptionId: String,
    cloudEventConverter: CloudEventConverter<E>,
    query: DcbQuery = DcbQuery.all(),
    startAt: StartAt? = null,
    waitUntilStarted: Boolean = true,
    fn: (DcbEventMetadata, E) -> Unit
): Subscription {
    val consumer: (CloudEvent) -> Unit = { cloudEvent ->
        if (DcbCloudEvents.getPosition(cloudEvent) > 0 && DcbCloudEvents.matches(cloudEvent, query)) {
            val event = cloudEventConverter[cloudEvent]
            val metadata = DcbEventMetadata(cloudEvent.extensionNames.associateWith { extensionName -> cloudEvent.getExtension(extensionName) })
            fn(metadata, event)
        }
    }

    val filter = OccurrentSubscriptionFilter.filter(Filter.all())
    val subscription = if (startAt == null) {
        subscribe(subscriptionId, filter, consumer)
    } else {
        subscribe(subscriptionId, filter, startAt, consumer)
    }

    return subscription.apply {
        if (waitUntilStarted) {
            waitUntilStarted()
        }
    }
}

private fun tags(value: Any?): Set<String> = when (value) {
    null -> emptySet()
    is String -> if (value.isEmpty()) emptySet() else DcbCloudEvents.canonicalizeTags(value.split("\n"))
    else -> throw IllegalArgumentException("DCB tags extension must be a String")
}

private fun position(value: Any?): Long = when (value) {
    null -> 0
    is Number -> value.toLong()
    is String -> value.toLong()
    else -> throw IllegalArgumentException("DCB position extension must be a Number or String")
}
