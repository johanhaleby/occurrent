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
import org.occurrent.dsl.subscription.blocking.EventMetadata
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbQuery
import org.occurrent.subscription.DcbSubscriptionFilter
import org.occurrent.subscription.StartAt
import org.occurrent.subscription.api.blocking.Subscribable
import org.occurrent.subscription.api.blocking.Subscription

/**
 * The DCB sequence position of an event, or `null` when the event has no DCB position.
 *
 * Events delivered by `subscribeDcb` are DCB-tagged and therefore have a non-null position.
 */
val EventMetadata.dcbPosition: Long?
    get() {
        val position = DcbEventMetadata.decodePosition(data[DcbCloudEvents.POSITION])
        return if (position.isPresent) position.asLong else null
    }

/**
 * The canonical DCB tags of an event, or an empty set when the event has no DCB tags.
 */
val EventMetadata.dcbTags: Set<String> get() = DcbEventMetadata.decodeTags(data[DcbCloudEvents.TAGS])

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
 *
 * The callback receives the regular subscription DSL [EventMetadata]. Import [dcbPosition] and [dcbTags]
 * from this package to read DCB-specific metadata from it.
 */
@JvmOverloads
@JvmName("subscribeDcbWithMetadata")
fun <E : Any> Subscribable.subscribeDcb(
    subscriptionId: String,
    cloudEventConverter: CloudEventConverter<E>,
    query: DcbQuery = DcbQuery.all(),
    startAt: StartAt? = null,
    waitUntilStarted: Boolean = true,
    fn: (EventMetadata, E) -> Unit
): Subscription {
    // A capable subscription model filters DCB events server-side from the DcbSubscriptionFilter. The in-process
    // check stays as a correctness floor for any Subscribable that does not honor the filter.
    val consumer: (CloudEvent) -> Unit = { cloudEvent ->
        if (DcbCloudEvents.getPosition(cloudEvent) > 0 && DcbCloudEvents.matches(cloudEvent, query)) {
            val event = cloudEventConverter[cloudEvent]
            val metadata = EventMetadata(cloudEvent.extensionNames.associateWith { extensionName -> cloudEvent.getExtension(extensionName) })
            fn(metadata, event)
        }
    }

    val filter = DcbSubscriptionFilter.filter(query)
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
