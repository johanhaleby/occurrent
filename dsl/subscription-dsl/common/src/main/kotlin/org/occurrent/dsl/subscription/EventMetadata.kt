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

import io.cloudevents.CloudEvent
import org.occurrent.cloudevents.OccurrentCloudEventExtension

/**
 * Metadata associated with the event, such as stream id and version and other CloudEvent extensions
 * associated with the event.
 */
data class EventMetadata(val data: Map<String, Any?>) {
    /**
     * The streamId of the event.
     *
     * Note that for an event delivered through the capability-agnostic or DCB path, this is the internal generated
     * partition id (for example "dcb:partition:...") rather than a domain stream id. It is always non-null, it is just
     * semantically an internal id in that case.
     */
    val streamId: String get() = data[OccurrentCloudEventExtension.STREAM_ID] as String

    /**
     * The version of the event in the stream
     */
    val streamVersion: Long get() = data[OccurrentCloudEventExtension.STREAM_VERSION] as Long

    /**
     * The global, monotonic sequence position of the event, or `null` when the event has no position (for example a
     * stream-written event on a store that does not write stream position). DCB-written events always have a
     * position. Java callers can use `DcbEventMetadata.position()` for an `OptionalLong` view of the same value.
     */
    val position: Long?
        get() = when (val value = data[OccurrentCloudEventExtension.POSITION]) {
            null -> null
            is Number -> value.toLong()
            is String -> value.toLong()
            else -> throw IllegalArgumentException("Position extension must be a Number or String")
        }

    /**
     * Reads an arbitrary extension [key] from the metadata and casts it to [T]. The cast is unchecked, so an extension
     * whose stored value is not a [T] throws a [ClassCastException] here, at the point of the cast, since being
     * `inline` this call is effectively inlined into the caller. Prefer the typed accessors ([streamId],
     * [streamVersion], [position]) where they exist.
     */
    inline operator fun <reified T : Any?> get(key: String) = data[key] as T

    companion object {
        private val EMPTY = EventMetadata(emptyMap())

        /**
         * Metadata with no extensions, for a fold or reaction invoked without a CloudEvent (for example on-demand
         * replay from a query, where events never carried a CloudEvent). [position] is `null` and the stream accessors
         * throw, since there is no stream id or version to read.
         */
        @JvmStatic
        fun empty(): EventMetadata = EMPTY

        /**
         * Build the metadata from a [CloudEvent], capturing its extensions (stream id and version, the DCB position and
         * tags, and any others). Used by both the blocking and the reactive subscription DSLs so the two read the same
         * thing.
         */
        @JvmStatic
        fun from(cloudEvent: CloudEvent): EventMetadata =
            EventMetadata(cloudEvent.extensionNames.mapNotNull { name -> cloudEvent.getExtension(name)?.let { value -> name to value } }.toMap())
    }
}
