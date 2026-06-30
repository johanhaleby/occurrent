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
     * The streamId of the event
     */
    val streamId: String get() = data[OccurrentCloudEventExtension.STREAM_ID] as String

    /**
     * The version of the event in the stream
     */
    val streamVersion: Long get() = data[OccurrentCloudEventExtension.STREAM_VERSION] as Long

    inline operator fun <reified T : Any?> get(key: String) = data[key] as T

    companion object {
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
