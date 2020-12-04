/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.eventstore.api.blocking

import io.cloudevents.CloudEvent
import org.occurrent.eventstore.api.WriteCondition
import kotlin.streams.asStream


/**
 * A convenience function that writes events to an event store if the stream version is equal to {@code expectedStreamVersion}.
 * @see ConditionallyWriteToEventStream.write
 */
fun ConditionallyWriteToEventStream.write(streamId: String, expectedStreamVersion: Long, events: Sequence<CloudEvent>) {
    write(streamId, expectedStreamVersion, events.asStream())
}

/**
 * Conditionally write to an event store
 * @see ConditionallyWriteToEventStream.write
 */
fun ConditionallyWriteToEventStream.write(streamId: String, writeCondition: WriteCondition, events: Sequence<CloudEvent>) {
    write(streamId, writeCondition, events.asStream())
}