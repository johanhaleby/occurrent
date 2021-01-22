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

package org.occurrent.application.composition.command

import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import java.time.LocalDateTime


object NameWithSequenceCommand {

    fun defineName(@Suppress("UNUSED_PARAMETER") events: Sequence<DomainEvent>, eventId: String, time: LocalDateTime, name: String): Sequence<DomainEvent> =
        sequenceOf(NameDefined(eventId, time, name))

    fun changeName(events: Sequence<DomainEvent>, eventId: String, time: LocalDateTime, newName: String): Sequence<DomainEvent> {
        val currentName = events.fold("") { _, e ->
            when (e) {
                is NameDefined -> e.name
                is NameWasChanged -> e.name
                else -> throw IllegalStateException()
            }
        }

        return when {
            currentName == "John Doe" -> throw IllegalArgumentException("Cannot change name from John Doe since this is the ultimate name")
            currentName.isBlank() -> throw IllegalArgumentException("Cannot change name since it is currently undefined")
            else -> sequenceOf(NameWasChanged(eventId, time, newName))
        }
    }
}