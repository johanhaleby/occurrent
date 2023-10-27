/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.dsl.decider

import org.junit.jupiter.api.Test
import org.occurrent.command.ChangeName
import org.occurrent.command.DefineName
import org.occurrent.command.NameCommand
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import java.time.LocalDateTime
import java.util.*


class MyDeciderTest {

    @Test
    fun `simple decider test`() {
        // Given
        val decider = decider<NameCommand, String, DomainEvent>(
            initialState = null,
            decide = { cmd, state ->
                when (cmd) {
                    is DefineName -> Name.defineTheName(UUID.randomUUID().toString(), cmd.time, cmd.name)
                    is ChangeName -> Name.changeNameFromCurrent(UUID.randomUUID().toString(), cmd.time, state, cmd.newName)
                }
            },
            evolve = { _, e ->
                when (e) {
                    is NameDefined -> e.name
                    is NameWasChanged -> e.name
                }
            }
        )

        // When
        val (state, events) = decider.decide(
            events = listOf(
                NameDefined("event1", LocalDateTime.now(), "Johan Haleby"),
                NameWasChanged("event2", LocalDateTime.now(), "Eric Evans"),
                NameWasChanged("event3", LocalDateTime.now(), "Tina Haleby"),
                NameWasChanged("event4", LocalDateTime.now(), "John Doe")
            ),
            command = ChangeName("id", LocalDateTime.now(), "Another Name")
        )

        // Then
    }
}