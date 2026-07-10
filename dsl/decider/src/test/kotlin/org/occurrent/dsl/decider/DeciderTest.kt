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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.command.ChangeName
import org.occurrent.command.DefineName
import org.occurrent.command.NameCommand
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.Name
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import java.time.LocalDateTime
import java.util.*


@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class DeciderTest {

    @Test
    fun `simple decider test`() {
        // Given
        val decider = decider<NameCommand, String?, DomainEvent>(
            initialState = null,
            decide = { cmd, state ->
                when (cmd) {
                    is DefineName -> Name.defineTheName(UUID.randomUUID().toString(), cmd.time, cmd.userId, cmd.name)
                    is ChangeName -> Name.changeNameFromCurrent(UUID.randomUUID().toString(), cmd.time, cmd.userId, state, cmd.newName)
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
                NameDefined("event1", LocalDateTime.now(), "name", "Johan Haleby"),
                NameWasChanged("event2", LocalDateTime.now(), "name", "Eric Evans"),
                NameWasChanged("event3", LocalDateTime.now(), "name", "Tina Haleby"),
                NameWasChanged("event4", LocalDateTime.now(), "name", "Some Doe")
            ),
            command = ChangeName("id", LocalDateTime.now(), "name", "Another Name")
        )

        // Then
        assertAll(
            { assertThat(state).isEqualTo("Another Name") },
            { assertThat(events).hasSize(1) },
            { assertThat(events[0].name()).isEqualTo("Another Name") },
        )
    }

    // Regression: over a non-null state type, one decision's resulting state must feed straight into the
    // next decide call. A @Nullable S return would type these as SwitchState? and fail to compile under
    // the module's JSpecify null-marking.
    @Test
    fun `chaining decideOnStateAndReturnState compiles and transitions for a non-null state`() {
        // Given
        val decider = decider<ToggleCommand, SwitchState, SwitchEvent>(
            initialState = SwitchState.Off,
            decide = { _, _ -> listOf(SwitchEvent.Toggled) },
            evolve = { state, _ ->
                when (state) {
                    SwitchState.Off -> SwitchState.On
                    SwitchState.On -> SwitchState.Off
                }
            }
        )

        // When
        val state1: SwitchState = decider.decideOnStateAndReturnState(SwitchState.Off, ToggleCommand.Toggle)
        val state2: SwitchState = decider.decideOnStateAndReturnState(state1, ToggleCommand.Toggle)

        // Then
        assertAll(
            { assertThat(state1).isEqualTo(SwitchState.On) },
            { assertThat(state2).isEqualTo(SwitchState.Off) },
        )
    }
}

private sealed interface SwitchState {
    object Off : SwitchState
    object On : SwitchState
}

private sealed interface ToggleCommand {
    object Toggle : ToggleCommand
}

private sealed interface SwitchEvent {
    object Toggled : SwitchEvent
}
