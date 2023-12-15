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

import arrow.core.Either.Companion.catch
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
import org.occurrent.dsl.decider.arrow.decider
import java.time.LocalDateTime
import java.util.*


@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class MyDeciderTest {

    @Test
    fun `simple decider test`() {
        // Given
        val decider = decider<NameCommand, String?, DomainEvent, Throwable>(
            initialState = null,
            decide = { cmd, currentName ->
                when (cmd) {
                    is DefineName -> catch {
                        Name.defineTheName(UUID.randomUUID().toString(), cmd.time, cmd.userId, cmd.name)
                    }

                    is ChangeName -> catch {
                        Name.changeNameFromCurrent(UUID.randomUUID().toString(), cmd.time, cmd.userId, currentName, cmd.newName)
                    }
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
        val success = decider.decide(
            command = ChangeName("id", LocalDateTime.now(), "name", "Another Name"),
            state = "Jane Doe"
        )

        val fail = decider.decide(
            command = ChangeName("id", LocalDateTime.now(), "name", "Another Name"),
            state = "John Doe"
        )

        // Then
        val (nameChanged) = success.getOrNull()!!
        assertAll(
            { assertThat(nameChanged.name()).isEqualTo("Another Name") },
            { assertThat(fail.leftOrNull()).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("Cannot change name from John Doe since this is the ultimate name") }
        )
    }
}