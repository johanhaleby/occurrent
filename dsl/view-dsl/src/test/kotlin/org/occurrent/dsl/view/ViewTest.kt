/*
 *
 *  Copyright 2025 Johan Haleby
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

package org.occurrent.dsl.view

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.dsl.view.ViewTest.Name.Defined
import org.occurrent.dsl.view.testsupport.NameState
import org.occurrent.dsl.view.testsupport.nameChanged
import org.occurrent.dsl.view.testsupport.nameDefined
import java.util.*
import java.util.stream.Stream

@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class ViewTest {

    sealed interface Name {
        data object Undefined : Name
        data class Defined(val state: NameState) : Name
    }

    companion object {
        private val nullableView: View<NameState?, DomainEvent> = view<NameState?, DomainEvent>(null) { s, e ->
            when (e) {
                is NameDefined -> NameState(e.userId(), e.name)
                is NameWasChanged -> s!!.copy(name = e.name)
            }
        }

        private val nonNullableView: View<Name, DomainEvent> = view<Name, DomainEvent>(Name.Undefined) { s, e ->
            when (e) {
                is NameDefined -> Defined(NameState(e.userId(), e.name))
                is NameWasChanged -> (s as Defined).copy(state = s.state.copy(name = e.name))
            }
        }
    }

    @Nested
    @DisplayName("nullable state")
    inner class NullableState {

        @Test
        fun `evolve with varargs when specifying initial state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                null,
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2"),
                nameChanged(userId, "name3"),
                nameChanged(userId, "name4"),
                nameChanged(userId, "name5"),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolve with varargs when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2"),
                nameChanged(userId, "name3"),
                nameChanged(userId, "name4"),
                nameChanged(userId, "name5"),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolveFrom from sequence when specifying initial state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolveFrom(
                null,
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolve from sequence when specifying initial state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                null,
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolveAll from sequence when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolveAll(
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolve from list when specifying initial state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                null,
                listOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolve from list when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                listOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolve from Stream when specifying initial state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                null,
                Stream.of(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }

        @Test
        fun `evolve from Stream when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolve(
                Stream.of(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }
    }

    @Nested
    @DisplayName("non-nullable state")
    inner class NonNullableState {

        @Test
        fun `evolve with varargs when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveFrom(
                Name.Undefined,
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2"),
                nameChanged(userId, "name3"),
                nameChanged(userId, "name4"),
                nameChanged(userId, "name5"),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolve with varargs from initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveAll(
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2"),
                nameChanged(userId, "name3"),
                nameChanged(userId, "name4"),
                nameChanged(userId, "name5"),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolveFrom from sequence when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveFrom(
                Name.Undefined,
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolveAll from sequence from initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveAll(
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolve from sequence when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolve(
                Name.Undefined,
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolve from list when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveFrom(
                Name.Undefined,
                listOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolve from list when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveAll(
                listOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolve from Stream when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveFrom(
                Name.Undefined,
                Stream.of(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name5")))
        }

        @Test
        fun `evolve from Stream from initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolveAll(
                Stream.of(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3"),
                    nameChanged(userId, "name4"),
                    nameChanged(userId, "name5")
                ),
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name5"))
        }
    }
}