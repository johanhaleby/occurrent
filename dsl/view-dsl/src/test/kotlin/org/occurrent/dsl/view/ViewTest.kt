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
import java.util.concurrent.atomic.AtomicBoolean

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
        fun `evolve from sequence when specifying initial state explicitly`() {
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
        fun `evolveAll from sequence when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolveAll(
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
        fun `evolveAll from a lazy Sequence when null initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nullableView.evolveAll(
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2"),
                    nameChanged(userId, "name3")
                )
            )

            // Then
            assertThat(state).isEqualTo(NameState(userId, "name3"))
        }

        @Test
        fun `evolve from a java Stream without initial state produces the same result as the List-based fold`() {
            // Given
            val userId = UUID.randomUUID().toString()
            val events = listOf<DomainEvent>(
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2"),
                nameChanged(userId, "name3"),
                nameChanged(userId, "name4"),
                nameChanged(userId, "name5")
            )

            // When
            val stateFromList = nullableView.evolve(events)
            val stateFromStream = nullableView.evolve(events.stream())

            // Then
            assertThat(stateFromStream).isEqualTo(stateFromList)
        }

        @Test
        fun `evolve from a java Stream without initial state closes the stream after folding`() {
            // Given
            val userId = UUID.randomUUID().toString()
            val closed = AtomicBoolean(false)
            val stream = listOf<DomainEvent>(
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2")
            ).stream().onClose { closed.set(true) }

            // When
            nullableView.evolve(stream)

            // Then
            assertThat(closed.get()).isTrue()
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
        fun `evolveAll from sequence from initial state`() {
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
        fun `evolve from sequence when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolve(
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
        fun `evolveAll from a lazy Sequence from initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveAll(
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2")
                )
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name2")))
        }

        @Test
        fun `evolveFrom a lazy Sequence when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveFrom(
                Name.Undefined,
                sequenceOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2")
                )
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name2")))
        }

        @Test
        fun `evolve from a java Stream from initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolve(
                listOf<DomainEvent>(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2")
                ).stream()
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name2")))
        }

        @Test
        fun `evolve from a java Stream with an explicit state produces the same result as the List-based fold`() {
            // Given
            val userId = UUID.randomUUID().toString()
            val events = listOf<DomainEvent>(
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2"),
                nameChanged(userId, "name3"),
                nameChanged(userId, "name4"),
                nameChanged(userId, "name5")
            )

            // When
            val stateFromList = nonNullableView.evolve(Name.Undefined, events)
            val stateFromStream = nonNullableView.evolve(Name.Undefined, events.stream())

            // Then
            assertThat(stateFromStream).isEqualTo(stateFromList)
        }

        @Test
        fun `evolve from a java Stream with an explicit state closes the stream after folding`() {
            // Given
            val userId = UUID.randomUUID().toString()
            val closed = AtomicBoolean(false)
            val stream = listOf<DomainEvent>(
                nameDefined(userId, "name1"),
                nameChanged(userId, "name2")
            ).stream().onClose { closed.set(true) }

            // When
            nonNullableView.evolve(Name.Undefined, stream)

            // Then
            assertThat(closed.get()).isTrue()
        }

        @Test
        fun `evolveAll from an Iterable from initial state`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveAll(
                listOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2")
                ).asIterable()
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name2")))
        }

        @Test
        fun `evolveFrom an Iterable when specifying state explicitly`() {
            // Given
            val userId = UUID.randomUUID().toString()

            // When
            val state = nonNullableView.evolveFrom(
                Name.Undefined,
                listOf(
                    nameDefined(userId, "name1"),
                    nameChanged(userId, "name2")
                ).asIterable()
            )

            // Then
            assertThat(state).isEqualTo(Defined(NameState(userId, "name2")))
        }

    }

    @Nested
    inner class NoArgDsl {

        @Test
        fun `view with no argument starts from null like initialState null`() {
            // A witness with no ? still receives a nullable state, since the no-argument overload forces S?.
            val view = view<NameState, DomainEvent> { s, e ->
                when (e) {
                    is NameDefined -> NameState(e.userId(), e.name)
                    is NameWasChanged -> s?.copy(name = e.name)
                }
            }

            assertThat(view.initialState()).isNull()
        }

        @Test
        fun `metadata-aware view with no argument starts from null like initialState null`() {
            val view = view<Long, DomainEvent> { s, metadata, _ -> metadata.position ?: s }

            assertThat(view.initialState()).isNull()
        }
    }
}