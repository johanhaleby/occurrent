/*
 *
 *  Copyright 2021 Johan Haleby
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

package org.occurrent.example.domain.rps.model

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.ObjectAssert
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

@DisplayName("Rock Paper Scissors")
class GamePlayTest {

    @Nested
    @DisplayName("when game is not created")
    inner class WhenGameIsNotCreated {

        @Test
        fun `then creating game will return GameCreatedEvent`() {
            // Given
            val currentEvents = emptySequence<GameEvent>()
            val gameId = GameId.random()
            val timestamp = Timestamp.now()
            val creator = GameCreatorId.random()
            val maxNumberOfRounds = MaxNumberOfRounds(1)

            // When
            val newEvents = handle(currentEvents, CreateGame(gameId, timestamp, creator, maxNumberOfRounds))

            // Then
            assertThat(newEvents).containsExactly(GameCreated(gameId, timestamp, creator, maxNumberOfRounds))
        }

        @Test
        fun `then play hand will throw GameDoesNotExist exception`() {
            // Given
            val currentEvents = emptySequence<GameEvent>()

            // When
            val throwable = catchThrowable { handle(currentEvents, PlayHand(Timestamp.now(), PlayerId.random(), Shape.PAPER)) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameDoesNotExist::class.java)
        }
    }

    @Nested
    @DisplayName("when game is created")
    inner class WhenGameIsCreated {

        @Test
        fun `then game cannot be created again`() {
            // Given
            val currentEvents = sequenceOf(GameCreated(GameId.random(), Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))

            // When
            val throwable = catchThrowable { handle(currentEvents, CreateGame(GameId.random(), Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1))) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameCannotBeCreatedMoreThanOnce::class.java)
        }

        @Nested
        @DisplayName("and first hand is played")
        inner class AndFirstHandIsPlayed {

            private val gameId = GameId.random()
            private val currentEvents = sequenceOf(GameCreated(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))

            @Test
            fun `then game is started`() {
                // Given
                val timestamp = Timestamp.now()
                val playerId = PlayerId.random()

                // When
                val newEvents = handle(currentEvents, PlayHand(timestamp, playerId, Shape.PAPER))

                // Then
                assertThat(newEvents).contains(GameStarted(gameId, timestamp))
            }

            @Test
            fun `then first player joined the game`() {
                // Given
                val timestamp = Timestamp.now()
                val playerId = PlayerId.random()

                // When
                val newEvents = handle(currentEvents, PlayHand(timestamp, playerId, Shape.PAPER))

                // Then
                assertThat(newEvents).contains(FirstPlayerJoinedGame(gameId, timestamp, playerId))
            }

            @Test
            fun `then a new round is started`() {
                // Given
                val timestamp = Timestamp.now()
                val playerId = PlayerId.random()

                // When
                val newEvents = handle(currentEvents, PlayHand(timestamp, playerId, Shape.PAPER))

                // Then
                assertThat(newEvents).contains(RoundStarted(gameId, timestamp, RoundNumber(1)))
            }

            @Test
            fun `then hand is played in round one`() {
                // Given
                val currentEvents = sequenceOf(GameCreated(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))
                val timestamp = Timestamp.now()
                val playerId = PlayerId.random()

                // When
                val newEvents = handle(currentEvents, PlayHand(timestamp, playerId, Shape.PAPER))

                // Then
                assertThat(newEvents).contains(HandPlayed(gameId, timestamp, playerId, Shape.PAPER, RoundNumber(1)))
            }

            @Test
            fun `then 4 events are returned`() {
                // Given
                val timestamp = Timestamp.now()
                val playerId = PlayerId.random()

                // When
                val newEvents = handle(currentEvents, PlayHand(timestamp, playerId, Shape.PAPER))

                // Then
                assertThat(newEvents.map { it::class }).containsExactly(RoundStarted::class, GameStarted::class, FirstPlayerJoinedGame::class, HandPlayed::class)
            }
        }
    }
}

// Extension functions to better support sequences in assertj
private fun <T> ObjectAssert<Sequence<T>>.containsOnly(vararg elements: T?) = satisfies { seq ->
    assertThat(seq.toList()).containsOnly(*elements)
}

private fun <T> ObjectAssert<Sequence<T>>.containsExactly(vararg elements: T?) = satisfies { seq ->
    assertThat(seq.toList()).containsExactly(*elements)
}

private fun <T> ObjectAssert<Sequence<T>>.contains(vararg elements: T?) = satisfies { seq ->
    assertThat(seq.toList()).contains(*elements)
}

private fun <T> ObjectAssert<Sequence<T>>.doesNotContain(vararg elements: T?) = satisfies { seq ->
    assertThat(seq.toList()).doesNotContain(*elements)
}

private fun <T> ObjectAssert<Sequence<T>>.hasSize(size : Int) = satisfies { seq ->
    assertThat(seq.toList()).hasSize(size)
}