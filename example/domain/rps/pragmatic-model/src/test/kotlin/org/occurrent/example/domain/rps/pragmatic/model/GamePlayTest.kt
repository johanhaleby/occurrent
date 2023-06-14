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

package org.occurrent.example.domain.rps.pragmatic.model

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
import org.occurrent.example.domain.rps.pragmatic.model.*
import org.occurrent.example.domain.rps.pragmatic.model.HandShape.*

@DisplayName("Rock Paper Scissors")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class GamePlayTest {
    private val gameId = GameId.random()
    private val timestamp = Timestamp.now()
    private val firstPlayerId = PlayerId.random()
    private val secondPlayerId = PlayerId.random()

    @Nested
    @DisplayName("when game is not created")
    inner class WhenGameIsNotCreated {

        @Test
        fun `then creating a game will return GameCreatedEvent`() {
            // Given
            val currentEvents = emptyList<GameEvent>()
            val creator = GameCreatorId.random()

            // When
            val gameCreated = RPS.create(currentEvents, gameId, timestamp, creator)

            // Then
            assertThat(gameCreated).isEqualTo(GameCreated(gameId, timestamp, creator))
        }

        @Test
        fun `then playing a game will throw GameDoesNotExist exception`() {
            // Given
            val currentEvents = emptyList<GameEvent>()

            // When
            val throwable = catchThrowable { RPS.play(currentEvents, Timestamp.now(), PlayerId.random(), PAPER) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameDoesNotExist::class.java)
        }

        @Test
        fun `then joining a game will throw GameDoesNotExist exception`() {
            // Given
            val currentEvents = emptyList<GameEvent>()

            // When
            val throwable = catchThrowable { RPS.join(currentEvents, Timestamp.now(), PlayerId.random()) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameDoesNotExist::class.java)
        }
    }

    @Nested
    @DisplayName("when game is created but not ongoing")
    inner class WhenGameIsCreatedButNotOngoing {

        @Test
        fun `then game cannot be created again`() {
            // Given
            val currentEvents = listOf(GameCreated(gameId, Timestamp.now(), GameCreatorId.random()))

            // When
            val throwable = catchThrowable { RPS.create(currentEvents, gameId, Timestamp.now(), GameCreatorId.random()) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameCannotBeCreatedMoreThanOnce::class.java)
        }

        @Nested
        @DisplayName("and no player has joined")
        inner class AndNoPlayerHasJoined {
            private val currentEvents = listOf(GameCreated(gameId, Timestamp.now(), GameCreatorId.random()))

            @Test
            fun `then first player can join the game`() {
                // Given
                val playerId = PlayerId.random()

                // When
                val event = RPS.join(currentEvents, timestamp, playerId)

                // Then
                assertThat(event).isEqualTo(FirstPlayerBecameReady(gameId, timestamp, playerId))
            }

            @Test
            fun `then it's not possible to play the game`() {
                // Given
                val timestamp = Timestamp.now()
                val playerId = PlayerId.random()

                // When
                val throwable = catchThrowable { RPS.play(currentEvents, timestamp, playerId, PAPER) }

                // Then
                assertThat(throwable).isExactlyInstanceOf(CannotPlayHandBecauseWaitingForBothPlayersToBeReady::class.java)
            }
        }

        @Nested
        @DisplayName("and first player joined")
        inner class AndFirstPlayerJoined {

            @Test
            fun `then first player cannot join the game again`() {
                // Given
                val firstPlayerId = PlayerId.random()
                val timestamp = Timestamp.now()
                val currentEvents = listOf(
                    GameCreated(gameId, timestamp, GameCreatorId.random()),
                    FirstPlayerBecameReady(gameId, timestamp, firstPlayerId)
                )

                // When
                val exception = catchThrowable { RPS.join(currentEvents, timestamp, firstPlayerId) }

                // Then
                assertThat(exception).isExactlyInstanceOf(CannotJoinTheGameTwice::class.java)
            }

            @Test
            fun `then a second player can join the game`() {
                // Given
                val currentEvents = listOf(
                    GameCreated(gameId, timestamp, GameCreatorId.random()),
                    FirstPlayerBecameReady(gameId, timestamp, firstPlayerId)
                )

                // When
                val event = RPS.join(currentEvents, timestamp, secondPlayerId)

                // Then
                assertThat(event).isEqualTo(SecondPlayerBecameReady(gameId, timestamp, secondPlayerId))
            }
        }

        @Nested
        @DisplayName("and both players joined but no one has played yet")
        inner class AndBothPlayersJoinedButNoOneHasPlayed {
            private val firstPlayerId = PlayerId.random()
            private val secondPlayerId = PlayerId.random()
            val timestamp = Timestamp.of(2023, 6, 2, 13, 29, 0)

            val currentEvents = listOf(
                GameCreated(gameId, timestamp, GameCreatorId.random()),
                FirstPlayerBecameReady(gameId, timestamp, firstPlayerId),
                SecondPlayerBecameReady(gameId, timestamp, secondPlayerId)
            )

            @ParameterizedTest(name = "player {0} plays the first hand")
            @ValueSource(ints = [1, 2])
            fun `then the game is started when`(playerNumber: Int) {
                // Given
                val playTime = Timestamp.now()
                val playerId = if (playerNumber == 1) firstPlayerId else secondPlayerId

                // When
                val events = RPS.play(currentEvents, playTime, playerId, PAPER)

                // Then
                assertThat(events).contains(GameStarted(gameId, playTime))
            }

            @Test
            fun `then the first player can play their hand first`() {
                // Given
                val playTime = Timestamp.now()

                // When
                val events = RPS.play(currentEvents, playTime, firstPlayerId, ROCK)

                // Then
                assertThat(events).contains(HandPlayed(gameId, playTime, firstPlayerId, ROCK))
            }

            @Test
            fun `then the second player can play their hand first`() {
                // Given
                val playTime = Timestamp.now()

                // When
                val events = RPS.play(currentEvents, playTime, secondPlayerId, SCISSORS)

                // Then
                assertThat(events).contains(HandPlayed(gameId, playTime, secondPlayerId, SCISSORS))
            }

            @Test
            fun `then a third player cannot join the game`() {
                // Given
                val timestamp = Timestamp.now()

                // When
                val exception = catchThrowable { RPS.join(currentEvents, timestamp, PlayerId.random()) }

                // Then
                assertThat(exception).isExactlyInstanceOf(TooLateToJoinGame::class.java)
            }

            @Test
            fun `then a third player cannot play the game`() {
                // Given
                val timestamp = Timestamp.now()

                // When
                val exception = catchThrowable { RPS.play(currentEvents, timestamp, PlayerId.random(), HandShape.random()) }

                // Then
                assertThat(exception).isExactlyInstanceOf(GameAlreadyHasTwoPlayers::class.java)
            }
        }
    }

    @Nested
    @DisplayName("when game is ongoing")
    inner class WhenGameIsOngoing {
        private val firstPlayerId = PlayerId.random()
        private val secondPlayerId = PlayerId.random()
        private val timestamp = Timestamp.of(2023, 6, 2, 13, 29, 0)

        private val currentEvents = mutableListOf(
            GameCreated(gameId, timestamp, GameCreatorId.random()),
            FirstPlayerBecameReady(gameId, timestamp, firstPlayerId),
            SecondPlayerBecameReady(gameId, timestamp, secondPlayerId),
            GameStarted(gameId, timestamp)
        )

        @Test
        fun `then a third player cannot join the game`() {
            // Given
            val timestamp = Timestamp.now()

            // When
            val exception = catchThrowable { RPS.join(currentEvents, timestamp, PlayerId.random()) }

            // Then
            assertThat(exception).isExactlyInstanceOf(TooLateToJoinGame::class.java)
        }

        @Test
        fun `then the game is ended when the last player plays their hand`() {
            // Given
            val playTime = Timestamp.now()
            currentEvents.add(HandPlayed(gameId, timestamp, firstPlayerId, HandShape.random()))

            // When
            val events = RPS.play(currentEvents, playTime, secondPlayerId, HandShape.random())

            // Then
            assertThat(events).contains(GameEnded(gameId, playTime))
        }

        @ParameterizedTest(name = "when first player chose {0} and second player chose {1}")
        @CsvSource(
            "ROCK, SCISSORS",
            "SCISSORS, PAPER",
            "PAPER, ROCK",
        )
        fun `then first player wins`(shape1: String, shape2: String) {
            // Given
            val playTime = Timestamp.now()
            val player1HandShape = HandShape.valueOf(shape1)
            val player2HandShape = HandShape.valueOf(shape2)
            currentEvents.add(HandPlayed(gameId, timestamp, firstPlayerId, player1HandShape))

            // When
            val events = RPS.play(currentEvents, playTime, secondPlayerId, player2HandShape)

            // Then
            assertThat(events).contains(GameWon(gameId, playTime, firstPlayerId))
        }

        @ParameterizedTest(name = "when first player chose {0} and second player chose {1}")
        @CsvSource(
            "SCISSORS, ROCK",
            "PAPER, SCISSORS",
            "ROCK, PAPER",
        )
        fun `then second player wins`(shape1: String, shape2: String) {
            // Given
            val playTime = Timestamp.now()
            val player1HandShape = HandShape.valueOf(shape1)
            val player2HandShape = HandShape.valueOf(shape2)
            currentEvents.add(HandPlayed(gameId, timestamp, firstPlayerId, player1HandShape))

            // When
            val events = RPS.play(currentEvents, playTime, secondPlayerId, player2HandShape)

            // Then
            assertThat(events).contains(GameWon(gameId, playTime, secondPlayerId))
        }

        @ParameterizedTest(name = "when both players chose {0}")
        @EnumSource(HandShape::class)
        fun `then game is tied`(shape: HandShape) {
            // Given
            val playTime = Timestamp.now()
            currentEvents.add(HandPlayed(gameId, timestamp, firstPlayerId, shape))

            // When
            val events = RPS.play(currentEvents, playTime, secondPlayerId, shape)

            // Then
            assertThat(events).contains(GameTied(gameId, playTime))
        }

        @Test
        fun `then a third player cannot play the game`() {
            // Given
            val timestamp = Timestamp.now()

            // When
            val exception = catchThrowable { RPS.play(currentEvents, timestamp, PlayerId.random(), HandShape.random()) }

            // Then
            assertThat(exception).isExactlyInstanceOf(GameAlreadyHasTwoPlayers::class.java)
        }
    }


    @Nested
    @DisplayName("when game is ended")
    inner class WhenGameHasEnded {
        private val currentEvents = mutableListOf(
            GameCreated(gameId, timestamp, GameCreatorId.random()),
            FirstPlayerBecameReady(gameId, timestamp, firstPlayerId),
            SecondPlayerBecameReady(gameId, timestamp, secondPlayerId),
            GameStarted(gameId, timestamp),
            HandPlayed(gameId, timestamp, firstPlayerId, ROCK),
            HandPlayed(gameId, timestamp, secondPlayerId, ROCK),
            GameTied(gameId, timestamp),
            GameEnded(gameId, timestamp)
        )

        @Test
        fun `then it's not possible to create the game`() {
            // When
            val throwable = catchThrowable { RPS.create(currentEvents, gameId, Timestamp.now(), GameCreatorId.random()) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameCannotBeCreatedMoreThanOnce::class.java)
        }

        @ParameterizedTest(name = "player {0} to play the game")
        @ValueSource(ints = [1, 2])
        fun `then it's not possible for`(playerNumber: Int) {
            // Given
            val playerId = if (playerNumber == 1) firstPlayerId else secondPlayerId

            // When
            val throwable = catchThrowable { RPS.play(currentEvents, Timestamp.now(), playerId, ROCK) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(CannotPlayHandBecauseGameEnded::class.java)
        }

        @Test
        fun `then it's not possible for a new player to play the game`() {
            // When
            val throwable = catchThrowable { RPS.play(currentEvents, Timestamp.now(), PlayerId.random(), ROCK) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(CannotPlayHandBecauseGameEnded::class.java)
        }

        @Test
        fun `then it's not possible for a new player to join the game`() {
            // When
            val throwable = catchThrowable { RPS.join(currentEvents, Timestamp.now(), PlayerId.random()) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(TooLateToJoinGame::class.java)
        }
    }
}