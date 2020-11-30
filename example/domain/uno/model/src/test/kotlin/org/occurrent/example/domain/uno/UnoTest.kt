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

package org.occurrent.example.domain.uno

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER
import org.junit.jupiter.params.provider.ValueSource
import org.occurrent.example.domain.uno.Card.*
import org.occurrent.example.domain.uno.Color.*
import org.occurrent.example.domain.uno.Digit.*
import org.occurrent.example.domain.uno.Direction.CounterClockwise
import org.occurrent.example.domain.uno.extensions.find
import org.occurrent.example.domain.uno.extensions.findMany

@DisplayName("uno")
class UnoTest {

    @Nested
    @DisplayName("when game is not started")
    inner class WhenGameIsNotStarted {

        @ParameterizedTest(name = "player count $ARGUMENTS_WITH_NAMES_PLACEHOLDER")
        @ValueSource(ints = [Integer.MIN_VALUE, 0, 1, 2])
        fun `it is not possible to start game with less than three players`(playerCount: PlayerCount) {
            // Given
            val previousEvents = emptySequence<Event>()

            val gameId = GameId.randomUUID()
            val timestamp = Timestamp.now()
            val card = DigitCard(digit = Five, color = Red)

            // When
            val throwable = catchThrowable { Uno.start(previousEvents, gameId, timestamp, playerCount, card) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("There should be at least 3 players")
        }

        @Test
        fun `then game can be started with a digit card`() {
            // Given
            val previousEvents = emptySequence<Event>()

            val gameId = GameId.randomUUID()
            val timestamp = Timestamp.now()
            val playerCount = 4
            val card = DigitCard(digit = Five, color = Red)

            // When
            val events = Uno.start(previousEvents, gameId, timestamp, playerCount, card).toList()

            // Then
            assertThat(events).hasSize(1)
            val gameStarted = events.find<GameStarted>()

            assertAll(
                    { assertThat(gameStarted.gameId).isEqualTo(gameId) },
                    { assertThat(gameStarted.timestamp).isEqualTo(timestamp) },
                    { assertThat(gameStarted.playerCount).isEqualTo(playerCount) },
                    { assertThat(gameStarted.firstCard).isEqualTo(card) },
                    { assertThat(gameStarted.firstPlayerId).isEqualTo(0) },
            )
        }

        @Test
        fun `then game can be started with a skip card`() {
            // Given
            val previousEvents = emptySequence<Event>()

            val gameId = GameId.randomUUID()
            val timestamp = Timestamp.now()
            val playerCount = 4
            val card = Skip(color = Red)

            // When
            val events = Uno.start(previousEvents, gameId, timestamp, playerCount, card).toList()

            // Then
            assertThat(events).hasSize(1)
            val gameStarted = events.find<GameStarted>()

            assertAll(
                    { assertThat(gameStarted.gameId).isEqualTo(gameId) },
                    { assertThat(gameStarted.timestamp).isEqualTo(timestamp) },
                    { assertThat(gameStarted.playerCount).isEqualTo(playerCount) },
                    { assertThat(gameStarted.firstCard).isEqualTo(card) },
                    { assertThat(gameStarted.firstPlayerId).isEqualTo(1) },
            )
        }

        @Test
        fun `then game can be started with a kickback card`() {
            // Given
            val previousEvents = emptySequence<Event>()

            val gameId = GameId.randomUUID()
            val timestamp = Timestamp.now()
            val playerCount = 4
            val card = KickBack(color = Red)

            // When
            val events = Uno.start(previousEvents, gameId, timestamp, playerCount, card).toList()

            // Then
            assertThat(events).hasSize(2)
            val (gameStarted, directionChanged) = events.findMany<GameStarted, DirectionChanged>()

            assertAll(
                    { assertThat(gameStarted.gameId).isEqualTo(gameId) },
                    { assertThat(gameStarted.timestamp).isEqualTo(timestamp) },
                    { assertThat(gameStarted.playerCount).isEqualTo(playerCount) },
                    { assertThat(gameStarted.firstCard).isEqualTo(card) },
                    { assertThat(gameStarted.firstPlayerId).isEqualTo(0) },

                    { assertThat(directionChanged.gameId).isEqualTo(gameId) },
                    { assertThat(directionChanged.timestamp).isEqualTo(timestamp) },
                    { assertThat(directionChanged.direction).isEqualTo(CounterClockwise) },
            )
        }

        @Test
        fun `then a game cannot be played`() {
            // Given
            val timestamp = Timestamp.now()
            val previousEvents = emptySequence<Event>()

            // When
            val exception = catchThrowable { Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 0, card = DigitCard(digit = Eight, color = Yellow)) }

            // Then
            assertThat(exception).isExactlyInstanceOf(IllegalStateException::class.java).hasMessage("Game has not been started")
        }
    }

    @Nested
    @DisplayName("when game is started")
    inner class WhenGameIsStarted {

        @Nested
        @DisplayName("and player tries to break the rules")
        inner class AndPlayerTriesToBreakTheRules {

            @Test
            fun `by playing when it's another players turn`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val events = Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 1, card = DigitCard(digit = Six, color = Yellow))

                // Then
                val playerPlayedAtWrongTurn = events.find<PlayerPlayedAtWrongTurn>()
                assertAll(
                        { assertThat(playerPlayedAtWrongTurn.gameId).isEqualTo(gameId) },
                        { assertThat(playerPlayedAtWrongTurn.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(playerPlayedAtWrongTurn.playerId).isEqualTo(1) },
                )
            }

            @Test
            fun `by playing a wrong card`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val events = Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 0, card = DigitCard(digit = Six, color = Red))

                // Then
                val playerPlayedWrongCard = events.find<PlayerPlayedWrongCard>()
                assertAll(
                        { assertThat(playerPlayedWrongCard.gameId).isEqualTo(gameId) },
                        { assertThat(playerPlayedWrongCard.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(playerPlayedWrongCard.playerId).isEqualTo(0) },
                )
            }
        }

        @Nested
        @DisplayName("and player plays according to the rules")
        inner class AndPlayerPlaysAccordingToTheRules {

            @Test
            fun `by playing a skip card with the same color as the top card will skip the next player`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val events = Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 0, card = Skip(color = Yellow))

                // Then
                val cardPlayed = events.find<CardPlayed>()
                assertAll(
                        { assertThat(cardPlayed.gameId).isEqualTo(gameId) },
                        { assertThat(cardPlayed.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(cardPlayed.playerId).isEqualTo(0) },
                        { assertThat(cardPlayed.card).isEqualTo(Skip(color = Yellow)) },
                        { assertThat(cardPlayed.nextPlayerId).isEqualTo(2) },
                )
            }

            @Test
            fun `by playing a digit card with the same color as the top card will give turn to next player`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val events = Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 0, card = DigitCard(digit = Five, color = Yellow))

                // Then
                val cardPlayed = events.find<CardPlayed>()
                assertAll(
                        { assertThat(cardPlayed.gameId).isEqualTo(gameId) },
                        { assertThat(cardPlayed.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(cardPlayed.playerId).isEqualTo(0) },
                        { assertThat(cardPlayed.card).isEqualTo(DigitCard(digit = Five, color = Yellow)) },
                        { assertThat(cardPlayed.nextPlayerId).isEqualTo(1) },
                )
            }

            @Test
            fun `by playing a kickback card with the same color as the top card will change direction and give turn to previous player`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val events = Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 0, card = KickBack(color = Yellow)).toList()

                // Then
                assertThat(events).hasSize(2)
                val cardPlayed = events.find<CardPlayed>()
                val directionChanged = events.find<DirectionChanged>()
                assertAll(
                        { assertThat(cardPlayed.gameId).isEqualTo(gameId) },
                        { assertThat(cardPlayed.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(cardPlayed.playerId).isEqualTo(0) },
                        { assertThat(cardPlayed.card).isEqualTo(KickBack(color = Yellow)) },
                        { assertThat(cardPlayed.nextPlayerId).isEqualTo(2) },

                        { assertThat(directionChanged.gameId).isEqualTo(gameId) },
                        { assertThat(directionChanged.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(directionChanged.direction).isEqualTo(CounterClockwise) },
                )
            }

            @Test
            fun `by playing a digit card with the a different color as the top card but with same digit will give turn to next player`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val events = Uno.play(previousEvents, timestamp.plusSeconds(1), playerId = 0, card = DigitCard(digit = Four, color = Blue))

                // Then
                val cardPlayed = events.find<CardPlayed>()
                assertAll(
                        { assertThat(cardPlayed.gameId).isEqualTo(gameId) },
                        { assertThat(cardPlayed.timestamp).isEqualTo(timestamp.plusSeconds(1)) },
                        { assertThat(cardPlayed.playerId).isEqualTo(0) },
                        { assertThat(cardPlayed.card).isEqualTo(DigitCard(digit = Four, color = Blue)) },
                        { assertThat(cardPlayed.nextPlayerId).isEqualTo(1) },
                )
            }
        }

        @Nested
        @DisplayName("and player tries to start a game that has already been started")
        inner class AndPlayerTriesToStartAGameThatHasAlreadyBeenStarted {

            @Test
            fun `then an IllegalStateException is thrown`() {
                // Given
                val gameId = GameId.randomUUID()
                val timestamp = Timestamp.now()
                val previousEvents = sequenceOf(GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId = 0, playerCount = 3, firstCard = DigitCard(digit = Four, color = Yellow)))

                // When
                val exception = catchThrowable { Uno.start(previousEvents, gameId, timestamp.plusSeconds(1), playerCount = 5, firstCard = DigitCard(digit = Eight, color = Yellow)) }

                // Then
                assertThat(exception).isExactlyInstanceOf(IllegalStateException::class.java).hasMessage("The game cannot be started more than once")
            }
        }
    }
}