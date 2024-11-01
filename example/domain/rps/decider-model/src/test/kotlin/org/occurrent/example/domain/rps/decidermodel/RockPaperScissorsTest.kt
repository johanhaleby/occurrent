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

package org.occurrent.example.domain.rps.decidermodel

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.occurrent.dsl.decider.component1
import org.occurrent.dsl.decider.component2
import org.occurrent.example.domain.rps.decidermodel.GameState.*
import org.occurrent.example.domain.rps.decidermodel.HandGesture.ROCK
import org.occurrent.example.domain.rps.decidermodel.HandGesture.SCISSORS
import java.util.*

@DisplayName("Rock Paper Scissors")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class RockPaperScissorsTest {

    @Nested
    @DisplayName("when game is not created")
    inner class GameNotCreated {

        @Test
        fun `then it can be created`() {
            // Given
            val c = InitiateNewGame(GameId.randomUUID(), Timestamp.now(), PlayerId.randomUUID())

            // When
            val (state, events) = rps.decideOnEvents(emptyList(), c)

            // Then
            assertAll(
                { assertThat(state).isEqualTo(WaitingForFirstPlayerToMakeGesture(c.playerId)) },
                { assertThat(events).containsOnly(NewGameInitiated(c.gameId, c.timestamp, c.playerId)) },
            )
        }
    }

    @Nested
    @DisplayName("when game is created")
    inner class NewGameInitiated {

        @Nested
        @DisplayName("but not started")
        inner class ButNotStarted {

            @Test
            fun `then it's not possible to create the game again`() {
                // Given
                val c = InitiateNewGame(GameId.randomUUID(), Timestamp.now(), PlayerId.randomUUID())

                // When
                val throwable = catchThrowable { rps.decideOnState(WaitingForFirstPlayerToMakeGesture(c.gameId), c) }

                // Then
                assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java)
                    .hasMessage("Cannot ${InitiateNewGame::class.simpleName} when game is ${WaitingForFirstPlayerToMakeGesture::class.simpleName}")
            }

            @Nested
            @DisplayName("and first player makes a hand gesture")
            inner class AndFirstPlayerMakesAHandGesture {

                @Test
                fun `then game is started`() {
                    // Given
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), PlayerId.randomUUID(), ROCK)

                    // When
                    val (state, events) = rps.decideOnState(WaitingForFirstPlayerToMakeGesture(c.playerId), c)

                    // Then
                    assertAll(
                        { assertThat(state).isEqualTo(WaitingForSecondPlayerToMakeGesture(c.playerId, c.gesture, c.playerId)) },
                        { assertThat(events).contains(GameStarted(c.gameId, c.timestamp)) },
                    )
                }

                @Test
                fun `then hand gesture is shown`() {
                    // Given
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), PlayerId.randomUUID(), ROCK)

                    // When
                    val events = rps.decideOnStateAndReturnEvents(WaitingForFirstPlayerToMakeGesture(c.playerId), c)

                    // Then
                    assertThat(events).contains(HandGestureShown(c.gameId, c.timestamp, c.playerId, c.gesture))
                }
            }
        }

        @Nested
        @DisplayName("and started")
        inner class Started {

            @Nested
            @DisplayName("and first player tries to make a different hand gesture")
            inner class FirstPlayerGesture {

                @Test
                fun `then the rules prevents it`() { 
                    // Given
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), PlayerId.randomUUID(), ROCK)

                    // When
                    val throwable = catchThrowable { rps.decideOnState(WaitingForSecondPlayerToMakeGesture(c.playerId, SCISSORS, c.playerId), c) }

                    // Then
                    assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("First player is not allowed to make another hand gesture")
                }
            }

            @Nested
            @DisplayName("and a third player tries to join that game by making a gesture")
            inner class FirstPlayerGesture2 {

                @Test
                fun `then the rules prevents it`() {
                    // Given
                    val firstPlayerId = PlayerId.randomUUID()
                    val secondPlayerId = PlayerId.randomUUID()
                    val playerIdThatCreatedThatGame = PlayerId.randomUUID()
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), secondPlayerId, ROCK)

                    // When
                    val throwable = catchThrowable { rps.decideOnState(WaitingForSecondPlayerToMakeGesture(firstPlayerId, SCISSORS, playerIdThatCreatedThatGame), c) }

                    // Then
                    assertThat(throwable).isExactlyInstanceOf(IllegalArgumentException::class.java).hasMessage("A third player cannot join the game")
                }
            }

            @Nested
            @DisplayName("and second player makes a hand gesture")
            inner class SecondPlayerGesture {

                @Test
                fun `then the gesture is shown`() {
                    // Given
                    val firstPlayerId = UUID.randomUUID()
                    val secondPlayerId = UUID.randomUUID()
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), secondPlayerId, ROCK)

                    // When
                    val events = rps.decideOnStateAndReturnEvents(WaitingForSecondPlayerToMakeGesture(firstPlayerId, SCISSORS, c.playerId), c)

                    // Then
                    assertThat(events).contains(HandGestureShown(c.gameId, c.timestamp, c.playerId, ROCK))
                }

                @Test
                fun `then the game is ended`() {
                    // Given
                    val firstPlayerId = UUID.randomUUID()
                    val secondPlayerId = UUID.randomUUID()
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), secondPlayerId, ROCK)

                    // When
                    val (state, events) = rps.decideOnState(WaitingForSecondPlayerToMakeGesture(firstPlayerId, SCISSORS, c.playerId), c)

                    // Then
                    assertAll(
                        { assertThat(state).isEqualTo(Ended) },
                        { assertThat(events).contains(GameEnded(c.gameId, c.timestamp)) },
                    )
                }

                @ParameterizedTest(name = "when first player makes {0} and second player makes {1}")
                @CsvSource(
                    "ROCK, SCISSORS",
                    "SCISSORS, PAPER",
                    "PAPER, ROCK",
                )
                @DisplayName("then first player wins")
                fun `then first player wins`(gesture1: HandGesture, gesture2: HandGesture) {
                    // Given
                    val firstPlayerId = UUID.randomUUID()
                    val secondPlayerId = UUID.randomUUID()
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), secondPlayerId, gesture2)

                    // When
                    val events = rps.decideOnStateAndReturnEvents(WaitingForSecondPlayerToMakeGesture(firstPlayerId, gesture1, c.playerId), c)

                    // Then
                    val (_, _, winnerId) = events.find { it is GameWon } as GameWon
                    assertThat(winnerId).isEqualTo(firstPlayerId)
                }

                @ParameterizedTest(name = "when first player makes {0} and second player makes {1}")
                @CsvSource(
                    "SCISSORS, ROCK",
                    "PAPER, SCISSORS",
                    "ROCK, PAPER",
                )
                @DisplayName("then second player wins")
                fun `then second player wins`(gesture1: HandGesture, gesture2: HandGesture) {
                    // Given
                    val firstPlayerId = UUID.randomUUID()
                    val secondPlayerId = UUID.randomUUID()
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), secondPlayerId, gesture2)

                    // When
                    val events = rps.decideOnStateAndReturnEvents(WaitingForSecondPlayerToMakeGesture(firstPlayerId, gesture1, c.playerId), c)

                    // Then
                    val (_, _, winnerId) = events.find { it is GameWon } as GameWon
                    assertThat(winnerId).isEqualTo(secondPlayerId)
                }

                @ParameterizedTest(name = "when first player makes {0} and second player makes {1}")
                @CsvSource(
                    "ROCK, ROCK",
                    "SCISSORS, SCISSORS",
                    "PAPER, PAPER",
                )
                @DisplayName("then game is tied")
                fun `then game is tied`(gesture1: HandGesture, gesture2: HandGesture) {
                    // Given
                    val firstPlayerId = UUID.randomUUID()
                    val secondPlayerId = UUID.randomUUID()
                    val c = MakeHandGesture(GameId.randomUUID(), Timestamp.now(), secondPlayerId, gesture2)

                    // When
                    val events = rps.decideOnStateAndReturnEvents(WaitingForSecondPlayerToMakeGesture(firstPlayerId, gesture1, c.playerId), c)

                    // Then
                    assertThat(events)
                        .contains(GameTied(c.gameId, c.timestamp))
                        .doesNotHaveAnyElementsOfTypes(GameWon::class.java)
                }
            }
        }
    }
}