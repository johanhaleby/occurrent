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
import org.occurrent.application.composition.command.composeCommands
import org.occurrent.application.composition.command.partial

@DisplayName("Rock Paper Scissors")
class GamePlayTest {
    private val gameId = GameId.random()

    @Nested
    @DisplayName("when game is not created and not ended")
    inner class WhenGameIsNotCreated {

        @Test
        fun `then creating game will return GameCreatedEvent`() {
            // Given
            val currentEvents = emptySequence<GameEvent>()
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
    @DisplayName("when game is created and not ended")
    inner class WhenGameIsCreated {

        @Test
        fun `then game cannot be created again`() {
            // Given
            val currentEvents = sequenceOf(GameCreated(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)))

            // When
            val throwable = catchThrowable { handle(currentEvents, CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1))) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameCannotBeCreatedMoreThanOnce::class.java)
        }

        @Nested
        @DisplayName("and no player has joined")
        inner class AndNoPlayerHasJoined {
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

        @Nested
        @DisplayName("and first player joined")
        inner class AndFirstPlayerJoined {


            @Test
            fun `then first player cannot join the game again`() {
                // Given
                val firstPlayerId = PlayerId.random()
                val currentEvents = composeEvents(
                    CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)),
                    PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER)
                )
                val timestamp = Timestamp.now()

                // When
                val exception = catchThrowable { handle(currentEvents, PlayHand(timestamp, firstPlayerId, Shape.PAPER)) }

                // Then
                assertThat(exception).isExactlyInstanceOf(CannotJoinTheGameTwice::class.java)
            }

            @Nested
            @DisplayName("and max number of rounds is one")
            inner class AndMaxNumberOfRoundsIsOne {
                private val firstPlayerId = PlayerId.random()
                private val currentEvents = composeEvents(
                    CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)),
                    PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER)
                )

                @Test
                fun `then second player can join the game`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.PAPER))

                    // Then
                    assertThat(newEvents).contains(SecondPlayerJoinedGame(gameId, timestamp, secondPlayerId))
                }

                @Test
                fun `then round is tied when first and second player has the same hand`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.PAPER))

                    // Then
                    assertThat(newEvents).contains(RoundTied(gameId, timestamp, RoundNumber(1)))
                }

                @Test
                fun `then round is ended`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents).contains(RoundEnded(gameId, timestamp, RoundNumber(1)))
                }

                @Test
                fun `then game is ended`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents).contains(GameEnded(gameId, timestamp))
                }

                @Test
                fun `then first player wins round when first player's shape wins over second player's shape`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.ROCK))

                    // Then
                    assertThat(newEvents).contains(RoundWon(gameId, timestamp, RoundNumber(1), firstPlayerId))
                }

                @Test
                fun `then second player wins round when second player's shape wins over first player's shape`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents).contains(RoundWon(gameId, timestamp, RoundNumber(1), secondPlayerId))
                }

                @Test
                fun `then first player wins game when first player's shape wins over second player's shape`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.ROCK))

                    // Then
                    assertThat(newEvents).contains(GameWon(gameId, timestamp, firstPlayerId))
                }

                @Test
                fun `then second player wins game when second player's shape wins over first player's shape`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents).contains(GameWon(gameId, timestamp, secondPlayerId))
                }
            }

            @Nested
            @DisplayName("and max number of rounds is more than one")
            inner class AndMaxNumberOfRoundsIsMoreThanOne {
                private val firstPlayerId = PlayerId.random()
                private val currentEvents = composeEvents(
                    CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                    PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER)
                )

                @Test
                fun `then second player can join the game`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.PAPER))

                    // Then
                    assertThat(newEvents).contains(SecondPlayerJoinedGame(gameId, timestamp, secondPlayerId))
                }

                @Test
                fun `then round is tied when first and second player has the same hand`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.PAPER))

                    // Then
                    assertThat(newEvents).contains(RoundTied(gameId, timestamp, RoundNumber(1)))
                }

                @Test
                fun `then round is ended`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents).contains(RoundEnded(gameId, timestamp, RoundNumber(1)))
                }

                @Test
                fun `then game is not ended`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents.map { it::class }).doesNotContain(GameEnded::class)
                }

                @Test
                fun `then first player wins round when first player's shape wins over second player's shape`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.ROCK))

                    // Then
                    assertThat(newEvents).contains(RoundWon(gameId, timestamp, RoundNumber(1), firstPlayerId))
                }

                @Test
                fun `then second player wins round when second player's shape wins over first player's shape`() {
                    // Given
                    val secondPlayerId = PlayerId.random()
                    val timestamp = Timestamp.now()

                    // When
                    val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.SCISSORS))

                    // Then
                    assertThat(newEvents).contains(RoundWon(gameId, timestamp, RoundNumber(1), secondPlayerId))
                }
            }
        }

        @Nested
        @DisplayName("and both players joined")
        inner class AndBothPlayersJoined {
            private val firstPlayerId = PlayerId.random()
            private val secondPlayerId = PlayerId.random()

            @Test
            fun `then the same player cannot play in the same round twice`() {
                // Given
                val currentEvents = composeEvents(
                    CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                    PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                    PlayHand(Timestamp.now(), secondPlayerId, Shape.PAPER),
                    PlayHand(Timestamp.now(), firstPlayerId, Shape.ROCK)
                )

                // When
                val exception = catchThrowable { handle(currentEvents, PlayHand(Timestamp.now(), firstPlayerId, Shape.SCISSORS)) }

                // Then
                assertThat(exception).isExactlyInstanceOf(PlayerAlreadyPlayedInRound::class.java)
            }

            @Test
            fun `then a third player cannot join the game`() {
                // Given
                val currentEvents = composeEvents(
                    CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                    PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                    PlayHand(Timestamp.now(), secondPlayerId, Shape.PAPER)
                )
                val timestamp = Timestamp.now()

                // When
                val exception = catchThrowable { handle(currentEvents, PlayHand(timestamp, PlayerId.random(), Shape.PAPER)) }

                // Then
                assertThat(exception).isExactlyInstanceOf(GameAlreadyHasTwoPlayers::class.java)
            }


            @Nested
            @DisplayName("and max number of rounds is more than one")
            inner class AndMaxNumberOfRoundsIsMoreThanOne {

                @Nested
                @DisplayName("and next round is the last round in game")
                inner class AndNextRoundIsTheLastRoundInGame {

                    @Test
                    fun `then game is tied when all rounds are tied in the game`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.ROCK),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.ROCK),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.SCISSORS)
                        )
                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, firstPlayerId, Shape.SCISSORS))

                        // Then
                        assertThat(newEvents).contains(GameEnded(gameId, timestamp), GameTied(gameId, timestamp))
                    }

                    @Test
                    fun `then game is won when a player wins in the last round`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.ROCK),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.ROCK),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.SCISSORS)
                        )
                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, firstPlayerId, Shape.ROCK))

                        // Then
                        assertThat(newEvents).contains(GameEnded(gameId, timestamp), GameWon(gameId, timestamp, firstPlayerId))
                    }

                    @Test
                    fun `then game is won when first player wins the majority of the rounds`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.ROCK),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.SCISSORS),
                        )

                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.PAPER))

                        // Then
                        assertThat(newEvents).contains(GameEnded(gameId, timestamp), GameWon(gameId, timestamp, firstPlayerId))
                    }

                    @Test
                    fun `then game is won when second player wins the majority of the rounds`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.SCISSORS),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.SCISSORS),
                        )

                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.ROCK))

                        // Then
                        assertThat(newEvents).contains(GameEnded(gameId, timestamp), GameWon(gameId, timestamp, secondPlayerId))
                    }
                }

                @Nested
                @DisplayName("and next round is not the last round in game")
                inner class AndNextRoundIsNotTheLastRoundInGame {

                    @Test
                    fun `then a new round is started when first player plays`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.ROCK)
                        )
                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, firstPlayerId, Shape.PAPER))

                        // Then
                        assertThat(newEvents).contains(RoundStarted(gameId, timestamp, RoundNumber(2)))
                    }

                    @Test
                    fun `then a new round is started when second player plays`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.ROCK)
                        )
                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, secondPlayerId, Shape.PAPER))

                        // Then
                        assertThat(newEvents).contains(RoundStarted(gameId, timestamp, RoundNumber(2)))
                    }

                    @Test
                    fun `then a new round is ended when both players have played`() {
                        // Given
                        val currentEvents = composeEvents(
                            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(3)),
                            PlayHand(Timestamp.now(), firstPlayerId, Shape.PAPER),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.ROCK),
                            PlayHand(Timestamp.now(), secondPlayerId, Shape.PAPER),
                        )
                        val timestamp = Timestamp.now()

                        // When
                        val newEvents = handle(currentEvents, PlayHand(timestamp, firstPlayerId, Shape.PAPER))

                        // Then
                        assertThat(newEvents).contains(RoundTied(gameId, timestamp, RoundNumber(2)), RoundEnded(gameId, timestamp, RoundNumber(2)))
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("when game has ended")
    inner class WhenGameHasEnded {
        private val currentEvents = composeEvents(
            CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1)),
            PlayHand(Timestamp.now(), PlayerId.random(), Shape.PAPER),
            PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK)
        )

        @Test
        fun `then it's not possible to create the game`() {
            // When
            val throwable = catchThrowable { handle(currentEvents, CreateGame(gameId, Timestamp.now(), GameCreatorId.random(), MaxNumberOfRounds(1))) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(GameCannotBeCreatedMoreThanOnce::class.java)
        }

        @Test
        fun `then it's not possible to play the game`() {
            // When
            val throwable = catchThrowable { handle(currentEvents, PlayHand(Timestamp.now(), PlayerId.random(), Shape.ROCK)) }

            // Then
            assertThat(throwable).isExactlyInstanceOf(CannotPlayHandBecauseGameEnded::class.java)
        }
    }
}

private fun composeEvents(vararg command: Command): Sequence<GameEvent> = composeCommands(command.asSequence().map { cmd -> ::handle.partial(cmd) })(emptySequence())

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

private fun <T> ObjectAssert<Sequence<T>>.hasSize(size: Int) = satisfies { seq ->
    assertThat(seq.toList()).hasSize(size)
}