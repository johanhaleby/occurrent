/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.decider

import org.occurrent.dsl.decider.Decider
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasLost
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.NumberOfGuessesWasExhaustedForPlayer
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.example.domain.wordguessinggame.writemodel.MaxNumberOfGuessesPerPlayer
import org.occurrent.example.domain.wordguessinggame.writemodel.MaxNumberOfGuessesTotal
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import org.occurrent.example.domain.wordguessinggame.writemodel.Word
import org.occurrent.example.domain.wordguessinggame.writemodel.WordCategory
import java.util.UUID

fun wordGuessingGameDecider(): Decider<WordGuessingGameCommand, WordGuessingGameState, GameEvent> =
    decider(
        initialState = WordGuessingGameState.NotStarted,
        decide = { command, state -> decide(command, state) },
        evolve = { state, event -> evolve(state, event) },
        isTerminal = { state -> state is WordGuessingGameState.Ended }
    )

sealed interface WordGuessingGameCommand {
    data class StartGame(
        val eventId: UUID,
        val gameId: GameId,
        val timestamp: Timestamp,
        val startedBy: PlayerId,
        val category: WordCategory,
        val wordToGuess: Word
    ) : WordGuessingGameCommand

    data class GuessWord(
        val guessEventId: UUID,
        val guessesExhaustedEventId: UUID,
        val gameEndedEventId: UUID,
        val timestamp: Timestamp,
        val playerId: PlayerId,
        val word: Word
    ) : WordGuessingGameCommand
}

sealed interface WordGuessingGameState {
    data object NotStarted : WordGuessingGameState

    data class Ongoing(
        val gameId: GameId,
        val wordToGuess: String,
        val maxNumberOfGuessesPerPlayer: Int,
        val maxNumberOfGuessesTotal: Int,
        val guesses: List<Guess> = emptyList()
    ) : WordGuessingGameState {
        fun numberOfGuessesFor(playerId: PlayerId): Int = guesses.count { it.playerId == playerId }
        fun isMaxNumberOfGuessesExceededFor(playerId: PlayerId): Boolean = numberOfGuessesFor(playerId) == maxNumberOfGuessesPerPlayer
        fun isLastGuessFor(playerId: PlayerId): Boolean = numberOfGuessesFor(playerId) + 1 == maxNumberOfGuessesPerPlayer
        fun isLastGuessForGame(): Boolean = guesses.size == maxNumberOfGuessesTotal - 1
        fun isRightGuess(word: Word): Boolean = word.value.equals(wordToGuess, ignoreCase = true)
    }

    data object Ended : WordGuessingGameState
}

data class Guess(val playerId: PlayerId, val timestamp: Timestamp, val word: String)

private fun decide(command: WordGuessingGameCommand, state: WordGuessingGameState): List<GameEvent> =
    when (command) {
        is WordGuessingGameCommand.StartGame -> {
            check(state is WordGuessingGameState.NotStarted) { "Cannot start game ${command.gameId} since it has already been started" }
            listOf(
                GameWasStarted(
                    eventId = command.eventId,
                    timestamp = command.timestamp,
                    gameId = command.gameId,
                    startedBy = command.startedBy,
                    category = command.category.value,
                    wordToGuess = command.wordToGuess.value,
                    maxNumberOfGuessesPerPlayer = MaxNumberOfGuessesPerPlayer.value,
                    maxNumberOfGuessesTotal = MaxNumberOfGuessesTotal.value
                )
            )
        }

        is WordGuessingGameCommand.GuessWord -> {
            check(state is WordGuessingGameState.Ongoing) { "Cannot guess word for a game that is not ongoing" }
            require(!state.isMaxNumberOfGuessesExceededFor(command.playerId)) {
                "Number of guessing attempts exhausted for player ${command.playerId}."
            }

            if (state.isRightGuess(command.word)) {
                listOf(
                    PlayerGuessedTheRightWord(command.guessEventId, command.timestamp, state.gameId, command.playerId, command.word.value),
                    GameWasWon(command.gameEndedEventId, command.timestamp, state.gameId, command.playerId)
                )
            } else {
                buildList {
                    add(PlayerGuessedTheWrongWord(command.guessEventId, command.timestamp, state.gameId, command.playerId, command.word.value))
                    if (state.isLastGuessFor(command.playerId)) {
                        add(NumberOfGuessesWasExhaustedForPlayer(command.guessesExhaustedEventId, command.timestamp, state.gameId, command.playerId))
                    }
                    if (state.isLastGuessForGame()) {
                        add(GameWasLost(command.gameEndedEventId, command.timestamp, state.gameId))
                    }
                }
            }
        }
    }

private fun evolve(state: WordGuessingGameState, event: GameEvent): WordGuessingGameState =
    when (event) {
        is GameWasStarted -> WordGuessingGameState.Ongoing(
            gameId = event.gameId,
            wordToGuess = event.wordToGuess,
            maxNumberOfGuessesPerPlayer = event.maxNumberOfGuessesPerPlayer,
            maxNumberOfGuessesTotal = event.maxNumberOfGuessesTotal
        )

        is PlayerGuessedTheWrongWord -> state.withOngoingGame {
            copy(guesses = guesses + Guess(event.playerId, event.timestamp, event.guessedWord))
        }

        is PlayerGuessedTheRightWord -> state.withOngoingGame {
            copy(guesses = guesses + Guess(event.playerId, event.timestamp, event.guessedWord))
        }

        is NumberOfGuessesWasExhaustedForPlayer -> state
        is GameWasWon -> WordGuessingGameState.Ended
        is GameWasLost -> WordGuessingGameState.Ended
        else -> state
    }

private fun WordGuessingGameState.withOngoingGame(block: WordGuessingGameState.Ongoing.() -> WordGuessingGameState): WordGuessingGameState =
    if (this is WordGuessingGameState.Ongoing) {
        block()
    } else {
        error("Invalid state: Expected ${WordGuessingGameState.Ongoing::class.simpleName}, was ${this::class.simpleName}")
    }
