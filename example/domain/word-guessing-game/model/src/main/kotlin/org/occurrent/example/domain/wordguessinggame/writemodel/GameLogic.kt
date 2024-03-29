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

package org.occurrent.example.domain.wordguessinggame.writemodel

import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.support.add
import java.util.*

/**
 * Start game
 */
fun startGame(
    previousEvents: Sequence<GameEvent>, gameId: GameId, timestamp: Timestamp, playerId: PlayerId, wordList: WordList,
    maxNumberOfGuessesPerPlayer: MaxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal: MaxNumberOfGuessesTotal
): Sequence<GameEvent> {
    val state = previousEvents.evolve()

    if (state !is NotStarted) {
        throw IllegalStateException("Cannot start game $gameId since it has already been started")
    }

    val wordToGuess = wordList.words.random()

    val gameStarted = GameWasStarted(
        eventId = UUID.randomUUID(), timestamp = timestamp, gameId = gameId, startedBy = playerId, category = wordList.category.value,
        wordToGuess = wordToGuess.value, maxNumberOfGuessesPerPlayer = maxNumberOfGuessesPerPlayer.value, maxNumberOfGuessesTotal = maxNumberOfGuessesTotal.value
    )

    return sequenceOf(gameStarted)
}

fun guessWord(previousEvents: Sequence<GameEvent>, timestamp: Timestamp, playerId: PlayerId, word: Word): Sequence<GameEvent> = when (val game = previousEvents.evolve()) {
    NotStarted -> throw IllegalStateException("Cannot guess word for a game that is not started")
    is Ended -> throw IllegalStateException("Cannot guess word for a game that is already ended")
    is Ongoing -> {
        if (game.isMaxNumberOfGuessesExceededForPlayer(playerId)) {
            throw IllegalArgumentException("Number of guessing attempts exhausted for player $playerId.")
        }

        val events = mutableListOf<GameEvent>()

        if (game.isRightGuess(word)) {
            events.add(PlayerGuessedTheRightWord(UUID.randomUUID(), timestamp, game.gameId, playerId, word.value))
            events.add(GameWasWon(UUID.randomUUID(), timestamp, game.gameId, playerId))
        } else {
            events.add(PlayerGuessedTheWrongWord(UUID.randomUUID(), timestamp, game.gameId, playerId, word.value))

            if (game.isLastGuessForPlayer(playerId)) {
                events.add(NumberOfGuessesWasExhaustedForPlayer(UUID.randomUUID(), timestamp, game.gameId, playerId))
            }

            if (game.isLastGuessForGame()) {
                events.add(GameWasLost(UUID.randomUUID(), timestamp, game.gameId))
            }
        }

        events.asSequence()
    }
}

private data class Guess(val playerId: PlayerId, val timestamp: Timestamp, val word: String)

// States
private sealed class GameState
private object NotStarted : GameState()
private data class Ongoing(val gameId: GameId, val wordToGuess: String, val maxNumberOfGuessesPerPlayer: Int, val maxNumberOfGuessesTotal: Int, val startedBy: PlayerId, val guesses: List<Guess> = emptyList()) : GameState() {
    fun numberOfGuessesForPlayer(playerId: PlayerId) = guesses.count { it.playerId == playerId }
    fun isMaxNumberOfGuessesExceededForPlayer(playerId: PlayerId): Boolean = numberOfGuessesForPlayer(playerId) == maxNumberOfGuessesPerPlayer
    fun isLastGuessForPlayer(playerId: PlayerId): Boolean = numberOfGuessesForPlayer(playerId) + 1 == maxNumberOfGuessesPerPlayer
    fun isLastGuessForGame(): Boolean = guesses.size == maxNumberOfGuessesTotal - 1
    fun isRightGuess(guessedWord: Word) = guessedWord.value.equals(wordToGuess, ignoreCase = true)
}

private object Ended : GameState()


// Evolve
private fun Sequence<GameEvent>.evolve(): GameState = fold<GameEvent, GameState>(NotStarted) { state, event ->
    when (event) {
        is GameWasStarted -> Ongoing(event.gameId, event.wordToGuess, event.maxNumberOfGuessesPerPlayer, event.maxNumberOfGuessesTotal, event.startedBy)
        is PlayerGuessedTheWrongWord -> state.coerce<Ongoing> { copy(guesses = guesses.add(Guess(event.playerId, event.timestamp, event.guessedWord))) }
        is PlayerGuessedTheRightWord -> state.coerce<Ongoing> { copy(guesses = guesses.add(Guess(event.playerId, event.timestamp, event.guessedWord))) }
        is NumberOfGuessesWasExhaustedForPlayer -> state
        is GameWasWon -> Ended
        is GameWasLost -> Ended
        // TODO Refactor events into different kinds!
        is CharacterInWordHintWasRevealed -> state
        is PlayerWasAwardedPointsForGuessingTheRightWord -> state
        is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> state
    }
}

private inline fun <reified ExpectedState : GameState> GameState.coerce(doWithState: ExpectedState.() -> GameState): GameState =
    if (this is ExpectedState) doWithState(this) else throw IllegalStateException("Invalid state: Expecting ${ExpectedState::class.simpleName}, was ${this::class.simpleName}")
