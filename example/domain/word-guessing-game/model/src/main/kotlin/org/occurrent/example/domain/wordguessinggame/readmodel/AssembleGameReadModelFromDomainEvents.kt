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

package org.occurrent.example.domain.wordguessinggame.readmodel

import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameReadModel.Guess
import org.occurrent.example.domain.wordguessinggame.support.add


fun Sequence<GameEvent>.rehydrateToGameReadModel(): GameReadModel? = fold(AssembleGameReadModelFromDomainEvents(), AssembleGameReadModelFromDomainEvents::applyEvent).gameReadModel

internal class AssembleGameReadModelFromDomainEvents(val gameReadModel: GameReadModel? = null) {

    fun applyEvent(e: GameEvent) = when (e) {
        is GameWasStarted -> applyEvent(e)
        is PlayerGuessedTheWrongWord -> applyEvent(e)
        is PlayerGuessedTheRightWord -> applyEvent(e)
        is GameWasWon -> applyEvent(e)
        is GameWasLost -> applyEvent(e)
        is NumberOfGuessesWasExhaustedForPlayer -> this
        is CharacterInWordHintWasRevealed -> applyEvent(e)
        is PlayerWasAwardedPointsForGuessingTheRightWord -> applyEvent(e)
        is PlayerWasNotAwardedAnyPointsForGuessingTheRightWord -> applyEvent(e)
    }

    private fun applyEvent(e: GameWasStarted): AssembleGameReadModelFromDomainEvents = e.run {
        val upperCaseWordToGuess = wordToGuess.toUpperCase()
        val initialWordHint = wordToGuess.map { char -> if (char.isDash()) char else '_' }.joinToString("")
        AssembleGameReadModelFromDomainEvents(OngoingGameReadModel(gameId, timestamp, category, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal, initialWordHint, emptyList(), upperCaseWordToGuess))
    }

    private fun applyEvent(e: PlayerGuessedTheWrongWord): AssembleGameReadModelFromDomainEvents = e.run {
        val ongoingGameReadModel = gameReadModel as OngoingGameReadModel

        AssembleGameReadModelFromDomainEvents(ongoingGameReadModel.copy(guesses = ongoingGameReadModel.guesses.add(Guess(playerId, guessedWord, timestamp))))
    }

    private fun applyEvent(e: CharacterInWordHintWasRevealed): AssembleGameReadModelFromDomainEvents = e.run {
        val ongoingGameReadModel = gameReadModel as OngoingGameReadModel

        val newHint = ongoingGameReadModel.hint.replaceCharAt(characterPositionInWord - 1, ongoingGameReadModel.wordToGuess[characterPositionInWord - 1])

        AssembleGameReadModelFromDomainEvents(ongoingGameReadModel.copy(hint = newHint))
    }

    private fun applyEvent(e: PlayerGuessedTheRightWord): AssembleGameReadModelFromDomainEvents = e.run {
        val ongoingGameReadModel = gameReadModel as OngoingGameReadModel

        AssembleGameReadModelFromDomainEvents(ongoingGameReadModel.copy(guesses = ongoingGameReadModel.guesses.add(Guess(playerId, guessedWord, timestamp))))
    }

    private fun applyEvent(e: PlayerWasAwardedPointsForGuessingTheRightWord): AssembleGameReadModelFromDomainEvents = e.run {
        val model = gameReadModel as GameWasWonReadModel

        AssembleGameReadModelFromDomainEvents(model.copy(pointsAwardedToWinner = points))
    }

    private fun applyEvent(e: PlayerWasNotAwardedAnyPointsForGuessingTheRightWord): AssembleGameReadModelFromDomainEvents = e.run {
        val model = gameReadModel as GameWasWonReadModel

        AssembleGameReadModelFromDomainEvents(model.copy(pointsAwardedToWinner = 0))
    }

    private fun applyEvent(e: GameWasWon): AssembleGameReadModelFromDomainEvents = e.run {
        val (gameId, startedAt, category, _, _, _, guesses, wordToGuess) = gameReadModel as OngoingGameReadModel
        val numberOfGuessesByWinner = guesses.count { it.playerId == winnerId }
        val numberOfPlayersInGame = guesses.distinctBy { it.playerId }.count()

        AssembleGameReadModelFromDomainEvents(GameWasWonReadModel(gameId, startedAt, timestamp, category, guesses.size, numberOfPlayersInGame, wordToGuess, winnerId, numberOfGuessesByWinner))
    }

    private fun applyEvent(e: GameWasLost): AssembleGameReadModelFromDomainEvents = e.run {
        val (gameId, startedAt, category, _, _, _, guesses, wordToGuess) = gameReadModel as OngoingGameReadModel
        val numberOfPlayersInGame = guesses.distinctBy { it.playerId }.count()

        AssembleGameReadModelFromDomainEvents(GameWasLostReadModel(gameId, startedAt, timestamp, category, guesses.size, numberOfPlayersInGame, wordToGuess))
    }

    private fun String.replaceCharAt(index: Int, char: Char): String {
        val b = StringBuilder(this)
        b.setCharAt(index, char)
        return b.toString()
    }
}

private fun Char.isDash(): Boolean = this == '-'
