package org.occurrent.example.domain.wordguessinggame.readmodel.game

import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.readmodel.game.OngoingGameReadModel.Guess
import org.occurrent.example.domain.wordguessinggame.readmodel.game.WordHintGenerator.generateNewHint
import org.occurrent.example.domain.wordguessinggame.readmodel.game.WordHintGenerator.revealAdditionalCharacterFrom
import org.occurrent.example.domain.wordguessinggame.support.add

class GameReadModelStateMachine internal constructor(val state: GameReadModel?) {

    constructor() : this(null)

    fun applyEvent(e: DomainEvent) = when (e) {
        is GameWasStarted -> applyEvent(e)
        is PlayerGuessedTheWrongWord -> applyEvent(e)
        is PlayerGuessedTheRightWord -> applyEvent(e)
        is GameWasWon -> applyEvent(e)
        is GameWasLost -> applyEvent(e)
        is NumberOfGuessesWasExhaustedForPlayer -> this
    }

    private fun applyEvent(e: GameWasStarted): GameReadModelStateMachine = e.run {
        val upperCaseWordToGuess = wordToGuess.toUpperCase()
        GameReadModelStateMachine(OngoingGameReadModel(gameId, timestamp, category, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal, upperCaseWordToGuess.generateNewHint(), emptyList(), upperCaseWordToGuess))
    }

    private fun applyEvent(e: PlayerGuessedTheWrongWord): GameReadModelStateMachine = e.run {
        val ongoingGameReadModel = state as OngoingGameReadModel

        GameReadModelStateMachine(ongoingGameReadModel.copy(
                guesses = ongoingGameReadModel.guesses.add(Guess(playerId, guessedWord, timestamp)),
                hint = ongoingGameReadModel.hint.revealAdditionalCharacterFrom(ongoingGameReadModel.wordToGuess)))
    }

    private fun applyEvent(e: PlayerGuessedTheRightWord): GameReadModelStateMachine = e.run {
        val ongoingGameReadModel = state as OngoingGameReadModel

        GameReadModelStateMachine(ongoingGameReadModel.copy(guesses = ongoingGameReadModel.guesses.add(Guess(playerId, guessedWord, timestamp))))
    }

    private fun applyEvent(e: GameWasWon): GameReadModelStateMachine = e.run {
        val (gameId, startedAt, category, _, _, _, guesses, wordToGuess) = state as OngoingGameReadModel
        GameReadModelStateMachine(EndedGameReadModel(gameId, startedAt, timestamp, category, guesses.size, wordToGuess, winnerId))
    }

    private fun applyEvent(e: GameWasLost): GameReadModelStateMachine = e.run {
        val (gameId, startedAt, category, _, _, _, guesses, wordToGuess) = state as OngoingGameReadModel
        GameReadModelStateMachine(EndedGameReadModel(gameId, startedAt, timestamp, category, guesses.size, wordToGuess))
    }
}