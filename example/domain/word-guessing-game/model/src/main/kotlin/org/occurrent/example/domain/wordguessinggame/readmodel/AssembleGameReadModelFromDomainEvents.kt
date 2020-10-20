package org.occurrent.example.domain.wordguessinggame.readmodel

import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameReadModel.Guess
import org.occurrent.example.domain.wordguessinggame.support.add

class AssembleGameReadModelFromDomainEvents internal constructor(val gameReadModel: GameReadModel?) {

    constructor() : this(null)

    fun applyEvent(e: DomainEvent) = when (e) {
        is GameWasStarted -> applyEvent(e)
        is PlayerGuessedTheWrongWord -> applyEvent(e)
        is PlayerGuessedTheRightWord -> applyEvent(e)
        is GameWasWon -> applyEvent(e)
        is GameWasLost -> applyEvent(e)
        is NumberOfGuessesWasExhaustedForPlayer -> this
        is PlayerWasAwardedPointsForGuessingTheRightWord -> applyEvent(e)
        is CharacterInWordHintWasRevealed -> applyEvent(e)
    }

    private fun applyEvent(e: GameWasStarted): AssembleGameReadModelFromDomainEvents = e.run {
        val upperCaseWordToGuess = wordToGuess.toUpperCase()
        val fullyObfuscatedWordHint = wordToGuess.indices.map { '_' }.joinToString("")
        AssembleGameReadModelFromDomainEvents(OngoingGameReadModel(gameId, timestamp, category, maxNumberOfGuessesPerPlayer, maxNumberOfGuessesTotal, fullyObfuscatedWordHint, emptyList(), upperCaseWordToGuess))
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