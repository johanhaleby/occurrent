package org.occurrent.example.domain.wordguessinggame.readmodel.game

import java.util.*

typealias WordToGuess = String
typealias WordHint = String
typealias GuessedWord = String
typealias Category = String


sealed class GameReadModel {
    abstract val gameId: UUID
}

class EndedGameReadModel(override val gameId: UUID, val startedAt: Date, val endedAt: Date,
                         val category: String, val numberOfGuesses: Int, val wordToGuess: String,
                         val winner: UUID? = null) : GameReadModel()

data class OngoingGameReadModel(override val gameId: UUID, val startedAt: Date, val category: Category,
                                val maxNumberOfGuessesPerPlayer: Int, val maxNumberOfGuessesTotal: Int,
                                val hint: WordHint, val guesses: List<Guess>, val wordToGuess: WordToGuess) : GameReadModel() {

    class Guess internal constructor(val playerId: UUID, val word: GuessedWord, val guessMadeAt: Date)

    val totalNumberOfGuessesLeft
        get() = maxNumberOfGuessesTotal - guesses.size

    fun numberOfGuessesLeftForPlayer(playerId: UUID) = maxNumberOfGuessesPerPlayer - guesses.count { it.playerId == playerId }
}
