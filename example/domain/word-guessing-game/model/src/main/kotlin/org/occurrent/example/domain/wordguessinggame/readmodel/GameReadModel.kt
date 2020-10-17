package org.occurrent.example.domain.wordguessinggame.readmodel

import java.util.*
import kotlin.math.min

typealias WordToGuess = String
typealias WordHint = String
typealias GuessedWord = String
typealias Category = String


sealed class GameReadModel {
    abstract val gameId: UUID
}

abstract class GameEndedReadModel(val status : String) : GameReadModel() {
    abstract val startedAt: Date
    abstract val endedAt: Date
    abstract val category: String
    abstract val totalNumberOfGuesses: Int
    abstract val numberOfPlayersInGame: Int
    abstract val wordToGuess: String
}


class GameWasLostReadModel(override val gameId: UUID, override val startedAt: Date, override val endedAt: Date,
                           override val category: String, override val totalNumberOfGuesses: Int,
                           override val numberOfPlayersInGame: Int, override val wordToGuess: String) : GameEndedReadModel("lost")

data class GameWasWonReadModel(override val gameId: UUID, override val startedAt: Date, override val endedAt: Date,
                               override val category: String, override val totalNumberOfGuesses: Int,
                               override val numberOfPlayersInGame: Int, override val wordToGuess: String,
                               val winner: UUID, val numberOfGuessesByWinner: Int, val pointsAwardedToWinner: Int? = null) : GameEndedReadModel("won")

data class OngoingGameReadModel(override val gameId: UUID, val startedAt: Date, val category: Category,
                                val maxNumberOfGuessesPerPlayer: Int, val maxNumberOfGuessesTotal: Int,
                                val hint: WordHint, val guesses: List<Guess>, val wordToGuess: WordToGuess) : GameReadModel() {

    class Guess internal constructor(val playerId: UUID, val word: GuessedWord, val guessMadeAt: Date)

    val totalNumberOfGuessesLeft
        get() = maxNumberOfGuessesTotal - guesses.size

    fun numberOfGuessesLeftForPlayer(playerId: UUID): Int {
        val numberOfGuessesLeftForPlayer = maxNumberOfGuessesPerPlayer - guesses.count { it.playerId == playerId }
        return min(numberOfGuessesLeftForPlayer, totalNumberOfGuessesLeft)
    }
}