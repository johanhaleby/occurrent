package org.occurrent.example.domain.wordguessinggame.model

import java.util.*

sealed class DomainEvent {
    abstract val eventId: UUID
    abstract val timestamp: Date
    open val type: String = this::class.simpleName!!
}

data class GameWasStarted(override val eventId: UUID, override val timestamp: Date, val startedBy: PlayerId,
                          val gameId: UUID, val category: String, val wordToGuess: String, val maxNumberOfGuessesPerPlayer: Int,
                          val maxNumberOfGuessesTotal: Int) : DomainEvent()

data class PlayerGuessedTheWrongWord(override val eventId: UUID, override val timestamp: Date, val playerId: PlayerId, val guessedWord: String) : DomainEvent()

data class NumberOfGuessesWasExhaustedForPlayer(override val eventId: UUID, override val timestamp: Date, val playerId: PlayerId) : DomainEvent()

data class PlayerGuessedTheRightWord(override val eventId: UUID, override val timestamp: Date, val playerId: PlayerId, val guessedWord: String) : DomainEvent()

data class GameWasWon(override val eventId: UUID, override val timestamp: Date, val winnerId: PlayerId) : DomainEvent()

data class GameWasLost(override val eventId: UUID, override val timestamp: Date) : DomainEvent()