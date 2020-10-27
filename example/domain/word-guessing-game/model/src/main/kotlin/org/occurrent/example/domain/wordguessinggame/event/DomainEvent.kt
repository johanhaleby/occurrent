package org.occurrent.example.domain.wordguessinggame.event

import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Points
import java.util.*
import kotlin.reflect.KClass

sealed class DomainEvent {
    abstract val eventId: UUID
    abstract val timestamp: Date
    abstract val gameId: UUID
    open val type: String = this::class.eventType()
}

data class GameWasStarted(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID,
                          val startedBy: PlayerId, val category: String, val wordToGuess: String, val maxNumberOfGuessesPerPlayer: Int,
                          val maxNumberOfGuessesTotal: Int) : DomainEvent()

data class PlayerGuessedTheWrongWord(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val playerId: PlayerId, val guessedWord: String) : DomainEvent()

data class NumberOfGuessesWasExhaustedForPlayer(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val playerId: PlayerId) : DomainEvent()

data class PlayerGuessedTheRightWord(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val playerId: PlayerId, val guessedWord: String) : DomainEvent()

data class PlayerWasAwardedPointsForGuessingTheRightWord(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val playerId: PlayerId, val points: Points) : DomainEvent()

sealed class ReasonForNotBeingAwardedPoints {
    object PlayerCreatedListOfWords : ReasonForNotBeingAwardedPoints()
}
data class PlayerWasNotAwardedAnyPointsForGuessingTheRightWord(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val playerId: PlayerId, val reason: ReasonForNotBeingAwardedPoints) : DomainEvent()

data class GameWasWon(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val winnerId: PlayerId) : DomainEvent()

data class GameWasLost(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID) : DomainEvent()

data class CharacterInWordHintWasRevealed(override val eventId: UUID, override val timestamp: Date, override val gameId: UUID, val character: Char, val characterPositionInWord: Int) : DomainEvent()

fun <T : DomainEvent> KClass<out T>.eventType() = simpleName!!