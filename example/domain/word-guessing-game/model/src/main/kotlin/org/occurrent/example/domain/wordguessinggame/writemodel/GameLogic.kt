package org.occurrent.example.domain.wordguessinggame.writemodel

import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.support.add
import java.util.*

fun startGame(previousEvents: Sequence<DomainEvent>, gameId: GameId, timestamp: Timestamp, playerId: PlayerId, wordsToChooseFrom: WordsToChooseFrom, maxNumberOfGuessesPerPlayer: MaxNumberOfGuessesPerPlayer,
              maxNumberOfGuessesTotal: MaxNumberOfGuessesTotal): Sequence<DomainEvent> {
    val state = previousEvents.deriveGameState()

    if (state !is NotStarted) {
        throw IllegalStateException("Cannot start game $gameId since it has already been started")
    }

    val wordToGuess = wordsToChooseFrom.words.random()

    val gameStarted = GameWasStarted(eventId = UUID.randomUUID(), timestamp = timestamp, gameId = gameId, startedBy = playerId, category = wordsToChooseFrom.category.value,
            wordToGuess = wordToGuess.value, maxNumberOfGuessesPerPlayer = MaxNumberOfGuessesPerPlayer.value, maxNumberOfGuessesTotal = MaxNumberOfGuessesTotal.value)

    return sequenceOf(gameStarted)
}

fun guessWord(previousEvents: Sequence<DomainEvent>, timestamp: Timestamp, playerId: PlayerId, guessedWord: Word): Sequence<DomainEvent> = when (val state = previousEvents.deriveGameState()) {
    NotStarted -> throw IllegalStateException("Cannot guess word for a game that is not started")
    is Ended -> throw IllegalStateException("Cannot guess word for a game that is already ended")
    is Ongoing -> {
        if (state.isMaxNumberOfGuessesExceededForPlayer(playerId)) {
            throw IllegalArgumentException("Number of guessing attempts exhausted for player $playerId.")
        }

        val events = mutableListOf<DomainEvent>()

        if (state.isRightGuess(guessedWord)) {
            events.add(PlayerGuessedTheRightWord(UUID.randomUUID(), timestamp, state.gameId, playerId, guessedWord.value))
            events.add(GameWasWon(UUID.randomUUID(), timestamp, state.gameId, playerId))
            events.addAll(awardPointsToPlayerThatGuessedTheRightWord(playerId, timestamp, state))
        } else {
            events.add(PlayerGuessedTheWrongWord(UUID.randomUUID(), timestamp, state.gameId, playerId, guessedWord.value))

            if (state.isLastGuessForPlayer(playerId)) {
                events.add(NumberOfGuessesWasExhaustedForPlayer(UUID.randomUUID(), timestamp, state.gameId, playerId))
            }

            if (state.isLastGuessForGame()) {
                events.add(GameWasLost(UUID.randomUUID(), timestamp, state.gameId))
            }
        }

        events.asSequence()
    }
}

private data class Guess(val playerId: PlayerId, val timestamp: Timestamp, val word: String)

// States
private sealed class GameState
private object NotStarted : GameState()
private data class Ongoing(val gameId: GameId, val wordToGuess: String, val maxNumberOfGuessesPerPlayer: Int, val maxNumberOfGuessesTotal: Int, val guesses: List<Guess> = emptyList()) : GameState() {
    fun numberOfGuessesForPlayer(playerId: PlayerId) = guesses.count { it.playerId == playerId }
    fun isMaxNumberOfGuessesExceededForPlayer(playerId: PlayerId): Boolean = numberOfGuessesForPlayer(playerId) == maxNumberOfGuessesPerPlayer
    fun isLastGuessForPlayer(playerId: PlayerId): Boolean = numberOfGuessesForPlayer(playerId) + 1 == maxNumberOfGuessesPerPlayer
    fun isLastGuessForGame(): Boolean = guesses.size == maxNumberOfGuessesTotal - 1
    fun isRightGuess(guessedWord: Word) = guessedWord.value.toUpperCase() == wordToGuess.toUpperCase()
}

private object Ended : GameState()

private fun Sequence<DomainEvent>.deriveGameState(): GameState = fold<DomainEvent, GameState>(NotStarted) { state, event ->
    when {
        state is NotStarted && event is GameWasStarted -> Ongoing(event.gameId, event.wordToGuess, event.maxNumberOfGuessesPerPlayer, event.maxNumberOfGuessesTotal)
        state is Ongoing && event is PlayerGuessedTheWrongWord -> state.copy(guesses = state.guesses.add(Guess(event.playerId, event.timestamp, event.guessedWord)))
        state is Ongoing && event is PlayerGuessedTheRightWord -> state.copy(guesses = state.guesses.add(Guess(event.playerId, event.timestamp, event.guessedWord)))
        state is Ongoing && event is NumberOfGuessesWasExhaustedForPlayer -> state
        state is Ongoing && event is GameWasWon -> Ended
        state is Ongoing && event is GameWasLost -> Ended
        else -> throw IllegalStateException("Event ${event.type} is not applicable in state ${state::class.simpleName!!}")
    }
}

// Helper functions
private fun awardPointsToPlayerThatGuessedTheRightWord(playerId: PlayerId, timestamp: Timestamp, state: Ongoing): List<DomainEvent> {
    val numberOfWrongGuessesForPlayerInGame = state.guesses.count { it.playerId == playerId }
    val totalGuessesForPlayerInGame = numberOfWrongGuessesForPlayerInGame + 1
    val points = PointCalculationLogic.calculatePointsToAwardPlayerAfterSuccessfullyGuessedTheRightWord(totalGuessesForPlayerInGame)
    return listOf(PlayerWasAwardedPointsForGuessingTheRightWord(UUID.randomUUID(), timestamp, state.gameId, playerId, points))
}
