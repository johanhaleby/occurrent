package org.occurrent.example.domain.rps.multirounddecidermodel

import java.time.ZonedDateTime
import java.util.*

typealias GameId = UUID
typealias Timestamp = ZonedDateTime
typealias PlayerId = UUID
typealias RoundNumber = Int
typealias NumberOfRounds = Int

enum class HandGesture {
    ROCK, PAPER, SCISSORS
}

data class PlayerHandGesture(val playerId: PlayerId, val handGesture: HandGesture)


sealed interface RoundState {
    data object Started : RoundState
    sealed interface Ended : RoundState {
        data object Tied : Ended
        data class WonBy(val playerId: PlayerId) : Ended
    }
}

data class Round(val roundNumber: RoundNumber, val startedBy: PlayerId, val handGestures: List<PlayerHandGesture>, val state: RoundState)

enum class GameStatus {
    WaitingForFirstHandGestureInGame, WaitingForSecondHandGestureInGame, Ongoing, Ended
}

data class GameState(val gameId: GameId, val initiatedBy: PlayerId, val firstPlayerId: PlayerId?, val secondPlayerId: PlayerId?, val totalNumberOfRounds: NumberOfRounds, val rounds: List<Round>, val status: GameStatus)