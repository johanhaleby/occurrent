package org.occurrent.example.domain.rps.decidermodel

import java.time.ZonedDateTime
import java.util.*

typealias GameId = UUID
typealias Timestamp = ZonedDateTime
typealias PlayerId = UUID

enum class HandGesture {
    ROCK, PAPER, SCISSORS
}

sealed interface GameState {
    data object DoesNotExist : GameState
    data object Created : GameState
    data class Ongoing(val firstPlayerId: PlayerId, val firstPlayerShape: HandGesture) : GameState
    data object Ended : GameState
}