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
    data class WaitingForFirstPlayerToMakeGesture(val idOfPlayerThatInitiatedTheGame: PlayerId) : GameState
    data class WaitingForSecondPlayerToMakeGesture(val firstPlayerId: PlayerId, val firstPlayerGesture: HandGesture, val idOfPlayerThatInitiatedTheGame: PlayerId) : GameState
    data object Ended : GameState
}