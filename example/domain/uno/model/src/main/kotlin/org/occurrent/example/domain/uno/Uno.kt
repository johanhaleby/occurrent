/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.example.domain.uno

import org.occurrent.example.domain.uno.Card.*
import org.occurrent.example.domain.uno.Direction.Clockwise
import org.occurrent.example.domain.uno.Direction.CounterClockwise
import java.time.LocalDateTime
import java.util.*

typealias EventId = UUID
typealias GameId = UUID
typealias PlayerId = Int
typealias PlayerCount = Int
typealias Timestamp = LocalDateTime

object Uno {

    fun start(events: Sequence<Event>, gameId: GameId, timestamp: Timestamp, playerCount: PlayerCount, firstCard: Card): Sequence<Event> {
        require(playerCount > 2) {
            "There should be at least 3 players"
        }

        return when (events.evolve()) {
            NotStarted -> {
                val firstPlayerId = if (firstCard is Skip) 1 else 0
                val gameStarted = GameStarted(EventId.randomUUID(), gameId, timestamp, firstPlayerId, playerCount, firstCard)
                val directionChanged = if (firstCard is KickBack) DirectionChanged(EventId.randomUUID(), gameId, timestamp, direction = CounterClockwise) else null
                sequenceOf(gameStarted, directionChanged).filterNotNull()
            }
            is Ongoing -> throw IllegalStateException("The game cannot be started more than once")
        }
    }

    fun play(events: Sequence<Event>, timestamp: Timestamp, playerId: PlayerId, card: Card): Sequence<Event> = when (val state = events.evolve()) {
        NotStarted -> throw IllegalStateException("Game has not been started")
        is Ongoing -> {
            val (gameId, turn, topCard) = state
            val expectedPlayerId = turn.playerId
            when {
                expectedPlayerId != playerId -> sequenceOf(PlayerPlayedAtWrongTurn(EventId.randomUUID(), gameId, timestamp, playerId, card))
                card.hasSameColorAs(topCard) || card.hasSameValueAs(topCard) -> {
                    fun cardPlayed(nextPlayerId: PlayerId) = CardPlayed(EventId.randomUUID(), gameId, timestamp, playerId, card, nextPlayerId)
                    when (card) {
                        is DigitCard -> sequenceOf(cardPlayed(turn.next().playerId))
                        is KickBack -> {
                            val nextTurn = turn.reverse().next()
                            sequenceOf(cardPlayed(nextTurn.playerId), DirectionChanged(EventId.randomUUID(), gameId, timestamp, nextTurn.direction))
                        }
                        is Skip -> sequenceOf(cardPlayed(turn.skip().playerId))
                    }
                }
                else -> sequenceOf(PlayerPlayedWrongCard(EventId.randomUUID(), gameId, timestamp, playerId, card))
            }
        }
    }
}


private fun Card.hasSameColorAs(otherCard: Card): Boolean = color == otherCard.color
private fun Card.hasSameValueAs(otherCard: Card): Boolean = when {
    this is DigitCard && otherCard is DigitCard -> this.digit == otherCard.digit
    this is KickBack && otherCard is KickBack -> true
    this is Skip && otherCard is Skip -> true
    else -> false
}

private fun Direction.reverse() = when (this) {
    Clockwise -> CounterClockwise
    CounterClockwise -> Clockwise
}

private data class Turn(val playerId: PlayerId, val playerCount: PlayerCount, val direction: Direction) {
    init {
        require(playerId in 0 until playerCount) {
            "The player value should be between 0 and player count"
        }
    }

    fun next() = when (direction) {
        Clockwise -> copy(playerId = (playerId + 1) % playerCount)
        CounterClockwise -> copy(playerId = (playerId + playerCount - 1) % playerCount)
    }

    fun skip() = next().next()

    fun reverse() = copy(direction = direction.reverse())

    fun setPlayer(playerId: PlayerId) = copy(playerId = playerId)

    fun setDirection(direction: Direction) = copy(direction = direction)
}

private sealed class State
private object NotStarted : State()
private data class Ongoing(val gameId: GameId, val turn: Turn, val topCard: Card) : State()

private fun Sequence<Event>.evolve(): State = fold<Event, State>(NotStarted) { currentState, event ->
    when (event) {
        is GameStarted -> Ongoing(event.gameId, Turn(event.firstPlayerId, event.playerCount, Clockwise), event.firstCard)
        is CardPlayed -> if (currentState is Ongoing) currentState.copy(turn = currentState.turn.setPlayer(event.nextPlayerId), topCard = event.card) else currentState
        is PlayerPlayedAtWrongTurn -> currentState
        is PlayerPlayedWrongCard -> currentState
        is DirectionChanged -> if (currentState is Ongoing) currentState.copy(turn = currentState.turn.setDirection(event.direction)) else currentState
    }
}