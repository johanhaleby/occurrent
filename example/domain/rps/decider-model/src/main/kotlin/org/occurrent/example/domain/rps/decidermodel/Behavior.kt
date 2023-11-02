/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.example.domain.rps.decidermodel

import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.rps.decidermodel.GameState.*
import org.occurrent.example.domain.rps.decidermodel.HandGesture.*

infix fun HandGesture.beats(other: HandGesture): Boolean = when {
    this == ROCK && other == SCISSORS -> true
    this == SCISSORS && other == PAPER -> true
    this == PAPER && other == ROCK -> true
    else -> false
}

val rps = decider<GameCommand, GameState, GameEvent>(
    initialState = DoesNotExist,
    decide = { c, s ->
        when {
            c is CreateGame && s is DoesNotExist -> listOf(GameCreated(c.gameId, c.timestamp, c.playerId))
            c is ShowHandGesture && s is Created -> listOf(GameStarted(c.gameId, c.timestamp), HandGestureShown(c.gameId, c.timestamp, c.playerId, c.gesture))
            c is ShowHandGesture && s is Ongoing -> {
                val (firstPlayerId, firstPlayerGesture) = s
                val (gameId, timestamp, secondPlayerId, secondPayerGesture) = c
                if (firstPlayerId == secondPlayerId) {
                    throw IllegalArgumentException("First player cannot show hand again")
                }

                val gameResultEvent = when {
                    firstPlayerGesture == secondPayerGesture -> GameTied(gameId, timestamp)
                    firstPlayerGesture beats secondPayerGesture -> GameWon(gameId, timestamp, firstPlayerId)
                    else -> GameWon(gameId, timestamp, secondPlayerId)
                }
                listOf(
                    HandGestureShown(gameId, timestamp, secondPlayerId, secondPayerGesture),
                    gameResultEvent,
                    GameEnded(gameId, timestamp)
                )
            }

            else -> throw IllegalArgumentException("Cannot ${c::class.simpleName} when game is ${s::class.simpleName}")
        }
    },
    evolve = { s, e ->
        when (e) {
            is GameCreated -> Created
            is HandGestureShown -> if (s is Created) Ongoing(e.player, e.gesture) else Ended
            else -> s
        }
    },
    isTerminal = { s -> s is Ended }
)