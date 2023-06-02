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

package org.occurrent.example.domain.rps.pragmatic.model

import org.occurrent.example.domain.rps.pragmatic.model.HandShape.*
import org.occurrent.example.domain.rps.pragmatic.model.StateEvolution.EvolvedGameState.*
import org.occurrent.example.domain.rps.pragmatic.model.StateEvolution.evolve

private typealias WinnerId = PlayerId

object RPS {

    fun create(events: List<GameEvent>, gameId: GameId, timestamp: Timestamp, creator: GameCreatorId): GameCreated {
        val gameAlreadyStarted = events.evolve() != null
        if (gameAlreadyStarted) throw GameCannotBeCreatedMoreThanOnce()
        return GameCreated(gameId, timestamp, createdBy = creator)
    }

    fun join(events: List<GameEvent>, timestamp: Timestamp, playerId: PlayerId): PlayerReadyEvent {
        val (gameId, gameState, _, firstPlayer) = events.evolve() ?: throw GameDoesNotExist()
        return when (gameState) {
            Created -> FirstPlayerBecameReady(gameId, timestamp, playerId)
            FirstPlayerReady -> {
                if (firstPlayer == playerId) throw CannotJoinTheGameTwice()
                SecondPlayerBecameReady(gameId, timestamp, playerId)
            }

            else -> throw TooLateToJoinGame()
        }
    }

    fun play(events: List<GameEvent>, timestamp: Timestamp, playerId: PlayerId, shape: HandShape): List<GameEvent> {
        val (gameId, gameState, _, firstPlayerId, secondPlayerId, moves) = events.evolve() ?: throw GameDoesNotExist()

        return when (gameState) {
            Created, FirstPlayerReady -> throw CannotPlayHandBecauseWaitingPlayersToBeReady()
            BothPlayersReady -> {
                if (playerId != firstPlayerId && playerId != secondPlayerId) {
                    throw GameAlreadyHasTwoPlayers()
                }
                listOf(GameStarted(gameId, timestamp), HandPlayed(gameId, timestamp, playerId, shape))
            }

            Ongoing -> {
                if (playerId != firstPlayerId && playerId != secondPlayerId) {
                    throw GameAlreadyHasTwoPlayers()
                }

                val firstMove = moves.first()
                val thisMove = PlayerHand(playerId, shape)
                if (firstMove.playerId == thisMove.playerId) {
                    throw CannotPlayTheSameGameTwice()
                }

                val newEvents = mutableListOf<GameEvent>(HandPlayed(gameId, timestamp, playerId, shape))
                when (val winnerId = firstMove.beats(thisMove)) {
                    null -> newEvents.add(GameTied(gameId, timestamp))
                    else -> newEvents.add(GameWon(gameId, timestamp, winnerId))
                }
                newEvents + GameEnded(gameId, timestamp)
            }

            Ended -> throw CannotPlayHandBecauseGameEnded()
        }
    }
}

private fun PlayerHand.beats(other: PlayerHand): WinnerId? = when {
    shape == other.shape -> null
    shape == ROCK && other.shape == SCISSORS -> playerId
    shape == SCISSORS && other.shape == PAPER -> playerId
    shape == PAPER && other.shape == ROCK -> playerId
    else -> other.playerId
}

private data class PlayerHand(val playerId: PlayerId, val shape: HandShape)

// Evolving from events
private object StateEvolution {

    // Models for state evolution
    data class EvolvedState(
        val gameId: GameId, val gameState: EvolvedGameState, val createdBy: GameCreatorId, val firstPlayer: PlayerId? = null, val secondPlayer: PlayerId? = null, val hands: List<PlayerHand> = emptyList()
    )

    enum class EvolvedGameState {
        Created, FirstPlayerReady, BothPlayersReady, Ongoing, Ended
    }


    fun List<GameEvent>.evolve(currentState: EvolvedState? = null): EvolvedState? = fold(currentState, ::evolve)

    fun evolve(currentState: EvolvedState?, e: GameEvent) = when (e) {
        is GameCreated -> EvolvedState(gameId = e.game, createdBy = e.createdBy, gameState = Created)
        is FirstPlayerBecameReady -> currentState!!.copy(gameState = FirstPlayerReady, firstPlayer = e.player)
        is SecondPlayerBecameReady -> currentState!!.copy(gameState = BothPlayersReady, secondPlayer = e.player)
        is GameStarted -> currentState!!.copy(gameState = Ongoing)
        is HandPlayed -> currentState!!.copy(hands = currentState.hands + PlayerHand(e.player, e.shape))
        is GameTied -> currentState
        is GameWon -> currentState
        is GameEnded -> currentState!!.copy(gameState = Ended)
    }

    private operator fun List<PlayerHand>.plus(playerHand: PlayerHand): List<PlayerHand> = toMutableList().apply { add(playerHand) }
}