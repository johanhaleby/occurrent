/*
 *
 *  Copyright 2021 Johan Haleby
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

package org.occurrent.example.domain.rps.model

import kotlinx.collections.immutable.PersistentList
import kotlinx.collections.immutable.persistentListOf
import org.occurrent.example.domain.rps.model.GameState.*

fun handle(events: Sequence<GameEvent>, cmd: CreateGameCommand): Sequence<GameEvent> = StateMachine(events.evolve()).handle(cmd)
fun handle(events: Sequence<GameEvent>, cmd: MakeMoveCommand): Sequence<GameEvent> = when (val currentState = events.evolve()) {
    is CurrentState -> makeMove(currentState, cmd)
    else -> throw GameDoesNotExist()
}

private class StateMachine(val state: CurrentState?, val events: PersistentList<GameEvent> = persistentListOf()) : Sequence<GameEvent> {

    fun handle(cmd: CreateGameCommand) = when (state) {
        is CurrentState -> throw GameCannotBeCreatedMoreThanOnce()
        else -> {
            val (gameId, timestamp, creator, numberOfRounds) = cmd
            sequenceOf(GameCreated(gameId, timestamp, creator, numberOfRounds))
        }
    }

    fun handle(cmd: MakeMoveCommand): StateMachine {
        val (timestamp, playerId, move) = cmd
        val gameId = state.gameId
        return when (state.state) {
            WaitingForFirstPlayer -> apply(FirstPlayerJoinedGame(state.gameId, timestamp, playerId) + startNewRound(state, timestamp, PlayerMove(playerId, move))
            WaitingForSecondPlayer -> if (cmd.playerId == state.firstPlayer) throw CannotJoinTheGameTwice() else sequenceOf(
                SecondPlayerJoinedGame(gameId, timestamp, playerId),
                GameStarted(gameId, timestamp, playerId),
                MoveMade(gameId, timestamp, playerId, move)
            )
            Ongoing -> TODO()
            Ended -> throw CannotMakeMoveBecauseGameEnded()
        }
    }

    private fun apply(e: GameEvent): StateMachine = StateMachine(evolve(state, e), events.add(e))

    override fun iterator(): Iterator<GameEvent> = events.iterator()
}


private fun startNewRound(state: CurrentState, timestamp: Timestamp, playerMove: PlayerMove): Sequence<GameEvent> {
    val currentNumberOfRounds = state.rounds.size
    return if (currentNumberOfRounds < state.numberOfRounds.value) {
        sequenceOf(
            RoundStarted(state.gameId, timestamp, RoundNumber(currentNumberOfRounds.inc())),
            MoveMade(state.gameId, timestamp, playerMove.playerId, playerMove.move)
        )
    } else {
        throw IllegalStateException("Cannot start round since it would exceed ${state.numberOfRounds.value}")
    }
}

private fun makeMove(state: CurrentState, timestamp: Timestamp, playerMove: PlayerMove) {

}

// Internal models
private enum class GameState {
    WaitingForFirstPlayer, WaitingForSecondPlayer, Ongoing, Ended
}

private data class PlayerMove(val playerId: PlayerId, val move: Move)

private enum class RoundState {
    Started, MoveMade, Tied, Won
}

private data class Round(val state: RoundState, val moves: PersistentList<PlayerMove> = persistentListOf(), val winner: PlayerId? = null)

private fun Round.play(player: PlayerId, move: Move) {

}

// Evolving from events
private data class CurrentState(
    val gameId: GameId, val state: GameState, val numberOfRounds: NumberOfRounds, val firstPlayer: PlayerId? = null, val secondPlayer: PlayerId? = null,
    val rounds: PersistentList<Round> = persistentListOf()
)

private fun Sequence<GameEvent>.evolve(): CurrentState? = fold(null, ::evolve)

private fun evolve(currentState: CurrentState?, e: GameEvent) = when (e) {
    is GameCreated -> CurrentState(gameId = e.game, state = WaitingForFirstPlayer, numberOfRounds = e.numberOfRounds)
    is FirstPlayerJoinedGame -> currentState!!.copy(state = WaitingForSecondPlayer, firstPlayer = e.player)
    is SecondPlayerJoinedGame -> currentState!!.copy(secondPlayer = e.player)
    is GameStarted -> currentState!!.copy(state = Ongoing)
    is RoundStarted -> currentState!!.copy(rounds = currentState.rounds.add(Round(RoundState.Started)))
    is MoveMade -> currentState!!.updateRound(RoundNumber.unsafe(currentState.rounds.size)) {
        copy(
            moves = moves.add(PlayerMove(e.player, e.move)),
            state = RoundState.MoveMade
        )
    }
    is RoundWon -> currentState!!.updateRound(e.roundNumber) {
        copy(
            winner = e.winner,
            state = RoundState.Won
        )
    }
    is RoundTied -> currentState!!.updateRound(e.roundNumber) {
        copy(state = RoundState.Tied)
    }
    is RoundEnded -> currentState
    is GameTied -> currentState
    is GameWon -> currentState
    is GameEnded -> currentState!!.copy(state = Ended)
}

private fun CurrentState.updateRound(roundNumber: RoundNumber, fn: Round.() -> Round): CurrentState = copy(rounds = rounds.updateRound(roundNumber, fn))

private fun PersistentList<Round>.updateRound(roundNumber: RoundNumber, fn: Round.() -> Round): PersistentList<Round> {
    val roundIndex = roundNumber.value - 1
    val updatedRound = fn(get(roundIndex))
    return set(roundIndex, updatedRound)
}