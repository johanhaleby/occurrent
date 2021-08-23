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
import org.occurrent.example.domain.rps.model.GameLogic.play
import org.occurrent.example.domain.rps.model.GameState.*
import org.occurrent.example.domain.rps.model.Shape.*

fun handle(events: Sequence<GameEvent>, cmd: CreateGameCommand): Sequence<GameEvent> = when (events.evolve()) {
    is CurrentState -> throw GameCannotBeCreatedMoreThanOnce()
    else -> {
        val (gameId, timestamp, creator, numberOfRounds) = cmd
        sequenceOf(GameCreated(gameId, timestamp, creator, numberOfRounds))
    }
}

fun handle(events: Sequence<GameEvent>, cmd: PlayHandCommand): Sequence<GameEvent> = when (val currentState = events.evolve()) {
    is CurrentState -> play(CommandState(cmd.timestamp, currentState), Hand(cmd.playerId, cmd.shape))
    else -> throw GameDoesNotExist()
}

private object GameLogic {

    fun startNewRound(commandState: CommandState): CommandState {
        val (timestamp, state) = commandState
        val currentNumberOfRounds = state.rounds.size
        val newEvent = if (currentNumberOfRounds < state.numberOfRounds.value) {
            RoundStarted(state.gameId, timestamp, RoundNumber(currentNumberOfRounds.inc()))
        } else {
            throw IllegalStateException("Cannot start round since it would exceed ${state.numberOfRounds.value}")
        }
        return commandState + newEvent
    }

    fun play(commandState: CommandState, hand: Hand): CommandState {
        val (timestamp, state) = commandState
        val (playerId) = hand
        val gameId = state.gameId
        return when (state.state) {
            WaitingForFirstPlayer -> commandState + FirstPlayerJoinedGame(gameId, timestamp, playerId) + ::startNewRound + ::playHandAndEvaluateGameRules.partial(hand)
            WaitingForSecondPlayer -> if (hand.playerId == state.firstPlayer) {
                throw CannotJoinTheGameTwice()
            } else {
                commandState +
                        listOf(
                            SecondPlayerJoinedGame(gameId, timestamp, playerId),
                            GameStarted(gameId, timestamp, playerId)
                        ) + ::playHandAndEvaluateGameRules.partial(hand)

            }
            Ongoing -> {
                if (commandState.isRoundOngoing()) {
                    commandState
                } else {
                    commandState + ::startNewRound
                } + ::playHandAndEvaluateGameRules.partial(hand)
            }
            Ended -> throw CannotMakeMoveBecauseGameEnded()
        }
    }

    private fun playHandAndEvaluateGameRules(commandState: CommandState, hand: Hand): CommandState {
        val (timestamp, state) = commandState
        val gameId = state.gameId
        val commandStateAfterHandPlayed = commandState + HandPlayed(gameId, timestamp, hand.playerId, hand.shape)
        val currentRound = commandStateAfterHandPlayed.currentRound
        val roundStatus = currentRound.status
        val additionalEvents = if (roundStatus == RoundStatus.NotEnded) {
            emptyList()
        } else {
            val roundNumber = RoundNumber(state.numberOfRounds.value)
            val roundStatusEvent = if (roundStatus == RoundStatus.Tied) {
                RoundTied(gameId, timestamp, roundNumber)
            } else {
                RoundWon(gameId, timestamp, roundNumber, (roundStatus as RoundStatus.Won).playerId)
            }

            val roundEndedEvents = persistentListOf(roundStatusEvent, RoundEnded(gameId, timestamp, roundNumber))
            if (commandState.isLastMoveInGame()) {
                val winnerId = determineGameOutcome(commandState)
                roundEndedEvents + (if (winnerId == null) GameTied(gameId, timestamp) else GameWon(gameId, timestamp, winnerId)) + GameEnded(gameId, timestamp)
            } else {
                roundEndedEvents
            }
        }
        return commandStateAfterHandPlayed + additionalEvents
    }

    private fun determineGameOutcome(commandState: CommandState): PlayerId? {
        val numberOfWinsPerPlayer = commandState.state.rounds
            .groupBy { round -> (round.status as? RoundStatus.Won)?.playerId }
            .mapValues { (_, wonRounds) -> wonRounds.size }

        val numberOfWinsForPlayer1 = numberOfWinsPerPlayer[commandState.state.firstPlayer] ?: 0
        val numberOfWinsForPlayer2 = numberOfWinsPerPlayer[commandState.state.secondPlayer] ?: 0

        val winnerId = when {
            numberOfWinsForPlayer1 > numberOfWinsForPlayer2 -> commandState.state.firstPlayer
            numberOfWinsForPlayer2 > numberOfWinsForPlayer1 -> commandState.state.secondPlayer
            else -> null
        }
        return winnerId
    }

    private val Round.status: RoundStatus
        get() = if (hasMoves(2)) {
            val (firstMove, secondMove) = moves
            if (firstMove.beats(secondMove)) {
                RoundStatus.Won(firstMove.playerId)
            } else {
                RoundStatus.Tied
            }
        } else {
            RoundStatus.NotEnded
        }

    private val CommandState.currentRound: Round
        get() = state.rounds.last()

    private fun CommandState.isLastMoveInGame(): Boolean = state.rounds.size == state.numberOfRounds.value
            && currentRound.moves.size == 2

    private fun CommandState.isRoundOngoing(): Boolean = currentRound.numberOfMoves == 1
}

// Internal models
private enum class GameState {
    WaitingForFirstPlayer, WaitingForSecondPlayer, Ongoing, Ended
}

private data class Hand(val playerId: PlayerId, val shape: Shape)

private enum class RoundState {
    Started, MoveMade, Tied, Won
}

private data class Round(val state: RoundState, val moves: PersistentList<Hand> = persistentListOf(), val winner: PlayerId? = null)

private val Round.numberOfMoves: Int
    get() = moves.size

private fun Round.hasMoves(expectedNumberOfMoves: Int) = numberOfMoves == expectedNumberOfMoves

private sealed interface RoundStatus {
    data class Won(val playerId: PlayerId) : RoundStatus
    object Tied : RoundStatus
    object NotEnded : RoundStatus
}

private fun Hand.beats(other: Hand): Boolean = shape.beats(other.shape)

private fun Shape.beats(other: Shape): Boolean = when (this to other) {
    Pair(ROCK, SCISSORS) -> true
    Pair(PAPER, ROCK) -> true
    Pair(SCISSORS, PAPER) -> true
    else -> false
}


// Command evolution
private data class CommandState(val timestamp: Timestamp, val state: CurrentState, val events: PersistentList<GameEvent> = persistentListOf()) : Sequence<GameEvent> {
    override fun iterator(): Iterator<GameEvent> = events.iterator()
    fun evolve(e: GameEvent, vararg es: GameEvent) = CommandState(
        timestamp, sequenceOf(e, *es).evolve(state)!!, events.addAll(listOf(e, *es)),
    )

    operator fun plus(e: GameEvent) = evolve(e)
    operator fun plus(fn: (CommandState) -> CommandState): CommandState = fn(this)
    operator fun plus(es: List<GameEvent>): CommandState = es.fold(this) { state, e ->
        state + e
    }
}


// Evolving from events
private data class CurrentState(
    val gameId: GameId, val state: GameState, val numberOfRounds: NumberOfRounds, val firstPlayer: PlayerId? = null, val secondPlayer: PlayerId? = null,
    val rounds: PersistentList<Round> = persistentListOf()
)

private fun Sequence<GameEvent>.evolve(currentState: CurrentState? = null): CurrentState? = fold(currentState, ::evolve)

private fun evolve(currentState: CurrentState?, e: GameEvent) = when (e) {
    is GameCreated -> CurrentState(gameId = e.game, state = WaitingForFirstPlayer, numberOfRounds = e.numberOfRounds)
    is FirstPlayerJoinedGame -> currentState!!.copy(state = WaitingForSecondPlayer, firstPlayer = e.player)
    is SecondPlayerJoinedGame -> currentState!!.copy(secondPlayer = e.player)
    is GameStarted -> currentState!!.copy(state = Ongoing)
    is RoundStarted -> currentState!!.copy(rounds = currentState.rounds.add(Round(RoundState.Started)))
    is HandPlayed -> currentState!!.updateRound(RoundNumber.unsafe(currentState.rounds.size)) {
        copy(
            moves = moves.add(Hand(e.player, e.shape)),
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

private fun <A, B> ((A, B) -> A).partial(b: B): (A) -> A = { a ->
    this(a, b)
}