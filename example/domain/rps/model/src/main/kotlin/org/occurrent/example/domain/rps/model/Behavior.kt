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
import kotlinx.collections.immutable.toPersistentList
import org.occurrent.example.domain.rps.model.CurrentGameState.*
import org.occurrent.example.domain.rps.model.EventEvolution.EvolvedGameState
import org.occurrent.example.domain.rps.model.EventEvolution.EvolvedRound.State.*
import org.occurrent.example.domain.rps.model.EventEvolution.EvolvedState
import org.occurrent.example.domain.rps.model.EventEvolution.evolve
import org.occurrent.example.domain.rps.model.GameLogic.play
import org.occurrent.example.domain.rps.model.Shape.*
import org.occurrent.example.domain.rps.model.StateTranslation.translateToDomain

fun handle(events: Sequence<GameEvent>, cmd: CreateGameCommand): Sequence<GameEvent> = when (events.evolve()) {
    is EvolvedState -> throw GameCannotBeCreatedMoreThanOnce()
    else -> {
        val (gameId, timestamp, creator, numberOfRounds) = cmd
        sequenceOf(GameCreated(gameId, timestamp, creator, numberOfRounds))
    }
}

fun handle(events: Sequence<GameEvent>, cmd: PlayHandCommand): Sequence<GameEvent> = when (val state = events.evolve()) {
    is EvolvedState -> play(cmd, StateChange(state))
    else -> throw GameDoesNotExist()
}

private object GameLogic {

    fun play(cmd: PlayHandCommand, stateChange: StateChange): StateChange {
        val (timestamp, playerId) = cmd
        val state = stateChange.currentState
        val gameId = state.gameId
        return when (state) {
            is WaitingForFirstPlayer -> stateChange + FirstPlayerJoinedGame(gameId, timestamp, playerId) + ::startNewRound.partial(cmd.timestamp) + ::playHandAndEvaluateGameRules.partial(cmd)
            is WaitingForSecondPlayer -> if (playerId == state.firstPlayer) {
                throw CannotJoinTheGameTwice()
            } else {
                stateChange +
                        listOf(
                            SecondPlayerJoinedGame(gameId, timestamp, playerId),
                            GameStarted(gameId, timestamp, playerId)
                        ) + ::playHandAndEvaluateGameRules.partial(cmd)

            }
            is Ongoing -> {
                if (stateChange.isRoundOngoing()) {
                    stateChange
                } else {
                    stateChange + ::startNewRound.partial(cmd.timestamp)
                } + ::playHandAndEvaluateGameRules.partial(cmd)
            }
            is Ended -> throw CannotMakeMoveBecauseGameEnded()
        }
    }

    private fun startNewRound(timestamp: Timestamp, stateChange: StateChange): StateChange {
        val state = stateChange.currentState
        val currentNumberOfRounds = (state as? Ongoing)?.rounds?.size ?: 0  // TODO Fix!
        val newEvent = if (currentNumberOfRounds < state.maxNumberOfRounds.value) {
            RoundStarted(state.gameId, timestamp, RoundNumber(currentNumberOfRounds.inc()))
        } else {
            throw IllegalStateException("Cannot start round since it would exceed ${state.maxNumberOfRounds.value}")
        }
        return stateChange + newEvent
    }

    private fun playHandAndEvaluateGameRules(cmd: PlayHandCommand, stateChange: StateChange): StateChange {
        val state = stateChange.currentState
        val (timestamp, playerId, shapeOfHand) = cmd
        val gameId = state.gameId
        val stateChangeAfterHandPlayed = stateChange + HandPlayed(gameId, timestamp, playerId, shapeOfHand)

        return when (val currentRound = stateChangeAfterHandPlayed.currentRound) {
            is Round.WaitingForFirstHand -> stateChangeAfterHandPlayed
            is Round.WaitingForSecondHand -> {
                val roundNumber = RoundNumber.unsafe(3) //  TODO("Add round number to Round")
                val firstHand = currentRound.firstHand
                val secondHand = Hand(playerId, shapeOfHand)

                val roundOutcomeEvent = when {
                    firstHand.shape == secondHand.shape -> RoundTied(gameId, timestamp, roundNumber)
                    firstHand.beats(secondHand) -> RoundWon(gameId, timestamp, roundNumber, firstHand.playerId)
                    else -> RoundWon(gameId, timestamp, roundNumber, secondHand.playerId)
                }

                val roundEndedEvents = persistentListOf(roundOutcomeEvent, RoundEnded(gameId, timestamp, roundNumber))

                val additionalEvents = if (stateChange.isLastMoveInGame()) {
                    val winnerId = determineGameOutcome(stateChange)
                    roundEndedEvents + (if (winnerId == null) GameTied(gameId, timestamp) else GameWon(gameId, timestamp, winnerId)) + GameEnded(gameId, timestamp)
                } else {
                    roundEndedEvents
                }
                stateChangeAfterHandPlayed + additionalEvents
            }
            else -> throw IllegalStateException("Cannot play round when round is in state ${currentRound::class.simpleName}")
        }
    }

    private fun determineGameOutcome(stateChange: StateChange): PlayerId? = when (val state = stateChange.currentState) {
        is Ongoing -> {
            val numberOfWinsPerPlayer = state.rounds
                .groupBy { round -> (round as? Round.Won)?.winner }
                .mapValues { (_, wonRounds) -> wonRounds.size }

            val numberOfWinsForPlayer1 = numberOfWinsPerPlayer[state.firstPlayer] ?: 0
            val numberOfWinsForPlayer2 = numberOfWinsPerPlayer[state.secondPlayer] ?: 0

            val winnerId = when {
                numberOfWinsForPlayer1 > numberOfWinsForPlayer2 -> state.firstPlayer
                numberOfWinsForPlayer2 > numberOfWinsForPlayer1 -> state.secondPlayer
                else -> null
            }
            winnerId
        }
        else -> throw IllegalStateException("Cannot determine game outcome when game is in state ${state::class.simpleName}")
    }

    private val StateChange.currentRound: Round
        get() = when (val state = currentState) {
            is Ongoing -> state.rounds.last()
            else -> throw IllegalStateException("There's no current round")
        }

    private fun StateChange.isLastMoveInGame(): Boolean {
        val state = currentState
        return state is Ongoing && state.rounds.size == state.maxNumberOfRounds.value && currentRound is Round.Ended
    }

    private fun StateChange.isRoundOngoing(): Boolean = when (currentRound) {
        is Round.Tied -> false
        is Round.Won -> false
        Round.WaitingForFirstHand -> true
        is Round.WaitingForSecondHand -> true
    }
}

private data class Hand(val playerId: PlayerId, val shape: Shape)


private fun Hand.beats(other: Hand): Boolean = shape.beats(other.shape)

private fun Shape.beats(other: Shape): Boolean = when (this to other) {
    Pair(ROCK, SCISSORS) -> true
    Pair(PAPER, ROCK) -> true
    Pair(SCISSORS, PAPER) -> true
    else -> false
}


// Command evolution
private data class StateChange(private val evolvedState: EvolvedState, private val events: PersistentList<GameEvent> = persistentListOf()) : Sequence<GameEvent> {
    val currentState: CurrentGameState by lazy {
        evolvedState.translateToDomain()
    }

    override fun iterator(): Iterator<GameEvent> = events.iterator()
    fun evolve(e: GameEvent, vararg es: GameEvent) = StateChange(
        sequenceOf(e, *es).evolve(evolvedState)!!, events.addAll(listOf(e, *es)),
    )

    operator fun plus(e: GameEvent) = evolve(e)
    operator fun plus(fn: (StateChange) -> StateChange): StateChange = fn(this)
    operator fun plus(es: List<GameEvent>): StateChange = es.fold(this) { state, e ->
        state + e
    }
}

private sealed interface CurrentGameState {
    val gameId: GameId
    val maxNumberOfRounds: MaxNumberOfRounds

    data class WaitingForFirstPlayer(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds) : CurrentGameState
    data class WaitingForSecondPlayer(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId) : CurrentGameState
    data class Ongoing(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId, val secondPlayer: PlayerId, val rounds: PersistentList<Round> = persistentListOf()) : CurrentGameState
    data class Ended(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds) : CurrentGameState
}

private sealed interface Round {
    sealed interface Ongoing : Round
    object WaitingForFirstHand : Ongoing
    data class WaitingForSecondHand(val firstHand: Hand) : Ongoing
    sealed interface Ended : Round
    data class Tied(val firstHand: Hand, val secondHand: Hand) : Ended
    data class Won(val firstHand: Hand, val secondHand: Hand, val winner: PlayerId) : Ended
}

private object StateTranslation {

    fun EvolvedState.translateToDomain(): CurrentGameState = when (state) {
        EvolvedGameState.WaitingForFirstPlayer -> WaitingForFirstPlayer(gameId, maxNumberOfRounds)
        EvolvedGameState.WaitingForSecondPlayer -> WaitingForSecondPlayer(gameId, maxNumberOfRounds, firstPlayer!!)
        EvolvedGameState.Ongoing -> Ongoing(gameId, maxNumberOfRounds, firstPlayer!!, secondPlayer!!, rounds.map { round ->
            when (round.state) {
                Started -> Round.WaitingForFirstHand
                OneHandPlayed -> Round.WaitingForSecondHand(round.hands[0])
                Tied -> Round.Tied(round.hands[0], round.hands[1])
                Won -> Round.Won(round.hands[0], round.hands[1], round.winner!!)
            }
        }.toPersistentList())
        EvolvedGameState.Ended -> Ended(gameId, maxNumberOfRounds)
    }
}


// Evolving from events
private object EventEvolution {

    data class EvolvedState(
        val gameId: GameId, val state: EvolvedGameState, val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId? = null, val secondPlayer: PlayerId? = null,
        val rounds: PersistentList<EvolvedRound> = persistentListOf()
    )

    data class EvolvedRound(val state: State, val hands: PersistentList<Hand> = persistentListOf(), val winner: PlayerId? = null) {
        enum class State {
            Started, OneHandPlayed, Tied, Won
        }
    }

    // Internal models
    enum class EvolvedGameState {
        WaitingForFirstPlayer, WaitingForSecondPlayer, Ongoing, Ended
    }


    fun Sequence<GameEvent>.evolve(currentState: EvolvedState? = null): EvolvedState? = fold(currentState, ::evolve)

    fun evolve(currentState: EvolvedState?, e: GameEvent) = when (e) {
        is GameCreated -> EvolvedState(gameId = e.game, state = EvolvedGameState.WaitingForFirstPlayer, maxNumberOfRounds = e.maxNumberOfRounds)
        is FirstPlayerJoinedGame -> currentState!!.copy(state = EvolvedGameState.WaitingForSecondPlayer, firstPlayer = e.player)
        is SecondPlayerJoinedGame -> currentState!!.copy(secondPlayer = e.player)
        is GameStarted -> currentState!!.copy(state = EvolvedGameState.Ongoing)
        is RoundStarted -> currentState!!.copy(rounds = currentState.rounds.add(EvolvedRound(Started)))
        is HandPlayed -> currentState!!.updateRound(RoundNumber.unsafe(currentState.rounds.size)) {
            copy(
                hands = hands.add(Hand(e.player, e.shape)),
                state = OneHandPlayed
            )
        }
        is RoundWon -> currentState!!.updateRound(e.roundNumber) {
            copy(
                winner = e.winner,
                state = Won
            )
        }
        is RoundTied -> currentState!!.updateRound(e.roundNumber) {
            copy(state = Tied)
        }
        is RoundEnded -> currentState
        is GameTied -> currentState
        is GameWon -> currentState
        is GameEnded -> currentState!!.copy(state = EvolvedGameState.Ended)
    }


    private fun EvolvedState.updateRound(roundNumber: RoundNumber, fn: EvolvedRound.() -> EvolvedRound): EvolvedState = copy(rounds = rounds.updateRound(roundNumber, fn))

    private fun PersistentList<EvolvedRound>.updateRound(roundNumber: RoundNumber, fn: EvolvedRound.() -> EvolvedRound): PersistentList<EvolvedRound> {
        val roundIndex = roundNumber.value - 1
        val updatedRound = fn(get(roundIndex))
        return set(roundIndex, updatedRound)
    }
}

private fun <A, B> ((A, B) -> B).partial(a: A): (B) -> B = { b ->
    this(a, b)
}