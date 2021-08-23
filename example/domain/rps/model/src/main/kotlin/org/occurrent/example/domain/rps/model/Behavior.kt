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
import org.occurrent.example.domain.rps.model.GameLogic.play
import org.occurrent.example.domain.rps.model.Shape.*
import org.occurrent.example.domain.rps.model.StateEvolution.EvolvedGameState
import org.occurrent.example.domain.rps.model.StateEvolution.EvolvedRound
import org.occurrent.example.domain.rps.model.StateEvolution.EvolvedRound.RoundState.*
import org.occurrent.example.domain.rps.model.StateEvolution.EvolvedState
import org.occurrent.example.domain.rps.model.StateEvolution.evolve
import org.occurrent.example.domain.rps.model.StateTranslation.translateToDomain

fun handle(events: Sequence<GameEvent>, cmd: CreateGameCommand): Sequence<GameEvent> = when (events.evolve()) {
    is EvolvedState -> throw GameCannotBeCreatedMoreThanOnce()
    else -> {
        val (gameId, timestamp, creator, numberOfRounds) = cmd
        sequenceOf(GameCreated(gameId, timestamp, creator, numberOfRounds))
    }
}

fun handle(events: Sequence<GameEvent>, cmd: PlayHandCommand): Sequence<GameEvent> = when (val state = events.evolve()) {
    is EvolvedState -> play(cmd, EventRecorder.initializeFrom(state))
    else -> throw GameDoesNotExist()
}

private object GameLogic {

    fun play(cmd: PlayHandCommand, eventRecorder: EventRecorder): EventRecorder {
        val (timestamp, playerId) = cmd
        val state = eventRecorder.currentState
        val gameId = state.gameId
        return when (state) {
            is WaitingForFirstPlayer -> eventRecorder +
                    FirstPlayerJoinedGame(gameId, timestamp, playerId) +
                    GameStarted(gameId, timestamp, playerId) +
                    ::startNewRound.partial(cmd.timestamp) +
                    ::playHandAndEvaluateGameRules.partial(cmd)
            is WaitingForSecondPlayer -> if (playerId == state.firstPlayer) {
                throw CannotJoinTheGameTwice()
            } else {
                eventRecorder + SecondPlayerJoinedGame(gameId, timestamp, playerId) + ::playHandAndEvaluateGameRules.partial(cmd)

            }
            is Ongoing -> {
                if (eventRecorder.isRoundOngoing()) {
                    eventRecorder
                } else {
                    eventRecorder + ::startNewRound.partial(cmd.timestamp)
                } + ::playHandAndEvaluateGameRules.partial(cmd)
            }
            is Ended -> throw CannotMakeMoveBecauseGameEnded()
        }
    }

    private fun startNewRound(timestamp: Timestamp, eventRecorder: EventRecorder): EventRecorder {
        val state = eventRecorder.currentState
        val currentRoundNumber = state.currentRoundNumber()
        val newEvent = if (currentRoundNumber.value < state.maxNumberOfRounds.value) {
            RoundStarted(state.gameId, timestamp, currentRoundNumber.next())
        } else {
            throw IllegalStateException("Cannot start round since it would exceed ${state.maxNumberOfRounds.value}")
        }
        return eventRecorder + newEvent
    }

    private fun playHandAndEvaluateGameRules(cmd: PlayHandCommand, eventRecorder: EventRecorder): EventRecorder {
        val state = eventRecorder.currentState
        val (timestamp, playerId, shapeOfHand) = cmd
        val gameId = state.gameId
        val roundNumber = state.currentRoundNumber()
        val stateChangeAfterHandPlayed = eventRecorder + HandPlayed(gameId, timestamp, playerId, shapeOfHand, roundNumber)

        return when (val currentRound = stateChangeAfterHandPlayed.currentRound) {
            is Round.WaitingForFirstHand -> stateChangeAfterHandPlayed
            is Round.WaitingForSecondHand -> {
                val firstHand = currentRound.firstHand
                val secondHand = Hand(playerId, shapeOfHand)

                val roundOutcomeEvent = when {
                    firstHand.shape == secondHand.shape -> RoundTied(gameId, timestamp, roundNumber)
                    firstHand.beats(secondHand) -> RoundWon(gameId, timestamp, roundNumber, firstHand.playerId)
                    else -> RoundWon(gameId, timestamp, roundNumber, secondHand.playerId)
                }

                val roundEndedEvents = persistentListOf(roundOutcomeEvent, RoundEnded(gameId, timestamp, roundNumber))

                // TODO Game should end if any player can't win! Not all rounds needs to be played!
                val additionalEvents = if (eventRecorder.isLastMoveInGame()) {
                    val winnerId = determineGameOutcome(eventRecorder)
                    roundEndedEvents + (if (winnerId == null) GameTied(gameId, timestamp) else GameWon(gameId, timestamp, winnerId)) + GameEnded(gameId, timestamp)
                } else {
                    roundEndedEvents
                }
                stateChangeAfterHandPlayed + additionalEvents
            }
            else -> throw IllegalStateException("Cannot play round when round is in state ${currentRound::class.simpleName}")
        }
    }

    private fun determineGameOutcome(eventRecorder: EventRecorder): PlayerId? = when (val state = eventRecorder.currentState) {
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

    private val EventRecorder.currentRound: Round
        get() = when (val state = currentState) {
            is Ongoing -> state.rounds.last()
            else -> throw IllegalStateException("There's no current round")
        }

    private fun EventRecorder.isLastMoveInGame(): Boolean {
        val state = currentState
        return state is Ongoing && state.rounds.size == state.maxNumberOfRounds.value && currentRound is Round.Ended
    }

    private fun EventRecorder.isRoundOngoing(): Boolean = when (currentRound) {
        is Round.Tied -> false
        is Round.Won -> false
        is Round.WaitingForFirstHand -> true
        is Round.WaitingForSecondHand -> true
    }

    private fun CurrentGameState.currentRoundNumber() = when (this) {
        is Ongoing -> rounds.last().roundNumber
        is WaitingForSecondPlayer -> round.roundNumber
        else -> throw IllegalStateException("Cannot get round number in ${this::class.simpleName}")
    }

    private fun RoundNumber.next() = RoundNumber(value.inc())

    private fun Hand.beats(other: Hand): Boolean = shape.beats(other.shape)

    private fun Shape.beats(other: Shape): Boolean = when (this to other) {
        Pair(ROCK, SCISSORS) -> true
        Pair(PAPER, ROCK) -> true
        Pair(SCISSORS, PAPER) -> true
        else -> false
    }
}

private data class Hand(val playerId: PlayerId, val shape: Shape)

private class EventRecorder private constructor(private val evolvedState: EvolvedState, private val events: PersistentList<GameEvent>) : Sequence<GameEvent> {
    val currentState: CurrentGameState by lazy {
        evolvedState.translateToDomain()
    }

    override fun iterator(): Iterator<GameEvent> = events.iterator()
    fun evolve(e: GameEvent, vararg es: GameEvent) = EventRecorder(
        sequenceOf(e, *es).evolve(evolvedState)!!, events.addAll(listOf(e, *es)),
    )

    operator fun plus(e: GameEvent) = evolve(e)
    operator fun plus(fn: (EventRecorder) -> EventRecorder): EventRecorder = fn(this)
    operator fun plus(es: List<GameEvent>): EventRecorder = es.fold(this) { state, e ->
        state + e
    }

    companion object {
        fun initializeFrom(evolvedState: EvolvedState) = EventRecorder(evolvedState, persistentListOf())
    }
}

private sealed interface CurrentGameState {
    val gameId: GameId
    val maxNumberOfRounds: MaxNumberOfRounds

    data class WaitingForFirstPlayer(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds) : CurrentGameState
    data class WaitingForSecondPlayer(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId, val round: Round) : CurrentGameState
    data class Ongoing(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId, val secondPlayer: PlayerId, val rounds: PersistentList<Round> = persistentListOf()) : CurrentGameState
    data class Ended(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds) : CurrentGameState
}

private sealed interface Round {
    val roundNumber: RoundNumber

    sealed interface Ongoing : Round
    data class WaitingForFirstHand(override val roundNumber: RoundNumber) : Ongoing
    data class WaitingForSecondHand(override val roundNumber: RoundNumber, val firstHand: Hand) : Ongoing
    sealed interface Ended : Round
    data class Tied(override val roundNumber: RoundNumber, val firstHand: Hand, val secondHand: Hand) : Ended
    data class Won(override val roundNumber: RoundNumber, val firstHand: Hand, val secondHand: Hand, val winner: PlayerId) : Ended
}

private object StateTranslation {

    fun EvolvedState.translateToDomain(): CurrentGameState = when (state) {
        EvolvedGameState.WaitingForFirstPlayer -> WaitingForFirstPlayer(gameId, maxNumberOfRounds)
        EvolvedGameState.WaitingForSecondPlayer -> WaitingForSecondPlayer(gameId, maxNumberOfRounds, firstPlayer!!, rounds[0].toDomain())
        EvolvedGameState.Ongoing -> Ongoing(gameId, maxNumberOfRounds, firstPlayer!!, secondPlayer!!, rounds.map { round ->
            round.toDomain()
        }.toPersistentList())
        EvolvedGameState.Ended -> Ended(gameId, maxNumberOfRounds)
    }

    private fun EvolvedRound.toDomain() = when (state) {
        WaitingForFirstHand -> Round.WaitingForFirstHand(roundNumber)
        WaitingForSecondHand -> Round.WaitingForSecondHand(roundNumber, hands[0])
        Tied -> Round.Tied(roundNumber, hands[0], hands[1])
        Won -> Round.Won(roundNumber, hands[0], hands[1], winner!!)
    }
}


// Evolving from events
private object StateEvolution {

    data class EvolvedState(
        val gameId: GameId, val state: EvolvedGameState, val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId? = null, val secondPlayer: PlayerId? = null,
        val rounds: PersistentList<EvolvedRound> = persistentListOf()
    )

    data class EvolvedRound(val state: RoundState, val roundNumber: RoundNumber, val hands: PersistentList<Hand> = persistentListOf(), val winner: PlayerId? = null) {
        enum class RoundState {
            WaitingForFirstHand, WaitingForSecondHand, Tied, Won
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
        is RoundStarted -> currentState!!.copy(rounds = currentState.rounds.add(EvolvedRound(WaitingForFirstHand, e.roundNumber)))
        is HandPlayed -> currentState!!.updateRound(e.roundNumber) {
            copy(
                hands = hands.add(Hand(e.player, e.shape)),
                state = WaitingForSecondHand
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