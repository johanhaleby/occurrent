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

fun handle(events: Sequence<GameEvent>, cmd: Command): Sequence<GameEvent> {
    val state = events.evolve()
    return when (cmd) {
        is CreateGame -> when (state) {
            is EvolvedState -> throw GameCannotBeCreatedMoreThanOnce()
            else -> {
                val (gameId, timestamp, creator, numberOfRounds) = cmd
                sequenceOf(GameCreated(gameId, timestamp, creator, numberOfRounds))
            }
        }
        is PlayHand -> when (state) {
            is EvolvedState -> play(cmd, AccumulatedChanges.initializeFrom(state))
            else -> throw GameDoesNotExist()
        }
    }
}

private object GameLogic {

    fun play(cmd: PlayHand, accumulatedChanges: AccumulatedChanges): AccumulatedChanges {
        val (timestamp, playerId) = cmd
        val state = accumulatedChanges.currentState
        val gameId = state.gameId
        return when (state) {
            is Created -> accumulatedChanges +
                    ::startNewRound.partial(cmd.timestamp) +
                    GameStarted(gameId, timestamp) +
                    ::play.partial(cmd)
            is Started -> accumulatedChanges +
                    FirstPlayerJoinedGame(gameId, timestamp, playerId) +
                    ::playHandAndEvaluateGameRules.partial(cmd)
            is FirstPlayerJoined -> if (playerId == state.firstPlayer) {
                throw CannotJoinTheGameTwice()
            } else {
                accumulatedChanges + SecondPlayerJoinedGame(gameId, timestamp, playerId) + ::playHandAndEvaluateGameRules.partial(cmd)

            }
            is BothPlayersJoined -> {
                if (state.firstPlayer != playerId && state.secondPlayer != playerId) {
                    throw GameAlreadyHasTwoPlayers()
                }

                if (accumulatedChanges.isRoundOngoing()) {
                    accumulatedChanges
                } else {
                    accumulatedChanges + ::startNewRound.partial(cmd.timestamp)
                } + ::playHandAndEvaluateGameRules.partial(cmd)
            }
            is Ended -> throw CannotPlayHandBecauseGameEnded()
        }
    }

    private fun startNewRound(timestamp: Timestamp, accumulatedChanges: AccumulatedChanges): AccumulatedChanges {
        val state = accumulatedChanges.currentState
        val currentRoundNumber = state.currentRound()?.roundNumber
        val newEvent = when {
            currentRoundNumber == null -> RoundStarted(state.gameId, timestamp, RoundNumber(1))
            currentRoundNumber.value < state.maxNumberOfRounds.value -> RoundStarted(state.gameId, timestamp, currentRoundNumber.next())
            else -> throw IllegalStateException("Cannot start round since it would exceed ${state.maxNumberOfRounds.value}")
        }
        return accumulatedChanges + newEvent
    }

    private fun playHandAndEvaluateGameRules(cmd: PlayHand, accumulatedChanges: AccumulatedChanges): AccumulatedChanges {
        val state = accumulatedChanges.currentState
        val (timestamp, playerId, shapeOfHand) = cmd
        val gameId = state.gameId
        val round = state.currentRound() ?: throw IllegalStateException("Cannot play when round is not started")
        if (round is Round.WaitingForSecondHand && round.firstHand.playerId == cmd.playerId) {
            throw PlayerAlreadyPlayedInRound()
        }
        val roundNumber = round.roundNumber
        val changesAfterHandPlayed = accumulatedChanges + HandPlayed(gameId, timestamp, playerId, shapeOfHand, roundNumber)

        return when (val currentRound = accumulatedChanges.currentRound) {
            is Round.WaitingForFirstHand -> changesAfterHandPlayed
            is Round.WaitingForSecondHand -> {
                val firstHand = currentRound.firstHand
                val secondHand = Hand(playerId, shapeOfHand)

                val roundOutcomeEvent = when {
                    firstHand.shape == secondHand.shape -> RoundTied(gameId, timestamp, roundNumber)
                    firstHand.beats(secondHand) -> RoundWon(gameId, timestamp, roundNumber, firstHand.playerId)
                    else -> RoundWon(gameId, timestamp, roundNumber, secondHand.playerId)
                }

                val changesAfterRoundEnded = changesAfterHandPlayed + listOf(roundOutcomeEvent, RoundEnded(gameId, timestamp, roundNumber))
                when (val status = determineGameStatus(changesAfterRoundEnded)) {
                    GameStatus.NotEnded -> changesAfterRoundEnded
                    GameStatus.Tied -> changesAfterRoundEnded + GameTied(gameId, timestamp) + GameEnded(gameId, timestamp)
                    is GameStatus.Won -> changesAfterRoundEnded + GameWon(gameId, timestamp, status.winner) + GameEnded(gameId, timestamp)
                }
            }
            else -> throw IllegalStateException("Cannot play round when round is in state ${currentRound::class.simpleName}")
        }
    }

    private sealed interface GameStatus {
        data class Won(val winner: PlayerId) : GameStatus
        object Tied : GameStatus
        object NotEnded : GameStatus
    }

    private fun determineGameStatus(accumulatedChanges: AccumulatedChanges): GameStatus = when (val state = accumulatedChanges.currentState) {
        is BothPlayersJoined -> {
            val maxNumberOfRounds = state.maxNumberOfRounds.value
            val roundNumber = state.currentRound()!!.roundNumber.value

            val numberOfWinsPerPlayer = state.rounds
                .groupBy { round -> (round as? Round.Won)?.winner }
                .mapValues { (_, wonRounds) -> wonRounds.size }

            val numberOfWinsForPlayer1 = numberOfWinsPerPlayer[state.firstPlayer] ?: 0
            val numberOfWinsForPlayer2 = numberOfWinsPerPlayer[state.secondPlayer] ?: 0

            if (maxNumberOfRounds == roundNumber) {
                when {
                    numberOfWinsForPlayer1 > numberOfWinsForPlayer2 -> GameStatus.Won(state.firstPlayer)
                    numberOfWinsForPlayer2 > numberOfWinsForPlayer1 -> GameStatus.Won(state.secondPlayer)
                    else -> GameStatus.Tied
                }
            } else {
                val numberOfRoundsRequiredForMajorityWin = (maxNumberOfRounds / 2).inc()
                when {
                    numberOfWinsForPlayer1 == numberOfRoundsRequiredForMajorityWin -> GameStatus.Won(state.firstPlayer)
                    numberOfWinsForPlayer2 == numberOfRoundsRequiredForMajorityWin -> GameStatus.Won(state.secondPlayer)
                    else -> GameStatus.NotEnded
                }
            }
        }
        else -> throw IllegalStateException("Cannot determine game outcome when game is in state ${state::class.simpleName}")
    }

    private val AccumulatedChanges.currentRound: Round
        get() = when (val state = currentState) {
            is Started -> state.round
            is FirstPlayerJoined -> state.round
            is BothPlayersJoined -> state.rounds.last()
            else -> throw IllegalStateException("No round is started")
        }

    private fun AccumulatedChanges.isRoundOngoing(): Boolean = when (currentRound) {
        is Round.Tied -> false
        is Round.Won -> false
        is Round.WaitingForFirstHand -> true
        is Round.WaitingForSecondHand -> true
    }

    private fun CurrentGameState.currentRound(): Round? = when (this) {
        is Created -> null
        is Started -> round
        is FirstPlayerJoined -> round
        is BothPlayersJoined -> rounds.last()
        is Ended -> throw IllegalStateException("Cannot get round number when game is in state ${this::class.simpleName}")
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

private class AccumulatedChanges private constructor(private val evolvedState: EvolvedState, private val events: PersistentList<GameEvent>) : Sequence<GameEvent> {
    val currentState: CurrentGameState by lazy {
        evolvedState.translateToDomain()
    }

    override fun iterator(): Iterator<GameEvent> = events.iterator()
    fun evolve(e: GameEvent, vararg es: GameEvent) = AccumulatedChanges(
        sequenceOf(e, *es).evolve(evolvedState)!!, events.addAll(listOf(e, *es)),
    )

    operator fun plus(e: GameEvent) = evolve(e)
    operator fun plus(fn: (AccumulatedChanges) -> AccumulatedChanges): AccumulatedChanges = fn(this)
    operator fun plus(es: List<GameEvent>): AccumulatedChanges = es.fold(this) { state, e ->
        state + e
    }

    companion object {
        fun initializeFrom(evolvedState: EvolvedState) = AccumulatedChanges(evolvedState, persistentListOf())
    }
}

private sealed interface CurrentGameState {
    val gameId: GameId
    val maxNumberOfRounds: MaxNumberOfRounds

    data class Created(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds) : CurrentGameState
    data class Started(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val round: Round) : CurrentGameState
    data class FirstPlayerJoined(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId, val round: Round) : CurrentGameState
    data class BothPlayersJoined(override val gameId: GameId, override val maxNumberOfRounds: MaxNumberOfRounds, val firstPlayer: PlayerId, val secondPlayer: PlayerId, val rounds: PersistentList<Round> = persistentListOf()) : CurrentGameState
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
        EvolvedGameState.Created -> Created(gameId, maxNumberOfRounds)
        EvolvedGameState.Started -> Started(gameId, maxNumberOfRounds, rounds.first().toDomain())
        EvolvedGameState.FirstPlayerJoined -> FirstPlayerJoined(gameId, maxNumberOfRounds, firstPlayer!!, rounds.first().toDomain())
        EvolvedGameState.BothPlayersJoined -> BothPlayersJoined(gameId, maxNumberOfRounds, firstPlayer!!, secondPlayer!!, rounds.map { round ->
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
        Created, Started, FirstPlayerJoined, BothPlayersJoined, Ended
    }


    fun Sequence<GameEvent>.evolve(currentState: EvolvedState? = null): EvolvedState? = fold(currentState, ::evolve)

    fun evolve(currentState: EvolvedState?, e: GameEvent) = when (e) {
        is GameCreated -> EvolvedState(gameId = e.game, state = EvolvedGameState.Created, maxNumberOfRounds = e.maxNumberOfRounds)
        is GameStarted -> currentState!!.copy(state = EvolvedGameState.Started)
        is FirstPlayerJoinedGame -> currentState!!.copy(state = EvolvedGameState.FirstPlayerJoined, firstPlayer = e.player)
        is SecondPlayerJoinedGame -> currentState!!.copy(state = EvolvedGameState.BothPlayersJoined, secondPlayer = e.player)
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