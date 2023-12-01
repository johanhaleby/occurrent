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

package org.occurrent.example.domain.rps.multirounddecidermodel

import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.rps.multirounddecidermodel.GameLogic.handleHandGesture
import org.occurrent.example.domain.rps.multirounddecidermodel.GameLogic.initiateNewGame
import org.occurrent.example.domain.rps.multirounddecidermodel.GameStatus.*
import org.occurrent.example.domain.rps.multirounddecidermodel.GameStatus.Ended
import org.occurrent.example.domain.rps.multirounddecidermodel.HandGesture.*
import org.occurrent.example.domain.rps.multirounddecidermodel.InitiateNewGame.NumberOfRounds.*
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.*
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.AllRoundsPlayed.RoundOutcome
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.WaitingForHandGesture.WaitingForFirstHandGestureInRound
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.WaitingForHandGesture.WaitingForSecondHandGestureInRound
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.evolveToDecisionState
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.toDecisionState

val rps = decider<GameCommand, GameState?, GameEvent>(
    initialState = null,
    decide = { c, gameState ->
        val decisionState = gameState.toDecisionState()
        when {
            c is InitiateNewGame && decisionState == DoesNotExist -> initiateNewGame(c)
            c is ShowHandGesture && decisionState is WaitingForHandGesture -> handleHandGesture(gameState, decisionState, c)
            else -> throw IllegalArgumentException("Cannot ${c::class.simpleName} when game is ${decisionState::class.simpleName}")
        }
    },
    evolve = StateManagement::evolveDomainState,
    isTerminal = { s -> s?.status == Ended })


private object GameLogic {
    fun initiateNewGame(c: InitiateNewGame) = listOf(NewGameInitiated(c.gameId, c.timestamp, c.playerId, c.numberOfRounds.toInt()))

    fun handleHandGesture(domainState: GameState?, decisionState: WaitingForHandGesture, c: ShowHandGesture): List<GameEvent> =
        when (decisionState) {
            is WaitingForHandGesture.WaitingForFirstHandGestureInGame -> listOf(
                GameStarted(c.gameId, c.timestamp),
                RoundStarted(c.gameId, c.timestamp, roundNumber = decisionState.roundNumber, c.playerId),
                HandGestureShown(c.gameId, c.timestamp, c.playerId, c.gesture),
            )

            is WaitingForHandGesture.WaitingForSecondHandGestureInGame -> playRound(c.gameId, c.timestamp, decisionState.roundNumber, decisionState.firstHandGestureInGame, PlayerHandGesture(c.playerId, c.gesture))
            is WaitingForFirstHandGestureInRound -> {
                require(decisionState.playerId in setOf(decisionState.firstPlayerId, decisionState.secondPlayerId)) { "A third player cannot join the game" }
                listOf(
                    RoundStarted(c.gameId, c.timestamp, decisionState.lastRoundNumber.inc(), c.playerId),
                    HandGestureShown(c.gameId, c.timestamp, c.playerId, c.gesture),
                )
            }

            is WaitingForSecondHandGestureInRound -> {
                val (gameId, timestamp, thisPlayerId, thisHandGesture) = c
                val events = mutableListOf<GameEvent>()
                val secondHandGesture = PlayerHandGesture(thisPlayerId, thisHandGesture)
                events.addAll(playRound(gameId, timestamp, decisionState.roundNumber, decisionState.firstHandGestureInRound, secondHandGesture))

                if (decisionState.isLastRound()) {
                    val stateAfterAllRoundsPlayed = evolveToDecisionState(domainState, events) as? AllRoundsPlayed ?: throw IllegalStateException("Expected state ${AllRoundsPlayed::class.simpleName}")
                    val gameResultEvent = when (val gameResult = stateAfterAllRoundsPlayed.gameResult) {
                        GameResult.Tied -> GameTied(gameId, timestamp)
                        is GameResult.WonBy -> GameWon(gameId, timestamp, gameResult.playerId)
                    }

                    events.add(gameResultEvent)
                    events.add(GameEnded(gameId, timestamp))
                }
                events
            }
        }

    private fun playRound(gameId: GameId, timestamp: Timestamp, roundNumber: RoundNumber, firstHandGesture: PlayerHandGesture, secondHandGesture: PlayerHandGesture): List<GameEvent> {
        val handGestureShown = HandGestureShown(gameId, timestamp, secondHandGesture.playerId, secondHandGesture.handGesture)
        val roundResultEvent = when {
            firstHandGesture hasSameHandGesture secondHandGesture -> RoundTied(gameId, timestamp, roundNumber)
            firstHandGesture beats secondHandGesture -> RoundWon(gameId, timestamp, roundNumber, firstHandGesture.playerId)
            else -> RoundWon(gameId, timestamp, roundNumber, secondHandGesture.playerId)
        }
        return listOf(handGestureShown, roundResultEvent)
    }

    private val AllRoundsPlayed.gameResult
        get() : GameResult {
            data class GameStats(val numberOfTiedGames: Int = 0, val wins: MutableMap<PlayerId, Int> = mutableMapOf())

            val (numberOfTiedGames, wins) = roundOutcomes.fold(GameStats()) { gameStats, roundOutcome ->
                when (roundOutcome) {
                    RoundOutcome.Tied -> gameStats.copy(numberOfTiedGames = gameStats.numberOfTiedGames.inc())
                    is RoundOutcome.WonBy -> {
                        gameStats.wins.compute(roundOutcome.winnerId) { _, numberOfWins ->
                            numberOfWins?.inc() ?: 1
                        }
                        gameStats.copy(wins = gameStats.wins)
                    }

                }
            }

            return when {
                numberOfTiedGames == roundOutcomes.size -> GameResult.Tied
                wins[firstPlayerId] == wins[secondPlayerId] -> GameResult.Tied
                else -> {
                    val winnerId = wins.maxBy { (_, numberOfWins) -> numberOfWins }.key
                    GameResult.WonBy(winnerId)
                }
            }
        }

    private fun InitiateNewGame.NumberOfRounds.toInt(): NumberOfRounds = when (this) {
        ONE -> 1
        THREE -> 3
        FIVE -> 5
    }

    private infix fun PlayerHandGesture.beats(other: PlayerHandGesture): Boolean = this.handGesture.beats(other.handGesture)
    private infix fun PlayerHandGesture.hasSameHandGesture(other: PlayerHandGesture): Boolean = this.handGesture == other.handGesture

    private infix fun HandGesture.beats(other: HandGesture): Boolean = when {
        this == ROCK && other == SCISSORS -> true
        this == SCISSORS && other == PAPER -> true
        this == PAPER && other == ROCK -> true
        else -> false
    }

    private sealed interface GameResult {
        data object Tied : GameResult
        data class WonBy(val playerId: PlayerId) : GameResult
    }
}

private object StateManagement {

    sealed interface DecisionState {
        data object DoesNotExist : DecisionState
        sealed interface WaitingForHandGesture : DecisionState {

            data class WaitingForFirstHandGestureInGame(val gameInitiatedBy: PlayerId) : WaitingForHandGesture {
                val roundNumber: RoundNumber get() = 1
            }

            data class WaitingForSecondHandGestureInGame(val firstHandGestureInGame: PlayerHandGesture) : WaitingForHandGesture {
                val roundNumber: RoundNumber get() = 1
            }

            data class WaitingForFirstHandGestureInRound(val playerId: PlayerId, val lastRoundNumber: RoundNumber, val firstPlayerId: PlayerId, val secondPlayerId: PlayerId) : WaitingForHandGesture
            data class WaitingForSecondHandGestureInRound(
                val firstHandGestureInRound: PlayerHandGesture,
                val roundNumber: RoundNumber,
                val totalNumberOfRounds: NumberOfRounds,
                val firstPlayerId: PlayerId,
                val secondPlayerId: PlayerId,
            ) : WaitingForHandGesture {
                fun isLastRound() = roundNumber == totalNumberOfRounds
            }
        }

        data class AllRoundsPlayed(val firstPlayerId: PlayerId, val secondPlayerId: PlayerId, val roundOutcomes: List<RoundOutcome>) : DecisionState {
            sealed interface RoundOutcome {
                data object Tied : RoundOutcome
                data class WonBy(val winnerId: PlayerId) : RoundOutcome
            }
        }

        data object Ended : DecisionState
    }

    fun GameState?.toDecisionState(): DecisionState = when (this) {
        null -> DoesNotExist
        else -> when (status) {
            WaitingForFirstHandGestureInGame -> WaitingForHandGesture.WaitingForFirstHandGestureInGame(initiatedBy)
            WaitingForSecondHandGestureInGame -> {
                val round = rounds.last()
                val firstHandGestureInGame = round.handGestures.first()
                WaitingForHandGesture.WaitingForSecondHandGestureInGame(firstHandGestureInGame)
            }

            Ongoing -> {
                val currentRound = rounds.last()
                val isLastRound = rounds.size == totalNumberOfRounds
                val (roundNumber, startedBy, handGestures, state) = currentRound

                when (state) {
                    RoundState.Started -> WaitingForSecondHandGestureInRound(handGestures[0], roundNumber, totalNumberOfRounds, firstPlayerId!!, secondPlayerId!!)
                    is RoundState.Ended -> if (isLastRound) {
                        val roundOutcomes = rounds.mapNotNull { round ->
                            when (round.state) {
                                RoundState.Ended.Tied -> RoundOutcome.Tied
                                is RoundState.Ended.WonBy -> RoundOutcome.WonBy(round.state.playerId)
                                RoundState.Started -> null
                            }
                        }
                        AllRoundsPlayed(firstPlayerId!!, secondPlayerId!!, roundOutcomes)
                    } else {
                        WaitingForFirstHandGestureInRound(startedBy, roundNumber, firstPlayerId!!, secondPlayerId!!)
                    }
                }
            }

            Ended -> DecisionState.Ended
        }
    }

    fun evolveToDecisionState(s: GameState?, es: List<GameEvent>): DecisionState = es.fold(s, ::evolveDomainState).toDecisionState()

    fun evolveDomainState(s: GameState?, e: GameEvent): GameState {
        fun updateRoundNumber(roundNumber: RoundNumber, fn: (Round) -> Round): GameState {
            val thisRound = s!!.rounds.first { it.roundNumber == roundNumber }
            val updatedRound = fn(thisRound)
            val newRounds = s.rounds.map { round -> if (round.roundNumber == roundNumber) updatedRound else round }
            return s.copy(rounds = newRounds)
        }

        return when (e) {
            is NewGameInitiated -> GameState(e.gameId, initiatedBy = e.playerId, firstPlayerId = null, secondPlayerId = null, e.numberOfRounds, rounds = emptyList(), status = WaitingForFirstHandGestureInGame)
            is GameStarted, is GameWon, is GameTied -> s!!
            is GameEnded -> s!!.copy(status = Ended)
            is RoundStarted -> s!!.copy(rounds = s.rounds + Round(e.roundNumber, startedBy = e.startedBy, handGestures = emptyList(), state = RoundState.Started))
            is HandGestureShown -> {
                val roundNumber = s!!.rounds.last().roundNumber
                val gameWithNewHandGesture = updateRoundNumber(roundNumber) { round ->
                    round.copy(handGestures = round.handGestures + PlayerHandGesture(e.playerId, e.gesture))
                }

                when (gameWithNewHandGesture.status) {
                    WaitingForFirstHandGestureInGame -> gameWithNewHandGesture.copy(
                        firstPlayerId = e.playerId,
                        status = WaitingForSecondHandGestureInGame
                    )

                    WaitingForSecondHandGestureInGame -> gameWithNewHandGesture.copy(
                        secondPlayerId = e.playerId,
                        status = Ongoing
                    )

                    Ongoing, Ended -> gameWithNewHandGesture
                }
            }

            is RoundTied -> updateRoundNumber(e.roundNumber) { round ->
                round.copy(state = RoundState.Ended.Tied)
            }

            is RoundWon -> updateRoundNumber(e.roundNumber) { round ->
                round.copy(state = RoundState.Ended.WonBy(e.playerId))
            }
        }
    }
}