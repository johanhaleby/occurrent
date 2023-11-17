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
import org.occurrent.example.domain.rps.multirounddecidermodel.GameLogic.initiateNewGame
import org.occurrent.example.domain.rps.multirounddecidermodel.GameLogic.handleFirstHandGesture
import org.occurrent.example.domain.rps.multirounddecidermodel.GameLogic.handleSecondHandGesture
import org.occurrent.example.domain.rps.multirounddecidermodel.GameStatus.*
import org.occurrent.example.domain.rps.multirounddecidermodel.GameStatus.Ended
import org.occurrent.example.domain.rps.multirounddecidermodel.HandGesture.*
import org.occurrent.example.domain.rps.multirounddecidermodel.InitiateNewGame.NumberOfRounds.*
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.*
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.WaitingForFirstHandGesture.WaitingForFirstPlayerToShowFirstHandGestureInGame
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.WaitingForFirstHandGesture.WaitingForPlayerToShowFirstHandGestureInRound
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.WaitingForSecondHandGesture.*
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.DecisionState.WaitingForSecondHandGesture.RoundOutcome.WonBy
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.evolveDecisionState
import org.occurrent.example.domain.rps.multirounddecidermodel.StateManagement.toDecisionState

val rps = decider<GameCommand, GameState?, GameEvent>(
    initialState = null,
    decide = { c, gameState ->
        val s = gameState.toDecisionState()
        when {
            c is InitiateNewGame && s == DoesNotExist -> initiateNewGame(c)
            c is ShowHandGesture && s is WaitingForFirstHandGesture -> handleFirstHandGesture(s, c)
            c is ShowHandGesture && s is WaitingForSecondHandGesture -> handleSecondHandGesture(c, s, gameState)
            else -> throw IllegalArgumentException("Cannot ${c::class.simpleName} when game is ${s::class.simpleName}")
        }
    },
    evolve = StateManagement::evolveDomainState,
    isTerminal = { s -> s?.status == Ended })


private object GameLogic {
    fun initiateNewGame(c: InitiateNewGame) = listOf(NewGameInitiated(c.gameId, c.timestamp, c.playerId, c.numberOfRounds.toInt()))

    fun handleFirstHandGesture(s: WaitingForFirstHandGesture, c: ShowHandGesture): List<GameEvent> {
        val events = mutableListOf<GameEvent>()
        val roundNumber = when (s) {
            is WaitingForFirstPlayerToShowFirstHandGestureInGame -> {
                events.add(GameStarted(c.gameId, c.timestamp))
                1
            }

            is WaitingForPlayerToShowFirstHandGestureInRound -> {
                require(s.playerId in setOf(s.firstPlayerId, s.secondPlayerId)) { "A third player cannot join the game" }
                s.lastRoundNumber.inc()
            }
        }
        return events + RoundStarted(c.gameId, c.timestamp, roundNumber, c.playerId) + HandGestureShown(c.gameId, c.timestamp, c.playerId, c.gesture)
    }

    fun handleSecondHandGesture(c: ShowHandGesture, s: WaitingForSecondHandGesture, gameState: GameState?): List<GameEvent> {
        val (gameId, timestamp, thisPlayerId, secondHandGesture) = c

        require(s.playerIdThatMadeTheFirstHandGestureInRound != thisPlayerId) {
            "Player ${s.playerIdThatMadeTheFirstHandGestureInRound} is not allowed to make another hand gesture"
        }

        if (s is WaitingForPlayerToMakeSecondHandGestureInRound) {
            require(thisPlayerId in setOf(s.firstPlayerId, s.secondPlayerId)) {
                throw IllegalArgumentException("A third player cannot join the game")
            }
        }

        val roundResultEvent = when {
            s.firstHandGestureInRound hasHandGesture secondHandGesture -> RoundTied(gameId, timestamp, s.roundNumber)
            s.firstHandGestureInRound beats secondHandGesture -> RoundWon(gameId, timestamp, s.roundNumber, s.playerIdThatMadeTheFirstHandGestureInRound)
            else -> RoundWon(gameId, timestamp, s.roundNumber, thisPlayerId)
        }

        val events = mutableListOf(HandGestureShown(gameId, timestamp, thisPlayerId, secondHandGesture), roundResultEvent)

        if (s.isLastRound()) {
            val stateAfterAllRoundsPlayed = evolveDecisionState(gameState, roundResultEvent) as AllRoundsPlayed
            val gameResultEvent = when (val gameResult = stateAfterAllRoundsPlayed.gameResult) {
                GameResult.Tied -> GameTied(gameId, timestamp)
                is GameResult.WonBy -> GameWon(gameId, timestamp, gameResult.playerId)
            }

            events.add(gameResultEvent)
            events.add(GameEnded(gameId, timestamp))
        }

        return events
    }

    private val AllRoundsPlayed.gameResult
        get() : GameResult {
            data class GameStats(val numberOfTiedGames: Int = 0, val wins: MutableMap<PlayerId, Int> = mutableMapOf())

            val (numberOfTiedGames, wins) = roundOutcomes.fold(GameStats()) { gameStats, roundOutcome ->
                when (roundOutcome) {
                    RoundOutcome.Tied -> gameStats.copy(numberOfTiedGames = gameStats.numberOfTiedGames.inc())
                    is WonBy -> {
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

    private infix fun PlayerHandGesture.beats(other: HandGesture): Boolean = this.handGesture.beats(other)
    private infix fun PlayerHandGesture.hasHandGesture(other: HandGesture): Boolean = this.handGesture == other

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
        sealed interface WaitingForFirstHandGesture : DecisionState {
            val playerId: PlayerId

            data class WaitingForFirstPlayerToShowFirstHandGestureInGame(override val playerId: PlayerId) : WaitingForFirstHandGesture

            data class WaitingForPlayerToShowFirstHandGestureInRound(override val playerId: PlayerId, val lastRoundNumber: RoundNumber, val firstPlayerId: PlayerId, val secondPlayerId: PlayerId) :
                WaitingForFirstHandGesture
        }

        sealed interface WaitingForSecondHandGesture : DecisionState {
            val firstHandGestureInRound: PlayerHandGesture
            val roundNumber: RoundNumber
            val totalNumberOfRounds: NumberOfRounds

            sealed interface RoundOutcome {
                data object Tied : RoundOutcome
                data class WonBy(val winnerId: PlayerId) : RoundOutcome
            }


            val playerIdThatMadeTheFirstHandGestureInRound: PlayerId get() = firstHandGestureInRound.playerId
            fun isLastRound() = totalNumberOfRounds == roundNumber

            data class WaitingForSecondPlayerToMakeFirstHandGestureInGame(
                override val firstHandGestureInRound: PlayerHandGesture,
                override val roundNumber: RoundNumber,
                override val totalNumberOfRounds: NumberOfRounds,
            ) : WaitingForSecondHandGesture

            data class WaitingForPlayerToMakeSecondHandGestureInRound(
                override val firstHandGestureInRound: PlayerHandGesture,
                override val roundNumber: RoundNumber,
                override val totalNumberOfRounds: NumberOfRounds,
                val firstPlayerId: PlayerId,
                val secondPlayerId: PlayerId,
            ) : WaitingForSecondHandGesture
        }

        data object WaitingForNextRoundToStart : DecisionState
        data class AllRoundsPlayed(val firstPlayerId: PlayerId, val secondPlayerId: PlayerId, val roundOutcomes: List<RoundOutcome>) : DecisionState
        data object Ended : DecisionState

    }

    fun GameState?.toDecisionState(): DecisionState = when (this) {
        null -> DoesNotExist
        else -> when (status) {
            WaitingForFirstHandGestureInGame -> WaitingForFirstPlayerToShowFirstHandGestureInGame(initiatedBy)
            WaitingForSecondHandGestureInGame -> {
                val round = rounds.last()
                val firstHandGestureInGame = round.handGestures.first()
                WaitingForSecondPlayerToMakeFirstHandGestureInGame(firstHandGestureInGame, round.roundNumber, totalNumberOfRounds)
            }

            Ongoing -> {
                val currentRound = rounds.last()
                val isLastRound = rounds.size == totalNumberOfRounds
                val (roundNumber, startedBy, handGestures, state) = currentRound

                when (state) {
                    RoundState.Ongoing ->
                        if (handGestures.isEmpty()) {
                            WaitingForPlayerToShowFirstHandGestureInRound(startedBy, roundNumber, firstPlayerId!!, secondPlayerId!!)
                        } else {
                            WaitingForPlayerToMakeSecondHandGestureInRound(handGestures[0], roundNumber, totalNumberOfRounds, firstPlayerId!!, secondPlayerId!!)
                        }

                    RoundState.Ended.Tied, is RoundState.Ended.WonBy -> if (isLastRound) {
                        val roundOutcomes = rounds.mapNotNull { round ->
                            when (round.state) {
                                RoundState.Ended.Tied -> RoundOutcome.Tied
                                is RoundState.Ended.WonBy -> WonBy(round.state.playerId)
                                RoundState.Ongoing -> null
                            }
                        }
                        AllRoundsPlayed(firstPlayerId!!, secondPlayerId!!, roundOutcomes)
                    } else {
                        WaitingForNextRoundToStart
                    }
                }
            }

            Ended -> DecisionState.Ended
        }
    }

    fun evolveDecisionState(s: GameState?, e: GameEvent): DecisionState = evolveDomainState(s, e).toDecisionState()

    fun evolveDomainState(s: GameState?, e: GameEvent): GameState {
        fun updateLastRound(fn: (Round) -> Round): GameState {
            val thisRound = s!!.rounds.last()
            val updatedRound = fn(thisRound)
            return s.copy(rounds = s.rounds.dropLast(1) + updatedRound)
        }

        return when (e) {
            is NewGameInitiated -> GameState(e.gameId, initiatedBy = e.playerId, firstPlayerId = null, secondPlayerId = null, e.numberOfRounds, rounds = emptyList(), status = WaitingForFirstHandGestureInGame)
            is GameStarted, is GameWon, is GameTied -> s!!
            is GameEnded -> s!!.copy(status = Ended)
            is RoundStarted -> s!!.copy(rounds = s.rounds + Round(e.roundNumber, startedBy = e.startedBy, handGestures = emptyList(), state = RoundState.Ongoing))
            is HandGestureShown -> {
                val gameWithNewHandGesture = updateLastRound { round ->
                    round.copy(handGestures = round.handGestures + PlayerHandGesture(e.playerId, e.gesture))
                }
                val handGesturesInFirstRound = gameWithNewHandGesture.rounds.first().handGestures
                when (gameWithNewHandGesture.status) {
                    WaitingForFirstHandGestureInGame -> gameWithNewHandGesture.copy(
                        firstPlayerId = handGesturesInFirstRound.first().playerId,
                        status = WaitingForSecondHandGestureInGame
                    )

                    WaitingForSecondHandGestureInGame -> gameWithNewHandGesture.copy(
                        secondPlayerId = handGesturesInFirstRound[1].playerId,
                        status = Ongoing
                    )

                    Ongoing, Ended -> gameWithNewHandGesture
                }
            }

            is RoundTied -> updateLastRound { round ->
                round.copy(state = RoundState.Ended.Tied)
            }

            is RoundWon -> updateLastRound { round ->
                round.copy(state = RoundState.Ended.WonBy(e.playerId))
            }
        }
    }
}