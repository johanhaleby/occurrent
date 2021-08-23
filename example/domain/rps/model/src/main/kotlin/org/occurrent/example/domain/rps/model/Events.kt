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

sealed interface GameEvent {
    val game: GameId
    val timestamp: Timestamp
}

data class FirstPlayerJoinedGame(override val game: GameId, override val timestamp: Timestamp, val player: PlayerId) : GameEvent
data class SecondPlayerJoinedGame(override val game: GameId, override val timestamp: Timestamp, val player: PlayerId) : GameEvent

data class GameCreated(override val game: GameId, override val timestamp: Timestamp, val createdBy: GameCreatorId, val maxNumberOfRounds: MaxNumberOfRounds) : GameEvent
data class GameStarted(override val game: GameId, override val timestamp: Timestamp, val startedBy: PlayerId) : GameEvent
data class GameEnded(override val game: GameId, override val timestamp: Timestamp) : GameEvent
data class GameTied(override val game: GameId, override val timestamp: Timestamp) : GameEvent
data class GameWon(override val game: GameId, override val timestamp: Timestamp, val winner: PlayerId) : GameEvent
data class HandPlayed(override val game: GameId, override val timestamp: Timestamp, val player: PlayerId, val shape: Shape) : GameEvent
data class RoundStarted(override val game: GameId, override val timestamp: Timestamp, val roundNumber: RoundNumber) : GameEvent
data class RoundEnded(override val game: GameId, override val timestamp: Timestamp, val roundNumber: RoundNumber) : GameEvent
data class RoundWon(override val game: GameId, override val timestamp: Timestamp, val roundNumber: RoundNumber, val winner: PlayerId) : GameEvent
data class RoundTied(override val game: GameId, override val timestamp: Timestamp, val roundNumber: RoundNumber) : GameEvent