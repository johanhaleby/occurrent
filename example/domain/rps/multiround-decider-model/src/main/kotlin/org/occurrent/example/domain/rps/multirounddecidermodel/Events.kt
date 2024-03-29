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


sealed interface GameEvent {
    val gameId: GameId
    val timestamp: Timestamp
}

sealed interface RoundEvent : GameEvent {
    val roundNumber: RoundNumber
}

data class NewGameInitiated(override val gameId: GameId, override val timestamp: Timestamp, val playerId: PlayerId, val numberOfRounds : NumberOfRounds) : GameEvent
data class RoundStarted(override val gameId: GameId, override val timestamp: Timestamp, override val roundNumber: RoundNumber, val startedBy: PlayerId) : RoundEvent
data class RoundWon(override val gameId: GameId, override val timestamp: Timestamp, override val roundNumber: RoundNumber, val playerId: PlayerId) : RoundEvent
data class RoundTied(override val gameId: GameId, override val timestamp: Timestamp, override val roundNumber: RoundNumber) : RoundEvent
data class GameStarted(override val gameId: GameId, override val timestamp: Timestamp) : GameEvent
data class GameEnded(override val gameId: GameId, override val timestamp: Timestamp) : GameEvent
data class GameTied(override val gameId: GameId, override val timestamp: Timestamp) : GameEvent
data class GameWon(override val gameId: GameId, override val timestamp: Timestamp, val winner: PlayerId) : GameEvent
data class HandGestureShown(override val gameId: GameId, override val timestamp: Timestamp, val playerId: PlayerId, val gesture: HandGesture) : GameEvent