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

sealed interface GameCommand {
    val gameId: GameId
    val timestamp: Timestamp

}

data class InitiateNewGame(override val gameId: GameId, override val timestamp: Timestamp, val playerId: PlayerId, val numberOfRounds: NumberOfRounds) : GameCommand {
    enum class NumberOfRounds {
        ONE, THREE, FIVE
    }
}

data class ShowHandGesture(override val gameId: GameId, override val timestamp: Timestamp, val playerId: PlayerId, val gesture: HandGesture) : GameCommand