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


import java.time.ZonedDateTime
import java.util.*

@JvmInline
value class PlayerId(val value: UUID)

@JvmInline
value class GameId(val value: UUID)

@JvmInline
value class GameCreatorId(val value: UUID)

@JvmInline
value class Timestamp(val value: ZonedDateTime)

enum class Move {
    ROCK, PAPER, SCISSORS
}

@JvmInline
value class NumberOfRounds private constructor(val value: Int) {

    companion object {
        operator fun invoke(value: Int): NumberOfRounds {
            require(value in 1..5) {
                "Number of rounds must be between 1 and 5"
            }
            return NumberOfRounds(value)
        }

        internal fun unsafe(value: Int) = NumberOfRounds(value)
    }
}

@JvmInline
value class RoundNumber private constructor(val value: Int) {

    companion object {
        operator fun invoke(value: Int): RoundNumber {
            require(value > 0) {
                "${RoundNumber::class.simpleName} must be greater than 0"
            }
            return RoundNumber(value)
        }

        internal fun unsafe(value: Int) = RoundNumber(value)
    }
}

// Commands
data class CreateGameCommand(val gameId: GameId, val timestamp: Timestamp, val creator: GameCreatorId, val numberOfRounds: NumberOfRounds)
data class MakeMoveCommand(val timestamp: Timestamp, val playerId: PlayerId, val move: Move)

class GameCannotBeCreatedMoreThanOnce : IllegalArgumentException()
class GameDoesNotExist : IllegalArgumentException()
class CannotMakeMoveBecauseGameEnded : IllegalArgumentException()
class CannotJoinTheGameTwice : IllegalArgumentException()