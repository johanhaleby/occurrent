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
value class PlayerId(val value: UUID) {
    companion object {
        fun random() = PlayerId(UUID.randomUUID())
    }
}

@JvmInline
value class GameId(val value: UUID) {
    companion object {
        fun random() = GameId(UUID.randomUUID())
    }
}

@JvmInline
value class GameCreatorId(val value: UUID) {
    companion object {
        fun random() = GameCreatorId(UUID.randomUUID())
    }
}


@JvmInline
value class Timestamp(val value: ZonedDateTime) {
    companion object {
        fun now() = Timestamp(ZonedDateTime.now())
    }
}

enum class Shape {
    ROCK, PAPER, SCISSORS
}

@JvmInline
value class MaxNumberOfRounds private constructor(val value: Int) {

    companion object {
        operator fun invoke(value: Int): MaxNumberOfRounds {
            require(value in 1..5 && value % 2 == 1) {
                "Number of rounds can only be 1, 3 or 5"
            }
            return MaxNumberOfRounds(value)
        }

        internal fun unsafe(value: Int) = MaxNumberOfRounds(value)
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

// TODO remove commands
// Commands
sealed interface Command
data class CreateGame(val gameId: GameId, val timestamp: Timestamp, val creator: GameCreatorId, val maxNumberOfRounds: MaxNumberOfRounds) : Command
data class PlayHand(val timestamp: Timestamp, val playerId: PlayerId, val shape: Shape) : Command

class GameCannotBeCreatedMoreThanOnce : IllegalArgumentException()
class GameDoesNotExist : IllegalArgumentException()
class CannotPlayHandBecauseGameEnded : IllegalArgumentException()
class CannotJoinTheGameTwice : IllegalArgumentException()
class GameAlreadyHasTwoPlayers : IllegalArgumentException()
class PlayerAlreadyPlayedInRound : IllegalArgumentException()