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

package org.occurrent.example.domain.rps.pragmatic.model


import java.time.ZoneOffset
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
        fun now() = Timestamp(ZonedDateTime.now(ZoneOffset.UTC))
        fun of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int) = Timestamp(ZonedDateTime.of(year, month, day, hour, minute, second, 0, ZoneOffset.UTC))
    }
}

enum class HandShape {
    ROCK, PAPER, SCISSORS;

    companion object {
        fun random() = values().random()
    }
}

class GameCannotBeCreatedMoreThanOnce : IllegalArgumentException()
class GameDoesNotExist : IllegalArgumentException()

class TooLateToJoinGame : IllegalArgumentException()
class CannotJoinTheGameTwice : IllegalArgumentException()
class GameAlreadyHasTwoPlayers : IllegalArgumentException()

class CannotPlayHandBecauseGameEnded : IllegalArgumentException()
class CannotPlayHandBecauseWaitingForBothPlayersToBeReady : IllegalArgumentException()
class CannotPlayTheSameGameTwice : IllegalArgumentException()
