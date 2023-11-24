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

package org.occurrent.example.domain.mastermind

import java.time.ZonedDateTime
import java.util.*


@JvmInline
value class GameId(val value: UUID) {
    companion object {
        fun random() = GameId(UUID.randomUUID())
    }
}

@JvmInline
value class CodeMakerId(val value: UUID) {
    companion object {
        fun random() = CodeMakerId(UUID.randomUUID())
    }
}

@JvmInline
value class CodebreakerId(val value: UUID) {
    companion object {
        fun random() = CodebreakerId(UUID.randomUUID())
    }
}

typealias Timestamp = ZonedDateTime

enum class CodePeg {
    Red, Green, Blue, Yellow, Orange, Purple,
}

enum class KeyPeg {
    Black, White
}


typealias MaxNumberOfGuesses = Int

sealed interface KeyPegHoleStatus {
    data object Empty : KeyPegHoleStatus
    data class Occupied(val keyPeg: KeyPeg) : KeyPegHoleStatus
}

data class Feedback(val hole1: KeyPegHoleStatus, val hole2: KeyPegHoleStatus, val hole3: KeyPegHoleStatus, val hole4: KeyPegHoleStatus)
data class SecretCode(val peg1: CodePeg, val peg2: CodePeg, val peg3: CodePeg, val peg4: CodePeg)
data class Guess(val peg1: CodePeg, val peg2: CodePeg, val peg3: CodePeg, val peg4: CodePeg)

sealed class MasterMindException : RuntimeException() {

    sealed class StartGameException : MasterMindException() {
        data class GameAlreadyStarted(val gameId: GameId) : StartGameException()
        data class GameAlreadyEnded(val gameId: GameId) : StartGameException()
    }

    sealed class MakeGuessException : MasterMindException() {
        data class GameNotStarted(val gameId: GameId) : StartGameException()
        data class GameAlreadyEnded(val gameId: GameId) : StartGameException()
        data class GameAlreadyHasAnotherCodeBreaker(val gameId: GameId) : StartGameException()
    }
}

