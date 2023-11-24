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

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.dsl.decider.component1
import org.occurrent.dsl.decider.component2
import org.occurrent.dsl.decider.decide
import org.occurrent.example.domain.mastermind.CodePeg.*
import org.occurrent.example.domain.mastermind.TotalNumberOfGuesses.THREE


@DisplayName("MasterMind")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class MasterMindExample {


    @Test
    fun example() {
        // Given
        val gameId = GameId.random()
        val timestamp = Timestamp.now()
        val codebreakerId = CodebreakerId.random()
        val codeMakerId = CodeMakerId.random()
        val secretCode = SecretCode(Blue, Green, Green, Orange)


        // When
        val (state, events) = mastermind.decide(
            events = emptyList(),
            commands = listOf(
                StartGame(gameId, timestamp, codebreakerId, codeMakerId, secretCode, totalNumberOfGuesses = THREE),
                MakeGuess(gameId, timestamp, codebreakerId, Guess(Red, Green, Blue, Purple)),
                MakeGuess(gameId, timestamp, codebreakerId, Guess(Yellow, Green, Blue, Green)),
                MakeGuess(gameId, timestamp, codebreakerId, Guess(Blue, Green, Green, Orange)),
            )
        )

        // Then
        println("### state: $state")
        println("### events:\n" + events.joinToString("\n"))
    }
}