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

import GuessMade
import org.occurrent.dsl.decider.component1
import org.occurrent.dsl.decider.decide
import org.occurrent.example.domain.mastermind.KeyPeg.Black
import org.occurrent.example.domain.mastermind.KeyPeg.White
import org.occurrent.example.domain.mastermind.MasterMindState.Ended
import org.occurrent.example.domain.mastermind.MasterMindState.Started
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.component3
import kotlin.collections.component4


fun main() {
    val gameId = GameId.random()
    val codebreakerId = CodebreakerId.random()
    val codeMakerId = CodeMakerId.random()
    val secretCode = randomSecretCode()
    val totalNumberOfGuesses = TotalNumberOfGuesses.SIX

    var (state) = mastermind.decide(events = emptyList(), command = StartGame(gameId, Timestamp.now(), codebreakerId, codeMakerId, secretCode, totalNumberOfGuesses))

    val inputExample = "Enter guess in this format: RGBP"
    println("Welcome to MasterMind, you have ${totalNumberOfGuesses.name.lowercase()} guesses, use them wisely. $inputExample")

    while (state !is Ended) {
        print("> ")
        val input = readlnOrNull()
        if (input == null) {
            println("Bad input, try again. $inputExample")
            continue
        }
        val codePegs = input.trim().uppercase().toCharArray().map { char ->
            when (char) {
                'R' -> CodePeg.Red
                'G' -> CodePeg.Green
                'B' -> CodePeg.Blue
                'Y' -> CodePeg.Yellow
                'O' -> CodePeg.Orange
                'P' -> CodePeg.Purple
                else -> null
            }
        }

        val codePegsNotNull = codePegs.filterNotNull()

        if (codePegsNotNull.size != codePegs.size || codePegs.size != 4) {
            println("Bad input \"$input\", try again. $inputExample")
            continue
        }
        val (guess1, guess2, guess3, guess4) = codePegsNotNull
        val guess = Guess(guess1, guess2, guess3, guess4)
        val decision = mastermind.decide(state = state!!, command = MakeGuess(gameId, Timestamp.now(), codebreakerId, guess))
        state = decision.state
        if (state is Started) {
            val event = decision.events.last { it is GuessMade } as GuessMade
            println("\"$input\" was wrong, feedback: ${event.feedback.describe()}")
        }
    }

    println(state)
}

private fun randomSecretCode(): SecretCode {
    // We do this seemingly complex piece of code to allow for duplicate code pegs
    val (codePeg1, codePeg2, codePeg3, codePeg4) =
        generateSequence { sequenceOf(*CodePeg.entries.toTypedArray()) }
            .flatMap { it }
            .take(100)
            .shuffled()
            .take(4)
            .toList()
    return SecretCode(codePeg1, codePeg2, codePeg3, codePeg4)
}

private fun Feedback.describe(): String {
    fun KeyPegHoleStatus.describe() = when (this) {
        KeyPegHoleStatus.Empty -> "X"
        is KeyPegHoleStatus.Occupied -> when (this.keyPeg) {
            Black -> "B"
            White -> "W"
        }
    }
    val (hole1, hole2, hole3, hole4) = this
    return sequenceOf(hole1, hole2, hole3, hole4).joinToString("") { it.describe() }
}