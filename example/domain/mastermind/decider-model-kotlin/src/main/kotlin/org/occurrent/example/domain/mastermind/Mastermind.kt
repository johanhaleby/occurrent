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

import GameLost
import GameStarted
import GameWon
import GuessMade
import MasterMindEvent
import org.occurrent.dsl.decider.decider
import org.occurrent.example.domain.mastermind.DetermineFeedback.determineFeedback
import org.occurrent.example.domain.mastermind.GameResult.LOST
import org.occurrent.example.domain.mastermind.GameResult.WON
import org.occurrent.example.domain.mastermind.KeyPeg.Black
import org.occurrent.example.domain.mastermind.KeyPeg.White
import org.occurrent.example.domain.mastermind.MasterMindException.MakeGuessException.GameAlreadyHasAnotherCodeBreaker
import org.occurrent.example.domain.mastermind.MasterMindException.MakeGuessException.GameNotStarted
import org.occurrent.example.domain.mastermind.MasterMindException.StartGameException.GameAlreadyStarted
import org.occurrent.example.domain.mastermind.MasterMindState.*
import org.occurrent.example.domain.mastermind.MasterMindException.MakeGuessException.GameAlreadyEnded as CannotMakeGuessBecauseGameHasAlreadyEnded
import org.occurrent.example.domain.mastermind.MasterMindException.StartGameException.GameAlreadyEnded as CannotStartGameThatHasAlreadyEnded

private val evolve: (MasterMindState, MasterMindEvent) -> MasterMindState = { s, e ->
    when (e) {
        is GameStarted -> Started(e.gameId, e.timestamp, e.codeBreakerId, e.codeMakerId, e.secretCode, currentNumberOfGuesses = 0, e.maxNumberOfGuesses)
        is GuessMade -> (s as Started).copy(currentNumberOfGuesses = s.currentNumberOfGuesses.inc())
        is GameWon -> Ended(WON)
        is GameLost -> Ended(LOST)
    }
}

val mastermind = decider<MasterMindCommand, MasterMindState, MasterMindEvent>(
    initialState = NotStarted,
    decide = { c, s ->
        when (c) {
            is StartGame -> when (s) {
                is NotStarted -> listOf(GameStarted(c.gameId, c.timestamp, c.codeBreakerId, c.codeMakerId, c.secretCode, c.totalNumberOfGuesses.toInt()))
                is Started -> throw GameAlreadyStarted(c.gameId)
                is Ended -> throw CannotStartGameThatHasAlreadyEnded(c.gameId)
            }

            is MakeGuess -> when (s) {
                is NotStarted -> throw GameNotStarted(c.gameId)
                is Ended -> throw CannotMakeGuessBecauseGameHasAlreadyEnded(c.gameId)
                is Started -> {
                    if (c.codeBreakerId != s.codebreakerId) throw GameAlreadyHasAnotherCodeBreaker(c.gameId)
                    val feedback = determineFeedback(s.secretCode, c.guess)
                    val guessMade = GuessMade(c.gameId, c.timestamp, c.guess, feedback)
                    val events = mutableListOf<MasterMindEvent>(guessMade)
                    when {
                        feedback.isSecretCodeGuessed() -> events.add(GameWon(c.gameId, c.timestamp))
                        (evolve(s, guessMade) as Started).currentNumberOfGuesses == s.maxNumberOfGuesses -> events.add(GameLost(c.gameId, c.timestamp))
                    }
                    events
                }
            }
        }
    },
    evolve = evolve
)

private fun TotalNumberOfGuesses.toInt() = when (this) {
    TotalNumberOfGuesses.THREE -> 3
    TotalNumberOfGuesses.SIX -> 6
    TotalNumberOfGuesses.TWELVE -> 12
}

private fun Feedback.isSecretCodeGuessed() = listOf(peg1, peg2, peg3, peg4).all { peg -> peg == Black }

internal object DetermineFeedback {
    private fun Guess.toList() = listOf(peg1, peg2, peg3, peg4)
    private fun SecretCode.toList() = listOf(peg1, peg2, peg3, peg4)
    private fun emptyFeedback() = InternalFeedback(
        mutableListOf(
            CodePegFeedback(null, null),
            CodePegFeedback(null, null),
            CodePegFeedback(null, null),
            CodePegFeedback(null, null),
        )
    )

    private data class CodePegFeedback(val codePeg: CodePeg?, val keyPeg: KeyPeg?)
    private data class InternalFeedback(val current: MutableList<CodePegFeedback>) {
        fun setFeedback(position: Int, codePeg: CodePeg, result: KeyPeg): InternalFeedback {
            current[position] = CodePegFeedback(codePeg, result)
            return this
        }
    }

    private fun containsPegOfThisColorThatHasNotYetBeenRevealed(secretCode: SecretCode, feedback: InternalFeedback, peg: CodePeg): Boolean {
        val previousMatches = feedback.current.count { (p) -> p == peg }
        val numberOfPegsOfThisColorInSecret = secretCode.toList().count { it == peg }
        return numberOfPegsOfThisColorInSecret > previousMatches
    }

    private fun removePreviousWhiteKeyPegsForCodePeg(position: Int, secretCode: SecretCode, feedback: InternalFeedback, guessedPeg: CodePeg): InternalFeedback {
        fun CodePegFeedback.hasWhiteKeyPegForCodePeg(guessedPeg: CodePeg) = codePeg == guessedPeg && keyPeg == White
        val remainingSecretsWithSamePegColorAfterThisGuess = secretCode.toList().drop(position.inc()).count { it == guessedPeg }
        return feedback.copy(current = feedback.current
            .map { codePegFeedback -> if (codePegFeedback.hasWhiteKeyPegForCodePeg(guessedPeg) && remainingSecretsWithSamePegColorAfterThisGuess == 0) CodePegFeedback(codePegFeedback.codePeg, null) else codePegFeedback }
            .toMutableList())
    }


    fun determineFeedback(secretCode: SecretCode, guess: Guess): Feedback {
        val guessedPegs = guess.toList()
        val secretCombination = secretCode.toList()

        val internalFeedback = guessedPegs
            .foldIndexed(emptyFeedback()) { position, feedback, guessedPeg ->
                when {
                    secretCombination[position] == guessedPeg -> removePreviousWhiteKeyPegsForCodePeg(position, secretCode, feedback, guessedPeg).setFeedback(position, guessedPeg, Black)
                    containsPegOfThisColorThatHasNotYetBeenRevealed(secretCode, feedback, guessedPeg) -> feedback.setFeedback(position, guessedPeg, White)
                    else -> feedback
                }
            }
        val (keyPeg1, keyPeg2, keyPeg3, keyPeg4) = internalFeedback.current.map { it.keyPeg }
        return Feedback(keyPeg1, keyPeg2, keyPeg3, keyPeg4)
    }
}