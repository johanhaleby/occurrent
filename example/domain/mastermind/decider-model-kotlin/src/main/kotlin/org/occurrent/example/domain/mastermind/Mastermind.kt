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
        is GameWon -> Ended.Won((s as Started).currentNumberOfGuesses)
        is GameLost -> Ended.Lost((s as Started).secretCode)
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

private fun Feedback.isSecretCodeGuessed() = listOf(hole1, hole2, hole3, hole4).all { hole -> hole is KeyPegHoleStatus.Occupied && hole.keyPeg == Black }

internal object DetermineFeedback {
    private fun Guess.toList() = listOf(peg1, peg2, peg3, peg4)
    private fun SecretCode.toList() = listOf(peg1, peg2, peg3, peg4)

    private data class FeedbackSlot(val hole: KeyPegHoleStatus, val guessedCodePegForThisSlot: CodePeg?)
    private data class InternalFeedback(val slots: MutableList<FeedbackSlot>) {
        fun addFeedbackKeyPeg(position: Int, codePeg: CodePeg, keyPeg: KeyPeg): InternalFeedback {
            slots[position] = FeedbackSlot(KeyPegHoleStatus.Occupied(keyPeg), codePeg)
            return this
        }

        companion object {
            fun empty() = InternalFeedback(
                mutableListOf(
                    FeedbackSlot(KeyPegHoleStatus.Empty, null),
                    FeedbackSlot(KeyPegHoleStatus.Empty, null),
                    FeedbackSlot(KeyPegHoleStatus.Empty, null),
                    FeedbackSlot(KeyPegHoleStatus.Empty, null),
                )
            )
        }
    }

    private fun containsPegWithSameColorThatHasNotYetBeenRevealed(secretCodePegs: List<CodePeg>, feedback: InternalFeedback, peg: CodePeg): Boolean {
        val previousMatches = feedback.slots.count { (_, guessedCodePegForSlot) -> guessedCodePegForSlot == peg }
        val numberOfPegsWithSameColorInSecret = secretCodePegs.count { it == peg }
        return numberOfPegsWithSameColorInSecret > previousMatches
    }

    private fun removePreviousWhiteKeyPegsForCodePegIfApplicable(position: Int, secretCodePegs: List<CodePeg>, feedback: InternalFeedback, guessedPeg: CodePeg): InternalFeedback {
        fun FeedbackSlot.hasWhiteKeyPegWithCodePeg(codePeg: CodePeg) = guessedCodePegForThisSlot == codePeg && hole is KeyPegHoleStatus.Occupied && hole.keyPeg == White
        fun FeedbackSlot.removeKeyPeg() = copy(hole = KeyPegHoleStatus.Empty)

        // We should only remove if there are no more code pegs of the same color in the secret
        val numberOfRemainingSecretsWithSamePegColorAfterThisGuess = secretCodePegs.drop(position.inc()).count { it == guessedPeg }
        return feedback.copy(slots = feedback.slots.map { feedbackSlot ->
            if (feedbackSlot.hasWhiteKeyPegWithCodePeg(guessedPeg) && numberOfRemainingSecretsWithSamePegColorAfterThisGuess == 0) {
                feedbackSlot.removeKeyPeg()
            } else {
                feedbackSlot
            }
        }.toMutableList())
    }

    fun determineFeedback(secretCode: SecretCode, guess: Guess): Feedback {
        val guessedPegs = guess.toList()
        val secretCodePegs = secretCode.toList()

        val internalFeedback = guessedPegs.foldIndexed(InternalFeedback.empty()) { position, feedback, guessedPeg ->
            when {
                secretCodePegs[position] == guessedPeg -> removePreviousWhiteKeyPegsForCodePegIfApplicable(position, secretCodePegs, feedback, guessedPeg).addFeedbackKeyPeg(position, guessedPeg, Black)
                containsPegWithSameColorThatHasNotYetBeenRevealed(secretCodePegs, feedback, guessedPeg) -> feedback.addFeedbackKeyPeg(position, guessedPeg, White)
                else -> feedback
            }
        }
        val (hole1, hole2, hole3, hole4) = internalFeedback.slots.map { slot -> slot.hole }
        return Feedback(hole1, hole2, hole3, hole4)
    }
}